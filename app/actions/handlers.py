from datetime import date, datetime, timedelta, timezone
import logging
from typing import Dict, List

import httpx
from gundi_core.schemas.v2 import Integration, LogLevel
from pyinaturalist import Observation

from app.actions.configurations import PullEventsConfig
from app.services.activity_logger import activity_logger, log_action_activity
from app.services.gundi import (
    send_event_attachments_to_gundi,
    send_events_to_gundi,
    update_event_in_gundi,
)
from app.datasource.inaturalist import get_observations
from app.services.state import IntegrationStateManager

GUNDI_SUBMISSION_CHUNK_SIZE = 100

# Pull-events state: single key for the cursor (legacy "updated_to" still read for backward compatibility)
STATE_LAST_RUN_KEY = "last_run"
STATE_DATETIME_FMT = "%Y-%m-%d %H:%M:%S%z"
# Per-observation state: when we last synced this observation to Gundi (so we only patch when it changes)
STATE_INAT_UPDATED_AT_KEY = "inat_updated_at"

logger = logging.getLogger(__name__)
state_manager = IntegrationStateManager()


def _get_load_since(state: dict, fallback_days: int) -> datetime:
    """Return the datetime to use for updated_since. Uses stored cursor or now - fallback_days."""
    raw = state.get(STATE_LAST_RUN_KEY) or state.get("updated_to")
    if raw:
        return datetime.strptime(raw, STATE_DATETIME_FMT)
    return datetime.now(tz=timezone.utc) - timedelta(days=fallback_days)


def _build_pull_events_state(last_updated: datetime) -> dict:
    """Build the state dict to persist after a pull_events run."""
    return {STATE_LAST_RUN_KEY: last_updated.strftime(STATE_DATETIME_FMT)}

async def handle_transformed_data(transformed_data, integration_id, action_id):
    try:
        response = await send_events_to_gundi(
            events=transformed_data,
            integration_id=integration_id
        )
    except httpx.HTTPError as e:
        msg = f'Sensors API returned error for integration_id: {integration_id}. Exception: {e}'
        logger.exception(
            msg,
            extra={
                'needs_attention': True,
                'integration_id': integration_id,
                'action_id': action_id
            }
        )
        return [msg]
    else:
        return response

def chunk_list(list_a, chunk_size):
  for i in range(0, len(list_a), chunk_size):
    yield list_a[i:i + chunk_size]

@activity_logger()
async def action_pull_events(integration: Integration, action_config: PullEventsConfig):

    logger.info(f"Executing 'pull_events' action with integration {integration} and action_config {action_config}...")

    state = await state_manager.get_state(integration.id, "pull_events")
    load_since = _get_load_since(state, action_config.days_to_load)

    # Todo: write an async version of get_observations that uses httpx.AsyncClient to fetch the observations.
    observations = get_observations(
        load_since,
        bounding_box=action_config.bounding_box,
        taxa=action_config.taxa,
        projects=action_config.projects,
        quality_grade=action_config.quality_grade,
        annotations=action_config.annotations,
    )

    if not observations:
        msg = f"No new iNaturalist observations to process for integration ID: {str(integration.id)}."
        logger.info(msg)
        await log_action_activity(
            integration_id=integration.id,
            action_id="pull_events",
            level=LogLevel.WARNING,
            title=msg,
            data={"message": msg}
        )
        # Advance cursor so next run doesn't re-query the same window (avoids repeated heavy requests)
        now = datetime.now(tz=timezone.utc)
        await state_manager.set_state(
            str(integration.id), "pull_events", _build_pull_events_state(now)
        )
        return {'result': {'events_extracted': 0,
                           'events_updated': 0,
                           'photos_attached': 0}}

    logger.info(f"Processing {len(observations)} observations from iNaturalist.")

    async def get_inaturalist_events_to_patch():
        # Split observations into: new (create in Gundi) vs existing (patch only if observation changed).
        patch_these_events = []
        process_these_events = []
        for event_id, observation in observations.items():
            saved_event = await state_manager.get_state(str(integration.id), "pull_events", str(event_id))
            if not saved_event:
                process_these_events.append(observation)
                continue
            # Only patch when the observation has changed since we last synced it (avoids updating every run)
            last_synced_at = saved_event.get(STATE_INAT_UPDATED_AT_KEY)
            if last_synced_at:
                try:
                    if isinstance(last_synced_at, str):
                        last_synced_at = datetime.strptime(last_synced_at, STATE_DATETIME_FMT)
                    ob_updated = observation.updated_at
                    if ob_updated.tzinfo is None:
                        ob_updated = ob_updated.replace(tzinfo=timezone.utc)
                    if last_synced_at.tzinfo is None:
                        last_synced_at = last_synced_at.replace(tzinfo=timezone.utc)
                    if ob_updated <= last_synced_at:
                        continue  # Already in sync, skip patch
                except (ValueError, TypeError):
                    pass  # Bad or legacy value, patch to be safe
            patch_these_events.append((saved_event.get("object_id"), observation))
        return process_these_events, patch_these_events

    filtered_observations, events_to_patch = await get_inaturalist_events_to_patch()

    events_to_process = []

    updated_count = 0
    added_count = 0
    attachment_count = 0

    if filtered_observations:
        all_event_photos = {}
        inat_updated_at_map = {}  # inat_id -> updated_at for state we persist after create
        newest = None
        for ob in filtered_observations:

            if(not newest or (newest < ob.created_at)):
                newest = ob.created_at

            e = _transform_inat_to_gundi_event(ob, action_config)
            events_to_process.append(e)

            inat_id = e['event_details']['inat_id']
            inat_updated_at_map[inat_id] = ob.updated_at
            all_event_photos[inat_id] = []
            for photo in ob.photos:
                all_event_photos[inat_id].append((photo.id, photo.large_url if photo.large_url else photo.url))

        logger.info(f"Submitting {len(events_to_process)} iNaturalist observations to Gundi")

        for i, to_add_chunk in enumerate(chunk_list(events_to_process, GUNDI_SUBMISSION_CHUNK_SIZE)):

            logger.info(f"Processing chunk #{i+1}")

            response = await send_events_to_gundi(events=to_add_chunk, integration_id=str(integration.id))
            added_count += len(response)

            if response:
                # Send images as attachments (if available)
                if action_config.include_photos:
                    attachments_response = await process_attachments(to_add_chunk, response, all_event_photos, integration)
                    attachment_count += attachments_response
                # Process events to patch
                await save_events_state(response, to_add_chunk, integration, inat_updated_at_map)

    else:
        logger.info(f"No new iNaturalist observations to process for integration ID: {str(integration.id)}.")

    if events_to_patch:
        # Process events to patch
        logger.info(f"Updating {len(events_to_patch)} events from iNaturalist observations to Gundi for integration ID: {str(integration.id)}.")
        response = await patch_events(events_to_patch, action_config, integration)
        updated_count += len(response)
        await save_patched_events_state(events_to_patch, integration)

    last_updated = max(ob.updated_at for ob in observations.values())
    logger.info("Updating state through %s", last_updated)
    await state_manager.set_state(
        str(integration.id), "pull_events", _build_pull_events_state(last_updated)
    )
        
    return {'result': {'events_extracted': added_count,
                       'events_updated': updated_count,
                       'photos_attached': attachment_count}}


async def process_attachments(events, response, all_event_photos, integration):
    attachments_processed = 0
    for event, event_id in zip(events, response):
        inat_id = event['event_details']['inat_id']
        gundi_id = event_id['object_id']
        available_photos = all_event_photos.get(inat_id, [])
        if not available_photos:
            continue
        attachments = []
        try:
            for photo_id, photo_url in available_photos:
                logger.info(f"Adding {photo_url} from iNat event {inat_id} to Gundi event {gundi_id}")

                filename = str(photo_id) + "." + photo_url.split(".")[-1]

                async with httpx.AsyncClient(timeout=120, verify=False) as session:
                    image_response = await session.get(photo_url)
                    image_response.raise_for_status()

                img = await image_response.aread()

                attachments.append((filename, img))

            attachments_response = await send_event_attachments_to_gundi(
                event_id=gundi_id,
                attachments=attachments,
                integration_id=str(integration.id)
            )
            if attachments_response:
                attachments_processed += len(attachments)
        except Exception as e:
            request = {
                "event_id": gundi_id,
                "attachments": attachments,
                "integration_id": str(integration.id)
            }
            message = f"Error while processing event attachments for event ID '{event_id['object_id']}'. Exception: {e}. Request: {request}"
            logger.exception(message, extra={
                "integration_id": str(integration.id),
                "attention_needed": True
            })
            log_data = {"message": message}
            if server_response := getattr(e, "response", None):
                log_data["server_response_body"] = server_response.text
            await log_action_activity(
                integration_id=integration.id,
                action_id="pull_events",
                level=LogLevel.WARNING,
                title=message,
                data=log_data
            )
            continue
    return attachments_processed


async def patch_events(events, updated_config_data, integration):
    responses = []
    for event in events:
        gundi_object_id = event[0]
        new_event = event[1]
        transformed_data = _transform_inat_to_gundi_event(new_event, updated_config_data)
        if transformed_data:
            response = await update_event_in_gundi(
                event_id=gundi_object_id,
                event=transformed_data,
                integration_id=str(integration.id)
            )
            responses.append(response)
    return responses


async def save_events_state(response, events, integration, inat_updated_at_map=None):
    """Persist Gundi event state per observation so we know what we created and when we last synced."""
    inat_updated_at_map = inat_updated_at_map or {}
    for saved_event, event in zip(response, events):
        try:
            event_id = event["event_details"]["inat_id"]
            state = dict(saved_event)
            updated_at = inat_updated_at_map.get(event_id)
            if updated_at is not None:
                state[STATE_INAT_UPDATED_AT_KEY] = (
                    updated_at.strftime(STATE_DATETIME_FMT)
                    if hasattr(updated_at, "strftime") else str(updated_at)
                )
            await state_manager.set_state(
                integration_id=str(integration.id),
                action_id="pull_events",
                state=state,
                source_id=event_id
            )
        except Exception as e:
            inat_id = event.get("event_details", {}).get("inat_id", "unknown")
            message = f"Error while saving event ID '{inat_id}'. Exception: {e}."
            logger.exception(message, extra={
                "integration_id": str(integration.id),
                "attention_needed": True
            })
            raise e


async def save_patched_events_state(events_to_patch, integration):
    """After patching, update per-observation state so we don't patch again until the observation changes."""
    for gundi_object_id, observation in events_to_patch:
        try:
            updated_at = observation.updated_at
            state = {
                "object_id": gundi_object_id,
                STATE_INAT_UPDATED_AT_KEY: (
                    updated_at.strftime(STATE_DATETIME_FMT)
                    if hasattr(updated_at, "strftime") else str(updated_at)
                ),
            }
            await state_manager.set_state(
                integration_id=str(integration.id),
                action_id="pull_events",
                state=state,
                source_id=str(observation.id),
            )
        except Exception as e:
            logger.exception(
                "Error saving state for patched observation %s: %s",
                observation.id,
                e,
                extra={"integration_id": str(integration.id), "attention_needed": True},
            )
            raise e


def _normalize_recorded_at(observed_on, created_at):
    """Return a timezone-aware datetime for Gundi recorded_at (date or datetime, naive or aware)."""
    if not observed_on:
        return created_at
    if isinstance(observed_on, date) and not isinstance(observed_on, datetime):
        return datetime.combine(observed_on, datetime.min.time(), tzinfo=timezone.utc)
    if getattr(observed_on, "tzinfo", None) is None:
        return observed_on.replace(tzinfo=timezone.utc)
    return observed_on


def _transform_inat_to_gundi_event(ob: Observation, config: PullEventsConfig):
    
    event = {
        "event_type": config.event_type,
        "recorded_at": _normalize_recorded_at(ob.observed_on, ob.created_at),
        "event_details": {
            "inat_id": str(ob.id),
            "captive": ob.captive,
            "location_obscured": ob.obscured,
            "created_at": ob.created_at,
            "place_guess": ob.place_guess,
            "quality_grade": ob.quality_grade,
            "species_guess": ob.species_guess,
            "updated_at": ob.updated_at,
            "inat_url": ob.uri
        }
    }

    if(ob.user):
        event['event_details']['user_id'] = ob.user.id
        event['event_details']['user_name'] = ob.user.name if ob.user.name else ob.user.login

    if(ob.location):
        event["location"] = {
            "lat": ob.location[0],
            "lon": ob.location[1] }

    if ob.place_ids:
        event["event_details"]["place_ids"] = ",".join(str(pid) for pid in ob.place_ids)

    if(ob.taxon):
        event["event_details"].update({
            "taxon_id": ob.taxon.id,
            "taxon_rank": ob.taxon.rank,
            "taxon_name": ob.taxon.name,
            "taxon_common_name": ob.taxon.preferred_common_name,
            "taxon_wikipedia_url": ob.taxon.wikipedia_url,
            "taxon_conservation_status": ob.taxon.conservation_status
        })

        if(ob.taxon.preferred_common_name):
            event["title"] = ob.taxon.preferred_common_name
        if ob.taxon.ancestor_ids:
            event["event_details"]["taxon_ancestors"] = ",".join(str(aid) for aid in ob.taxon.ancestor_ids)

    if(not event.get("title")):
        event["title"] = "Unknown" if not ob.species_guess else ob.species_guess

    event["title"] = config.event_prefix + event["title"]

    return event