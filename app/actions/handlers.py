import httpx
import logging
import json
from datetime import datetime, timezone, timedelta
from math import ceil
from app.actions.configurations import AuthenticateConfig, PullEventsConfig
from app.services.activity_logger import activity_logger
from app.services.gundi import send_events_to_gundi, update_event_in_gundi, send_event_attachments_to_gundi
from app.services.state import IntegrationStateManager
from app.services.errors import ConfigurationNotFound, ConfigurationValidationError
from app.services.utils import find_config_for_action
from gundi_core.schemas.v2 import Integration
from pyinaturalist import get_observations_v2, Observation, Annotation
from pydantic import BaseModel, parse_obj_as
from typing import Dict, List, Optional
from urllib.parse import urlparse
from urllib.request import urlretrieve

import re

GUNDI_SUBMISSION_CHUNK_SIZE = 10

logger = logging.getLogger(__name__)
state_manager = IntegrationStateManager()

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

def get_inaturalist_observations(integration: Integration, config: PullEventsConfig, since: datetime):

    nelat = nelng = swlat = swlng = None
    if(config.bounding_box):
        nelat, nelng, swlat, swlng = config.bounding_box

    target_taxa = []
    for taxa in config.taxa:
        target_taxa.append(str(taxa))
    target_taxa = ",".join(target_taxa)

    fields = ",".join(["observed_on", "created_at", "id", "captive", "obscured", "place_guess", "quality_grade", "species_guess", "updated_at", 
                       "uri", "photos", "user", "location", "place_ids", "taxon", "photos.large_url", "photos.url", "taxon.id", "taxon.rank", "taxon.name",
                       "taxon.preferred_common_name", "taxon.wikipedia_url", "taxon.conservation_status", "user.id", "user.name", "user.login",
                       "annotations.controlled_attribute_id", "annotations.controlled_value_id"])

    inat_count_req = get_observations_v2(page = 1, per_page = 0, updated_since = since, project_id = config.projects, quality_grade = config.quality_grade,
                                      taxon_id=target_taxa, nelat = float(nelat), nelng = float(nelng), swlat = float(swlat), swlng = float(swlng),
                                      order_by = "updated_at", order="asc")

    inat_count = inat_count_req.get("total_results")
    pages = ceil(inat_count/200)

    observation_map = {}
    for page in range(1,pages+1):
        logger.debug(f"Loading page {page} of {pages} from iNaturalist")

        response = get_observations_v2(page = page, per_page = 200, updated_since = since, project_id = config.projects,
                                       quality_grade = config.quality_grade, taxon_id = target_taxa,
                                       nelat = nelat, nelng = nelng, swlat = swlat, swlng = swlng,
                                       order_by = 'updated_at', order="asc", fields=fields)

        observations = Observation.from_json_list(response)

        logger.info(f"Loaded {len(observations)} observations from iNaturalist before annotation filters.")
        for o in observations:
            if(config.annotations):
                if(_match_annotations_to_config(o.annotations, config.annotations)):    
                    observation_map[o.id] = o
            else:
                observation_map[o.id] = o

    return observation_map

def chunk_list(list_a, chunk_size):
  for i in range(0, len(list_a), chunk_size):
    yield list_a[i:i + chunk_size]

@activity_logger()
async def action_pull_events(integration:Integration, action_config: PullEventsConfig):

    logger.info(f"Executing 'pull_events' action with integration {integration} and action_config {action_config}...")

    state = await state_manager.get_state(integration.id, "pull_events")

    last_run = state.get('updated_to') or state.get('last_run')
    load_since = None
    now = datetime.now(tz=timezone.utc)
    if(last_run):
        load_since = datetime.strptime(last_run, '%Y-%m-%d %H:%M:%S%z')
    else:
        load_since = now - timedelta(days=action_config.days_to_load)


    observations = get_inaturalist_observations(integration, action_config, load_since)
    logger.info(f"Processing {len(observations)} observations from iNaturalist.")
    
    all_event_photos = {}
    newest = None
    events_to_process = []
    for ob in observations.values():
        
        if(not newest or (newest < ob.created_at)):
            newest = ob.created_at

        e = _transform_inat_to_gundi_event(ob, action_config)
        events_to_process.append(e)
        
        inat_id = e['event_details']['inat_id']
        all_event_photos[inat_id] = []
        for photo in ob.photos:
            all_event_photos[inat_id].append((photo.id, photo.large_url if photo.large_url else photo.url))

    id_map = state.get("inat_to_gundi", {})

    logger.info(f"Submitting {len(events_to_process)} iNaturalist observations to Gundi")
    i = 0
    updated_count = 0
    added_count = 0
    attachment_count = 0
    for to_add_chunk in chunk_list(events_to_process, GUNDI_SUBMISSION_CHUNK_SIZE):
        
        if(len(events_to_process) > GUNDI_SUBMISSION_CHUNK_SIZE):
            i += 1
            logger.info(f"Processing chunk #{i}")
    
        for e in to_add_chunk:
            e_id = e['event_details']['inat_id']
            if(e_id) in id_map: 
                await update_event_in_gundi(event_id = id_map[e_id], event = e, integration_id=str(integration.id))
                updated_count += 1
            
            else:
                response = await send_events_to_gundi(events=[e], integration_id=str(integration.id))
                added_count += 1
                
                inat_id = e['event_details']['inat_id']
                id_map[inat_id] = response[0]['object_id']

                if(action_config.include_photos):
                    for (photo_id, photo_url) in all_event_photos[inat_id]:
                        gundi_id = response[0]['object_id']

                        logger.info(f"Adding {photo_url} from iNat event {inat_id} to Gundi event {gundi_id}")
                        fp = urlretrieve(photo_url)
                        path = urlparse(photo_url).path
                        ext = re.split(r".*\.", path)[1]
                        filename = str(photo_id) + "." + ext
                        attachments = [(filename, open(fp[0], 'rb'))]
                        await send_event_attachments_to_gundi(event_id = gundi_id, attachments = attachments, integration_id=str(integration.id))
                        attachment_count += 1

            last_updated = e['event_details']['updated_at']

        logger.info(f"Updating state through {last_updated}")
        state = {"last_run": last_updated.strftime('%Y-%m-%d %H:%M:%S%z'), "inat_to_gundi": id_map}
        await state_manager.set_state(str(integration.id), "pull_events", state)
        
    return {'result': {'events_extracted': added_count,
                       'events_updated': updated_count,
                       'photos_attached': attachment_count}}

def _match_annotations_to_config(annotations: List[Annotation], config: Dict[int, List[int]]):
    annot_map = {}
    for annotation in annotations:
        if(annotation.term not in annot_map):
            annot_map[annotation.term] = []
        annot_map[annotation.term].append(annotation.value)

    for term, values in config:
        if(str(term) not in annot_map):
            return False
        for value in values:
            if(str(value) not in annot_map[str(term)]):
                return False
    
    return True


def _transform_inat_to_gundi_event(ob: Observation, config: PullEventsConfig):
    
    event = {
        "event_type": config.event_type,
        "recorded_at": ob.observed_on if ob.observed_on else ob.created_at,
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

    if(ob.place_ids):
        event["event_details"]["place_ids"] = ",".join([str(int) for int in ob.place_ids])

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
        if(ob.taxon.ancestor_ids):
            event["event_details"]["taxon_ancestors"] = ",".join([str(int) for int in ob.taxon.ancestor_ids])

    if(not event.get("title")):
        event["title"] = "Unknown" if not ob.species_guess else ob.species_guess

    event["title"] = config.event_prefix + event["title"]

    return event