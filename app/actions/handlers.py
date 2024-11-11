import httpx
import logging
from datetime import datetime, timezone, timedelta
from math import ceil
from app.actions.configurations import AuthenticateConfig, PullEventsConfig
from app.services.activity_logger import activity_logger
from app.services.gundi import send_events_to_gundi
from app.services.state import IntegrationStateManager
from app.services.errors import ConfigurationNotFound, ConfigurationValidationError
from app.services.utils import find_config_for_action
from gundi_core.schemas.v2 import Integration
from pyinaturalist import get_observations, Observation, Annotation
from pydantic import BaseModel, parse_obj_as
from typing import Dict, List, Optional
from urllib.parse import urlparse, urlretrieve
import re

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

def get_inaturalist_observations(integration, config, since):

    bounding_box = integration.get("bounding_box")
    nelat = nelng = swlat = swlng = None
    if(bounding_box):
        nelat, nelng, swlat, swlng = bounding_box

    inat_count_req = get_observations(page = 1, per_page = 0, updated_since = since, project_id = config.projects,
                                      taxon_id=config.taxa, nelat = nelat, nelng = nelng, swlat = swlat, swlng = swlng,
                                      order_by = "updated_since", order="asc")

    inat_count = inat_count_req.get("total_results")
    pages = ceil(inat_count/200)

    observation_map = {}
    for page in range(1,pages+1):
        logger.debug(f"Loading page {page} of {pages} from iNaturalist")

        response = get_observations(page=page,per_page=200, updated_since = since, project_id = config.projects,
                                    taxon_id=config.target_taxa, nelat = nelat, nelng = nelng, swlat = swlat, swlng = swlng,
                                    order_by = 'updated_since', order="asc")

        observations = Observation.from_json_list(response)
        for o in observations:
            if(config.annotations):
                if(_match_annotations_to_config(o.annotations, config.annotations)):    
                    observation_map[o.id] = o
            else:
                observation_map[o.id] = o

    return observation_map

@activity_logger()
async def action_pull_events(integration:Integration, action_config: PullEventsConfig):

    logger.info(f"Executing 'pull_events' action with integration {integration} and action_config {action_config}...")

    state = await state_manager.get_state(integration.id, "pull_events")
    last_run = state.get('updated_to') or state.get('last_run')
    load_since = None
    now = datetime.now(tz=timezone.utc)
    if(last_run):
        load_since = datetime.datetime.strptime(last_run, '%Y-%m-%d %H:%M:%S%z')
    else:
        load_since = now - timedelta(days=action_config.days_to_load)


    observations = get_inaturalist_observations(integration, load_since)
    logger.info(f"Processing {len(observations)} observations from iNaturalist for query {query}.")
    
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
            all_event_photos[inat_id].append((photo.id, photo.large_url))

    id_map = state.get("inat_to_gundi", {})
    to_add = {}
    to_update = {}
    for e in events_to_process:
        e_id = e['event_details']['inat_id']
        if(e_id) in id_map:
            to_update[e_id] = e
        else:
            to_add[e_id] = e
    
    logger.info(f"Submitting {len(to_update)} iNaturalist observations for update in Gundi")
    for e_id, e in to_update.items():
        await update_event_in_gundi(event_id = e_id, event = e)

    logger.info(f"Submitting {len(to_add)} new iNaturalist observations to Gundi")
    response = await send_events_to_gundi(events=to_add.values(), integration_id=str(integration.id))

    for (to_add, added) in zip(to_add.values(), response):
        inat_id = to_add['event_details']['inat_id']
        id_map[inat_id] = added['object_id']

        if(action_config.include_photos):
            for (photo_id, photo_url) in all_event_photos[inat_id]:
                gundi_id = added['object_id']

                logger.info(f"Adding {photo_url} from iNat event {inat_id} to Gundi event {gundi_id}")
                fp = urlretrieve(photo_url)
                path = urlparse(photo_url).path
                ext = re.split(r".*\.", path)[1]
                filename = str(photo_id) + "." + ext
                attachments = [(filename, open(fp, 'rb'))]
                send_event_attachments_to_gundi(gundi_id, attachments)


    state = {"last_run": now.strftime('%Y-%m-%d %H:%M:%S%z'),
             "inat_to_gundi": id_map}

    await state_manager.set_state(str(integration.id), "pull_events", state)

    return {'result': {'events_extracted': len(to_add),
                       'events_updated': len(to_update)}}

def _match_annotations_to_config(annotations: List[Annotation], config: Dict[int, List[int]]):
    annot_map = {}
    for annotation in annotations:
        if(annotation.term not in annot_map):
            annot_map[annotation.term] = []
        annot_map[annotation.term].append(annotation.value)

    for term, values in config:
        if(term not in annot_map):
            return False
        for value in values:
            if(value not in annot_map[term]):
                return False
            
    return True


def _transform_inat_to_gundi_event(ob: Observation, config: PullEventsConfig):
    
    event = {
        "event_type": config.event_type,
        "recorded_at": ob.observed_on if ob.observed_on else ob.created_at,
        "event_details": {
            "inat_id": ob.id,
            "captive": ob.captive,
            "location_obscured": ob.obscured,
            "created_at": ob.created_at,
            "place_guess": ob.place_guess,
            "quality_grade": ob.quality_grade,
            "species_guess": ob.species_guess,
            "updated_at": ob.updated_at,
            "inat_url": ob.uri,
            "user_id": ob.user.id,
            "user_name": ob.user.name
        }
    }

    if(ob.location):
        event["location"] = {
            "latitude": ob.location[0],
            "longitude": ob.location[1] }

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