import httpx
import logging
from datetime import datetime, timezone
from math import ceil
from app.actions.configurations import AuthenticateConfig, PullEventsConfig
from app.services.activity_logger import activity_logger
from app.services.gundi import send_events_to_gundi
from app.services.state import IntegrationStateManager
from app.services.errors import ConfigurationNotFound, ConfigurationValidationError
from app.services.utils import find_config_for_action
from gundi_core.schemas.v2 import Integration
from pydantic import BaseModel, parse_obj_as
from typing import List, Optional

logger = logging.getLogger(__name__)
state_manager = IntegrationStateManager()

EBIRD_API = "https://api.ebird.org/v2"
class eBirdObservation(BaseModel):
    speciesCode: str
    comName: str
    sciName: str
    locId: str
    locName: str
    obsDt: datetime
    howMany: Optional[int] = None
    lat: float
    lng: float
    obsValid: bool
    obsReviewed: bool
    locationPrivate: bool
    subId: str

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



async def action_auth(integration:Integration, action_config: AuthenticateConfig):
    logger.info(f"Executing auth action with integration {integration} and action_config {action_config}...")

    base_url = integration.base_url or EBIRD_API

    try:
        # Use a request for region info as a proxy for verifying credentials.
        us_region_info = await get_region_info(base_url, action_config.api_key.get_secret_value(), "US")
        return {"valid_credentials": True}
    except httpx.HTTPStatusError as e:
        return {"valid_credentials": False, "status_code": e.response.status_code}



def get_auth_config(integration):
    # Look for the login credentials, needed for any action
    auth_config = find_config_for_action(
        configurations=integration.configurations,
        action_id="auth"
    )
    if not auth_config:
        raise ConfigurationNotFound(
            f"Authentication settings for integration {str(integration.id)} "
            f"are missing. Please fix the integration setup in the portal."
        )
    return AuthenticateConfig.parse_obj(auth_config.data)

@activity_logger()
async def action_pull_events(integration:Integration, action_config: PullEventsConfig):

    logger.info(f"Executing 'pull_events' action with integration {integration} and action_config {action_config}...")

    auth_config = get_auth_config(integration)
    state = await state_manager.get_state(integration.id, "pull_events")
    last_run = state.get('last_run')
    num_days = None
    now = datetime.now(tz=timezone.utc)
    if(last_run):
        last_run = datetime.datetime.strptime(last_run, '%Y-%m-%d %H:%M:%S%z')
        num_days = ceil((now - last_run).total_seconds()/(60*60*24))
        logging.info(f"Using state with last_run of {last_run.isoformat()} to load data for last {num_days} days.")
    else:
        num_days = action_config.num_days_default

    base_url = integration.base_url or EBIRD_API

    if(action_config.region_code):
        if((action_config.latitude and action_config.latitude != 0) or
           (action_config.longitude and action_config.longitude != 0)):
            raise ConfigurationValidationError("If region code is included, latitude and longitude should be blank.")        
        obs = _get_recent_observations_by_region(base_url, auth_config.api_key.get_secret_value(), num_days, 
                                                 action_config.region_code, action_config.species_code,
                                                 action_config.include_provisional)

    else:
        if((not action_config.latitude or action_config.latitude == 0) or
           (not action_config.longitude or action_config.longitude == 0) or
           (not action_config.distance or action_config.distance == 0)):
            raise ConfigurationValidationError("Either a region code or a latitude and longitude should be included.")

        obs = _get_recent_observations_by_location(base_url, auth_config.api_key.get_secret_value(), action_config.num_days,
                                                   action_config.latitude, action_config.longitude, action_config.distance,
                                                   action_config.include_provisional)

    to_send = []
    async for ob in obs:
        to_send.append(_transform_ebird_to_gundi_event(ob))
    
    logger.info(f"Submitting {len(to_send)} eBird observations to Gundi")
    response = await send_events_to_gundi(
            events=to_send,
            integration_id=str(integration.id))

    state = {"last_run": now.strftime('%Y-%m-%d %H:%M:%S%z')}
    await state_manager.set_state(str(integration.id), "pull_events", state)

    return {'result': {'events_extracted': 0}}

async def _get_from_ebird(url: str, api_key: str, params: dict):
    headers = {
        "X-eBirdApiToken": api_key
    }

    async with httpx.AsyncClient() as client:
        r = await client.get(url, params=params, headers = headers)
        r.raise_for_status()
        return r.json()

async def _get_recent_observations_by_region(base_url: str, api_key: str, num_days: int, region_code: str, 
                                             species_code: str = None, include_provisional: bool = False):

        params = {
             "back": num_days,
             "includeProvisional": include_provisional
        }
        url = f"{base_url}/data/obs/{region_code}/recent"
        logger.info(f"Loading eBird observations for last {num_days} days near region code {region_code}.")
        
        return await _get_recent_observations(url, api_key, params, species_code)

async def _get_recent_observations_by_location(base_url: str, api_key: str, num_days: int, lat: float, 
                                               lng: float, dist: float, species_code: str = None,
                                               include_provisional: bool = False):

        params = {
            "dist": dist,
            "back": num_days,
            "includeProvisional": include_provisional
        }
        url = f"{base_url}/data/obs/geo/recent?lat={lat}&lng={lng}"

        logger.info(f"Loading eBird observations for last {num_days} days near ({lat}, {lng}).")
        async for item in _get_recent_observations(url, api_key, params, species_code):
            yield item


async def _get_recent_observations(url, api_key, params, species_code: str = None):

        if(species_code):
            species = species_code.split(",")
            for specie in species:
                url = f"{url}/{specie}"
                obs = _get_from_ebird(url, params=params)
                logger.info(f"Loading observations for species {species}.")
                for ob in obs:
                    yield parse_obj_as(eBirdObservation, ob)
        
        else:
            obs = await _get_from_ebird(url, api_key, params=params)
            for ob in obs:
                yield parse_obj_as(eBirdObservation, ob)


async def get_region_info(base_url: str, api_key: str, region_code: str):
    url = f"{base_url}/ref/region/info/{region_code}"
    return await _get_from_ebird(url, api_key, params=None)


def _transform_ebird_to_gundi_event(obs: eBirdObservation):
    
    return {
        "title": f"{obs.comName} observation",
        "event_type": "ebird_observation",
        "recorded_at": obs.obsDt.isoformat(),
        "location": {
            "lat": obs.lat,
            "lon": obs.lng
        },
        "event_details": {
            "common_name": obs.comName,
            "scientific_name": obs.sciName,
            "species_code": obs.speciesCode,
            "location_id": obs.locId,
            "location_name": obs.locName,
            "location_private": obs.locationPrivate,
            "quantity": obs.howMany,
            "valid": obs.obsValid,
            "reviewed": obs.obsReviewed,
            "submission_id": obs.subId,
            "attribution": "Data from https://eBird.org, Cornell Lab of Ornithology."
        }
    }