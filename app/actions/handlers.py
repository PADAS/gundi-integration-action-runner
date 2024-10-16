import asyncio
import httpx
import logging
import random
import app.settings

from datetime import timezone, timedelta, datetime

from app.actions.configurations import AuthenticateConfig, PullEventsConfig
from app.services.activity_logger import activity_logger
from app.services.gundi import send_events_to_gundi
from app.services.state import IntegrationStateManager
from app.services.errors import ConfigurationNotFound
from app.services.utils import find_config_for_action
from gundi_core.schemas.v2 import Integration


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



async def action_auth(integration, action_config: AuthenticateConfig):

    logger.info(f"Executing auth action with integration {integration} and action_config {action_config}...")
    return {"valid_credentials": action_config.username is not None and action_config.password is not None}


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

    logger.info(f'Fetching data for {auth_config.username}.')


    sample_event = {
        "title": "A dummy event",
        "event_type": "sit_rep",
        "recorded_at": datetime.now(tz=timezone.utc).isoformat(),
        "location": {
            "lat": -51.688645,
            "lon": -72.704421
        },
        "event_details": {
            "sitrep_currentactivity": "Creating an eBird integration."
        }
    }
    transformed_data = [sample_event]

    response = await send_events_to_gundi(
            events=transformed_data,
            integration_id=str(integration.id))

    return {'result': {'events_extracted': 0}}
