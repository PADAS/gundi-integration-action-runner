import logging
from enum import Enum
from typing import Tuple

from gundi_client_v2 import GundiClient
from gundi_core import schemas
from gundi_core.events import LogLevel
from gundi_core.schemas.v2 import Integration

from app.services.activity_logger import activity_logger, log_activity
from app.services.utils import find_config_for_action

from .configurations import EdgeTechConfiguration
from .edgetech import EdgeTechClient

logger = logging.getLogger(__name__)


class Environment(Enum):
    DEV = "Buoy Dev"
    STAGE = "Buoy Staging"
    PRODUCTION = "Buoy Prod"


@activity_logger()
async def action_pull_edgetech_observations(
    integration: Integration, action_config: EdgeTechConfiguration
):
    edgetech_client = EdgeTechClient(config=action_config)
    data = await edgetech_client.download_data()
    _client = GundiClient()
    connection_details = await _client.get_connection_details(integration.id)
    for destination in connection_details.destinations:
        environment = Environment(destination.name)

        er_token, er_destination = await get_er_token_and_site(integration, environment)

        logging.info(
            f"Executing pull action for integration {integration} and environment {environment}..."
        )

        await log_activity(
            integration_id=integration.id,
            action_id="pull_edgetech",
            level=LogLevel.INFO,
            title="Pulling data from EdgeTech API",
        )

        logger.info(
            f"Downloaded {len(data)} records from EdgeTech API for integration {integration}"
        )


async def get_er_token_and_site(
    integration: Integration, environment: Environment
) -> Tuple[str, str]:
    """
    Get the ER token and site for the given integration and environment

    :param integration: Integration object
    :param environment: Environment enum
    :return: Tuple of ER token and site
    """
    _client = GundiClient()
    connection_details = await _client.get_connection_details(integration.id)

    destination = (
        destination
        for destination in connection_details.destinations
        if environment.value in destination.name
    ).__next__()

    destination_details = await _client.get_integration_details(destination.id)
    auth_config = find_config_for_action(
        configurations=destination_details.configurations,
        action_id="auth",
    )

    auth_config = schemas.v2.ERAuthActionConfig.parse_obj(auth_config.data)
    if auth_config:
        return auth_config.token, destination_details.base_url
    return None, None
