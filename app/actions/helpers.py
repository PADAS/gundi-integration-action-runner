from enum import Enum
import logging
from typing import Tuple
from gundi_client_v2 import GundiClient

from app.services.utils import find_config_for_action
from gundi_core.schemas.v2 import Integration
from gundi_core import schemas


logger = logging.getLogger(__name__)


class Environment(Enum):
    DEV = "Buoy Dev"
    STAGE = "Buoy Staging"
    PRODUCTION = "Buoy Prod"


_client = GundiClient()
headers = {
    "Authorization": f"Bearer ",
}


async def get_er_token_and_site(
    integration: Integration, environment: Environment
) -> Tuple[str, str]:
    """
    Get the ER token and site for the given integration and environment

    :param integration: Integration object
    :param environment: Environment enum
    :return: Tuple of ER token and site
    """
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
