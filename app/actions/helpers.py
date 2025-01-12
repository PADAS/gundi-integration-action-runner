import logging
from typing import Tuple
from gundi_client_v2 import GundiClient

logger = logging.getLogger(__name__)

_client = GundiClient()
headers = {
    "Authorization": f"Bearer ",
}

# TODO: Test function
async def get_er_token_and_site(integration_id: str) -> Tuple[str, str]:
    connection_details = await _client.get_connection_details(integration_id)

    for destination in connection_details.destinations:
        destination_details = await _client.get_integration_details(destination.id)
        if destination_details.integration_type == "er":
            return destination_details.token, destination_details.site

    return None, None
