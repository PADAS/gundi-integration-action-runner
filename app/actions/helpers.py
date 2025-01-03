import logging
from gundi_client_v2 import GundiClient

logger = logging.getLogger(__name__)

_client = GundiClient()
headers = {
    "Authorization": f"Bearer ",
}

# TODO: Complete function
async def get_er_token_and_site():
    connection_details = await _client.get_connection_details(integration_id)

    for destination in connection_details.destinations:
        destination_details = await _client.get_integration_details(destination.id)
