import logging
from timeit import default_timer as timer
from typing import Dict, List, Tuple

from gundi_client_v2 import GundiClient
from gundi_core import schemas
from gundi_core.events import LogLevel
from gundi_core.schemas.v2 import ConnectionIntegration, Integration

from app.actions.configurations import EdgeTechAuthConfiguration, EdgeTechConfiguration
from app.actions.edgetech import EdgeTechClient
from app.actions.edgetech.exceptions import InvalidCredentials
from app.actions.edgetech.processor import EdgetTechProcessor
from app.services.action_scheduler import crontab_schedule
from app.services.activity_logger import activity_logger, log_action_activity
from app.services.gundi import send_observations_to_gundi
from app.services.utils import find_config_for_action

logger = logging.getLogger(__name__)
LOAD_BATCH_SIZE = 100


# --- Helper Functions ---


def generate_batches(iterable: List, n: int = LOAD_BATCH_SIZE):
    """Yield successive n-sized batches from iterable."""
    for i in range(0, len(iterable), n):
        yield iterable[i : i + n]


async def get_destination_credentials(
    gundi_client: GundiClient,
    destination: ConnectionIntegration,
) -> Tuple[str, str]:
    """
    Retrieve the ER destination token and URL for a given destination.

    :param gundi_client: Instance of GundiClient.
    :param destination: Destination object from connection_details.
    :return: Tuple of (token, base_url).
    """
    destination_details = await gundi_client.get_integration_details(destination.id)
    auth_config_data = find_config_for_action(
        configurations=destination_details.configurations, action_id="auth"
    )
    # Parse using the ER auth schema.
    er_auth_config = schemas.v2.ERAuthActionConfig.parse_obj(auth_config_data.data)
    if not er_auth_config:
        raise ValueError(f"Missing auth configuration for destination {destination.id}")
    return er_auth_config.token, destination_details.base_url


async def process_destination(
    gundi_client: GundiClient,
    integration: Integration,
    data: List[dict],
    destination: ConnectionIntegration,
) -> None:
    """
    Process a single destination: retrieve credentials, run the EdgeTech processor,
    send observations in batches, and log the activity.

    :param gundi_client: Instance of GundiClient.
    :param integration: Integration object.
    :param data: Raw data from EdgeTechClient.
    :param destination: Destination object.
    :return: Length of the observations list.
    """
    logger.info(
        f"Executing pull action for integration {integration} and destination {destination}..."
    )
    er_destination_token, er_destination_url = await get_destination_credentials(
        gundi_client, destination
    )
    processor = EdgetTechProcessor(data, er_destination_token, er_destination_url)
    observations = await processor.process()
    for batch in generate_batches(observations):
        logger.info(f"Sending {len(batch)} observations to Gundi...")
        await send_observations_to_gundi(
            observations=batch, integration_id=str(integration.id)
        )
    await log_action_activity(
        integration_id=integration.id,
        action_id="pull_edgetech",
        level=LogLevel.INFO,
        title="Pulled data from EdgeTech API",
    )
    logger.info(
        f"Downloaded {len(data)} records from EdgeTech API for integration {integration}"
    )
    return len(observations)

# --- Main Handler Functions ---


async def action_auth(
    integration: Integration, action_config: EdgeTechAuthConfiguration
) -> Dict:
    """
    Execute an auth action for the given integration.

    Attempts to fetch a token from EdgeTech and returns a dict indicating
    whether the credentials are valid.
    """
    logger.info(
        f"Executing auth action with integration {integration} and action_config {action_config}..."
    )
    try:
        edgetech_client = EdgeTechClient(auth_config=action_config, pull_config=None)
        await edgetech_client.get_token()
        return {"valid_credentials": True}
    except InvalidCredentials as e:
        logger.info(
            f"Invalid credentials for integration {integration}: {e.response_data}"
        )
        return {"valid_credentials": False, "error": str(e)}
    except Exception as e:
        logger.error(f"Error authenticating with EdgeTech: {e}")
        return {"valid_credentials": None, "error": str(e)}


@activity_logger()
@crontab_schedule("*/5 * * * *")  # Run every 5 minutes
async def action_pull_edgetech_observations(
    integration: Integration, action_config: EdgeTechConfiguration
):
    """
    Pull observations from EdgeTech and send them to Gundi.

    Retrieves connection details and authentication info from the integration,
    downloads data from EdgeTech, and for each destination, processes the data
    and sends observations in batches.
    """
    gundi_client = GundiClient()
    connection_details = await gundi_client.get_connection_details(integration.id)
    # Get the primary auth config from the integration.
    auth_config_data = find_config_for_action(
        configurations=integration.configurations, action_id="auth"
    )
    auth_config = EdgeTechAuthConfiguration.parse_obj(auth_config_data.data)
    edgetech_client = EdgeTechClient(auth_config=auth_config, pull_config=action_config)
    data = await edgetech_client.download_data()

    total_observations = 0
    for destination in connection_details.destinations:
        total_observations += await process_destination(
            gundi_client, integration, data, destination
        )

    return {
        "observations_extracted": len(data),
        "observations_sent": total_observations,
    }
