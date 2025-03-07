# actions/handlers.py
from enum import Enum
import logging
from datetime import datetime, timedelta
from typing import Tuple

from app.services.action_scheduler import crontab_schedule
from app.services.activity_logger import activity_logger, log_activity
from app.services.gundi import send_observations_to_gundi
from app.services.utils import find_config_for_action
from gundi_core import schemas
from gundi_core.events import LogLevel
from gundi_core.schemas.v2 import Integration
from gundi_client_v2 import GundiClient

from .configurations import PullRmwHubObservationsConfiguration, AuthenticateConfig
from .rmwhub import RmwHubAdapter

logger = logging.getLogger(__name__)


LOAD_BATCH_SIZE = 100

headers = {
    "Authorization": f"Bearer ",
}


class Environment(Enum):
    DEV = "Buoy Dev"
    STAGE = "Buoy Staging"
    PRODUCTION = "Buoy Prod"


async def action_auth(integration: Integration, action_config: AuthenticateConfig):
    logger.info(
        f"Executing auth action with integration {integration} and action_config {action_config}..."
    )

    # TODO: Do something to validate the API Key against rmwHUB APIs.

    api_key_is_valid = action_config.api_key is not None

    return {
        "valid_credentials": api_key_is_valid,
        "some_message": "something informative.",
    }


@activity_logger()
@crontab_schedule("*/5 * * * *")  # Run every 5 minutes
async def action_pull_observations(
    integration, action_config: PullRmwHubObservationsConfiguration
):
    current_datetime = datetime.now()
    sync_interval_minutes = 5
    start_datetime = current_datetime - timedelta(minutes=sync_interval_minutes)
    start_datetime_str = start_datetime.strftime("%Y-%m-%d %H:%M:%S")
    end_datetime = current_datetime
    end_datetime_str = end_datetime.strftime("%Y-%m-%d %H:%M:%S")

    # TODO: Create sub-actions for each destination
    total_observations = []
    _client = GundiClient()
    connection_details = await _client.get_connection_details(integration.id)
    for destination in connection_details.destinations:
        environment = Environment(destination.name)
        er_token, er_destination = await get_er_token_and_site(integration, environment)

        logger.info(
            f"Downloading data from rmwHub to the Earthranger destination: {str(environment)}..."
        )

        rmw_adapter = RmwHubAdapter(
            integration.id,
            action_config.api_key.get_secret_value(),
            action_config.rmw_url,
            er_token,
            er_destination + "api/v1.0",
        )

        logger.info(
            f"Downloading data from RMW Hub API...For the dates: {start_datetime_str} - {end_datetime_str}"
        )
        rmwSets = await rmw_adapter.download_data(
            start_datetime_str, sync_interval_minutes
        )

        await log_activity(
            integration_id=integration.id,
            action_id="pull_observations",
            level=LogLevel.INFO,
            title="Extracting observations with filter..",
            data={
                "start_date_time": start_datetime_str,
                "end_date_time": end_datetime_str,
                "environment": str(environment),
            },
            config_data=action_config.dict(),
        )

        observations = []
        if len(rmwSets.sets) != 0:
            logger.info(
                f"Processing updates from RMW Hub API...Number of gearsets returned: {len(rmwSets.sets)}"
            )
            observations = await rmw_adapter.process_rmw_download(
                rmwSets, start_datetime_str, sync_interval_minutes
            )
            total_observations.extend(observations)
        else:
            await log_activity(
                integration_id=integration.id,
                action_id="pull_observations",
                level=LogLevel.INFO,
                title="No gearsets returned from RMW Hub API.",
                data={
                    "start_date_time": start_datetime_str,
                    "end_date_time": end_datetime_str,
                    "environment": str(environment),
                },
                config_data=action_config.dict(),
            )

        # Upload changes from ER to RMW Hub
        # put_set_id_observations, rmw_response = await rmw_adapter.process_rmw_upload(
        #     rmwSets, start_datetime_str
        # )
        # total_observations.extend(put_set_id_observations)
        # observations.extend(put_set_id_observations)

        # # TODO: Handle failed response
        # await log_activity(
        #     integration_id=integration.id,
        #     action_id="pull_observations",
        #     level=LogLevel.INFO,
        #     title="Process upload to rmwHub completed.",
        #     data={
        #         "rmw_response": str(rmw_response),
        #     },
        #     config_data=action_config.dict(),
        # )

        # Send the extracted data to Gundi in batches
        for batch in generate_batches(observations):
            logger.info(f"Sending {len(batch)} observations to Gundi...")
            await send_observations_to_gundi(
                observations=batch, integration_id=str(integration.id)
            )

        # Patch subject status
        # TODO: Remove when status workaround fix verified in Earthranger.
        await rmw_adapter.push_status_updates(
            observations=observations, rmw_sets=rmwSets
        )

    # The result will be recorded in the portal if using the activity_logger decorator
    return {"observations_extracted": len(total_observations)}


@activity_logger()
@crontab_schedule("10 0 * * *")  # Run every 24 hours at 12:10 AM
async def action_pull_observations_24_hour_sync(
    integration, action_config: PullRmwHubObservationsConfiguration
):

    sync_interval_minutes = 1440  # 24 hours
    current_datetime = datetime.now()
    start_datetime = current_datetime - timedelta(minutes=sync_interval_minutes)
    start_datetime_str = start_datetime.strftime("%Y-%m-%d %H:%M:%S")
    end_datetime = current_datetime
    end_datetime_str = end_datetime.strftime("%Y-%m-%d %H:%M:%S")

    # TODO: Create sub-actions for each destination
    total_observations = []
    _client = GundiClient()
    connection_details = await _client.get_connection_details(integration.id)
    for destination in connection_details.destinations:
        environment = Environment(destination.name)
        er_token, er_destination = await get_er_token_and_site(integration, environment)

        logger.info(
            f"Downloading data from rmwHub to the Earthranger destination: {str(environment)}..."
        )

        rmw_adapter = RmwHubAdapter(
            integration.id,
            action_config.api_key.get_secret_value(),
            action_config.rmw_url,
            er_token,
            er_destination + "api/v1.0",
        )

        logger.info(
            f"Downloading data from RMW Hub API...For the dates: {start_datetime_str} - {end_datetime_str}"
        )
        rmwSets = await rmw_adapter.download_data(
            start_datetime_str, sync_interval_minutes
        )

        # Optionally, log a custom messages to be shown in the portal
        await log_activity(
            integration_id=integration.id,
            action_id="pull_observations",
            level=LogLevel.INFO,
            title="Extracting observations with filter..",
            data={
                "start_date_time": start_datetime_str,
                "end_date_time": end_datetime_str,
                "environment": str(environment),
            },
            config_data=action_config.dict(),
        )

        observations = []
        if len(rmwSets.sets) != 0:
            logger.info(
                f"Processing updates from RMW Hub API...Number of gearsets returned: {len(rmwSets.sets)}"
            )
            observations = await rmw_adapter.process_rmw_download(
                rmwSets, start_datetime_str, sync_interval_minutes
            )
            total_observations.extend(observations)
        else:
            await log_activity(
                integration_id=integration.id,
                action_id="pull_observations",
                level=LogLevel.INFO,
                title="No gearsets returned from RMW Hub API.",
                data={
                    "start_date_time": start_datetime_str,
                    "end_date_time": end_datetime_str,
                    "environment": str(environment),
                },
                config_data=action_config.dict(),
            )

        # Upload changes from ER to RMW Hub
        # put_set_id_observations, rmw_response = await rmw_adapter.process_rmw_upload(
        #     rmwSets, start_datetime_str
        # )
        # total_observations.extend(put_set_id_observations)
        # observations.extend(put_set_id_observations)

        # # TODO: Handle failed response
        # await log_activity(
        #     integration_id=integration.id,
        #     action_id="pull_observations",
        #     level=LogLevel.INFO,
        #     title="Process upload to rmwHub completed.",
        #     data={
        #         "rmw_response": str(rmw_response),
        #     },
        #     config_data=action_config.dict(),
        # )

        # Send the extracted data to Gundi in batches
        for batch in generate_batches(observations):
            logger.info(f"Sending {len(batch)} observations to Gundi...")
            await send_observations_to_gundi(
                observations=batch, integration_id=str(integration.id)
            )

        # Patch subject status
        await rmw_adapter.push_status_updates(
            observations=observations, rmw_sets=rmwSets
        )

    # The result will be recorded in the portal if using the activity_logger decorator
    return {"observations_extracted": len(total_observations)}


def generate_batches(iterable, n=LOAD_BATCH_SIZE):
    for i in range(0, len(iterable), n):
        yield iterable[i : i + n]


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
