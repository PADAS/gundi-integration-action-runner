# actions/handlers.py
import logging
from datetime import datetime, timedelta

from app.actions.helpers import Environment, get_er_token_and_site
from app.services.action_scheduler import crontab_schedule
from app.services.activity_logger import activity_logger, log_activity
from app.services.gundi import send_observations_to_gundi
from gundi_core.events import LogLevel
from gundi_core.schemas.v2 import Integration
from gundi_client_v2 import GundiClient

from .configurations import PullRmwHubObservationsConfiguration, AuthenticateConfig
from .rmwhub import RmwHubAdapter

logger = logging.getLogger(__name__)
_client = GundiClient()


LOAD_BATCH_SIZE = 100


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
    sync_interval_minutes = 20000
    start_datetime = current_datetime - timedelta(minutes=sync_interval_minutes)
    start_datetime_str = start_datetime.strftime("%Y-%m-%d %H:%M:%S")
    end_datetime = current_datetime + timedelta(minutes=sync_interval_minutes)
    end_datetime_str = end_datetime.strftime("%Y-%m-%d %H:%M:%S")

    # TODO: Create sub-actions for each destination
    total_observations = []
    connection_details = await _client.get_connection_details(integration.id)
    for destination in connection_details.destinations:
        environment = Environment(destination.name)

        er_token, er_destination = await get_er_token_and_site(integration, environment)

        rmw_adapter = RmwHubAdapter(
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
            },
            config_data=action_config.dict(),
        )

        if len(rmwSets.sets) == 0:
            logger.info("No gearsets returned from RMW Hub API.")
            return {"observations_extracted": 0}

        logger.info(
            f"Processing updates from RMW Hub API...Number of gearsets returned: {len(rmwSets.sets)}"
        )
        observations = await rmw_adapter.process_sets(
            rmwSets, start_datetime_str, sync_interval_minutes
        )
        total_observations.extend(observations)

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


@activity_logger()
@crontab_schedule("0 0 * * *")  # Run every 24 hours at midnight
async def action_pull_observations_24_hour_sync(
    integration, action_config: PullRmwHubObservationsConfiguration
):

    sync_interval_minutes = 1440  # 24 hours
    current_datetime = datetime.now()
    start_datetime = current_datetime - timedelta(minutes=sync_interval_minutes)
    start_datetime_str = start_datetime.strftime("%Y-%m-%d %H:%M:%S")
    end_datetime = current_datetime + timedelta(minutes=sync_interval_minutes)
    end_datetime_str = end_datetime.strftime("%Y-%m-%d %H:%M:%S")

    # TODO: Create sub-actions for each destination
    total_observations = []
    for destination in integration.destinations:
        environment = Environment(destination.name)
        er_token, er_destination = await get_er_token_and_site(integration, environment)

        rmw_adapter = RmwHubAdapter(
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
            },
            config_data=action_config.dict(),
        )

        if len(rmwSets.sets) == 0:
            logger.info("No gearsets returned from RMW Hub API.")
            return {"observations_extracted": 0}

        logger.info(
            f"Processing updates from RMW Hub API...Number of gearsets returned: {len(rmwSets.sets)}"
        )
        observations = await rmw_adapter.process_sets(
            rmwSets, start_datetime_str, sync_interval_minutes
        )
        total_observations.extend(observations)

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
