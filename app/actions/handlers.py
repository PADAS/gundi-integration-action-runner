# actions/handlers.py
import logging
from datetime import datetime, timedelta

from app.services.activity_logger import activity_logger, log_activity
from app.services.gundi import send_observations_to_gundi
from gundi_core.events import LogLevel
from gundi_core.schemas.v2 import Integration

from .configurations import PullRmwHubObservationsConfiguration, AuthenticateConfig
from .rmwhub import RmwHubAdapter

logger = logging.getLogger(__name__)


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
async def action_pull_observations(
    integration, action_config: PullRmwHubObservationsConfiguration
):

    # Add your business logic to extract data here...
    current_datetime = datetime.now()
    start_datetime = current_datetime - timedelta(
        minutes=action_config.sync_interval_minutes
    )
    start_datetime_str = start_datetime.strftime("%Y-%m-%d %H:%M:%S")
    end_datetime = current_datetime + timedelta(
        minutes=action_config.sync_interval_minutes
    )
    end_datetime_str = end_datetime.strftime("%Y-%m-%d %H:%M:%S")

    rmw_adapter = RmwHubAdapter(
        action_config.api_key.get_secret_value(), action_config.rmw_url
    )

    logger.info(
        f"Downloading data from RMW Hub API...For the dates: {start_datetime_str} - {end_datetime_str}"
    )
    updates, deletes = rmw_adapter.download_data(start_datetime_str)

    # Optionally, log a custom messages to be shown in the portal
    await log_activity(
        integration_id=integration.id,
        action_id="pull_observations",
        level=LogLevel.INFO,
        title="Extracting observations with filter..",
        data={"start_date_time": start_datetime_str, "end_date_time": end_datetime_str},
        config_data=action_config.dict(),
    )

    logger.info(
        f"Processing updates from RMW Hub API...Number of updates: {len(updates)}, Number of deletes: {len(deletes)}"
    )
    observations = rmw_adapter.process_updates(updates)
    # TODO: Implement process_deletes
    # rmw_adapter.process_deletes(deletes)

    # Send the extracted data to Gundi
    logger.info(f"Sending {len(observations)} observations to Gundi...")
    await send_observations_to_gundi(
        observations=observations, integration_id=str(integration.id)
    )

    # Patch subject status
    # TODO: Get ER_subjects by name and patch statuses for existing subjects

    # The result will be recorded in the portal if using the activity_logger decorator
    return {"observations_extracted": len(observations)}
