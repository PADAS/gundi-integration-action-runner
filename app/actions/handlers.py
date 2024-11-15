# actions/handlers.py
from app.services.activity_logger import activity_logger, log_activity
from app.services.gundi import send_observations_to_gundi
from gundi_core.events import LogLevel
from .configurations import PullRmwHubObservationsConfiguration

from .rmwhub import RmwHubAdapter

from datetime import datetime, timedelta


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

    rmw_adapter = RmwHubAdapter()
    updates, deletes = rmw_adapter.download_data(action_config, start_datetime_str)

    # Optionally, log a custom messages to be shown in the portal
    await log_activity(
        integration_id=integration.id,
        action_id="pull_observations",
        level=LogLevel.INFO,
        title="Extracting observations with filter..",
        data={"start_date_time": start_datetime_str, "end_date_time": end_datetime_str},
        config_data=action_config.dict(),
    )

    observations = rmw_adapter.process_updates(updates)
    rmw_adapter.process_deletes(deletes)  # TODO: Is it ok to process deletes here?

    # Send the extracted data to Gundi
    await send_observations_to_gundi(
        observations=observations, integration_id=integration.id
    )

    # The result will be recorded in the portal if using the activity_logger decorator
    return {"observations_extracted": 10}
