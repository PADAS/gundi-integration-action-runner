import httpx
import logging
import stamina
import pydantic

import app.services.gundi as gundi_tools
import app.actions.client as client
import app.settings.integration as settings

from datetime import datetime, timezone, timedelta

from app.actions.client import LotekException, LotekConnectionException
from app.actions.configurations import get_auth_config, AuthenticateConfig, PullObservationsConfig
from app.services.activity_logger import activity_logger, log_action_activity
from app.services.state import IntegrationStateManager
from gundi_core.schemas.v2.gundi import LogLevel

logger = logging.getLogger(__name__)
state_manager = IntegrationStateManager()

async def action_auth(integration, action_config: AuthenticateConfig):
    logger.info(f"Executing auth action with integration {integration} and action_config {action_config}...")
    try:
        token = await client.get_token(integration, action_config)
    except LotekConnectionException:
        logger.exception(f"Auth unsuccessful for integration {integration.id}. Lotek returned 400 (bad request)")
        return {"valid_credentials": False, "message": "Invalid credentials"}
    except httpx.HTTPError as e:
        logger.exception(f"Auth action failed for integration {integration.id}. Exception: {e}")
        return {"error": "An internal error occurred while trying to test credentials. Please try again later."}
    else:
        if token:
            logger.info(f"Auth successful for integration '{integration.name}'. Token: '{token}'")
            return {"valid_credentials": True}
        else:
            logger.error(f"Auth unsuccessful for integration {integration}.")
            return {"valid_credentials": False}

async def transform(p: client.LotekPosition, integration):
    try:
        if p.Longitude and p.Latitude:
            data = {
                "source": p.DeviceID,
                "source_name": p.DeviceID,
                'type': 'tracking-device',
                "recorded_at": ensure_timezone_aware(p.RecDateTime).isoformat(),
                "location": {
                    "lat": p.Latitude,
                    "lon": p.Longitude
                },
                "additional": p.dict(exclude={'DeviceID', 'Latitude', 'Longitude', 'RecDateTime'})
            }
            return data
        else:
            msg = f'Ignoring bad Lotek point: {p}'
            logger.warning(msg)
            await log_action_activity(
                integration_id=str(integration.id),
                action_id="pull_observations",
                title=msg,
                level=LogLevel.WARNING
            )
    except Exception as ex:
        logger.error(f"Failed to parse Lotek point: {p} for Integration ID {str(integration.id)}. Exception: {ex}")
        raise ex

def ensure_timezone_aware(val: datetime, default_tz: timezone = timezone.utc) -> datetime:
    if not val.tzinfo:
        val = val.replace(tzinfo=default_tz)
    return val

@activity_logger()
async def action_pull_observations(integration, action_config: PullObservationsConfig):
    logger.info(f"Executing pull_observations action with integration {integration} and action_config {action_config}...")

    present_time = datetime.now(tz=timezone.utc)

    try:
        saved_state = await state_manager.get_state(str(integration.id), "pull_observations")
        state = client.IntegrationState.parse_obj({"last_run": saved_state})
    except pydantic.ValidationError as ve:
        state = client.IntegrationState()

    logger.info(f"Running Lotek integration for integration '{integration.name}({integration.id})'. State: {state}")

    async for attempt in stamina.retry_context(on=(LotekException, LotekConnectionException), attempts=3, wait_initial=timedelta(seconds=10), wait_max=timedelta(seconds=10)):
        with attempt:
            auth = get_auth_config(integration)
            try:
                device_list = await client.get_devices(integration, auth)
            except client.LotekException as e:
                message = f"Error fetching devices from Lotek. Integration ID: {integration.id} Exception: {e}"
                logger.exception(message)
                await log_action_activity(
                    integration_id=str(integration.id),
                    action_id="pull_observations",
                    title=message,
                    level=LogLevel.WARNING
                )
                raise e

    logger.info(f"Extracted {len(device_list)} devices from Lotek for inbound: {integration.id}")
    observations_extracted = 0
    for device in device_list:
        cdip_positions = []
        lower_date = max(present_time - timedelta(days=7), state.last_run)
        while lower_date < present_time:
            upper_date = min(present_time, lower_date + timedelta(days=7))
            async for attempt in stamina.retry_context(on=(LotekException, LotekConnectionException), attempts=3, wait_initial=timedelta(seconds=10), wait_max=timedelta(seconds=10)):
                with attempt:
                    try:
                        positions = await client.get_positions(device.nDeviceID, auth, integration, lower_date, upper_date, True)
                    except httpx.HTTPError as e:
                        message = f"Error fetching positions from Lotek. Device: {device.nDeviceID}. Dates: [{lower_date},{upper_date}]. Integration ID: {integration.id} Exception: {e}"
                        logger.exception(message)
                        await log_action_activity(
                            integration_id=str(integration.id),
                            action_id="pull_observations",
                            title=message,
                            level=LogLevel.WARNING
                        )
                        raise LotekException(message=message, error=e)
            logger.info(f"Extracted {len(positions)} obs from Lotek for device: {device.nDeviceID} between {lower_date} and {upper_date}.")
            for position in positions:
                cdip_pos = transform(position, integration)
                if cdip_pos:
                    cdip_positions.append(cdip_pos)
            logger.debug(f"Extracted {len(cdip_positions)} of {len(positions)} points between {lower_date} and {upper_date}.")
            lower_date = upper_date

        if cdip_positions:
            logger.info(f"Observations pulled successfully for integration ID: {integration.id}.")

            def generate_batches(iterable, n=settings.OBSERVATIONS_BATCH_SIZE):
                for i in range(0, len(iterable), n):
                    yield iterable[i: i + n]

            for i, batch in enumerate(generate_batches(cdip_positions)):
                try:
                    logger.info(f'Sending observations batch #{i}: {len(batch)} observations. Device: {device.nDeviceID}')
                    await gundi_tools.send_observations_to_gundi(observations=batch, integration_id=integration.id)
                except httpx.HTTPError as e:
                    msg = f'Sensors API returned error for integration_id: {str(integration.id)}. Exception: {e}'
                    logger.exception(msg, extra={
                        'needs_attention': True,
                        'integration_id': integration.id,
                        'action_id': "pull_observations"
                    })
                    raise e
                else:
                    observations_extracted += len(cdip_positions)
        else:
            message = f"No positions fetched for device {device.nDeviceID} integration ID: {integration.id}."
            logger.info(message)
            await log_action_activity(
                integration_id=str(integration.id),
                action_id="pull_observations",
                title=message,
                level=LogLevel.DEBUG
            )
    await state_manager.set_state(
        str(integration.id),
        "pull_observations",
        upper_date
    )
    return {'observations_extracted': observations_extracted}
