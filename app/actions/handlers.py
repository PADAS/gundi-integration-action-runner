from datetime import datetime, timedelta, timezone
import logging

import httpx
from app import settings
from app.services.action_scheduler import trigger_action
from app.services.utils import generate_batches
import pydantic

import app.services.gundi as gundi_tools
from app.actions import client
from app.actions.configurations import AuthenticateConfig, PullObservationsConfig, PullObservationsFromDeviceBatch
from app.services.activity_logger import activity_logger, log_action_activity

from app.services.state import IntegrationStateManager
from gundi_core.schemas.v2.gundi import LogLevel



logger = logging.getLogger(__name__)


state_manager = IntegrationStateManager()


async def action_auth(integration, action_config: AuthenticateConfig):
    logger.info(f"Executing auth action with integration {integration} and action_config {action_config}...")
    if action_config.password == "valid_password" and action_config.username == "valid_username":
        return {"valid_credentials": True}
    else:
        return {"valid_credentials": False}


async def action_fetch_samples(integration, action_config: PullObservationsConfig):
    logger.info(f"Executing fetch_samples action with integration {integration} and action_config {action_config}...")
    observations = [
            {
                "object_id": "75848f54-312d-4e4b-a931-546880931f68",
                "location":{
                    "lat":27.192358,
                    "lon":13.273482
                }
            },
            {
                "object_id": "34236d0f-b02d-4bef-bb89-a7bb3bfafa97",
                "location":{
                    "lat":55.847321,
                    "lon":72.120293
                }
            },
            {
                "object_id": "41c7d231-7cf9-428f-8699-000723361e85",
                "location":{
                    "lat":31.847263,
                    "lon":44.758383
                }
            },
            {
                "object_id": "fe01afd6-3c18-487b-8359-6ad109ca4043",
                "location":{
                    "lat":29.925873,
                    "lon":75.473293
                }
            }]

    if action_config.password == "valid_password" and action_config.username == "valid_username":
        return {
            "observations_extracted": len(observations),
            "observations": observations
        }
    else:
        return {"valid_credentials": False}
    

def filter_and_transform_positions(positions):
    valid_positions = []

    for position in positions:
        cdip_pos = transform(position)
        valid_positions.append(cdip_pos)

    return valid_positions


def transform(p: client.OnyeshaPosition):
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


def ensure_timezone_aware(val: datetime, default_tz: timezone = timezone.utc) -> datetime:
    if not val.tzinfo:
        val = val.replace(tzinfo=default_tz)
    return val


@activity_logger()
async def action_pull_observations_from_device_batch(integration, action_config: PullObservationsFromDeviceBatch):
    logger.info(f"Executing pull_observations_by_date action with integration {integration} and action_config {action_config}...")
    device_list = action_config.devices or []
    observations_extracted = 0

    present_time = datetime.now(tz=timezone.utc)

    device_ids = [str(device.nDeviceID) for device in device_list]
    logger.info(
        f"Running Onyesha integration for integration '{integration.name}({integration.id})'. Devices: {device_ids}"
    )

    for device in device_list:
        cdip_positions = []
        try:
            saved_state = await state_manager.get_state(
                integration_id=str(integration.id), action_id="pull_observations", source_id=str(device.nDeviceID)
            )
            state = client.IntegrationState.parse_obj({"last_run": saved_state})
        except pydantic.ValidationError as e:
            state = client.IntegrationState()
        lower_date = max(present_time - timedelta(days=7), state.last_run)
        upper_date = min(present_time, lower_date + timedelta(days=7))
        while lower_date < present_time:
            positions = await client.get_positions()
            logger.info(
                f"Extracted {len(positions)} obs from Onyesha for device: {device.nDeviceID} between {lower_date} and {upper_date}.")
            cdip_positions = filter_and_transform_positions(positions)
            logger.debug(
                f"Extracted {len(cdip_positions)} of {len(positions)} points between {lower_date} and {upper_date}.")
            lower_date = upper_date

        if cdip_positions:
            logger.info(
                f"Observations pulled successfully for integration ID: {integration.id}, Device: {device.nDeviceID}"
            )
            for i, batch in enumerate(generate_batches(cdip_positions, settings.OBSERVATIONS_BATCH_SIZE)):
                try:
                    logger.info(
                        f'Sending observations batch #{i}: {len(batch)} observations. Device: {device.nDeviceID}')
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
            integration_id=str(integration.id),
            action_id="pull_observations",
            source_id=str(device.nDeviceID),
            state={"last_run": upper_date}
        )
    return {'observations_extracted': observations_extracted}


@activity_logger()
async def action_pull_observations(integration, action_config: PullObservationsConfig):
    logger.info(f"Executing pull_observations action with integration {integration} and action_config {action_config}...")
    device_list = await client.get_devices()
    logger.info(f"Extracted {len(device_list)} devices from Lotek for inbound: {integration.id}")

    subactions_triggered = 0
    for device_batch in generate_batches(device_list, settings.DEVICES_BATCH_SIZE):
        await trigger_action(
            integration_id=integration.id,
            action_id="pull_observations_from_device_batch",
            config=PullObservationsFromDeviceBatch(devices=device_batch)
        )
        subactions_triggered += 1

    return {"subactions_triggered": subactions_triggered}
