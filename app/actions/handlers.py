import httpx
import logging
from datetime import datetime, timezone, timedelta
from app.actions.configurations import AuthenticateConfig, PullEventsConfig
from app.services.activity_logger import activity_logger
from app.services.gundi import send_observations_to_gundi
from app.services.state import IntegrationStateManager
from app.services.errors import ConfigurationNotFound, ConfigurationValidationError
from app.services.utils import find_config_for_action
from gundi_core.schemas.v2 import Integration
from pydantic import BaseModel, parse_obj_as
from typing import List, Optional, Iterable, Generator, Any

from app.bluetrax import authenticate, get_assets, get_asset_history, HistoryItem

logger = logging.getLogger(__name__)
state_manager = IntegrationStateManager()


def get_auth_config(integration):
    # Look for the login credentials, needed for any action
    auth_config = find_config_for_action(
        configurations=integration.configurations,
        action_id="auth"
    )
    if not auth_config:
        raise ConfigurationNotFound(
            f"Authentication settings for integration {str(integration.id)} "
            f"are missing. Please fix the integration setup in the portal."
        )
    return AuthenticateConfig.parse_obj(auth_config.data)
async def action_auth(integration:Integration, action_config: AuthenticateConfig):
    logger.info(f"Executing auth action with integration {integration} and action_config {action_config}...")

    base_url = integration.base_url

    try:
        auth = await authenticate(action_config.username, action_config.password.get_secret_value())
        if auth and auth.users:
            
            return {"valid_credentials": True,
                    "user_id": auth.users[0].user_id,
                    "client_id": auth.users[0].client_id,
                    "contact_id": auth.users[0].contact_id,
                    "client_name": auth.users[0].client_name
                    }
    except httpx.HTTPStatusError as e:
        return {"valid_credentials": False, "status_code": e.response.status_code}


@activity_logger()
async def action_pull_observations(integration:Integration, action_config: PullEventsConfig):
    logger.info(f"Executing pull_observations action with integration {integration} and action_config {action_config}...")

    auth_config = get_auth_config(integration)
    auth = await authenticate(auth_config.username, auth_config.password.get_secret_value())

    assets = await get_assets(auth.users[0].user_id)

    asset_state = {}

    for asset in assets.userAssets:

        last_state = await  state_manager.get_state(integration_id=integration.id, action_id="pull_observations", source_id=asset.unit_id)
        start_time = datetime.fromisoformat(last_state["latest_fixtime"]) if last_state else datetime.now(tz=timezone.utc) - timedelta(hours=2)
        asset_history = await get_observations(user_id=auth.users[0].user_id, unit_id=asset.unit_id, start_time=start_time, end_time=datetime.now(tz=timezone.utc))

        if asset_history:

            for batch in batches(asset_history.data, 100):
                await send_observations_to_gundi(observations=[transform(item)for item in batch],
                                                  integration_id=integration.id)

        if asset_history.data:                
            asset_state[asset.unit_id] = max(item.fixtime for item in asset_history.data)       

    for unit_id, item in asset_state.items():
        await state_manager.set_state(integration_id=integration.id, action_id="pull_observations", state={"latest_fixtime": item.isoformat()}, source_id=unit_id)
    return {'assets': assets.userAssets}


def batches(source: List, batch_size: int) -> Generator[List[Any], None, None]:
    """
    Yields batches of a given size from a source iterable.
    """
    for i in range(0, len(source), batch_size):
        yield source[i:i + batch_size]


def transform(item: HistoryItem):
    return {
        "name": item.reg_no,
        "source": item.unit_id,
        "type": "tracking-device",
        "subject_type": "vehicle",
        "recorded_at": item.fixtime,
        "location": {
            "lat": item.latitude,
            "lon": item.longitude
        },
        "additional": {
            "speed_kmph": item.speed,
            "location": item.location,
            "driver": item.driver,
            "course": item.course,
            "device_timezone": item.device_timezone,
            "unit_id": item.unit_id,
            "subject_name": item.reg_no
        }
    }

async def get_observations(user_id: str, unit_id: str, start_time: datetime, end_time: datetime):

    end_time = end_time or datetime.now(tz=timezone.utc)
    history_result = await get_asset_history(unit_id, start_time, end_time)

    return history_result
