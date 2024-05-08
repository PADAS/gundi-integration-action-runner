import httpx
import pydantic

from datetime import datetime
from typing import Optional, List
from app.actions.configurations import (
    AuthenticateConfig,
    PullObservationsConfig
)
from app.services.errors import ConfigurationNotFound
from app.services.utils import find_config_for_action


class PullObservationsHeader(pydantic.BaseModel):
    Authorization: str


class VehiclesResponse(pydantic.BaseModel):
    deviceId: int
    vehicleId: Optional[int]
    x: float
    y: float
    name: str
    regNo: Optional[str]
    iconURL: Optional[str]
    address: Optional[str]
    alarm: Optional[str]
    unit_msisdn: Optional[str]
    speed: Optional[int]
    direction: Optional[int]
    time: Optional[int]
    timeStr: datetime
    ignOn: Optional[bool]


class PullObservationsResponse(pydantic.BaseModel):
    vehicles: List[VehiclesResponse]


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


def get_fetch_samples_config(integration):
    # Look for the login credentials, needed for any action
    auth_config = find_config_for_action(
        configurations=integration.configurations,
        action_id="fetch_samples"
    )
    if not auth_config:
        raise ConfigurationNotFound(
            f"Authentication settings for integration {str(integration.id)} "
            f"are missing. Please fix the integration setup in the portal."
        )
    return PullObservationsConfig.parse_obj(auth_config.data)


def get_pull_config(integration):
    # Look for the login credentials, needed for any action
    auth_config = find_config_for_action(
        configurations=integration.configurations,
        action_id="pull_observations"
    )
    if not auth_config:
        raise ConfigurationNotFound(
            f"Authentication settings for integration {str(integration.id)} "
            f"are missing. Please fix the integration setup in the portal."
        )
    return PullObservationsConfig.parse_obj(auth_config.data)


async def get_auth_token(integration, config):
    token_endpoint = config.endpoint
    # Remove endpoint from request
    del config.endpoint

    url = f"{integration.base_url}{token_endpoint}"

    async with httpx.AsyncClient(timeout=120) as session:
        response = await session.post(url, json=config.dict())
        response.raise_for_status()

    json_response = response.json()
    return json_response["token"]


async def get_vehicles_positions(integration, config):
    vehicles_endpoint = config.endpoint

    token = await get_auth_token(
        integration=integration,
        config=get_auth_config(integration)
    )

    headers = PullObservationsHeader(Authorization=f"Bearer {token}")
    url = f"{integration.base_url}{vehicles_endpoint}"

    async with httpx.AsyncClient(timeout=120) as session:
        response = await session.post(url, headers=headers.dict())
        response.raise_for_status()
        response = PullObservationsResponse.parse_obj(response.json().get("payload"))

    return response.vehicles
