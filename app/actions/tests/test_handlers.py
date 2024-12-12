import pytest
import httpx
from datetime import datetime, timezone
from unittest.mock import AsyncMock

from gundi_core.schemas.v2 import Integration
from app.actions.handlers import action_auth, transform, action_pull_observations
from app.actions.configurations import AuthenticateConfig, PullObservationsConfig
from app.actions.client import LotekPosition, LotekDevice, LotekException, LotekConnectionException

@pytest.fixture
def lotek_integration():
    return Integration.parse_obj(
        {
            "id": "779ff3ab-5589-4f4c-9e0a-ae8d6c9edff0",
            "name": "Lotek TEST",
            "base_url": "https://lotek-test.com",
            "enabled": True,
            "type": {
                "id": "50229e21-a9fe-4caa-862c-8592dfb2479b",
                "name": "Lotek",
                "value": "lotek",
                "description": "Integration type for Lotek",
            },
            "owner": {
                "id": "a91b400b-482a-4546-8fcb-ee42b01deeb6",
                "name": "Test Org",
                "description": "",
            },
            "configurations": [
                {
                    "id": "30f8878c-4a98-4c95-88eb-79f73c40fb2f",
                    "integration": "779ff3ab-5589-4f4c-9e0a-ae8d6c9edff0",
                    "action": {
                        "id": "80448d1c-4696-4b32-a59f-f3494fc949ac",
                        "type": "auth",
                        "name": "Authenticate",
                        "value": "auth",
                    },
                    "data": {"username": "test_user", "password": "test_pass"},
                },
            ],
            "additional": {},
            "status": "healthy",
            "status_details": ""
        }
    )

@pytest.fixture
def auth_config():
    return AuthenticateConfig(username="test_user", password="test_pass")

@pytest.fixture
def pull_config():
    return PullObservationsConfig()

@pytest.fixture
def lotek_position():
    return LotekPosition(
        ChannelStatus="OK",
        UploadTimeStamp=datetime.now(timezone.utc),
        Latitude=12.34,
        Longitude=56.78,
        Altitude=100.0,
        ECEFx=1,
        ECEFy=2,
        ECEFz=3,
        RxStatus=0,
        PDOP=1.0,
        MainV=3.7,
        BkUpV=3.7,
        Temperature=25.0,
        FixDuration=10,
        bHasTempVoltage=True,
        DevName="Device1",
        DeltaTime=0,
        FixType=1,
        CEPRadius=5,
        CRC=12345,
        DeviceID=1,
        RecDateTime=datetime.now(timezone.utc)
    )

@pytest.mark.asyncio
async def test_action_auth_success(mocker, lotek_integration, auth_config):
    mocker.patch("app.actions.client.get_token", new=AsyncMock(return_value="token"))
    result = await action_auth(lotek_integration, auth_config)
    assert result == {"valid_credentials": True}

@pytest.mark.asyncio
async def test_action_auth_invalid_credentials(mocker, lotek_integration, auth_config):
    mocker.patch("app.actions.client.get_token", new=AsyncMock(side_effect=LotekConnectionException(Exception(), "Invalid credentials")))
    result = await action_auth(lotek_integration, auth_config)
    assert result == {"valid_credentials": False, "message": "Invalid credentials"}

@pytest.mark.asyncio
async def test_action_auth_http_error(mocker, lotek_integration, auth_config):
    mocker.patch("app.actions.client.get_token", new=AsyncMock(side_effect=httpx.HTTPError("HTTP Error")))
    result = await action_auth(lotek_integration, auth_config)
    assert result == {"error": "An internal error occurred while trying to test credentials. Please try again later."}

@pytest.mark.asyncio
async def test_transform_success(mocker, lotek_position, lotek_integration):
    result = await transform(lotek_position, lotek_integration)
    assert result["source"] == lotek_position.DeviceID
    assert result["location"]["lat"] == lotek_position.Latitude
    assert result["location"]["lon"] == lotek_position.Longitude

@pytest.mark.asyncio
async def test_transform_invalid_position(mocker, lotek_position, lotek_integration):
    lotek_position.Latitude = None
    mock_log_action_activity = mocker.patch("app.actions.handlers.log_action_activity", new=AsyncMock())
    result = await transform(lotek_position, lotek_integration)
    assert result is None
    mock_log_action_activity.assert_called_once()

@pytest.mark.asyncio
async def test_action_pull_observations_success(mocker, lotek_integration, pull_config, mock_redis):
    mocker.patch("app.services.state.redis", mock_redis)
    mocker.patch("app.services.activity_logger.publish_event", new=AsyncMock())
    mocker.patch("app.actions.client.get_token", new=AsyncMock(return_value="token"))
    mocker.patch("app.actions.client.get_devices", new=AsyncMock(return_value=[LotekDevice(nDeviceID="1", strSpecialID="special", dtCreated=datetime.now(), strSatellite="satellite")]))
    mocker.patch("app.actions.client.get_positions", new=AsyncMock(return_value=[]))
    mocker.patch("app.services.state.IntegrationStateManager.get_state", new=AsyncMock(return_value=None))
    mocker.patch("app.services.state.IntegrationStateManager.set_state", new=AsyncMock(return_value=None))
    result = await action_pull_observations(lotek_integration, pull_config)
    assert result == {'observations_extracted': 0}

@pytest.mark.asyncio
async def test_action_pull_observations_error(mocker, lotek_integration, pull_config, mock_redis):
    mock_log_action_activity = mocker.patch("app.actions.handlers.log_action_activity", new=AsyncMock())
    mocker.patch("app.services.state.redis", mock_redis)
    mocker.patch("app.services.activity_logger.publish_event", new=AsyncMock())
    mocker.patch("app.actions.client.get_token", new=AsyncMock(return_value="token"))
    mocker.patch("app.actions.client.get_devices", new=AsyncMock(side_effect=httpx.HTTPError("Error")))
    mocker.patch("app.services.state.IntegrationStateManager.get_state", new=AsyncMock(return_value=None))

    with pytest.raises(LotekException):
        await action_pull_observations(lotek_integration, pull_config)

    mock_log_action_activity.assert_called_once()
