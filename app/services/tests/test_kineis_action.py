"""Tests for Kineis auth and pull_telemetry actions (CONNECTORS-836)."""

import pytest
from unittest.mock import AsyncMock, MagicMock

import httpx
import pydantic

from app.actions.configurations import AuthenticateKineisConfig, PullTelemetryConfiguration
from app.actions.handlers import action_auth, action_pull_telemetry


@pytest.fixture
def integration_with_id():
    integration = MagicMock()
    integration.id = "550e8400-e29b-41d4-a716-446655440000"
    return integration


@pytest.fixture
def pull_telemetry_config():
    return PullTelemetryConfiguration(
        lookback_hours=4,
        page_size=100,
        use_realtime=False,  # tests use bulk path with mocked fetch_telemetry
    )


def test_pull_telemetry_config_rejects_both_device_refs_and_uids():
    """API allows only one of device_refs or device_uids (manual 1.3.1.2)."""
    with pytest.raises(pydantic.ValidationError) as exc_info:
        PullTelemetryConfiguration(
            lookback_hours=4,
            page_size=100,
            device_refs=["ref1"],
            device_uids=[1788],
        )
    assert "device_refs" in str(exc_info.value) or "device_uids" in str(exc_info.value)


@pytest.fixture
def authenticate_kineis_config():
    return AuthenticateKineisConfig(
        username="authuser",
        password=pydantic.SecretStr("authpass"),
        client_id="api-telemetry",
    )


@pytest.mark.asyncio
async def test_action_auth_success(mocker, integration_with_id, authenticate_kineis_config):
    """Auth action returns valid_credentials True when get_access_token succeeds."""
    mocker.patch(
        "app.actions.handlers.get_access_token",
        AsyncMock(return_value={"access_token": "x", "expires_in": 300}),
    )
    result = await action_auth(
        integration=integration_with_id,
        action_config=authenticate_kineis_config,
    )
    assert result["valid_credentials"] is True
    assert result["expires_in"] == 300


@pytest.mark.asyncio
async def test_action_auth_failure(mocker, integration_with_id, authenticate_kineis_config):
    """Auth action returns valid_credentials False and message when get_access_token fails."""
    mocker.patch(
        "app.actions.handlers.get_access_token",
        AsyncMock(
            side_effect=httpx.HTTPStatusError(
                "Unauthorized",
                request=MagicMock(),
                response=MagicMock(status_code=401),
            )
        ),
    )
    result = await action_auth(
        integration=integration_with_id,
        action_config=authenticate_kineis_config,
    )
    assert result["valid_credentials"] is False
    assert "message" in result


@pytest.mark.asyncio
async def test_action_pull_telemetry_sends_observations(
    mocker, integration_with_id, pull_telemetry_config, authenticate_kineis_config
):
    """Action fetches telemetry (using auth config), maps to observations, and sends to Gundi."""
    sample_messages = [
        {
            "deviceRef": "D1",
            "recordedAt": "2024-01-15T10:00:00.000Z",
            "gps": {"lat": -1.5, "lon": 30.2},
        },
        {
            "deviceRef": "D2",
            "recordedAt": "2024-01-15T11:00:00.000Z",
            "gps": {"lat": -1.6, "lon": 30.3},
        },
    ]

    mock_fetch = AsyncMock(return_value=sample_messages)
    mock_send = AsyncMock(return_value={})
    mocker.patch("app.actions.handlers.fetch_telemetry", mock_fetch)
    mocker.patch("app.actions.handlers.fetch_device_list", AsyncMock(return_value=[]))
    mocker.patch("app.actions.handlers.send_observations_to_gundi", mock_send)
    mocker.patch("app.actions.handlers.log_action_activity", AsyncMock())
    mocker.patch("app.services.activity_logger.publish_event", AsyncMock())
    mocker.patch("app.actions.handlers.get_auth_config", return_value=authenticate_kineis_config)

    result = await action_pull_telemetry(
        integration=integration_with_id,
        action_config=pull_telemetry_config,
    )

    assert result["messages_fetched"] == 2
    assert result["observations_sent"] == 2
    assert result["skipped"] == 0
    mock_fetch.assert_called_once()
    mock_send.assert_called_once()
    call_kwargs = mock_send.call_args[1]
    assert call_kwargs["integration_id"] == integration_with_id.id
    observations = call_kwargs["observations"]
    assert len(observations) == 2
    assert observations[0]["source"] == "D1"
    assert observations[0]["location"] == {"lat": -1.5, "lon": 30.2}
    assert observations[1]["source"] == "D2"


@pytest.mark.asyncio
async def test_action_pull_telemetry_batches_large_result(
    mocker, integration_with_id, pull_telemetry_config, authenticate_kineis_config
):
    """Observations are sent in batches (OBSERVATION_BATCH_SIZE)."""
    from app.actions.handlers import OBSERVATION_BATCH_SIZE

    # 2.5 batches worth
    n = OBSERVATION_BATCH_SIZE * 2 + 50
    sample_messages = [
        {
            "deviceRef": f"D{i}",
            "recordedAt": "2024-01-15T10:00:00.000Z",
            "gps": {"lat": 0, "lon": 0},
        }
        for i in range(n)
    ]

    mock_fetch = AsyncMock(return_value=sample_messages)
    mock_send = AsyncMock(return_value={})
    mocker.patch("app.actions.handlers.fetch_telemetry", mock_fetch)
    mocker.patch("app.actions.handlers.fetch_device_list", AsyncMock(return_value=[]))
    mocker.patch("app.actions.handlers.send_observations_to_gundi", mock_send)
    mocker.patch("app.actions.handlers.log_action_activity", AsyncMock())
    mocker.patch("app.services.activity_logger.publish_event", AsyncMock())
    mocker.patch("app.actions.handlers.get_auth_config", return_value=authenticate_kineis_config)

    result = await action_pull_telemetry(
        integration=integration_with_id,
        action_config=pull_telemetry_config,
    )

    assert result["observations_sent"] == n
    assert mock_send.call_count == 3  # 200 + 200 + 50
    calls = mock_send.call_args_list
    assert len(calls[0][1]["observations"]) == OBSERVATION_BATCH_SIZE
    assert len(calls[1][1]["observations"]) == OBSERVATION_BATCH_SIZE
    assert len(calls[2][1]["observations"]) == 50


@pytest.mark.asyncio
async def test_action_pull_telemetry_uses_realtime_when_checkpoint_stored(
    mocker, integration_with_id, authenticate_kineis_config
):
    """When use_realtime is True and state has checkpoint, call fetch_telemetry_realtime and persist new checkpoint."""
    action_config = PullTelemetryConfiguration(
        lookback_hours=4,
        page_size=100,
        use_realtime=True,
    )
    sample_messages = [
        {"deviceRef": "R1", "msgTs": 1705312800000, "gpsLocLat": -1.0, "gpsLocLon": 30.0},
    ]
    mock_realtime = AsyncMock(return_value=(sample_messages, 12345))
    mock_state_get = AsyncMock(return_value={"kineis_realtime_checkpoint": 0})
    mock_state_set = AsyncMock(return_value=None)
    mocker.patch("app.actions.handlers.fetch_telemetry_realtime", mock_realtime)
    mocker.patch("app.actions.handlers.fetch_telemetry", AsyncMock())
    mocker.patch("app.actions.handlers.fetch_device_list", AsyncMock(return_value=[]))
    mocker.patch("app.actions.handlers.send_observations_to_gundi", AsyncMock(return_value={}))
    mocker.patch("app.actions.handlers.log_action_activity", AsyncMock())
    mocker.patch("app.services.activity_logger.publish_event", AsyncMock())
    mocker.patch("app.actions.handlers.get_auth_config", return_value=authenticate_kineis_config)
    state_mgr = MagicMock()
    state_mgr.get_state = mock_state_get
    state_mgr.set_state = mock_state_set
    mocker.patch("app.actions.handlers.IntegrationStateManager", return_value=state_mgr)

    result = await action_pull_telemetry(
        integration=integration_with_id,
        action_config=action_config,
    )

    assert result["messages_fetched"] == 1
    mock_realtime.assert_called_once()
    assert mock_realtime.call_args[1]["checkpoint"] == 0
    mock_state_set.assert_called_once()
    call_state = mock_state_set.call_args[0]
    assert call_state[2] == {"kineis_realtime_checkpoint": 12345}


@pytest.mark.asyncio
async def test_action_pull_telemetry_source_name_from_device_list(
    mocker, integration_with_id, pull_telemetry_config, authenticate_kineis_config
):
    """When fetch_device_list returns devices with customerName, observations get source_name 'deviceUid (customerName)'."""
    sample_messages = [
        {
            "deviceUid": 67899,
            "deviceRef": "7896",
            "gpsLocLat": 20.45,
            "gpsLocLon": 58.77,
            "msgTs": 1705312800000,
        },
    ]
    sample_devices = [
        {"deviceUid": 67899, "deviceRef": "7896", "customerName": "WILDLIFE COMPUTER"},
    ]
    mock_fetch_telemetry = AsyncMock(return_value=sample_messages)
    mock_fetch_device_list = AsyncMock(return_value=sample_devices)
    mock_send = AsyncMock(return_value={})
    mocker.patch("app.actions.handlers.fetch_telemetry", mock_fetch_telemetry)
    mocker.patch("app.actions.handlers.fetch_device_list", mock_fetch_device_list)
    mocker.patch("app.actions.handlers.send_observations_to_gundi", mock_send)
    mocker.patch("app.actions.handlers.log_action_activity", AsyncMock())
    mocker.patch("app.services.activity_logger.publish_event", AsyncMock())
    mocker.patch("app.actions.handlers.get_auth_config", return_value=authenticate_kineis_config)

    result = await action_pull_telemetry(
        integration=integration_with_id,
        action_config=pull_telemetry_config,
    )

    assert result["observations_sent"] == 1
    mock_fetch_device_list.assert_called_once()
    observations = mock_send.call_args[1]["observations"]
    assert len(observations) == 1
    assert observations[0]["source_name"] == "67899 (WILDLIFE COMPUTER)"
