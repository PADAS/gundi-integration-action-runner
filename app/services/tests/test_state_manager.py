import datetime
import json

import pytest
from app.services.state import IntegrationStateManager


@pytest.mark.asyncio
async def test_set_integration_state(mocker, mock_redis, integration_v2):
    mocker.patch("app.services.state.redis", mock_redis)
    state_manager = IntegrationStateManager()
    execution_timestamp = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
    integration_id = str(integration_v2.id)
    state = {"last_execution": execution_timestamp}

    await state_manager.set_state(
        integration_id=integration_id,
        action_id="pull_observations",
        # No source set
        state=state
    )

    mock_redis.Redis.return_value.set.assert_called_once_with(
        f"integration_state.{integration_id}.pull_observations.no-source",
        '{"last_execution": "' + execution_timestamp + '"}'
    )


@pytest.mark.asyncio
async def test_get_integration_state(mocker, mock_redis, integration_v2, mock_integration_state):
    mocker.patch("app.services.state.redis", mock_redis)
    state_manager = IntegrationStateManager()
    integration_id = str(integration_v2.id)

    state = await state_manager.get_state(
        integration_id=integration_id,
        action_id="pull_observations",
        # No source set
    )

    assert state == mock_integration_state
    mock_redis.Redis.return_value.get.assert_called_once_with(
        f"integration_state.{integration_id}.pull_observations.no-source"
    )


@pytest.mark.asyncio
async def test_set_source_state(mocker, mock_redis, integration_v2, mock_integration_state):
    mocker.patch("app.services.state.redis", mock_redis)
    state_manager = IntegrationStateManager()
    integration_id = str(integration_v2.id)
    source_id = "device-123"

    await state_manager.set_state(
        integration_id=integration_id,
        action_id="pull_observations",
        source_id=source_id,
        state=mock_integration_state
    )

    mock_redis.Redis.return_value.set.assert_called_once_with(
        f"integration_state.{integration_id}.pull_observations.{source_id}",
        json.dumps(mock_integration_state, default=str)
    )


@pytest.mark.asyncio
async def test_get_state_source_state(mocker, mock_redis, integration_v2, mock_integration_state):
    mocker.patch("app.services.state.redis", mock_redis)
    state_manager = IntegrationStateManager()
    integration_id = str(integration_v2.id)
    source_id = "device-123"

    state = await state_manager.get_state(
        integration_id=integration_id,
        action_id="pull_observations",
        source_id=source_id
    )

    assert state == mock_integration_state
    mock_redis.Redis.return_value.get.assert_called_once_with(
        f"integration_state.{integration_id}.pull_observations.{source_id}"
    )
