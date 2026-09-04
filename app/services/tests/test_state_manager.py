import datetime
import json

import pytest
from app.conftest import async_return
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
async def test_delete_integration_state(mocker, mock_redis, integration_v2):
    mocker.patch("app.services.state.redis", mock_redis)
    state_manager = IntegrationStateManager()

    execution_timestamp = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
    integration_id = str(integration_v2.id)

    # set state
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

    # then delete the state

    await state_manager.delete_state(
        integration_id=integration_id,
        action_id="pull_observations",
        # No source set
    )

    mock_redis.Redis.return_value.delete.assert_called_once_with(
        f"integration_state.{integration_id}.pull_observations.no-source"
    )


@pytest.mark.asyncio
async def test_set_if_absent(mocker, mock_redis, integration_v2):
    mocker.patch("app.services.state.redis", mock_redis)
    state_manager = IntegrationStateManager()
    integration_id = str(integration_v2.id)

    # Redis SET ... NX EX returns a truthy value when the key was absent (set),
    # and None when it already existed (not set / throttled).
    mock_redis.Redis.return_value.set.return_value = async_return("OK")
    was_set = await state_manager.set_if_absent(
        integration_id=integration_id,
        action_id="pull_observations",
        source_id="skip-invalid-config-warning",
        ttl_seconds=3600,
    )
    assert was_set is True
    mock_redis.Redis.return_value.set.assert_called_once_with(
        f"integration_state.{integration_id}.pull_observations.skip-invalid-config-warning",
        "1",
        ex=3600,
        nx=True,
    )

    # Key already present within the window → Redis returns None → False.
    mock_redis.Redis.return_value.set.return_value = async_return(None)
    was_set = await state_manager.set_if_absent(
        integration_id=integration_id,
        action_id="pull_observations",
        source_id="skip-invalid-config-warning",
        ttl_seconds=3600,
    )
    assert was_set is False


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


@pytest.mark.asyncio
async def test_delete_state_source_state(mocker, mock_redis, integration_v2, mock_integration_state):
    mocker.patch("app.services.state.redis", mock_redis)
    state_manager = IntegrationStateManager()
    integration_id = str(integration_v2.id)
    source_id = "device-123"

    # set state
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

    # delete state

    await state_manager.delete_state(
        integration_id=integration_id,
        action_id="pull_observations",
        source_id=source_id
    )

    mock_redis.Redis.return_value.delete.assert_called_once_with(
        f"integration_state.{integration_id}.pull_observations.{source_id}"
    )


@pytest.mark.asyncio
async def test_set_state_noops_on_ephemeral_run(mocker, mock_redis):
    # Ephemeral runs synthesize a fresh integration id per call. Persisting
    # state under it would create a permanent orphan (this key carries no
    # TTL). set_state must silently no-op so a well-meaning-but-buggy
    # reference or auth handler can't leak watermarks into Redis.
    from app.services.activity_logger import ephemeral_run
    mocker.patch("app.services.state.redis", mock_redis)
    state_manager = IntegrationStateManager()

    token = ephemeral_run.set(True)
    try:
        await state_manager.set_state(
            integration_id="synthetic-uuid",
            action_id="pull_observations",
            state={"last_execution": "irrelevant"},
        )
    finally:
        ephemeral_run.reset(token)

    mock_redis.Redis.return_value.set.assert_not_called()


@pytest.mark.asyncio
async def test_set_if_absent_noops_on_ephemeral_run(mocker, mock_redis):
    # Same invariant as set_state: no Redis writes under a synthetic
    # integration id. Returns False ("not set by this call") so a throttle
    # caller treats the window as already taken and stays quiet.
    from app.services.activity_logger import ephemeral_run
    mocker.patch("app.services.state.redis", mock_redis)
    state_manager = IntegrationStateManager()

    token = ephemeral_run.set(True)
    try:
        was_set = await state_manager.set_if_absent(
            integration_id="synthetic-uuid", action_id="pull_observations", ttl_seconds=60,
        )
    finally:
        ephemeral_run.reset(token)

    assert was_set is False
    mock_redis.Redis.return_value.set.assert_not_called()


@pytest.mark.asyncio
async def test_delete_state_noops_on_ephemeral_run(mocker, mock_redis):
    from app.services.activity_logger import ephemeral_run
    mocker.patch("app.services.state.redis", mock_redis)
    state_manager = IntegrationStateManager()

    token = ephemeral_run.set(True)
    try:
        await state_manager.delete_state(integration_id="synthetic-uuid", action_id="pull_observations")
    finally:
        ephemeral_run.reset(token)

    mock_redis.Redis.return_value.delete.assert_not_called()


@pytest.mark.asyncio
async def test_ephemeral_state_noops_leave_a_debug_trace(mocker, mock_redis, caplog):
    # A handler that writes then reads state in the same ephemeral run gets
    # {} back and fails in a way that looks unrelated; the log line is the
    # only clue that the write was suppressed.
    import logging
    from app.services.activity_logger import ephemeral_run
    mocker.patch("app.services.state.redis", mock_redis)
    state_manager = IntegrationStateManager()
    caplog.set_level(logging.DEBUG, logger="app.services.state")

    token = ephemeral_run.set(True)
    try:
        await state_manager.set_state(
            integration_id="synthetic-uuid", action_id="auth", state={"token": "t"},
        )
    finally:
        ephemeral_run.reset(token)

    assert any("ephemeral" in r.getMessage().lower() for r in caplog.records)


@pytest.mark.asyncio
async def test_state_redis_retry_backoff_does_not_block_the_event_loop(mocker, mock_redis, integration_v2):
    """stamina's sync iterator sleeps with time.sleep between attempts; inside a
    coroutine that freezes every other request on the worker. All four state
    calls must iterate asynchronously."""
    from unittest.mock import AsyncMock
    import redis.asyncio as redis
    from app.conftest import async_return
    from app.services.state import IntegrationStateManager
    calls = {"n": 0}
    def flaky_get(key):
        calls["n"] += 1
        if calls["n"] == 1:
            raise redis.RedisError("flap")
        return async_return(None)
    mock_redis.Redis.return_value.get.side_effect = flaky_get
    mocker.patch("app.services.state.redis", mock_redis)
    mocker.patch("time.sleep", side_effect=AssertionError("retry back-off blocked the event loop"))
    mocker.patch("asyncio.sleep", AsyncMock())

    state = await IntegrationStateManager().get_state(str(integration_v2.id), "pull_observations")

    assert state == {}
    assert calls["n"] == 2
