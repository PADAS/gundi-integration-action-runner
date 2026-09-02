import base64
import json

import httpx
import pytest
from fastapi.testclient import TestClient
from fastapi import status
from gundi_core.commands import RunIntegrationAction
from gundi_core.events import IntegrationActionFailed, IntegrationActionCustomLog, LogLevel
from gundi_core.events.transformers import ObservationTransformedER

from app import settings
import pydantic

from app.actions.core import AuthActionConfiguration, ReferenceActionConfiguration
from app.actions.core import GenericActionConfiguration
from app.conftest import AsyncMock, MockSubActionConfiguration, MockPushActionConfiguration, MockPullActionConfiguration, async_return
from app.main import app
from app.services.action_scheduler import trigger_action
from app.api_schemas import IntegrationState
from app.services.action_runner import execute_action
from app.services.errors import IntegrationAuthError
from app.services.utils import find_config_for_action

api_client = TestClient(app)


def _published_events_of_type(mock_publish_event, event_type):
    """Collect events of a given type passed to a mocked publish_event.

    publish_event is called as publish_event(event=..., topic_name=...) in some
    paths and publish_event(event, topic) positionally in others, so check both.
    """
    events = []
    for call in mock_publish_event.mock_calls:
        event = call.kwargs.get("event")
        if event is None and call.args:
            event = call.args[0]
        if isinstance(event, event_type):
            events.append(event)
    return events


@pytest.mark.asyncio
async def test_execute_pull_action_from_pubsub(
        mocker, mock_gundi_client_v2, mock_publish_event, mock_action_handlers, mock_config_manager,
        pubsub_message_request_headers, run_pull_action_pubsub_payload
):
    mocker.patch("app.services.action_runner.action_handlers", mock_action_handlers)
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)

    response = api_client.post(
        "/",
        headers=pubsub_message_request_headers,
        json=run_pull_action_pubsub_payload,
    )

    assert response.status_code == 200
    assert not mock_gundi_client_v2.get_integration_details.called
    payload = run_pull_action_pubsub_payload["message"]["data"]
    payload_dict = json.loads(base64.b64decode(payload).decode("utf-8"))
    integration_id = payload_dict.get("integration_id")
    action_id = payload_dict.get("action_id")
    assert mock_config_manager.get_integration_details.called
    mock_config_manager.get_integration_details.assert_called_with(integration_id)
    mock_action_handler, mock_config, mock_datamodel = mock_action_handlers[action_id]
    assert mock_action_handler.called


@pytest.mark.asyncio
async def test_execute_push_action_from_pubsub(
        mocker, mock_gundi_client_v2, mock_publish_event, mock_action_handlers, mock_config_manager,
        pubsub_message_request_headers, run_push_action_pubsub_payload, mock_push_observations_handler
):
    mocker.patch("app.services.action_runner.action_handlers", mock_action_handlers)
    mocker.patch("app.actions.action_handlers", mock_action_handlers)
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)

    response = api_client.post(
        "/push-data",
        headers=pubsub_message_request_headers,
        json=run_push_action_pubsub_payload,
    )

    assert response.status_code == 200
    payload = run_push_action_pubsub_payload["message"]["data"]
    payload_dict = json.loads(base64.b64decode(payload).decode("utf-8"))
    attributes = run_push_action_pubsub_payload["message"].get("attributes", {})
    # Check that the action config is retrieved for the integration
    integration_id = attributes.get("destination_id")
    assert mock_config_manager.get_integration_details.called
    mock_config_manager.get_integration_details.assert_called_with(integration_id)
    # Check that the right handler is called, with config and data
    assert mock_push_observations_handler.call_count == 1
    mock_call = mock_push_observations_handler.mock_calls[0]
    call_kwargs = mock_call.kwargs
    assert str(call_kwargs.get("integration").id) == integration_id
    config = call_kwargs.get("action_config")
    assert isinstance(config, MockPushActionConfiguration)
    data = call_kwargs.get("data")
    assert isinstance(data, ObservationTransformedER)
    assert data == ObservationTransformedER.parse_obj(payload_dict)
    metadata = call_kwargs.get("metadata")
    assert isinstance(metadata, dict)
    assert metadata == attributes


@pytest.mark.asyncio
async def test_execute_action_from_api(
        mocker, mock_gundi_client_v2, integration_v2, mock_config_manager,
        mock_publish_event, mock_action_handlers,
):
    mocker.patch("app.services.action_runner.action_handlers", mock_action_handlers)
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    integration_id = str(integration_v2.id)
    action_id = "pull_observations"

    response = api_client.post(
        "/v1/actions/execute/",
        json={
            "integration_id": integration_id,
            "action_id": action_id
        }
    )

    assert response.status_code == 200
    assert not mock_gundi_client_v2.get_integration_details.called
    assert mock_config_manager.get_integration_details.called
    mock_config_manager.get_integration_details.assert_called_with(integration_id)
    mock_action_handler, mock_config, mock_datamodel = mock_action_handlers[action_id]
    assert mock_action_handler.called


@pytest.mark.asyncio
async def test_execute_action_from_api_with_config_overrides(
        mocker, mock_gundi_client_v2, integration_v2, mock_config_manager,
        mock_publish_event, mock_action_handlers,
):
    mocker.patch("app.services.action_runner.action_handlers", mock_action_handlers)
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)

    config_overrides = {"lookback_days": 3}
    response = api_client.post(
        "/v1/actions/execute/",
        json={
            "integration_id": str(integration_v2.id),
            "action_id": "pull_observations",
            "config_overrides": config_overrides
        }
    )

    assert response.status_code == 200
    assert mock_config_manager.get_integration_details.called
    assert not mock_gundi_client_v2.get_integration_details.called
    mock_action_handler, mock_config, mock_datamodel = mock_action_handlers["pull_observations"]
    assert mock_action_handler.called
    for k, v in config_overrides.items():
        config = mock_action_handler.call_args.kwargs["action_config"]
        assert getattr(config, k) == v


@pytest.mark.asyncio
async def test_execute_action_from_pubsub_with_config_overrides(
        mocker, mock_gundi_client_v2, mock_publish_event, mock_action_handlers, mock_config_manager,
        pubsub_message_request_headers, run_pull_action_pubsub_payload_with_config_overrides
):
    mocker.patch("app.services.action_runner.action_handlers", mock_action_handlers)
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)

    response = api_client.post(
        "/",
        headers=pubsub_message_request_headers,
        json=run_pull_action_pubsub_payload_with_config_overrides,
    )

    assert response.status_code == 200
    assert mock_config_manager.get_integration_details.called
    assert not mock_gundi_client_v2.get_integration_details.called
    mock_action_handler, mock_config, mock_datamodel = mock_action_handlers["pull_observations"]
    assert mock_action_handler.called
    encoded_data = run_pull_action_pubsub_payload_with_config_overrides["message"]["data"]
    decoded_data = base64.b64decode(encoded_data).decode("utf-8")
    config_overrides = json.loads(decoded_data)["config_overrides"]
    for k, v in config_overrides.items():
        config = mock_action_handler.call_args.kwargs["action_config"]
        assert getattr(config, k) == v


@pytest.mark.asyncio
async def test_manual_pull_action_with_invalid_config_still_errors(
        mocker, mock_gundi_client_v2, integration_v2, mock_config_manager,
        mock_publish_event, mock_action_handlers,
):
    # A direct /execute call is a manual run → strict: invalid config 422s so
    # the operator sees the misconfiguration immediately.
    mocker.patch("app.services.action_runner.action_handlers", mock_action_handlers)
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)

    response = api_client.post(
        "/v1/actions/execute/",
        json={
            "integration_id": str(integration_v2.id),
            "action_id": "pull_observations",
            "config_overrides": {"lookback_days": "two"}  # should be an integer
        }
    )

    assert response.status_code == 422
    mock_action_handler, _, _ = mock_action_handlers["pull_observations"]
    assert not mock_action_handler.called


@pytest.mark.asyncio
async def test_triggered_by_marker_is_case_insensitive(
        mocker, mock_gundi_client_v2, integration_v2, mock_config_manager,
        mock_publish_event, mock_action_handlers, pubsub_message_request_headers,
):
    # A mixed-case "MANUAL" marker must be honored as a manual run (strict), not
    # silently fall through to the automated default. With an invalid config that
    # means it errors (IntegrationActionFailed) rather than skipping quietly.
    mocker.patch("app.services.action_runner.action_handlers", mock_action_handlers)
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    bad_config = mocker.MagicMock()
    bad_config.data = {"lookback_days": "two"}  # should be an integer
    mock_config_manager.get_action_configuration.return_value = async_return(bad_config)
    encoded = base64.b64encode(json.dumps({
        "integration_id": str(integration_v2.id),
        "action_id": "pull_observations",
        "triggered_by": "MANUAL",  # not the canonical lowercase "manual"
    }).encode("utf-8")).decode("utf-8")

    response = api_client.post(
        "/", headers=pubsub_message_request_headers, json={"message": {"data": encoded}},
    )

    assert response.status_code == 200  # POST / always returns {}; behavior is observed via events
    mock_action_handler, _, _ = mock_action_handlers["pull_observations"]
    assert not mock_action_handler.called
    # Treated as manual → strict → error published, NOT a quiet skip.
    assert _published_events_of_type(mock_publish_event, IntegrationActionFailed)
    assert not _published_events_of_type(mock_publish_event, IntegrationActionCustomLog)


@pytest.mark.asyncio
async def test_scheduled_pull_action_with_invalid_config_is_skipped(
        mocker, mock_gundi_client_v2, mock_config_manager, mock_publish_event,
        mock_action_handlers, mock_state_manager, pubsub_message_request_headers,
        run_pull_action_pubsub_payload,
):
    # A scheduled (PubSub, no triggered_by → automated) pull whose stored config
    # is invalid skips cleanly: no handler call, NO IntegrationActionFailed, and
    # — when the throttle window is open — one WARNING activity log with detail.
    mocker.patch("app.services.action_runner.action_handlers", mock_action_handlers)
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager)
    mocker.patch("app.services.action_runner.state_manager", mock_state_manager)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mock_state_manager.set_if_absent.return_value = async_return(True)  # window open
    bad_config = mocker.MagicMock()
    bad_config.data = {"lookback_days": "two"}  # should be an integer
    mock_config_manager.get_action_configuration.return_value = async_return(bad_config)

    response = api_client.post(
        "/", headers=pubsub_message_request_headers, json=run_pull_action_pubsub_payload,
    )

    assert response.status_code == 200
    mock_action_handler, _, _ = mock_action_handlers["pull_observations"]
    assert not mock_action_handler.called
    assert not _published_events_of_type(mock_publish_event, IntegrationActionFailed)
    skip_logs = _published_events_of_type(mock_publish_event, IntegrationActionCustomLog)
    assert len(skip_logs) == 1
    assert skip_logs[0].payload.level == LogLevel.WARNING
    assert "validation_error" in (skip_logs[0].payload.data or {})


@pytest.mark.asyncio
async def test_scheduled_pull_action_invalid_config_warning_is_throttled(
        mocker, mock_gundi_client_v2, mock_config_manager, mock_publish_event,
        mock_action_handlers, mock_state_manager, pubsub_message_request_headers,
        run_pull_action_pubsub_payload,
):
    # When the throttle window is closed (set_if_absent → False), the skip is
    # still logged locally but NO portal WARNING is published — so a
    # persistently misconfigured source doesn't emit a warning every tick.
    mocker.patch("app.services.action_runner.action_handlers", mock_action_handlers)
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager)
    mocker.patch("app.services.action_runner.state_manager", mock_state_manager)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mock_state_manager.set_if_absent.return_value = async_return(False)  # window closed
    bad_config = mocker.MagicMock()
    bad_config.data = {"lookback_days": "two"}
    mock_config_manager.get_action_configuration.return_value = async_return(bad_config)

    response = api_client.post(
        "/", headers=pubsub_message_request_headers, json=run_pull_action_pubsub_payload,
    )

    assert response.status_code == 200
    mock_action_handler, _, _ = mock_action_handlers["pull_observations"]
    assert not mock_action_handler.called
    assert not _published_events_of_type(mock_publish_event, IntegrationActionFailed)
    assert not _published_events_of_type(mock_publish_event, IntegrationActionCustomLog)


@pytest.mark.asyncio
async def test_scheduled_pull_action_invalid_config_skip_survives_throttle_failure(
        mocker, mock_gundi_client_v2, mock_config_manager, mock_publish_event,
        mock_action_handlers, mock_state_manager, pubsub_message_request_headers,
        run_pull_action_pubsub_payload,
):
    # If the throttle store (Redis) is unavailable, the skip must not crash the
    # request (which would 500 / trigger PubSub redelivery). It degrades open:
    # the WARNING is still published this time, and nothing is raised.
    mocker.patch("app.services.action_runner.action_handlers", mock_action_handlers)
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager)
    mocker.patch("app.services.action_runner.state_manager", mock_state_manager)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mock_state_manager.set_if_absent.side_effect = Exception("redis unavailable")
    bad_config = mocker.MagicMock()
    bad_config.data = {"lookback_days": "two"}
    mock_config_manager.get_action_configuration.return_value = async_return(bad_config)

    response = api_client.post(
        "/", headers=pubsub_message_request_headers, json=run_pull_action_pubsub_payload,
    )

    assert response.status_code == 200
    mock_action_handler, _, _ = mock_action_handlers["pull_observations"]
    assert not mock_action_handler.called
    assert not _published_events_of_type(mock_publish_event, IntegrationActionFailed)
    # Fail-open: the misconfiguration WARNING is still surfaced.
    skip_logs = _published_events_of_type(mock_publish_event, IntegrationActionCustomLog)
    assert len(skip_logs) == 1
    assert skip_logs[0].payload.level == LogLevel.WARNING


@pytest.mark.asyncio
async def test_scheduled_pull_action_with_missing_config_is_skipped(
        mocker, mock_gundi_client_v2, mock_config_manager, mock_publish_event,
        mock_action_handlers, pubsub_message_request_headers, run_pull_action_pubsub_payload,
):
    # Destination-only integrations have pull actions scheduled type-wide but no
    # pull config at all — an expected, quiet no-op: local log only, NO portal
    # activity-feed event at all.
    mocker.patch("app.services.action_runner.action_handlers", mock_action_handlers)
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mock_config_manager.get_action_configuration.return_value = async_return(None)

    response = api_client.post(
        "/", headers=pubsub_message_request_headers, json=run_pull_action_pubsub_payload,
    )

    assert response.status_code == 200
    mock_action_handler, _, _ = mock_action_handlers["pull_observations"]
    assert not mock_action_handler.called
    assert not _published_events_of_type(mock_publish_event, IntegrationActionFailed)
    assert not _published_events_of_type(mock_publish_event, IntegrationActionCustomLog)


@pytest.mark.asyncio
async def test_scheduled_pull_action_skipped_when_run_on_schedule_disabled(
        mocker, mock_gundi_client_v2, mock_config_manager, mock_publish_event,
        mock_action_handlers, pubsub_message_request_headers, run_pull_action_pubsub_payload,
):
    # A valid config with run_on_schedule off pauses scheduled execution — also
    # a quiet, local-log-only skip with no portal activity-feed event.
    mocker.patch("app.services.action_runner.action_handlers", mock_action_handlers)
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    paused_config = mocker.MagicMock()
    paused_config.data = {"lookback_days": 10, "run_on_schedule": False}
    mock_config_manager.get_action_configuration.return_value = async_return(paused_config)

    response = api_client.post(
        "/", headers=pubsub_message_request_headers, json=run_pull_action_pubsub_payload,
    )

    assert response.status_code == 200
    mock_action_handler, _, _ = mock_action_handlers["pull_observations"]
    assert not mock_action_handler.called
    assert not _published_events_of_type(mock_publish_event, IntegrationActionFailed)
    assert not _published_events_of_type(mock_publish_event, IntegrationActionCustomLog)


@pytest.mark.asyncio
async def test_manual_pull_action_runs_even_when_run_on_schedule_disabled(
        mocker, mock_gundi_client_v2, integration_v2, mock_config_manager,
        mock_publish_event, mock_action_handlers,
):
    # The pause toggle only gates scheduled runs — a manual /execute still runs.
    mocker.patch("app.services.action_runner.action_handlers", mock_action_handlers)
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    paused_config = mocker.MagicMock()
    paused_config.data = {"lookback_days": 10, "run_on_schedule": False}
    mock_config_manager.get_action_configuration.return_value = async_return(paused_config)

    response = api_client.post(
        "/v1/actions/execute/",
        json={
            "integration_id": str(integration_v2.id),
            "action_id": "pull_observations",
        }
    )

    assert response.status_code == 200
    mock_action_handler, _, _ = mock_action_handlers["pull_observations"]
    assert mock_action_handler.called


@pytest.mark.asyncio
async def test_non_pull_action_still_errors_on_invalid_config(
        mocker, mock_gundi_client_v2, integration_v2, mock_config_manager,
        mock_publish_event, mock_action_handlers,
):
    # The skip-on-invalid behavior is scoped to pull actions only — a non-pull
    # (here InternalActionConfiguration) action with a bad config still 422s.
    mocker.patch("app.services.action_runner.action_handlers", mock_action_handlers)
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    bad_config = mocker.MagicMock()
    bad_config.data = {"start_datetime": "not-a-datetime", "end_datetime": "also-bad"}
    mock_config_manager.get_action_configuration.return_value = async_return(bad_config)

    response = api_client.post(
        "/v1/actions/execute/",
        json={
            "integration_id": str(integration_v2.id),
            "action_id": "pull_observations_by_date",
        }
    )

    assert response.status_code == 422
    mock_action_handler, _, _ = mock_action_handlers["pull_observations_by_date"]
    assert not mock_action_handler.called


@pytest.mark.asyncio
async def test_trigger_subaction(
        mocker, mock_gundi_client_v2, integration_v2, mock_config_manager,
        mock_publish_event, mock_action_handlers,
):
    settings.TRIGGER_ACTIONS_ALWAYS_SYNC = False
    settings.INTEGRATION_COMMANDS_TOPIC = "integration-actions-topic"
    mocker.patch("app.services.action_runner.action_handlers", mock_action_handlers)
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_scheduler.publish_event", mock_publish_event)
    integration_id = str(integration_v2.id)
    action_id = "pull_observations_by_date"
    config = MockSubActionConfiguration(
        start_datetime="2024-12-01T00:00:00Z",
        end_datetime="2025-01-15T00:00:00Z"
    )

    await trigger_action(
        integration_id=integration_id,
        action_id=action_id,
        config=config
    )

    # Check that the action was not executed directly
    mock_action_handler, mock_config, mock_datamodel = mock_action_handlers[action_id]
    assert not mock_action_handler.called
    # Check that a command was published in the right topic to trigger the action
    assert mock_publish_event.call_count == 1
    call = mock_publish_event.mock_calls[0]
    command, topic = call.args
    assert isinstance(command, RunIntegrationAction)
    assert str(command.integration_id) == integration_id
    assert command.action_id == action_id
    assert command.config_overrides == config.dict()
    assert topic == settings.INTEGRATION_COMMANDS_TOPIC


@pytest.mark.asyncio
async def test_trigger_subaction_sync(
        mocker, mock_gundi_client_v2, integration_v2, mock_config_manager,
        mock_publish_event, mock_action_handlers,
):
    settings.TRIGGER_ACTIONS_ALWAYS_SYNC = True
    mocker.patch("app.services.action_runner.action_handlers", mock_action_handlers)
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_scheduler.publish_event", mock_publish_event)
    integration_id = str(integration_v2.id)
    action_id = "pull_observations_by_date"
    config = MockSubActionConfiguration(
        start_datetime="2024-12-01T00:00:00Z",
        end_datetime="2025-01-15T00:00:00Z"
    )

    await trigger_action(
        integration_id=integration_id,
        action_id=action_id,
        config=config
    )

    # Check that the action was executed directly
    mock_action_handler, mock_config, mock_datamodel = mock_action_handlers[action_id]
    assert mock_action_handler.called
    assert not mock_publish_event.called


@pytest.mark.parametrize(
    "mock_action_handlers_with_request_errors",
    ["bad_request", "internal_error" ],
    indirect=["mock_action_handlers_with_request_errors"]
)
@pytest.mark.asyncio
async def test_execute_action_with_handler_error(
        mocker, mock_gundi_client_v2, integration_v2, mock_config_manager,
        mock_publish_event, mock_action_handlers_with_request_errors
):
    mocker.patch("app.services.action_runner.action_handlers", mock_action_handlers_with_request_errors)
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)

    response = api_client.post(
        "/v1/actions/execute/",
        json={
            "integration_id": str(integration_v2.id),
            "action_id": "pull_observations"
        }
    )

    # Check that 500 is returned to indicate that the action execution failed
    assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
    # Check that extra details related to the error are returned when available
    response_data = response.json()
    mock_handler, _, _ = mock_action_handlers_with_request_errors["pull_observations"]
    expected_error = mock_handler.side_effect
    assert "detail" in response_data
    error_details = response_data["detail"]
    assert "error" in error_details
    assert "error_traceback" in error_details
    assert error_details.get("request_verb") == expected_error.request.method
    assert error_details.get("request_url") == str(expected_error.request.url)
    assert error_details.get("request_data") == str(expected_error.request.content or expected_error.request.body)
    assert error_details.get("server_response_status") == expected_error.response.status_code
    assert error_details.get("server_response_body") == str(expected_error.response.text)

    # Check that also an event with error details was published for the activity logs
    assert mock_publish_event.called
    assert mock_publish_event.call_count == 1
    call = mock_publish_event.mock_calls[0]
    assert call.kwargs.get("topic_name") == settings.INTEGRATION_EVENTS_TOPIC
    event = call.kwargs.get("event")
    assert event
    assert isinstance(event, IntegrationActionFailed)
    assert event.payload
    assert event.payload.error
    assert event.payload.error_traceback
    assert event.payload.request_verb == expected_error.request.method
    assert event.payload.request_url == str(expected_error.request.url)
    assert event.payload.request_data == str(expected_error.request.content or expected_error.request.body)
    assert event.payload.server_response_status == expected_error.response.status_code
    assert event.payload.server_response_body == str(expected_error.response.text)


_EPHEMERAL_SECRET = "ephemeral-token-abc123"


class _MockReferenceActionConfiguration(ReferenceActionConfiguration):
    tag_name: str = ""


class _MockAuthActionConfiguration(AuthActionConfiguration):
    token: str = ""


class _MockRequiredParamReferenceConfiguration(ReferenceActionConfiguration):
    event_type: str  # required query param


class _MockEchoingValidatorAuthConfiguration(AuthActionConfiguration):
    # Connector-defined validators are free to put the offending value in the
    # message; the ephemeral path must not forward it.
    token: str = ""

    @pydantic.validator("token")
    def _token_must_be_hex(cls, value):
        if value and not all(c in "0123456789abcdef" for c in value):
            raise ValueError(f"invalid token {value}")
        return value


# Curated classification titles the ephemeral path returns for third-party
# failures, in place of str(exc) which can embed our request or the source body.
_EXPECTED_SOURCE_STATUS_TEXT = {
    401: "Authentication failed (HTTP 401)",
    403: "Authentication failed (HTTP 403)",
    500: "Unexpected response from the provider (HTTP 500)",
}


class _MockGenericActionConfiguration(GenericActionConfiguration):
    pass


@pytest.fixture
def mock_reference_action_handler():
    handler = AsyncMock()
    handler.return_value = {"options": [{"value": "elephant", "label": "Elephant"}]}
    return handler


@pytest.fixture
def mock_auth_action_handler():
    handler = AsyncMock()
    handler.return_value = {"valid_credentials": True}
    return handler


@pytest.fixture
def mock_push_action_handler():
    handler = AsyncMock()
    handler.return_value = {"pushed": 1}
    return handler


@pytest.fixture
def mock_generic_action_handler():
    handler = AsyncMock()
    handler.return_value = {}
    return handler


@pytest.fixture
def mock_ephemeral_action_handlers(
        mock_reference_action_handler, mock_pull_observations_action_handler,
        mock_auth_action_handler, mock_push_action_handler, mock_generic_action_handler,
):
    return {
        "list_species": (mock_reference_action_handler, _MockReferenceActionConfiguration, None),
        "list_event_fields": (mock_reference_action_handler, _MockRequiredParamReferenceConfiguration, None),
        "auth_echoing": (mock_auth_action_handler, _MockEchoingValidatorAuthConfiguration, None),
        "pull_observations": (mock_pull_observations_action_handler, MockPullActionConfiguration, None),
        "auth": (mock_auth_action_handler, _MockAuthActionConfiguration, None),
        # Present so the parametrized rejection test can prove every
        # non-safe action type is rejected — the guard uses config-model
        # isinstance, and Generic is the fallback for handlers without an
        # explicit annotation, so leaving it untested is the highest-risk gap.
        "push_observations": (mock_push_action_handler, MockPushActionConfiguration, None),
        "generic_lookup": (mock_generic_action_handler, _MockGenericActionConfiguration, None),
    }


def _ephemeral_body(action_id="list_species", token=_EPHEMERAL_SECRET, base_url="https://sandbox.pamdas.org"):
    return {
        "action_id": action_id,
        "integration_state": {
            "type_value": "earth_ranger",
            "base_url": base_url,
            "configurations": [
                {"action_value": "auth", "data": {"token": token}},
                {"action_value": action_id, "data": {"tag_name": "elephant"}},
            ],
        },
    }


@pytest.mark.asyncio
async def test_ephemeral_run_builds_synthetic_integration(
        mocker, mock_gundi_client_v2, mock_config_manager,
        mock_publish_event, mock_ephemeral_action_handlers, mock_reference_action_handler,
):
    mocker.patch("app.services.action_runner.action_handlers", mock_ephemeral_action_handlers)
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)

    response = api_client.post("/v1/actions/execute/", json=_ephemeral_body())

    assert response.status_code == 200
    assert not mock_config_manager.get_integration_details.called
    assert not mock_config_manager.get_action_configuration.called
    assert mock_reference_action_handler.called
    call_kwargs = mock_reference_action_handler.call_args.kwargs
    integration = call_kwargs["integration"]
    assert integration.base_url == "https://sandbox.pamdas.org"
    auth_config = find_config_for_action(integration.configurations, "auth")
    assert auth_config is not None
    assert auth_config.data == {"token": _EPHEMERAL_SECRET}


@pytest.mark.parametrize("action_id", ["pull_observations", "push_observations", "generic_lookup"])
@pytest.mark.asyncio
async def test_ephemeral_run_rejects_non_reference_non_auth_action(
        action_id, mocker, mock_gundi_client_v2, mock_config_manager,
        mock_publish_event, mock_ephemeral_action_handlers,
):
    # Every non-safe action type must be rejected — parametrized so nobody
    # relaxes the guard for one type (e.g. Generic, the default fallback for
    # handlers without an explicit config annotation) without regressing tests.
    mocker.patch("app.services.action_runner.action_handlers", mock_ephemeral_action_handlers)
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)

    handler = mock_ephemeral_action_handlers[action_id][0]
    body = _ephemeral_body(action_id=action_id)
    response = api_client.post("/v1/actions/execute/", json=body)

    assert response.status_code == 422
    assert not handler.called


@pytest.mark.asyncio
async def test_ephemeral_run_allows_auth_action(
        mocker, mock_gundi_client_v2, mock_config_manager,
        mock_publish_event, mock_ephemeral_action_handlers, mock_auth_action_handler,
):
    # Auth actions verify credentials without side effects, so the ephemeral
    # path allows them — used by the portal's "Test Connection" button in the
    # creation wizard before an integration exists.
    mocker.patch("app.services.action_runner.action_handlers", mock_ephemeral_action_handlers)
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)

    response = api_client.post("/v1/actions/execute/", json=_ephemeral_body(action_id="auth"))

    assert response.status_code == 200
    assert mock_auth_action_handler.called
    # Handler receives the synthetic integration with auth already applied.
    integration = mock_auth_action_handler.call_args.kwargs["integration"]
    auth_config = find_config_for_action(integration.configurations, "auth")
    assert auth_config.data == {"token": _EPHEMERAL_SECRET}


@pytest.mark.parametrize("source_status", [401, 403, 500])
@pytest.mark.asyncio
async def test_ephemeral_handler_httpstatuserror_propagates_upstream_status(
        source_status, mocker, mock_gundi_client_v2, mock_config_manager,
        mock_publish_event, mock_reference_action_handler, mock_pull_observations_action_handler,
        mock_push_action_handler, mock_generic_action_handler,
):
    # httpx.HTTPStatusError from a handler forwards the source's status.
    # Parametrized so 401/403 (bad creds — portal classifies as invalid) and
    # 500 (source-side bug — portal classifies as error) both propagate
    # correctly. Ensures the check isn't 401-specific.
    import httpx

    async def rejecting_auth_handler(**kwargs):
        request = httpx.Request("GET", "https://source.example/status")
        response = httpx.Response(status_code=source_status, request=request, json={"detail": "source said no"})
        raise httpx.HTTPStatusError(f"{source_status}", request=request, response=response)

    handlers = {
        "list_species": (mock_reference_action_handler, _MockReferenceActionConfiguration, None),
        "pull_observations": (mock_pull_observations_action_handler, MockPullActionConfiguration, None),
        "auth": (rejecting_auth_handler, _MockAuthActionConfiguration, None),
        "push_observations": (mock_push_action_handler, MockPushActionConfiguration, None),
        "generic_lookup": (mock_generic_action_handler, _MockGenericActionConfiguration, None),
    }
    mocker.patch("app.services.action_runner.action_handlers", handlers)
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)

    response = api_client.post("/v1/actions/execute/", json=_ephemeral_body(action_id="auth"))

    assert response.status_code == source_status
    assert response.json() == {
        "detail": {"action_id": "auth", "error": _EXPECTED_SOURCE_STATUS_TEXT[source_status]},
    }
    assert not mock_publish_event.called


@pytest.mark.asyncio
async def test_ephemeral_handler_connect_error_falls_back_to_500(
        mocker, mock_gundi_client_v2, mock_config_manager,
        mock_publish_event, mock_reference_action_handler, mock_pull_observations_action_handler,
        mock_push_action_handler, mock_generic_action_handler,
):
    # Network-level failures don't carry a source status. Response must fall
    # back to 500 (via the else branch of the propagation logic), not accidentally
    # bubble up as some other status via .response access on None.
    import httpx

    async def unreachable_source_handler(**kwargs):
        raise httpx.ConnectError("source unreachable")

    handlers = {
        "list_species": (mock_reference_action_handler, _MockReferenceActionConfiguration, None),
        "pull_observations": (mock_pull_observations_action_handler, MockPullActionConfiguration, None),
        "auth": (unreachable_source_handler, _MockAuthActionConfiguration, None),
        "push_observations": (mock_push_action_handler, MockPushActionConfiguration, None),
        "generic_lookup": (mock_generic_action_handler, _MockGenericActionConfiguration, None),
    }
    mocker.patch("app.services.action_runner.action_handlers", handlers)
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)

    response = api_client.post("/v1/actions/execute/", json=_ephemeral_body(action_id="auth"))

    assert response.status_code == 500
    assert response.json() == {
        "detail": {"action_id": "auth", "error": "Could not reach the provider"},
    }
    assert not mock_publish_event.called


@pytest.mark.asyncio
async def test_ephemeral_handler_integration_error_propagates_status_code(
        mocker, mock_gundi_client_v2, mock_config_manager,
        mock_publish_event, mock_reference_action_handler, mock_pull_observations_action_handler,
        mock_push_action_handler, mock_generic_action_handler,
):
    # A handler that wraps the source failure as IntegrationAuthError (401)
    # instead of raising httpx directly must still propagate its status_code
    # so cdip's upstream_status matches the semantic verdict.
    from app.services.errors import IntegrationAuthError

    async def rejecting_auth_handler(**kwargs):
        raise IntegrationAuthError("bad creds", status_code=401)

    handlers = {
        "list_species": (mock_reference_action_handler, _MockReferenceActionConfiguration, None),
        "pull_observations": (mock_pull_observations_action_handler, MockPullActionConfiguration, None),
        "auth": (rejecting_auth_handler, _MockAuthActionConfiguration, None),
        "push_observations": (mock_push_action_handler, MockPushActionConfiguration, None),
        "generic_lookup": (mock_generic_action_handler, _MockGenericActionConfiguration, None),
    }
    mocker.patch("app.services.action_runner.action_handlers", handlers)
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)

    response = api_client.post("/v1/actions/execute/", json=_ephemeral_body(action_id="auth"))

    assert response.status_code == 401
    assert response.json() == {
        "detail": {"action_id": "auth", "error": "Authentication failed (HTTP 401)"},
    }
    assert not mock_publish_event.called


@pytest.mark.asyncio
async def test_ephemeral_auth_handler_write_is_blocked_end_to_end(
        mocker, mock_gundi_client_v2, mock_config_manager,
        mock_publish_event, mock_reference_action_handler, mock_pull_observations_action_handler,
        mock_push_action_handler, mock_generic_action_handler,
):
    # Composition test: even if a buggy auth handler tries to write to Gundi
    # from inside its own body, the write path's guard fires and the response
    # is sanitized to the standard {action_id, error} shape. This is the
    # defense-in-depth layer that keeps the "allow auth ephemeral" relaxation
    # safe — the config-model guard whitelists auth; the write-path guard makes
    # sure "auth" doesn't accidentally mean "arbitrary I/O against Gundi".
    from app.services.gundi import send_events_to_gundi, EphemeralWriteBlocked

    call_marker = {"called": False}
    async def buggy_auth_handler(**kwargs):
        call_marker["called"] = True
        await send_events_to_gundi(events=[], integration_id="x")
        return {"valid_credentials": True}

    handlers = {
        "list_species": (mock_reference_action_handler, _MockReferenceActionConfiguration, None),
        "pull_observations": (mock_pull_observations_action_handler, MockPullActionConfiguration, None),
        "auth": (buggy_auth_handler, _MockAuthActionConfiguration, None),
        "push_observations": (mock_push_action_handler, MockPushActionConfiguration, None),
        "generic_lookup": (mock_generic_action_handler, _MockGenericActionConfiguration, None),
    }
    mocker.patch("app.services.action_runner.action_handlers", handlers)
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)

    response = api_client.post("/v1/actions/execute/", json=_ephemeral_body(action_id="auth"))

    assert response.status_code == 500
    # Sanitized error shape — no configurations, no request/response bodies,
    # only the exception type name.
    assert response.json() == {
        "detail": {"action_id": "auth", "error": EphemeralWriteBlocked.__name__},
    }
    assert call_marker["called"], "buggy auth handler must have been reached"
    assert not mock_publish_event.called


@pytest.mark.asyncio
async def test_ephemeral_auth_publishes_no_activity_events(
        mocker, mock_gundi_client_v2, mock_config_manager,
        mock_publish_event, mock_ephemeral_action_handlers,
):
    # The reference-action version of this invariant already exists; auth is
    # covered separately because it's the newly-allowed shape and the guard
    # relaxation must not regress the "no audit trail on ephemeral" contract.
    mocker.patch("app.services.action_runner.action_handlers", mock_ephemeral_action_handlers)
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)

    response = api_client.post("/v1/actions/execute/", json=_ephemeral_body(action_id="auth"))

    assert response.status_code == 200
    assert not mock_publish_event.called


@pytest.mark.asyncio
async def test_ephemeral_run_publishes_no_activity_events(
        mocker, mock_gundi_client_v2, mock_config_manager,
        mock_publish_event, mock_ephemeral_action_handlers,
):
    mocker.patch("app.services.action_runner.action_handlers", mock_ephemeral_action_handlers)
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)

    response = api_client.post("/v1/actions/execute/", json=_ephemeral_body())

    assert response.status_code == 200
    assert not mock_publish_event.called


@pytest.mark.asyncio
async def test_ephemeral_run_error_payload_scrubs_credentials(
        mocker, mock_gundi_client_v2, mock_config_manager,
        mock_publish_event, mock_ephemeral_action_handlers, mock_reference_action_handler,
):
    mock_reference_action_handler.side_effect = RuntimeError("upstream unreachable")
    mocker.patch("app.services.action_runner.action_handlers", mock_ephemeral_action_handlers)
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)

    response = api_client.post("/v1/actions/execute/", json=_ephemeral_body())

    assert response.status_code == 500
    body = response.text
    assert _EPHEMERAL_SECRET not in body
    # Sanitized: only exception type + action_id, no traceback / request / response.
    detail = response.json()["detail"]
    assert detail == {"action_id": "list_species", "error": "RuntimeError"}
    assert not mock_publish_event.called


@pytest.mark.asyncio
async def test_ephemeral_run_error_does_not_leak_request_or_response_bodies(
        mocker, mock_gundi_client_v2, mock_config_manager,
        mock_publish_event, mock_ephemeral_action_handlers, mock_reference_action_handler,
):
    # A realistic upstream failure carries our outgoing request (which may
    # embed auth headers/tokens) and the source's response body. Neither must
    # appear in the response returned to the portal.
    import httpx
    outgoing_body = f'{{"authorization": "Bearer {_EPHEMERAL_SECRET}"}}'
    source_body = f'{{"error": "invalid credential {_EPHEMERAL_SECRET}"}}'
    request = httpx.Request("POST", "https://sandbox.pamdas.org/events/", content=outgoing_body)
    response = httpx.Response(401, text=source_body, request=request)
    mock_reference_action_handler.side_effect = httpx.HTTPStatusError(
        "401 Unauthorized", request=request, response=response,
    )
    mocker.patch("app.services.action_runner.action_handlers", mock_ephemeral_action_handlers)
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)

    resp = api_client.post("/v1/actions/execute/", json=_ephemeral_body())

    # Runner propagates HTTPStatusError.response.status_code so cdip's
    # upstream_status matches what the source returned (401 here).
    assert resp.status_code == 401
    body = resp.text
    assert _EPHEMERAL_SECRET not in body
    assert "authorization" not in body.lower()
    assert "Traceback" not in body
    assert not mock_publish_event.called


@pytest.mark.asyncio
async def test_ephemeral_run_rejects_background_execution(
        mocker, mock_gundi_client_v2, mock_config_manager,
        mock_publish_event, mock_ephemeral_action_handlers, mock_reference_action_handler,
):
    mocker.patch("app.services.action_runner.action_handlers", mock_ephemeral_action_handlers)
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)

    body = _ephemeral_body()
    body["run_in_background"] = True
    response = api_client.post("/v1/actions/execute/", json=body)

    assert response.status_code == 422
    # Router 422s now match the runner-side shape: {"detail": {"action_id", "error"}}.
    assert "background" in response.json()["detail"]["error"].lower()
    assert not mock_reference_action_handler.called


@pytest.mark.asyncio
async def test_request_with_both_integration_id_and_state_is_422(
        mocker, mock_gundi_client_v2, mock_config_manager,
        mock_publish_event, mock_ephemeral_action_handlers, mock_reference_action_handler,
):
    mocker.patch("app.services.action_runner.action_handlers", mock_ephemeral_action_handlers)
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)

    body = _ephemeral_body()
    body["integration_id"] = "779ff3ab-5589-4f4c-9e0a-ae8d6c9edff0"
    response = api_client.post("/v1/actions/execute/", json=body)

    assert response.status_code == 422
    assert not mock_reference_action_handler.called
    # Request-shape errors must not publish a bogus IntegrationActionFailed event
    # (would leak a phantom activity-log entry against a real integration_id).
    assert not _published_events_of_type(mock_publish_event, IntegrationActionFailed)


@pytest.mark.asyncio
async def test_execute_action_neither_id_nor_state_direct_call_logs_but_does_not_publish(
        mocker, mock_publish_event, caplog,
):
    # Bypass the router: main.py's PubSub route calls execute_action directly
    # with whatever the message carried, so a command with no integration_id
    # reaches this branch. It must not publish a phantom activity event, but
    # it must leave a server-log trace (before #98 it did both).
    import logging
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    caplog.set_level(logging.ERROR, logger="app.services.action_runner")

    response = await execute_action(
        integration_id=None,
        integration_state=None,
        action_id="list_species",
    )

    assert response.status_code == 422
    assert not mock_publish_event.called
    body = response.body.decode()
    assert "Provide either integration_id or integration_state." in body
    assert any("integration_id" in r.getMessage() and r.levelno == logging.ERROR for r in caplog.records)


@pytest.mark.asyncio
async def test_ephemeral_run_returns_handler_result(
        mocker, mock_gundi_client_v2, mock_config_manager,
        mock_publish_event, mock_ephemeral_action_handlers, mock_reference_action_handler,
):
    mocker.patch("app.services.action_runner.action_handlers", mock_ephemeral_action_handlers)
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)

    response = api_client.post("/v1/actions/execute/", json=_ephemeral_body())

    assert response.status_code == 200
    assert response.json() == {"options": [{"value": "elephant", "label": "Elephant"}]}


@pytest.mark.asyncio
async def test_request_without_integration_id_or_state_is_422(
        mocker, mock_gundi_client_v2, mock_config_manager,
        mock_publish_event, mock_ephemeral_action_handlers,
):
    mocker.patch("app.services.action_runner.action_handlers", mock_ephemeral_action_handlers)
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)

    response = api_client.post(
        "/v1/actions/execute/",
        json={"action_id": "list_species"},
    )

    assert response.status_code == 422
    # Validation errors must not publish a bogus IntegrationActionFailed event.
    assert not _published_events_of_type(mock_publish_event, IntegrationActionFailed)


@pytest.mark.asyncio
async def test_ephemeral_run_succeeds_for_parameter_less_reference_action(
        mocker, mock_gundi_client_v2, mock_config_manager,
        mock_publish_event, mock_ephemeral_action_handlers, mock_reference_action_handler,
):
    # The real portal wizard forwards `configurations` only for the sections
    # the user edited (auth). A parameter-less reference action like ER's
    # list_event_types has no config entry — the 404 branch used to fire
    # here and cdip wrapped it as a 502.
    mocker.patch("app.services.action_runner.action_handlers", mock_ephemeral_action_handlers)
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    # Body mirrors the actual portal: only the edited section (auth), no
    # config row for the reference action itself, empty config_overrides.
    body = {
        "action_id": "list_species",
        "integration_state": {
            "type_value": "earth_ranger",
            "base_url": "https://sandbox.pamdas.org",
            "configurations": [
                {"action_value": "auth", "data": {"token": _EPHEMERAL_SECRET}},
            ],
        },
    }

    response = api_client.post("/v1/actions/execute/", json=body)

    assert response.status_code == 200, response.text
    assert mock_reference_action_handler.called


@pytest.mark.asyncio
async def test_ephemeral_run_uses_unique_integration_id_per_run(
        mocker, mock_gundi_client_v2, mock_config_manager,
        mock_publish_event, mock_ephemeral_action_handlers, mock_reference_action_handler,
):
    # Every ephemeral run must get a fresh synthetic integration id so
    # concurrent runs by different users can't collide on any
    # IntegrationStateManager keys downstream handlers might set under
    # `integration.id`.
    mocker.patch("app.services.action_runner.action_handlers", mock_ephemeral_action_handlers)
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)

    api_client.post("/v1/actions/execute/", json=_ephemeral_body())
    api_client.post("/v1/actions/execute/", json=_ephemeral_body())

    assert mock_reference_action_handler.call_count == 2
    seen_ids = {
        str(call.kwargs["integration"].id)
        for call in mock_reference_action_handler.mock_calls
    }
    assert len(seen_ids) == 2, f"expected two distinct ids, got {seen_ids}"
    assert "00000000-0000-0000-0000-000000000000" not in seen_ids


@pytest.mark.asyncio
async def test_ephemeral_contextvar_or_folds_with_outer_state(mocker, mock_publish_event):
    # If ephemeral_run is already True when execute_action starts (a nested
    # execute inside a reference handler), a saved-integration inner call
    # (is_ephemeral=False for the inner) must NOT re-enable publishing.
    #
    # Old form of this test only asserted "ephemeral_run.get() is True after
    # the call returns", which passes trivially because the finally-reset
    # restores the outer value regardless of the OR-fold. The real invariant
    # is that publish_event stays suppressed inside — assert THAT so a
    # regression from `is_ephemeral or ephemeral_run.get()` to just
    # `is_ephemeral` fails the test loudly.
    from app.services.activity_logger import ephemeral_run
    mock_config_manager = mocker.MagicMock()
    async def _fail(*a, **kw):
        raise RuntimeError("config manager exploded — inner should still be ephemeral")
    mock_config_manager.get_integration_details = _fail
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)

    outer_token = ephemeral_run.set(True)
    try:
        # Inner call: saved integration_id (is_ephemeral=False for the inner).
        # The OR-fold with the outer True must keep the contextvar True so
        # _handle_error's ephemeral branch fires and no IntegrationActionFailed
        # event is published.
        try:
            await execute_action(integration_id="00000000-0000-0000-0000-000000000000", action_id="anything")
        except Exception:
            pass
        # Contract: no PubSub publish under an ephemeral outer context —
        # regardless of the inner call's own is_ephemeral flag.
        assert not mock_publish_event.called, (
            "publish_event fired despite the outer ephemeral_run being True — "
            "the OR-fold was regressed"
        )
        # Outer context restored on return (unchanged from earlier version).
        assert ephemeral_run.get() is True
    finally:
        ephemeral_run.reset(outer_token)


@pytest.mark.asyncio
async def test_saved_integration_reference_action_with_no_config_and_no_overrides_is_not_404(
        mocker, mock_gundi_client_v2, mock_config_manager, mock_publish_event,
        mock_ephemeral_action_handlers, mock_reference_action_handler, integration_v2,
):
    # Same rule the earthranger and inaturalist forks already ship: a
    # reference action is stateless, so "no stored config row and no
    # overrides" is a complete zero-param query on a *saved* integration too,
    # not a missing-configuration 404. config_model.parse_obj({}) below still
    # 422s when params are actually required.
    mock_config_manager.get_action_configuration = AsyncMock(return_value=None)
    mocker.patch("app.services.action_runner.action_handlers", mock_ephemeral_action_handlers)
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)

    response = api_client.post(
        "/v1/actions/execute/",
        json={"integration_id": str(integration_v2.id), "action_id": "list_species"},
    )

    assert response.status_code == 200, response.text
    assert mock_reference_action_handler.called
    assert mock_reference_action_handler.call_args.kwargs["action_config"].tag_name == ""
    # A stateless action has no row to look up, and a redis miss in
    # get_action_configuration reloads the integration from the portal, so
    # looking it up on every dropdown open would be a portal call each time.
    assert not mock_config_manager.get_action_configuration.called


@pytest.mark.asyncio
async def test_saved_integration_generic_action_with_no_config_and_no_overrides_still_404s(
        mocker, mock_gundi_client_v2, mock_config_manager, mock_publish_event,
        mock_ephemeral_action_handlers, mock_generic_action_handler, integration_v2,
):
    # Pin: relaxing the missing-config 404 for reference actions must not
    # loosen it for any other non-pull action type.
    mock_config_manager.get_action_configuration = AsyncMock(return_value=None)
    mocker.patch("app.services.action_runner.action_handlers", mock_ephemeral_action_handlers)
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)

    response = api_client.post(
        "/v1/actions/execute/",
        json={"integration_id": str(integration_v2.id), "action_id": "generic_lookup"},
    )

    assert response.status_code == 404
    assert not mock_generic_action_handler.called


@pytest.mark.asyncio
async def test_saved_integration_reference_action_error_never_carries_stored_configurations(
        mocker, mock_gundi_client_v2, mock_config_manager, mock_publish_event,
        mock_ephemeral_action_handlers, mock_reference_action_handler, integration_v2,
):
    # Reference actions run at dropdown-open frequency, so a handler failure is
    # routine, not exceptional. Like every ephemeral error, it must not carry
    # the saved integration's configurations (raw auth secrets) into the JSON
    # error response or the published IntegrationActionFailed event. Same rule
    # the earthranger and inaturalist forks already ship.
    mock_reference_action_handler.side_effect = RuntimeError("upstream unreachable")
    mock_config_manager.get_action_configuration = AsyncMock(return_value=None)
    mocker.patch("app.services.action_runner.action_handlers", mock_ephemeral_action_handlers)
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)

    response = api_client.post(
        "/v1/actions/execute/",
        json={"integration_id": str(integration_v2.id), "action_id": "list_species"},
    )

    assert response.status_code == 500
    stored_token = find_config_for_action(integration_v2.configurations, "auth").data["token"]
    assert stored_token not in response.text
    # _handle_error normalizes a None config_data to {}; the invariant is that
    # no configuration (and no secret) reaches the response or the event.
    assert not response.json()["detail"]["config_data"]
    # Whether a failing reference action publishes an activity event at all is
    # not this test's concern; whatever is published must be redacted.
    for event in _published_events_of_type(mock_publish_event, IntegrationActionFailed):
        assert not event.payload.config_data
        assert stored_token not in event.json()


def _patch_ephemeral_runner(mocker, handlers, mock_gundi_client_v2, mock_config_manager, mock_publish_event):
    mocker.patch("app.services.action_runner.action_handlers", handlers)
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)


@pytest.mark.asyncio
async def test_ephemeral_whitelist_rejection_explains_itself(
        mocker, mock_gundi_client_v2, mock_config_manager, mock_publish_event, mock_ephemeral_action_handlers,
):
    # Runner-authored errors carry their own text: the runner built the
    # message, so it cannot contain draft credentials. A bare "ValueError"
    # is what the portal used to render in the toast.
    _patch_ephemeral_runner(mocker, mock_ephemeral_action_handlers, mock_gundi_client_v2, mock_config_manager, mock_publish_event)

    response = api_client.post("/v1/actions/execute/", json=_ephemeral_body(action_id="pull_observations"))

    assert response.status_code == 422
    assert "only reference and auth actions are supported" in response.json()["detail"]["error"]


@pytest.mark.asyncio
async def test_ephemeral_unknown_action_explains_itself(
        mocker, mock_gundi_client_v2, mock_config_manager, mock_publish_event, mock_ephemeral_action_handlers,
):
    _patch_ephemeral_runner(mocker, mock_ephemeral_action_handlers, mock_gundi_client_v2, mock_config_manager, mock_publish_event)

    response = api_client.post("/v1/actions/execute/", json=_ephemeral_body(action_id="list_nothing"))

    assert response.status_code == 422
    assert "Action 'list_nothing' is not supported" in response.json()["detail"]["error"]


@pytest.mark.asyncio
async def test_ephemeral_missing_required_param_names_the_field(
        mocker, mock_gundi_client_v2, mock_config_manager, mock_publish_event, mock_ephemeral_action_handlers,
):
    # pydantic 1.x ValidationError.errors() carries loc/msg/type and never the
    # offending input, so naming the field is safe and is what the wizard
    # needs to tell the user what to supply.
    _patch_ephemeral_runner(mocker, mock_ephemeral_action_handlers, mock_gundi_client_v2, mock_config_manager, mock_publish_event)
    body = {
        "action_id": "list_event_fields",
        "config_overrides": {},
        "integration_state": {
            "type_value": "earth_ranger",
            "base_url": "https://sandbox.pamdas.org",
            "configurations": [{"action_value": "auth", "data": {"token": _EPHEMERAL_SECRET}}],
        },
    }

    response = api_client.post("/v1/actions/execute/", json=body)

    assert response.status_code == 422
    error = response.json()["detail"]["error"]
    assert "event_type" in error and "field required" in error
    assert _EPHEMERAL_SECRET not in response.text


def _auth_handlers_raising(exc, mock_reference_action_handler, mock_pull_observations_action_handler,
                           mock_push_action_handler, mock_generic_action_handler):
    async def failing_auth_handler(**kwargs):
        raise exc
    return {
        "list_species": (mock_reference_action_handler, _MockReferenceActionConfiguration, None),
        "pull_observations": (mock_pull_observations_action_handler, MockPullActionConfiguration, None),
        "auth": (failing_auth_handler, _MockAuthActionConfiguration, None),
        "push_observations": (mock_push_action_handler, MockPushActionConfiguration, None),
        "generic_lookup": (mock_generic_action_handler, _MockGenericActionConfiguration, None),
    }


@pytest.mark.asyncio
async def test_ephemeral_auth_error_without_status_code_is_401(
        mocker, mock_gundi_client_v2, mock_config_manager, mock_publish_event,
        mock_reference_action_handler, mock_pull_observations_action_handler,
        mock_push_action_handler, mock_generic_action_handler,
):
    # The idiomatic `raise IntegrationAuthError("Invalid API key")` sets no
    # status_code. On the Test Connection path that must still read as bad
    # credentials (401), not as an internal error (500).
    from app.services.errors import IntegrationAuthError
    handlers = _auth_handlers_raising(
        IntegrationAuthError("Invalid API key"), mock_reference_action_handler,
        mock_pull_observations_action_handler, mock_push_action_handler, mock_generic_action_handler,
    )
    _patch_ephemeral_runner(mocker, handlers, mock_gundi_client_v2, mock_config_manager, mock_publish_event)

    response = api_client.post("/v1/actions/execute/", json=_ephemeral_body(action_id="auth"))

    assert response.status_code == 401
    assert response.json()["detail"]["error"] == "Authentication failed"


@pytest.mark.asyncio
async def test_ephemeral_aiohttp_response_error_propagates_status(
        mocker, mock_gundi_client_v2, mock_config_manager, mock_publish_event,
        mock_reference_action_handler, mock_pull_observations_action_handler,
        mock_push_action_handler, mock_generic_action_handler,
):
    # aiohttp carries the source status on `.status`, not `.response.status_code`.
    import aiohttp
    from multidict import CIMultiDict, CIMultiDictProxy
    from yarl import URL
    request_info = aiohttp.RequestInfo(
        url=URL("https://source.example/me"), method="GET",
        headers=CIMultiDictProxy(CIMultiDict()), real_url=URL("https://source.example/me"),
    )
    exc = aiohttp.ClientResponseError(request_info, (), status=403, message="Forbidden")
    handlers = _auth_handlers_raising(
        exc, mock_reference_action_handler, mock_pull_observations_action_handler,
        mock_push_action_handler, mock_generic_action_handler,
    )
    _patch_ephemeral_runner(mocker, handlers, mock_gundi_client_v2, mock_config_manager, mock_publish_event)

    response = api_client.post("/v1/actions/execute/", json=_ephemeral_body(action_id="auth"))

    assert response.status_code == 403
    # Same curated text an httpx 403 gets; a bare "ClientResponseError" is
    # what the shared classifier produced before it learned aiohttp's .status.
    assert response.json()["detail"]["error"] == "Authentication failed (HTTP 403)"
    assert not mock_publish_event.called


@pytest.mark.asyncio
async def test_ephemeral_source_redirect_is_not_forwarded_as_runner_status(
        mocker, mock_gundi_client_v2, mock_config_manager, mock_publish_event,
        mock_reference_action_handler, mock_pull_observations_action_handler,
        mock_push_action_handler, mock_generic_action_handler,
):
    # raise_for_status raises on 3xx when redirects are off. Answering 301
    # with a JSON body and no Location header is not a response the portal
    # can act on; only source statuses >= 400 are forwarded.
    import httpx
    request = httpx.Request("GET", "http://source.example/me")
    response = httpx.Response(301, request=request, headers={"location": "https://source.example/me"})
    exc = httpx.HTTPStatusError("301 Moved Permanently", request=request, response=response)
    handlers = _auth_handlers_raising(
        exc, mock_reference_action_handler, mock_pull_observations_action_handler,
        mock_push_action_handler, mock_generic_action_handler,
    )
    _patch_ephemeral_runner(mocker, handlers, mock_gundi_client_v2, mock_config_manager, mock_publish_event)

    resp = api_client.post("/v1/actions/execute/", json=_ephemeral_body(action_id="auth"))

    assert resp.status_code == 500
    assert "location" not in {k.lower() for k in resp.headers}


@pytest.mark.asyncio
async def test_trigger_action_is_blocked_on_ephemeral_run(mocker, mock_publish_event):
    # The last unguarded outbound path from the #87 thread. Publishing a
    # RunIntegrationAction against a synthetic id would either vanish
    # silently (async) or run a saved-integration lookup that fails (sync);
    # raising is the only honest answer to a reference/auth handler.
    from app.services.activity_logger import ephemeral_run
    from app.services.gundi import EphemeralWriteBlocked
    mocker.patch("app.services.action_scheduler.publish_event", mock_publish_event)

    token = ephemeral_run.set(True)
    try:
        with pytest.raises(EphemeralWriteBlocked):
            await trigger_action(integration_id="synthetic-uuid", action_id="pull_observations")
    finally:
        ephemeral_run.reset(token)

    assert not mock_publish_event.called


def test_validation_error_log_does_not_carry_the_request_body(caplog):
    # Copilot on #98: the DEBUG log of a failed body validation wrote the
    # whole body, draft credentials included. Log access is usually broader
    # than access to the originating request, so only the sanitized errors
    # may be logged.
    import logging
    caplog.set_level(logging.DEBUG, logger="app.main")
    body = {
        "action_id": "auth",
        "integration_state": {
            "type_value": "earth_ranger",
            # `data` missing -> RequestValidationError; the token rides along
            # in the body that used to be logged verbatim.
            "configurations": [{"action_value": "auth", "token": _EPHEMERAL_SECRET}],
        },
    }

    response = api_client.post("/v1/actions/execute/", json=body)

    assert response.status_code == 422
    assert _EPHEMERAL_SECRET not in response.text
    assert _EPHEMERAL_SECRET not in caplog.text


@pytest.mark.asyncio
async def test_nested_saved_integration_call_under_ephemeral_context_keeps_the_whitelist(
        mocker, mock_gundi_client_v2, mock_config_manager, mock_publish_event,
        mock_ephemeral_action_handlers, mock_push_action_handler, integration_v2,
):
    # A reference/auth handler that calls execute_action(integration_id=...)
    # for a push action gets a nested run whose own is_ephemeral is False.
    # The read-only whitelist must key on the effective (OR-folded) context,
    # or the nested push runs against a saved integration with no activity
    # log and only the Gundi helpers standing in the way.
    from app.services.activity_logger import ephemeral_run
    _patch_ephemeral_runner(mocker, mock_ephemeral_action_handlers, mock_gundi_client_v2, mock_config_manager, mock_publish_event)

    token = ephemeral_run.set(True)
    try:
        response = await execute_action(
            integration_id=str(integration_v2.id), action_id="push_observations",
        )
    finally:
        ephemeral_run.reset(token)

    assert response.status_code == 422
    assert "only reference and auth actions are supported" in response.body.decode()
    assert not mock_push_action_handler.called
    assert not mock_publish_event.called


@pytest.mark.asyncio
async def test_ephemeral_validation_error_from_custom_validator_does_not_echo_the_value(
        mocker, mock_gundi_client_v2, mock_config_manager, mock_publish_event, mock_ephemeral_action_handlers,
):
    # pydantic's own messages ("field required", "value is not a valid
    # integer") never contain the input, but a connector validator's
    # ValueError text is arbitrary. Keep the field location; replace the
    # message unless it is one of pydantic's built-in (dotted-type) errors.
    _patch_ephemeral_runner(mocker, mock_ephemeral_action_handlers, mock_gundi_client_v2, mock_config_manager, mock_publish_event)
    body = {
        "action_id": "auth_echoing",
        "integration_state": {
            "type_value": "earth_ranger",
            "base_url": "https://sandbox.pamdas.org",
            "configurations": [{"action_value": "auth_echoing", "data": {"token": _EPHEMERAL_SECRET}}],
        },
    }

    response = api_client.post("/v1/actions/execute/", json=body)

    assert response.status_code == 422
    error = response.json()["detail"]["error"]
    assert _EPHEMERAL_SECRET not in response.text
    assert "token" in error and "invalid value" in error


@pytest.mark.asyncio
async def test_ephemeral_handler_error_is_not_logged_verbatim(
        mocker, mock_gundi_client_v2, mock_config_manager, mock_publish_event,
        mock_ephemeral_action_handlers, mock_reference_action_handler, caplog,
):
    # The same untrusted exception text the response scrubs (our outgoing
    # request with its auth header, the source's response body) must not
    # reach the application log either; logs outlive and outreach the request.
    import httpx
    import logging
    outgoing_body = f'{{"authorization": "Bearer {_EPHEMERAL_SECRET}"}}'
    source_body = f'{{"error": "invalid credential {_EPHEMERAL_SECRET}"}}'
    request = httpx.Request("POST", f"https://sandbox.pamdas.org/events/?token={_EPHEMERAL_SECRET}", content=outgoing_body)
    response = httpx.Response(401, text=source_body, request=request)
    mock_reference_action_handler.side_effect = httpx.HTTPStatusError(
        f"401 Unauthorized for url with {_EPHEMERAL_SECRET}", request=request, response=response,
    )
    _patch_ephemeral_runner(mocker, mock_ephemeral_action_handlers, mock_gundi_client_v2, mock_config_manager, mock_publish_event)
    caplog.set_level(logging.DEBUG, logger="app.services.action_runner")

    resp = api_client.post("/v1/actions/execute/", json=_ephemeral_body())

    assert resp.status_code == 401
    assert _EPHEMERAL_SECRET not in caplog.text
    # Still enough to debug: the action, the exception type, and the frames.
    assert "list_species" in caplog.text and "HTTPStatusError" in caplog.text


@pytest.mark.asyncio
async def test_push_data_acks_message_without_destination_id(
        mocker, pubsub_message_request_headers, run_push_action_pubsub_payload
):
    mock_execute_action = mocker.patch("app.main.execute_action")
    payload = json.loads(json.dumps(run_push_action_pubsub_payload))
    payload["message"]["attributes"].pop("destination_id", None)

    response = api_client.post(
        "/push-data",
        headers=pubsub_message_request_headers,
        json=payload,
    )

    # Malformed messages are acked (2xx) so PubSub doesn't redeliver them forever,
    # and the action runner is never invoked.
    assert response.status_code == 200
    assert response.json() == {}
    assert not mock_execute_action.called


@pytest.mark.asyncio
async def test_execute_action_reports_classified_auth_error_with_clean_text(
        mocker, mock_gundi_client_v2, integration_v2, mock_config_manager,
        mock_publish_event, mock_action_handlers,
):
    mock_handler, _, _ = mock_action_handlers["pull_observations"]
    mock_handler.side_effect = IntegrationAuthError("TrackIt rejected the credentials", status_code=401)
    mocker.patch("app.services.action_runner.action_handlers", mock_action_handlers)
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)

    response = await execute_action(
        integration_id=str(integration_v2.id),
        action_id="pull_observations",
    )

    expected_text = "Authentication failed — TrackIt rejected the credentials (HTTP 401)"
    failed_events = _published_events_of_type(mock_publish_event, IntegrationActionFailed)
    assert len(failed_events) >= 1
    for event in failed_events:
        assert event.payload.error == expected_text
    error_details = json.loads(response.body)["detail"]
    assert error_details["error"] == expected_text
    assert error_details["error_type"] == "auth"


@pytest.mark.asyncio
async def test_execute_action_keeps_generic_format_for_unclassified_errors(
        mocker, mock_gundi_client_v2, integration_v2, mock_config_manager,
        mock_publish_event, mock_action_handlers,
):
    mock_handler, _, _ = mock_action_handlers["pull_observations"]
    mock_handler.side_effect = ValueError("something unexpected")
    mocker.patch("app.services.action_runner.action_handlers", mock_action_handlers)
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)

    response = await execute_action(
        integration_id=str(integration_v2.id),
        action_id="pull_observations",
    )

    error_details = json.loads(response.body)["detail"]
    assert error_details["error"] == (
        f"Error in action 'pull_observations' for integration '{str(integration_v2.id)}': "
        f"ValueError: something unexpected"
    )
    assert error_details["error_type"] is None


@pytest.mark.asyncio
async def test_execute_action_keeps_generic_format_for_integration_details_failure(
        mocker, mock_gundi_client_v2, integration_v2, mock_config_manager,
        mock_publish_event, mock_action_handlers,
):
    # Heuristic classification must NOT apply to failures fetching the
    # integration details from the Gundi portal itself — a portal
    # connectivity/auth problem must not be misreported as a third-party
    # provider failure ("Could not reach the provider").
    # request= is set to avoid tripping httpx's own "request not set" RuntimeError
    # when _handle_error's getattr(exc, "request", None) touches the property below —
    # unrelated to what this test is verifying.
    mock_config_manager.get_integration_details.side_effect = httpx.ConnectError(
        "connection failed", request=httpx.Request("GET", "https://example.com")
    )
    mocker.patch("app.services.action_runner.action_handlers", mock_action_handlers)
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)

    response = await execute_action(
        integration_id=str(integration_v2.id),
        action_id="pull_observations",
    )

    error_details = json.loads(response.body)["detail"]
    assert error_details["error"].startswith("Error in action")
    assert error_details["error_type"] is None


@pytest.mark.asyncio
async def test_execute_action_handles_httpx_error_carrying_no_request(
        mocker, mock_gundi_client_v2, integration_v2, mock_config_manager,
        mock_publish_event, mock_action_handlers,
):
    # httpx exceptions expose .request as a property that raises RuntimeError
    # when constructed without one; _handle_error must not propagate that.
    mock_handler, _, _ = mock_action_handlers["pull_observations"]
    mock_handler.side_effect = httpx.ConnectError("connection failed")
    mocker.patch("app.services.action_runner.action_handlers", mock_action_handlers)
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)

    response = await execute_action(
        integration_id=str(integration_v2.id),
        action_id="pull_observations",
    )

    error_details = json.loads(response.body)["detail"]
    assert error_details["error"] == "Could not reach the provider — connection failed"
    assert error_details["error_type"] == "connectivity"
