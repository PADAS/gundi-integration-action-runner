import base64
import json

import pytest
from fastapi.testclient import TestClient
from fastapi import status
from gundi_core.commands import RunIntegrationAction
from gundi_core.events import IntegrationActionFailed, IntegrationActionCustomLog, LogLevel
from gundi_core.events.transformers import ObservationTransformedER

from app import settings
from app.conftest import MockSubActionConfiguration, MockPushActionConfiguration, async_return
from app.main import app
from app.services.action_scheduler import trigger_action

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

