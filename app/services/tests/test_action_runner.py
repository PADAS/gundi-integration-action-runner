import base64
import json

import pytest
from fastapi.testclient import TestClient
from fastapi import status
from gundi_core.commands import RunIntegrationAction
from gundi_core.events import IntegrationActionFailed

from app import settings
from app.conftest import MockSubActionConfiguration
from app.main import app
from app.services.action_scheduler import trigger_action

api_client = TestClient(app)


@pytest.mark.asyncio
async def test_execute_action_from_pubsub(
        mocker, mock_gundi_client_v2, mock_publish_event, mock_action_handlers, mock_config_manager,
        pubsub_message_request_headers, event_v2_pubsub_payload
):
    mocker.patch("app.services.action_runner.action_handlers", mock_action_handlers)
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)

    response = api_client.post(
        "/",
        headers=pubsub_message_request_headers,
        json=event_v2_pubsub_payload,
    )

    assert response.status_code == 200
    assert not mock_gundi_client_v2.get_integration_details.called
    payload = event_v2_pubsub_payload["message"]["data"]
    payload_dict = json.loads(base64.b64decode(payload).decode("utf-8"))
    integration_id = payload_dict.get("integration_id")
    action_id = payload_dict.get("action_id")
    assert mock_config_manager.get_integration_details.called
    mock_config_manager.get_integration_details.assert_called_with(integration_id)
    mock_action_handler, mock_config = mock_action_handlers[action_id]
    assert mock_action_handler.called


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
    mock_action_handler, mock_config = mock_action_handlers[action_id]
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
    mock_action_handler, mock_config = mock_action_handlers["pull_observations"]
    assert mock_action_handler.called
    for k, v in config_overrides.items():
        config = mock_action_handler.call_args.kwargs["action_config"]
        assert getattr(config, k) == v


@pytest.mark.asyncio
async def test_execute_action_from_pubsub_with_config_overrides(
        mocker, mock_gundi_client_v2, mock_publish_event, mock_action_handlers, mock_config_manager,
        pubsub_message_request_headers, event_v2_pubsub_payload_with_config_overrides
):
    mocker.patch("app.services.action_runner.action_handlers", mock_action_handlers)
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)

    response = api_client.post(
        "/",
        headers=pubsub_message_request_headers,
        json=event_v2_pubsub_payload_with_config_overrides,
    )

    assert response.status_code == 200
    assert mock_config_manager.get_integration_details.called
    assert not mock_gundi_client_v2.get_integration_details.called
    mock_action_handler, mock_config = mock_action_handlers["pull_observations"]
    assert mock_action_handler.called
    encoded_data = event_v2_pubsub_payload_with_config_overrides["message"]["data"]
    decoded_data = base64.b64decode(encoded_data).decode("utf-8")
    config_overrides = json.loads(decoded_data)["config_overrides"]
    for k, v in config_overrides.items():
        config = mock_action_handler.call_args.kwargs["action_config"]
        assert getattr(config, k) == v


@pytest.mark.asyncio
async def test_execute_action_from_api_with_invalid_config(
        mocker, mock_gundi_client_v2, integration_v2, mock_config_manager,
        mock_publish_event, mock_action_handlers,
):
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
    mock_action_handler, mock_config = mock_action_handlers[action_id]
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
    mock_action_handler, mock_config = mock_action_handlers[action_id]
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
    mock_handler, _ = mock_action_handlers_with_request_errors["pull_observations"]
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

