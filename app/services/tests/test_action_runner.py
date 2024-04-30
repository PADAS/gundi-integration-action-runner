import base64
import json

import pytest
from fastapi.testclient import TestClient
from app.main import app


api_client = TestClient(app)


@pytest.mark.asyncio
async def test_execute_action_from_pubsub(
        mocker, mock_gundi_client_v2, mock_publish_event, mock_action_handlers,
        event_v2_cloud_event_headers, event_v2_cloud_event_payload
):
    mocker.patch("app.services.action_runner.action_handlers", mock_action_handlers)
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)

    response = api_client.post(
        "/",
        headers=event_v2_cloud_event_headers,
        json=event_v2_cloud_event_payload,
    )

    assert response.status_code == 200
    assert mock_gundi_client_v2.get_integration_details.called
    mock_action_handler, mock_config = mock_action_handlers["pull_observations"]
    assert mock_action_handler.called


@pytest.mark.asyncio
async def test_execute_action_from_api(
        mocker, mock_gundi_client_v2, integration_v2,
        mock_publish_event, mock_action_handlers,
):
    mocker.patch("app.services.action_runner.action_handlers", mock_action_handlers)
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)

    response = api_client.post(
        "/v1/actions/execute/",
        json={
            "integration_id": str(integration_v2.id),
            "action_id": "pull_observations"
        }
    )

    assert response.status_code == 200
    assert mock_gundi_client_v2.get_integration_details.called
    mock_action_handler, mock_config = mock_action_handlers["pull_observations"]
    assert mock_action_handler.called


@pytest.mark.asyncio
async def test_execute_action_from_api_with_config_overrides(
        mocker, mock_gundi_client_v2, integration_v2,
        mock_publish_event, mock_action_handlers,
):
    mocker.patch("app.services.action_runner.action_handlers", mock_action_handlers)
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
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
    assert mock_gundi_client_v2.get_integration_details.called
    mock_action_handler, mock_config = mock_action_handlers["pull_observations"]
    assert mock_action_handler.called
    for k, v in config_overrides.items():
        config = mock_action_handler.call_args.kwargs["action_config"]
        assert getattr(config, k) == v


@pytest.mark.asyncio
async def test_execute_action_from_pubsub_with_config_overrides(
        mocker, mock_gundi_client_v2, mock_publish_event, mock_action_handlers,
        event_v2_cloud_event_headers, event_v2_cloud_event_payload_with_config_overrides
):
    mocker.patch("app.services.action_runner.action_handlers", mock_action_handlers)
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)

    response = api_client.post(
        "/",
        headers=event_v2_cloud_event_headers,
        json=event_v2_cloud_event_payload_with_config_overrides,
    )

    assert response.status_code == 200
    assert mock_gundi_client_v2.get_integration_details.called
    mock_action_handler, mock_config = mock_action_handlers["pull_observations"]
    assert mock_action_handler.called
    encoded_data = event_v2_cloud_event_payload_with_config_overrides["message"]["data"]
    decoded_data = base64.b64decode(encoded_data).decode("utf-8")
    config_overrides = json.loads(decoded_data)["config_overrides"]
    for k, v in config_overrides.items():
        config = mock_action_handler.call_args.kwargs["action_config"]
        assert getattr(config, k) == v


@pytest.mark.asyncio
async def test_execute_action_from_api_with_invalid_config(
        mocker, mock_gundi_client_v2, integration_v2,
        mock_publish_event, mock_action_handlers,
):
    mocker.patch("app.services.action_runner.action_handlers", mock_action_handlers)
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
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
