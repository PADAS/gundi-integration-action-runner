import pytest
from fastapi.testclient import TestClient

from app.main import app


api_client = TestClient(app)


@pytest.mark.asyncio
async def test_process_event_integration_created_from_pubsub(
        mocker, mock_gundi_client_v2, mock_publish_event, mock_action_handlers, mock_config_manager,
        pubsub_message_request_headers, integration_created_event_as_pubsub_message
):

    mocker.patch("app.services.config_events_consumer.config_manager", mock_config_manager)

    response = api_client.post(
        "/config-events/",
        headers=pubsub_message_request_headers,
        json=integration_created_event_as_pubsub_message,
    )

    assert response.status_code == 200
    assert mock_config_manager.set_integration.called


@pytest.mark.asyncio
async def test_process_event_integration_updated_from_pubsub(
        mocker, mock_gundi_client_v2, mock_publish_event, mock_action_handlers, mock_config_manager,
        pubsub_message_request_headers, integration_updated_event_as_pubsub_message
):

    mocker.patch("app.services.config_events_consumer.config_manager", mock_config_manager)

    response = api_client.post(
        "/config-events/",
        headers=pubsub_message_request_headers,
        json=integration_updated_event_as_pubsub_message,
    )

    assert response.status_code == 200
    assert mock_config_manager.get_integration.called
    assert mock_config_manager.set_integration.called


@pytest.mark.asyncio
async def test_process_event_integration_deleted_from_pubsub(
        mocker, mock_gundi_client_v2, mock_publish_event, mock_action_handlers, mock_config_manager,
        pubsub_message_request_headers, integration_deleted_event_as_pubsub_message
):

    mocker.patch("app.services.config_events_consumer.config_manager", mock_config_manager)

    response = api_client.post(
        "/config-events/",
        headers=pubsub_message_request_headers,
        json=integration_deleted_event_as_pubsub_message,
    )

    assert response.status_code == 200
    assert mock_config_manager.delete_integration.called


@pytest.mark.asyncio
async def test_process_event_action_config_created_from_pubsub(
        mocker, mock_gundi_client_v2, mock_publish_event, mock_action_handlers, mock_config_manager,
        pubsub_message_request_headers, action_config_created_event_as_pubsub_message
):

    mocker.patch("app.services.config_events_consumer.config_manager", mock_config_manager)

    response = api_client.post(
        "/config-events/",
        headers=pubsub_message_request_headers,
        json=action_config_created_event_as_pubsub_message,
    )

    assert response.status_code == 200
    assert mock_config_manager.set_action_configuration.called


@pytest.mark.asyncio
async def test_process_event_action_config_updated_from_pubsub(
        mocker, mock_gundi_client_v2, mock_publish_event, mock_action_handlers, mock_config_manager,
        pubsub_message_request_headers, action_config_updated_event_as_pubsub_message
):

    mocker.patch("app.services.config_events_consumer.config_manager", mock_config_manager)

    response = api_client.post(
        "/config-events/",
        headers=pubsub_message_request_headers,
        json=action_config_updated_event_as_pubsub_message,
    )

    assert response.status_code == 200
    assert mock_config_manager.get_action_configuration.called
    assert mock_config_manager.set_action_configuration.called


@pytest.mark.asyncio
async def test_process_event_action_config_deleted_from_pubsub(
        mocker, mock_gundi_client_v2, mock_publish_event, mock_action_handlers, mock_config_manager,
        pubsub_message_request_headers, action_config_deleted_event_as_pubsub_message
):

    mocker.patch("app.services.config_events_consumer.config_manager", mock_config_manager)

    response = api_client.post(
        "/config-events/",
        headers=pubsub_message_request_headers,
        json=action_config_deleted_event_as_pubsub_message,
    )

    assert response.status_code == 200
    assert mock_config_manager.delete_action_configuration.called

