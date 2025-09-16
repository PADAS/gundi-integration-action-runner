import base64
import json
from unittest.mock import ANY

import pytest
from unittest.mock import AsyncMock
from fastapi.testclient import TestClient

from app.conftest import MockWebhookPayloadModel, MockWebhookConfigModel
from app.main import app
from app.webhooks import GenericJsonTransformConfig
from app.services.config_manager import IntegrationConfigurationManager

api_client = TestClient(app)


@pytest.mark.asyncio
async def test_process_webhook_request_with_fixed_schema(
        mocker, integration_v2_with_webhook, mock_publish_event,
        mock_get_webhook_handler_for_fixed_json_payload, mock_webhook_handler,
        mock_webhook_request_headers_onyesha, mock_webhook_request_payload_for_fixed_schema
):
    mocker.patch("app.services.webhooks.get_webhook_handler", mock_get_webhook_handler_for_fixed_json_payload)
    mocker.patch("app.services.config_manager.IntegrationConfigurationManager.get_integration_details", AsyncMock(return_value=integration_v2_with_webhook))

    response = api_client.post(
        "/webhooks",
        headers=mock_webhook_request_headers_onyesha,
        json=mock_webhook_request_payload_for_fixed_schema,
    )

    assert response.status_code == 200
    assert mock_get_webhook_handler_for_fixed_json_payload.called
    expected_payload = MockWebhookPayloadModel.parse_obj(mock_webhook_request_payload_for_fixed_schema)
    expected_config = MockWebhookConfigModel.parse_obj(integration_v2_with_webhook.webhook_configuration.data)
    mock_webhook_handler.assert_called_once_with(
        payload=expected_payload,
        integration=integration_v2_with_webhook,
        webhook_config=expected_config
    )


@pytest.mark.asyncio
async def test_process_webhook_request_with_dynamic_schema(
        mocker, integration_v2_with_webhook_generic, mock_publish_event,
        mock_get_webhook_handler_for_generic_json_payload, mock_webhook_handler,
        mock_webhook_request_headers_onyesha, mock_webhook_request_payload_for_dynamic_schema
):
    mocker.patch("app.services.webhooks.get_webhook_handler", mock_get_webhook_handler_for_generic_json_payload)
    mocker.patch("app.services.config_manager.IntegrationConfigurationManager.get_integration_details", AsyncMock(return_value=integration_v2_with_webhook_generic))

    response = api_client.post(
        "/webhooks",
        headers=mock_webhook_request_headers_onyesha,
        json=mock_webhook_request_payload_for_dynamic_schema,
    )

    assert response.status_code == 200
    assert mock_get_webhook_handler_for_generic_json_payload.called
    expected_config = GenericJsonTransformConfig.parse_obj(integration_v2_with_webhook_generic.webhook_configuration.data)
    mock_webhook_handler.assert_called_once_with(
        payload=ANY,
        integration=integration_v2_with_webhook_generic,
        webhook_config=expected_config
    )


# TTL Configuration Tests

@pytest.mark.asyncio
async def test_get_integration_calls_config_manager_with_ttl(
        mocker, integration_v2_with_webhook
):
    """Test that get_integration calls config_manager.get_integration_details with ttl=60"""
    mock_config_manager = mocker.patch("app.services.webhooks.config_manager")
    mock_config_manager.get_integration_details = AsyncMock(return_value=integration_v2_with_webhook)
    
    from app.services.webhooks import get_integration
    # Create a mock request with integration ID in headers
    headers = {"x-gundi-integration-id": str(integration_v2_with_webhook.id)}
    request = mocker.Mock()
    request.headers = headers
    request.query_params = {}
    
    integration = await get_integration(request)
    
    assert integration == integration_v2_with_webhook
    mock_config_manager.get_integration_details.assert_called_once_with(
        str(integration_v2_with_webhook.id), 
        ttl=60
    )


@pytest.mark.asyncio
async def test_get_integration_with_integration_id_header(
        mocker, integration_v2_with_webhook
):
    """Test that get_integration works with x-gundi-integration-id header"""
    mock_config_manager = mocker.patch("app.services.webhooks.config_manager")
    mock_config_manager.get_integration_details = AsyncMock(return_value=integration_v2_with_webhook)
    
    from app.services.webhooks import get_integration
    from fastapi import Request
    
    # Create a mock request with x-gundi-integration-id header
    headers = {"x-gundi-integration-id": str(integration_v2_with_webhook.id)}
    request = Request({"type": "http", "method": "POST", "url": "http://test/webhooks"})
    request._headers = headers
    request._query_params = {}
    
    integration = await get_integration(request)
    
    assert integration == integration_v2_with_webhook
    mock_config_manager.get_integration_details.assert_called_once_with(
        str(integration_v2_with_webhook.id), 
        ttl=60
    )


@pytest.mark.asyncio
async def test_get_integration_with_query_param(
        mocker, integration_v2_with_webhook
):
    """Test that get_integration works with integration_id query parameter"""
    mock_config_manager = mocker.patch("app.services.webhooks.config_manager")
    mock_config_manager.get_integration_details = AsyncMock(return_value=integration_v2_with_webhook)
    
    from app.services.webhooks import get_integration
    from fastapi import Request
    
    # Create a mock request with integration_id query param
    request = Request({"type": "http", "method": "POST", "url": "http://test/webhooks"})
    request._headers = {}
    request._query_params = {"integration_id": str(integration_v2_with_webhook.id)}
    
    integration = await get_integration(request)
    
    assert integration == integration_v2_with_webhook
    mock_config_manager.get_integration_details.assert_called_once_with(
        str(integration_v2_with_webhook.id), 
        ttl=60
    )


@pytest.mark.asyncio
async def test_get_integration_handles_config_manager_exception(
        mocker, mock_publish_event
):
    """Test that get_integration handles config_manager exceptions gracefully"""
    mock_config_manager = mocker.patch("app.services.webhooks.config_manager")
    mock_config_manager.get_integration_details = AsyncMock(side_effect=Exception("Config manager error"))
    
    # Mock the publish_event function
    mocker.patch("app.services.webhooks.publish_event", mock_publish_event)
    
    from app.services.webhooks import get_integration
    from fastapi import Request
    
    # Create a mock request with integration ID
    request = Request({"type": "http", "method": "POST", "url": "http://test/webhooks"})
    request._headers = {"x-gundi-integration-id": "test-integration-id"}
    request._query_params = {}
    
    integration = await get_integration(request)
    
    assert integration is None
    mock_publish_event.assert_called_once()
    # Verify the event contains the expected error information
    call_args = mock_publish_event.call_args
    assert "IntegrationWebhookFailed" in str(call_args)


@pytest.mark.asyncio
async def test_process_webhook_calls_config_manager_with_ttl(
        mocker, integration_v2_with_webhook, mock_publish_event,
        mock_get_webhook_handler_for_fixed_json_payload, mock_webhook_handler,
        mock_webhook_request_payload_for_fixed_schema
):
    """Test that process_webhook calls config_manager.get_integration_details with ttl=60"""
    mocker.patch("app.services.webhooks.get_webhook_handler", mock_get_webhook_handler_for_fixed_json_payload)
    mock_config_manager = mocker.patch("app.services.webhooks.config_manager")
    mock_config_manager.get_integration_details = AsyncMock(return_value=integration_v2_with_webhook)

    # Use headers with the correct integration ID
    headers = {"x-gundi-integration-id": str(integration_v2_with_webhook.id)}

    response = api_client.post(
        "/webhooks",
        headers=headers,
        json=mock_webhook_request_payload_for_fixed_schema,
    )

    assert response.status_code == 200
    mock_config_manager.get_integration_details.assert_called_once_with(
        str(integration_v2_with_webhook.id),
        ttl=60
    )


@pytest.mark.asyncio
async def test_process_webhook_handles_no_integration_gracefully(
        mocker, mock_publish_event, mock_webhook_request_payload_for_fixed_schema
):
    """Test that process_webhook handles the case when get_integration returns None gracefully"""
    # Mock get_integration to return None
    mocker.patch("app.services.webhooks.get_integration", return_value=None)
    
    # Mock the logger to capture warning messages
    mock_logger = mocker.patch("app.services.webhooks.logger")
    
    from app.services.webhooks import process_webhook
    from fastapi import Request
    
    # Create a mock request
    request = Request({"type": "http", "method": "POST", "url": "http://test/webhooks"})
    request._headers = {}
    request._query_params = {}
    mocker.patch.object(request, "json", AsyncMock(return_value=mock_webhook_request_payload_for_fixed_schema))
    
    result = await process_webhook(request)
    
    # Verify that the function returns an empty dictionary
    assert result == {}
    
    # Verify that a warning was logged
    mock_logger.warning.assert_called_once()
    warning_call = mock_logger.warning.call_args[0][0]
    assert "No integration found for webhook request" in warning_call
    assert "headers:" in warning_call
    assert "query_params:" in warning_call


@pytest.mark.asyncio
async def test_process_webhook_no_integration_via_api(
        mocker, mock_publish_event, mock_webhook_request_payload_for_fixed_schema
):
    """Test that process_webhook handles no integration case via API endpoint"""
    # Mock get_integration to return None
    mocker.patch("app.services.webhooks.get_integration", return_value=None)
    
    # Mock the logger to capture warning messages
    mock_logger = mocker.patch("app.services.webhooks.logger")
    
    # Make a request without any integration identification headers/params
    response = api_client.post(
        "/webhooks",
        headers={},  # No integration identification
        json=mock_webhook_request_payload_for_fixed_schema,
    )
    
    # Verify that the API returns 200 (webhook processing doesn't fail)
    assert response.status_code == 200
    
    # Verify that a warning was logged
    mock_logger.warning.assert_called_once()
    warning_call = mock_logger.warning.call_args[0][0]
    assert "No integration found for webhook request" in warning_call


# TTL Expiration and Gundi API Reload Tests

@pytest.mark.asyncio
async def test_process_webhook_reloads_from_gundi_on_cache_miss(
        mocker, integration_v2_with_webhook, mock_publish_event,
        mock_get_webhook_handler_for_fixed_json_payload, mock_webhook_handler,
        mock_webhook_request_payload_for_fixed_schema, mock_redis_empty, mock_gundi_client_v2_class_for_webhooks
):
    """Test that process_webhook reloads integration from Gundi API when cache is empty (TTL expired)"""
    # Patch Redis and GundiClient at the module level to simulate cache miss
    mocker.patch("app.services.config_manager.redis", mock_redis_empty)
    mocker.patch("app.services.config_manager.GundiClient", mock_gundi_client_v2_class_for_webhooks)
    
    # Create a new config manager instance with the patched dependencies
    from app.services.config_manager import IntegrationConfigurationManager
    config_manager = IntegrationConfigurationManager()
    
    # Patch the config_manager instance in the webhooks module
    mocker.patch("app.services.webhooks.config_manager", config_manager)
    
    # Mock webhook handler
    mocker.patch("app.services.webhooks.get_webhook_handler", mock_get_webhook_handler_for_fixed_json_payload)
    
    # Make webhook request
    headers = {"x-gundi-integration-id": str(integration_v2_with_webhook.id)}
    response = api_client.post(
        "/webhooks",
        headers=headers,
        json=mock_webhook_request_payload_for_fixed_schema,
    )
    
    # Verify successful response
    assert response.status_code == 200
    
    # Verify that Redis was checked for cached data (cache miss)
    mock_redis_empty.Redis.return_value.get.assert_called()
    
    # Verify that GundiClient was called to reload the integration
    mock_gundi_instance = mock_gundi_client_v2_class_for_webhooks.return_value.__aenter__.return_value
    mock_gundi_instance.get_integration_details.assert_called_with(str(integration_v2_with_webhook.id))
    
    # Verify that data was saved back to cache with TTL
    mock_redis_empty.Redis.return_value.set.assert_called()


@pytest.mark.asyncio
async def test_get_integration_reloads_from_gundi_on_cache_miss(
        mocker, integration_v2_with_webhook, mock_redis_empty, mock_gundi_client_v2_class_for_webhooks
):
    """Test that get_integration reloads from Gundi API when cache is empty (TTL expired)"""
    # Patch Redis and GundiClient at the module level to simulate cache miss
    mocker.patch("app.services.config_manager.redis", mock_redis_empty)
    mocker.patch("app.services.config_manager.GundiClient", mock_gundi_client_v2_class_for_webhooks)
    
    # Create a new config manager instance with the patched dependencies
    from app.services.config_manager import IntegrationConfigurationManager
    config_manager = IntegrationConfigurationManager()
    
    # Patch the config_manager instance in the webhooks module
    mocker.patch("app.services.webhooks.config_manager", config_manager)
    
    from app.services.webhooks import get_integration
    from fastapi import Request
    
    # Create a mock request with integration ID
    headers = {"x-gundi-integration-id": str(integration_v2_with_webhook.id)}
    request = Request({"type": "http", "method": "POST", "url": "http://test/webhooks"})
    request._headers = headers
    request._query_params = {}
    
    integration = await get_integration(request)
    
    # Verify that integration was retrieved
    assert integration is not None
    assert integration.id == integration_v2_with_webhook.id
    assert integration.name == integration_v2_with_webhook.name
    
    # Verify that Redis was checked for cached data (cache miss)
    mock_redis_empty.Redis.return_value.get.assert_called()
    
    # Verify that GundiClient was called to reload the integration
    mock_gundi_instance = mock_gundi_client_v2_class_for_webhooks.return_value.__aenter__.return_value
    mock_gundi_instance.get_integration_details.assert_called_with(str(integration_v2_with_webhook.id))
    
    # Verify that data was saved back to cache with TTL
    mock_redis_empty.Redis.return_value.set.assert_called()


@pytest.mark.asyncio
async def test_process_webhook_handles_gundi_api_failure_gracefully(
        mocker, mock_publish_event, mock_webhook_request_payload_for_fixed_schema,
        mock_redis_empty, mock_gundi_client_v2_class_with_error
):
    """Test that process_webhook handles Gundi API failures gracefully when reloading from cache miss"""
    # Patch Redis and GundiClient at the module level to simulate cache miss and API failure
    mocker.patch("app.services.config_manager.redis", mock_redis_empty)
    mocker.patch("app.services.config_manager.GundiClient", mock_gundi_client_v2_class_with_error)
    
    # Create a new config manager instance with the patched dependencies
    from app.services.config_manager import IntegrationConfigurationManager
    config_manager = IntegrationConfigurationManager()
    
    # Patch the config_manager instance in the webhooks module
    mocker.patch("app.services.webhooks.config_manager", config_manager)
    
    # Mock publish_event to capture the error event
    mocker.patch("app.services.webhooks.publish_event", mock_publish_event)
    
    from app.services.webhooks import get_integration
    from fastapi import Request
    
    # Create a mock request with integration ID
    headers = {"x-gundi-integration-id": "test-integration-id"}
    request = Request({"type": "http", "method": "POST", "url": "http://test/webhooks"})
    request._headers = headers
    request._query_params = {}
    
    integration = await get_integration(request)
    
    # Verify that integration is None due to the error
    assert integration is None
    
    # Verify that Redis was checked for cached data (cache miss)
    mock_redis_empty.Redis.return_value.get.assert_called()
    
    # Verify that GundiClient was called but failed
    mock_gundi_instance = mock_gundi_client_v2_class_with_error.return_value.__aenter__.return_value
    mock_gundi_instance.get_integration_details.assert_called_once_with("test-integration-id")
    
    # Verify that an error event was published
    mock_publish_event.assert_called_once()
    call_args = mock_publish_event.call_args
    assert "IntegrationWebhookFailed" in str(call_args)
    assert "Gundi API unavailable" in str(call_args)


