import asyncio
import base64
import json
from unittest.mock import ANY, MagicMock, patch

import httpx
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


# Diagnostic Forwarding Tests

@pytest.mark.asyncio
async def test_diagnostic_forwarding_called_when_url_configured(
        mocker, integration_v2_with_diagnostic_webhook, mock_publish_event,
        mock_get_webhook_handler_for_generic_json_payload, mock_webhook_handler,
        mock_webhook_request_headers_onyesha, mock_webhook_request_payload_for_dynamic_schema
):
    mocker.patch("app.services.webhooks.get_webhook_handler", mock_get_webhook_handler_for_generic_json_payload)
    mocker.patch("app.services.config_manager.IntegrationConfigurationManager.get_integration_details", AsyncMock(return_value=integration_v2_with_diagnostic_webhook))
    mock_forward = mocker.patch(
        "app.services.webhooks.forward_payload_to_diagnostic_url",
        return_value=AsyncMock(),
    )
    mocker.patch("app.services.webhooks.asyncio.ensure_future", side_effect=lambda coro: coro.close())

    response = api_client.post(
        "/webhooks",
        headers=mock_webhook_request_headers_onyesha,
        json=mock_webhook_request_payload_for_dynamic_schema,
    )

    assert response.status_code == 200
    mock_forward.assert_called_once_with(
        destination_url="https://diagnostics.example.com/webhook-dump",
        integration_id=str(integration_v2_with_diagnostic_webhook.id),
        json_content=mock_webhook_request_payload_for_dynamic_schema,
    )


@pytest.mark.asyncio
async def test_diagnostic_forwarding_not_called_when_url_not_configured(
        mocker, integration_v2_with_webhook_generic, mock_publish_event,
        mock_get_webhook_handler_for_generic_json_payload, mock_webhook_handler,
        mock_webhook_request_headers_onyesha, mock_webhook_request_payload_for_dynamic_schema
):
    mocker.patch("app.services.webhooks.get_webhook_handler", mock_get_webhook_handler_for_generic_json_payload)
    mocker.patch("app.services.config_manager.IntegrationConfigurationManager.get_integration_details", AsyncMock(return_value=integration_v2_with_webhook_generic))
    mock_ensure_future = mocker.patch("app.services.webhooks.asyncio.ensure_future")

    response = api_client.post(
        "/webhooks",
        headers=mock_webhook_request_headers_onyesha,
        json=mock_webhook_request_payload_for_dynamic_schema,
    )

    assert response.status_code == 200
    mock_ensure_future.assert_not_called()


@pytest.mark.asyncio
async def test_diagnostic_forwarding_does_not_fail_main_webhook(
        mocker, integration_v2_with_diagnostic_webhook, mock_publish_event,
        mock_get_webhook_handler_for_generic_json_payload, mock_webhook_handler,
        mock_webhook_request_headers_onyesha, mock_webhook_request_payload_for_dynamic_schema
):
    mocker.patch("app.services.webhooks.get_webhook_handler", mock_get_webhook_handler_for_generic_json_payload)
    mocker.patch("app.services.config_manager.IntegrationConfigurationManager.get_integration_details", AsyncMock(return_value=integration_v2_with_diagnostic_webhook))
    mocker.patch("app.services.webhooks._validate_diagnostic_url", AsyncMock(return_value=None))  # skip DNS in this test
    # Simulate a real HTTP failure in the forwarding POST. forward_payload_to_diagnostic_url
    # catches all exceptions internally, so this must not propagate to the main flow.
    mock_client = MagicMock()
    mock_client.post = AsyncMock(side_effect=httpx.HTTPError("Connection refused"))
    mocker.patch("app.services.webhooks._get_diagnostic_client", return_value=mock_client)
    # Do not mock ensure_future — the coroutine is scheduled for real and its internal
    # try/except catches the HTTPError, proving isolation from the main request.

    response = api_client.post(
        "/webhooks",
        headers=mock_webhook_request_headers_onyesha,
        json=mock_webhook_request_payload_for_dynamic_schema,
    )

    assert response.status_code == 200
    mock_webhook_handler.assert_called_once()


@pytest.mark.asyncio
async def test_diagnostic_forwarding_called_even_when_payload_parsing_fails(
        mocker, integration_v2_with_diagnostic_webhook, mock_publish_event,
        mock_get_webhook_handler_for_generic_json_payload,
        mock_webhook_request_headers_onyesha, mock_webhook_request_payload_for_dynamic_schema
):
    mocker.patch("app.services.webhooks.get_webhook_handler", mock_get_webhook_handler_for_generic_json_payload)
    mocker.patch("app.services.config_manager.IntegrationConfigurationManager.get_integration_details", AsyncMock(return_value=integration_v2_with_diagnostic_webhook))
    mocker.patch("app.services.webhooks.publish_event", mock_publish_event)
    mock_ensure_future = mocker.patch("app.services.webhooks.asyncio.ensure_future")

    # Force payload parsing to fail
    mocker.patch("app.services.webhooks.DyntamicFactory", side_effect=Exception("Schema error"))

    response = api_client.post(
        "/webhooks",
        headers=mock_webhook_request_headers_onyesha,
        json=mock_webhook_request_payload_for_dynamic_schema,
    )

    # Webhook still returns 200 (errors are swallowed)
    assert response.status_code == 200
    # Diagnostic forwarding was still scheduled before the parse error occurred
    assert mock_ensure_future.called
    mock_ensure_future.call_args[0][0].close()


@pytest.mark.asyncio
async def test_diagnostic_forwarding_logs_warning_when_validation_fails(
        mocker, integration_v2_with_diagnostic_webhook, mock_publish_event,
        mock_get_webhook_handler_for_generic_json_payload, mock_webhook_handler,
        mock_webhook_request_headers_onyesha, mock_webhook_request_payload_for_dynamic_schema
):
    """Validation failure is handled inside the background task; main request is not affected."""
    mocker.patch("app.services.webhooks.get_webhook_handler", mock_get_webhook_handler_for_generic_json_payload)
    mocker.patch("app.services.config_manager.IntegrationConfigurationManager.get_integration_details", AsyncMock(return_value=integration_v2_with_diagnostic_webhook))
    # Validation fails inside the background coroutine
    mocker.patch("app.services.webhooks._validate_diagnostic_url", AsyncMock(side_effect=ValueError("blocked")))
    mock_ensure_future = mocker.patch("app.services.webhooks.asyncio.ensure_future")

    response = api_client.post(
        "/webhooks",
        headers=mock_webhook_request_headers_onyesha,
        json=mock_webhook_request_payload_for_dynamic_schema,
    )

    assert response.status_code == 200
    # ensure_future IS called — the coroutine is always scheduled when a URL is set
    mock_ensure_future.assert_called_once()
    mock_ensure_future.call_args[0][0].close()  # clean up unawaited coroutine
    mock_webhook_handler.assert_called_once()  # main handler still ran


# _validate_diagnostic_url unit tests

def _make_getaddrinfo(ip: str):
    """Return an AsyncMock that resolves to the given IP address."""
    return AsyncMock(return_value=[(None, None, None, None, (ip, 0))])


@pytest.mark.asyncio
async def test_validate_diagnostic_url_accepts_public_https(mocker):
    from app.services.webhooks import _validate_diagnostic_url
    mock_loop = MagicMock()
    mock_loop.getaddrinfo = _make_getaddrinfo("203.0.113.5")  # TEST-NET-3 (RFC 5737), not in blocked list
    mocker.patch("app.services.webhooks.asyncio.get_running_loop", return_value=mock_loop)
    await _validate_diagnostic_url("https://diagnostics.example.com/dump")  # should not raise


@pytest.mark.asyncio
async def test_validate_diagnostic_url_rejects_http_scheme(mocker):
    from app.services.webhooks import _validate_diagnostic_url
    with pytest.raises(ValueError, match="scheme"):
        await _validate_diagnostic_url("http://diagnostics.example.com/dump")


@pytest.mark.asyncio
async def test_validate_diagnostic_url_rejects_loopback(mocker):
    from app.services.webhooks import _validate_diagnostic_url
    mock_loop = MagicMock()
    mock_loop.getaddrinfo = _make_getaddrinfo("127.0.0.1")
    mocker.patch("app.services.webhooks.asyncio.get_running_loop", return_value=mock_loop)
    with pytest.raises(ValueError, match="private or reserved"):
        await _validate_diagnostic_url("https://internal.local/dump")


@pytest.mark.asyncio
async def test_validate_diagnostic_url_rejects_metadata_endpoint(mocker):
    from app.services.webhooks import _validate_diagnostic_url
    mock_loop = MagicMock()
    mock_loop.getaddrinfo = _make_getaddrinfo("169.254.169.254")  # GCP/AWS metadata
    mocker.patch("app.services.webhooks.asyncio.get_running_loop", return_value=mock_loop)
    with pytest.raises(ValueError, match="private or reserved"):
        await _validate_diagnostic_url("https://metadata.google.internal/")


@pytest.mark.asyncio
async def test_validate_diagnostic_url_rejects_private_rfc1918(mocker):
    from app.services.webhooks import _validate_diagnostic_url
    mock_loop = MagicMock()
    mock_loop.getaddrinfo = _make_getaddrinfo("192.168.1.100")
    mocker.patch("app.services.webhooks.asyncio.get_running_loop", return_value=mock_loop)
    with pytest.raises(ValueError, match="private or reserved"):
        await _validate_diagnostic_url("https://internal.corp/dump")


@pytest.mark.asyncio
async def test_validate_diagnostic_url_rejects_unresolvable_host(mocker):
    from app.services.webhooks import _validate_diagnostic_url
    import socket
    mock_loop = MagicMock()
    mock_loop.getaddrinfo = AsyncMock(side_effect=socket.gaierror("Name not found"))
    mocker.patch("app.services.webhooks.asyncio.get_running_loop", return_value=mock_loop)
    with pytest.raises(ValueError, match="Cannot resolve"):
        await _validate_diagnostic_url("https://no-such-host.invalid/dump")


@pytest.mark.asyncio
async def test_validate_diagnostic_url_enforces_allowlist(mocker):
    from app.services.webhooks import _validate_diagnostic_url
    mocker.patch("app.services.webhooks.settings.DIAGNOSTIC_URL_ALLOWLIST", ["allowed.example.com"])
    with pytest.raises(ValueError, match="allowlist"):
        await _validate_diagnostic_url("https://other.example.com/dump")


@pytest.mark.asyncio
async def test_validate_diagnostic_url_passes_allowlist(mocker):
    from app.services.webhooks import _validate_diagnostic_url
    mocker.patch("app.services.webhooks.settings.DIAGNOSTIC_URL_ALLOWLIST", ["allowed.example.com"])
    mock_loop = MagicMock()
    mock_loop.getaddrinfo = _make_getaddrinfo("203.0.113.5")
    mocker.patch("app.services.webhooks.asyncio.get_running_loop", return_value=mock_loop)
    await _validate_diagnostic_url("https://allowed.example.com/dump")  # should not raise


@pytest.mark.asyncio
async def test_forward_payload_to_diagnostic_url_success(mocker):
    from app.services.webhooks import forward_payload_to_diagnostic_url

    destination_url = "https://diagnostics.example.com/webhook-dump"
    integration_id = "test-integration-id"
    json_content = {"device": "sensor-1", "value": 42}

    mocker.patch("app.services.webhooks._validate_diagnostic_url", AsyncMock(return_value=None))
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.raise_for_status = MagicMock()

    mock_post = AsyncMock(return_value=mock_response)
    mock_client = MagicMock()
    mock_client.post = mock_post
    mocker.patch("app.services.webhooks._get_diagnostic_client", return_value=mock_client)

    await forward_payload_to_diagnostic_url(destination_url, integration_id, json_content)

    mock_post.assert_called_once()
    call_kwargs = mock_post.call_args
    assert call_kwargs[0][0] == destination_url
    forwarded_body = call_kwargs[1]["json"]
    assert forwarded_body["device"] == json_content["device"]
    assert forwarded_body["value"] == json_content["value"]
    assert forwarded_body["__gundi_diagnostic_metadata"]["integration_id"] == integration_id
    assert "received_at" in forwarded_body["__gundi_diagnostic_metadata"]


@pytest.mark.asyncio
async def test_forward_payload_to_diagnostic_url_list_payload(mocker):
    from app.services.webhooks import forward_payload_to_diagnostic_url

    destination_url = "https://diagnostics.example.com/webhook-dump"
    integration_id = "test-integration-id"
    json_content = [{"device": "sensor-1"}, {"device": "sensor-2"}]

    mocker.patch("app.services.webhooks._validate_diagnostic_url", AsyncMock(return_value=None))
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.raise_for_status = MagicMock()

    mock_post = AsyncMock(return_value=mock_response)
    mock_client = MagicMock()
    mock_client.post = mock_post
    mocker.patch("app.services.webhooks._get_diagnostic_client", return_value=mock_client)

    await forward_payload_to_diagnostic_url(destination_url, integration_id, json_content)

    mock_post.assert_called_once()
    forwarded_body = mock_post.call_args[1]["json"]
    assert forwarded_body["payload"] == json_content
    assert forwarded_body["__gundi_diagnostic_metadata"]["integration_id"] == integration_id
    assert "received_at" in forwarded_body["__gundi_diagnostic_metadata"]


@pytest.mark.asyncio
async def test_forward_payload_to_diagnostic_url_handles_http_error(mocker):
    from app.services.webhooks import forward_payload_to_diagnostic_url

    destination_url = "https://diagnostics.example.com/webhook-dump"
    integration_id = "test-integration-id"
    json_content = {"device": "sensor-1"}

    mocker.patch("app.services.webhooks._validate_diagnostic_url", AsyncMock(return_value=None))
    mock_client = MagicMock()
    mock_client.post = AsyncMock(side_effect=httpx.HTTPError("Connection refused"))
    mocker.patch("app.services.webhooks._get_diagnostic_client", return_value=mock_client)
    mock_logger = mocker.patch("app.services.webhooks.logger")

    # Should not raise
    await forward_payload_to_diagnostic_url(destination_url, integration_id, json_content)

    mock_logger.warning.assert_called_once()
    warning_msg = mock_logger.warning.call_args[0][0]
    assert destination_url in warning_msg
    assert integration_id in warning_msg


# Per-record output type tests

@pytest.fixture
def mock_integration_for_handler():
    integration = MagicMock()
    integration.id = "test-integration-id"
    return integration


@pytest.fixture
def mock_webhook_config_obv():
    from app.webhooks import GenericJsonTransformConfig
    return GenericJsonTransformConfig(output_type="obv", jq_filter=".", json_schema={})


@pytest.mark.asyncio
async def test_handler_routes_all_records_via_config_output_type(
        mocker, mock_integration_for_handler, mock_webhook_config_obv
):
    from app.webhooks.handlers import webhook_handler
    from app.webhooks.core import GenericJsonPayload

    mocker.patch("app.services.activity_logger.publish_event", new_callable=AsyncMock)
    records = [{"source": "device-1"}, {"source": "device-2"}]
    mocker.patch("app.webhooks.handlers.pyjq.all", return_value=records)
    mock_send = mocker.patch("app.webhooks.handlers.send_observations_to_gundi", new_callable=AsyncMock)
    mock_send.return_value = records

    payload = MagicMock(spec=GenericJsonPayload)
    payload.json.return_value = "{}"
    result = await webhook_handler(payload=payload, integration=mock_integration_for_handler, webhook_config=mock_webhook_config_obv)

    mock_send.assert_called_once()
    assert result == {"data_points_qty": 2}


@pytest.mark.asyncio
async def test_handler_routes_per_record_output_type(
        mocker, mock_integration_for_handler, mock_webhook_config_obv
):
    from app.webhooks.handlers import webhook_handler
    from app.webhooks.core import GenericJsonPayload

    mocker.patch("app.services.activity_logger.publish_event", new_callable=AsyncMock)
    obv1 = {"__gundi_output_type": "obv", "source": "device-1"}
    obv2 = {"__gundi_output_type": "obv", "source": "device-2"}
    ev1 = {"__gundi_output_type": "ev", "title": "Alert"}
    mocker.patch("app.webhooks.handlers.pyjq.all", return_value=[obv1, obv2, ev1])
    mock_send_obv = mocker.patch("app.webhooks.handlers.send_observations_to_gundi", new_callable=AsyncMock)
    mock_send_obv.return_value = [obv1, obv2]
    mock_send_ev = mocker.patch("app.webhooks.handlers.send_events_to_gundi", new_callable=AsyncMock)
    mock_send_ev.return_value = [ev1]

    payload = MagicMock(spec=GenericJsonPayload)
    payload.json.return_value = "{}"
    result = await webhook_handler(payload=payload, integration=mock_integration_for_handler, webhook_config=mock_webhook_config_obv)

    mock_send_obv.assert_called_once()
    mock_send_ev.assert_called_once()
    assert result == {"data_points_qty": 3}


@pytest.mark.asyncio
async def test_handler_strips_gundi_output_type_before_sending(
        mocker, mock_integration_for_handler, mock_webhook_config_obv
):
    from app.webhooks.handlers import webhook_handler
    from app.webhooks.core import GenericJsonPayload

    mocker.patch("app.services.activity_logger.publish_event", new_callable=AsyncMock)
    record = {"__gundi_output_type": "obv", "source": "device-1"}
    mocker.patch("app.webhooks.handlers.pyjq.all", return_value=[record])
    mock_send = mocker.patch("app.webhooks.handlers.send_observations_to_gundi", new_callable=AsyncMock)
    mock_send.return_value = [{"source": "device-1"}]

    payload = MagicMock(spec=GenericJsonPayload)
    payload.json.return_value = "{}"
    await webhook_handler(payload=payload, integration=mock_integration_for_handler, webhook_config=mock_webhook_config_obv)

    sent = mock_send.call_args[1]["observations"]
    assert len(sent) == 1
    assert "__gundi_output_type" not in sent[0]


@pytest.mark.asyncio
async def test_handler_per_record_type_overrides_config(
        mocker, mock_integration_for_handler, mock_webhook_config_obv
):
    from app.webhooks.handlers import webhook_handler
    from app.webhooks.core import GenericJsonPayload

    mocker.patch("app.services.activity_logger.publish_event", new_callable=AsyncMock)
    # Config says "obv" but this record overrides to "ev"
    record = {"__gundi_output_type": "ev", "title": "Alert"}
    mocker.patch("app.webhooks.handlers.pyjq.all", return_value=[record])
    mock_send_obv = mocker.patch("app.webhooks.handlers.send_observations_to_gundi", new_callable=AsyncMock)
    mock_send_ev = mocker.patch("app.webhooks.handlers.send_events_to_gundi", new_callable=AsyncMock)
    mock_send_ev.return_value = [record]

    payload = MagicMock(spec=GenericJsonPayload)
    payload.json.return_value = "{}"
    await webhook_handler(payload=payload, integration=mock_integration_for_handler, webhook_config=mock_webhook_config_obv)

    mock_send_obv.assert_not_called()
    mock_send_ev.assert_called_once()


@pytest.mark.asyncio
async def test_handler_raises_when_no_output_type_resolved(
        mocker, mock_integration_for_handler
):
    from app.webhooks.handlers import webhook_handler
    from app.webhooks.core import GenericJsonPayload, GenericJsonTransformConfig

    mocker.patch("app.services.activity_logger.publish_event", new_callable=AsyncMock)
    config_no_type = GenericJsonTransformConfig(output_type=None, jq_filter=".", json_schema={})
    record = {"source": "device-1"}  # no __gundi_output_type, no config default
    mocker.patch("app.webhooks.handlers.pyjq.all", return_value=[record])

    payload = MagicMock(spec=GenericJsonPayload)
    payload.json.return_value = "{}"
    with pytest.raises(ValueError, match="No output type for record"):
        await webhook_handler(payload=payload, integration=mock_integration_for_handler, webhook_config=config_no_type)
