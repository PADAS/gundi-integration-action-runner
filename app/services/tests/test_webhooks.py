import base64
import json
from unittest.mock import ANY

import pytest
from fastapi.testclient import TestClient

from app.conftest import MockWebhookPayloadModel, MockWebhookConfigModel
from app.main import app
from app.webhooks import GenericJsonTransformConfig

api_client = TestClient(app)


@pytest.mark.asyncio
async def test_process_webhook_request_with_fixed_schema(
        mocker, integration_v2_with_webhook, mock_gundi_client_v2_for_webhooks, mock_publish_event,
        mock_get_webhook_handler_for_fixed_json_payload, mock_webhook_handler,
        mock_webhook_request_headers_onyesha, mock_webhook_request_payload_for_fixed_schema
):
    mocker.patch("app.services.webhooks.get_webhook_handler", mock_get_webhook_handler_for_fixed_json_payload)
    mocker.patch("app.services.webhooks._portal", mock_gundi_client_v2_for_webhooks)

    response = api_client.post(
        "/webhooks",
        headers=mock_webhook_request_headers_onyesha,
        json=mock_webhook_request_payload_for_fixed_schema,
    )

    assert response.status_code == 200
    assert mock_gundi_client_v2_for_webhooks.get_integration_details.called
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
        mocker, integration_v2_with_webhook_generic, mock_gundi_client_v2_for_webhooks_generic, mock_publish_event,
        mock_get_webhook_handler_for_generic_json_payload, mock_webhook_handler,
        mock_webhook_request_headers_onyesha, mock_webhook_request_payload_for_dynamic_schema
):
    mocker.patch("app.services.webhooks.get_webhook_handler", mock_get_webhook_handler_for_generic_json_payload)
    mocker.patch("app.services.webhooks._portal", mock_gundi_client_v2_for_webhooks_generic)

    response = api_client.post(
        "/webhooks",
        headers=mock_webhook_request_headers_onyesha,
        json=mock_webhook_request_payload_for_dynamic_schema,
    )

    assert response.status_code == 200
    assert mock_gundi_client_v2_for_webhooks_generic.get_integration_details.called
    assert mock_get_webhook_handler_for_generic_json_payload.called
    expected_config = GenericJsonTransformConfig.parse_obj(integration_v2_with_webhook_generic.webhook_configuration.data)
    mock_webhook_handler.assert_called_once_with(
        payload=ANY,
        integration=integration_v2_with_webhook_generic,
        webhook_config=expected_config
    )


