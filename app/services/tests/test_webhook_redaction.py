from unittest.mock import AsyncMock

import pydantic
import pytest

from app.services import webhooks
from app.services.activity_logger import webhook_activity_logger
from app.services.redaction import REDACTED


@pytest.mark.parametrize("failure", ["payload", "handler", "configuration"])
@pytest.mark.asyncio
async def test_webhook_failure_events_redact_raw_config(
        mocker, integration_v2_with_webhook, failure,
):
    class Config(pydantic.BaseModel):
        code: str = pydantic.Field(..., format="password")
        count: int

    class Payload(pydantic.BaseModel):
        value: int

    raw = {"code": "webhook-secret", "password": "extra-secret", "count": 1}
    if failure == "configuration":
        raw["count"] = "invalid"
    integration = integration_v2_with_webhook
    integration.webhook_configuration.data = raw.copy()
    mocker.patch.object(webhooks, "get_integration", AsyncMock(return_value=integration))
    published = AsyncMock()
    mocker.patch.object(webhooks, "publish_event", published)
    mocker.patch("app.services.activity_logger.publish_event", published)

    @webhook_activity_logger()
    async def handler(**kwargs):
        raise RuntimeError("handler failed")

    mocker.patch.object(webhooks, "get_webhook_handler", return_value=(handler, Payload, Config))
    request = mocker.Mock()
    request.json = AsyncMock(return_value={"value": "invalid"} if failure == "payload" else {"value": 1})

    await webhooks.process_webhook(request)

    events = [call.kwargs["event"] for call in published.call_args_list]
    assert events
    for event in events:
        assert event.payload.config_data["code"] == REDACTED
        if "password" in event.payload.config_data:
            assert event.payload.config_data["password"] == REDACTED
        assert "webhook-secret" not in event.json()
        assert "extra-secret" not in event.json()
    assert integration.webhook_configuration.data == raw


@pytest.mark.asyncio
async def test_webhook_failure_before_config_is_known_still_publishes(mocker):
    mocker.patch.object(webhooks, "get_integration", AsyncMock(side_effect=RuntimeError("lookup failed")))
    published = AsyncMock()
    mocker.patch.object(webhooks, "publish_event", published)
    request = mocker.Mock()
    request.json = AsyncMock(return_value={})

    await webhooks.process_webhook(request)

    event = published.call_args.kwargs["event"]
    assert event.payload.integration_id is None
    assert event.payload.config_data == {}
    assert "lookup failed" in event.payload.error


@pytest.mark.asyncio
async def test_webhook_failure_event_redacts_secrets_in_the_error_text(mocker, integration_v2_with_webhook):
    integration = integration_v2_with_webhook
    mocker.patch.object(webhooks, "get_integration", AsyncMock(return_value=integration))
    published = AsyncMock()
    mocker.patch.object(webhooks, "publish_event", published)

    async def handler(**kwargs):
        raise RuntimeError("GET https://provider.example/data?api_key=url-secret failed")

    mocker.patch.object(webhooks, "get_webhook_handler", return_value=(handler, None, None))
    request = mocker.Mock()
    request.json = AsyncMock(return_value={})

    await webhooks.process_webhook(request)

    event = published.call_args.kwargs["event"]
    assert "provider.example" in event.payload.error
    assert "url-secret" not in event.json()
