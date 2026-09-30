"""PubSub delivery semantics of the actions endpoint.

Two guarantees the per-source fan-out (one RunIntegrationAction per device)
depends on:

- A run that failed for a transient reason answers PubSub with a non-2xx so
  the message is redelivered; a run that can never succeed (bad config, bad
  credentials, a provider 4xx, a bug) is acked so it is not retried forever.
- An internal action's whole configuration arrives in config_overrides, so
  running it must not look up a stored configuration or reload from the portal.
"""
import asyncio
import base64
import json

import httpx
import pytest
from fastapi.testclient import TestClient

from app import settings
from app.conftest import AsyncMock, MockPullActionConfiguration, MockSubActionConfiguration
from app.main import app
from app.services.errors import is_retryable_failure

api_client = TestClient(app)
INTEGRATION_ID = "843e0801-e81a-47e5-9ce2-b176e4736a85"


def _pubsub_message(action_id, config_overrides=None):
    payload = {"integration_id": INTEGRATION_ID, "action_id": action_id}
    if config_overrides is not None:
        payload["config_overrides"] = config_overrides
    return {
        "message": {
            "data": base64.b64encode(json.dumps(payload).encode()).decode(),
            "messageId": "10298788169291041",
            "publishTime": "2026-09-30T00:00:00.000Z",
        },
        "subscription": "projects/cdip-stage-78ca/subscriptions/integrationx-actions-sub",
    }


def _provider_error(status_code):
    response = httpx.Response(
        status_code=status_code,
        request=httpx.Request("POST", "https://provider.example/api"),
        content=b'{"message": "provider says no"}',
        headers={"Content-Type": "application/json"},
    )
    return httpx.HTTPStatusError(f"HTTP {status_code}", request=response.request, response=response)


@pytest.fixture
def runner_mocks(mocker, mock_gundi_client_v2, mock_config_manager, mock_publish_event):
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch.object(settings, "PROCESS_PUBSUB_MESSAGES_IN_BACKGROUND", False)
    return mock_config_manager


def _install_handler(mocker, handler, action_id="pull_observations", config_model=MockPullActionConfiguration):
    mocker.patch("app.services.action_runner.action_handlers", {action_id: (handler, config_model, None)})
    return handler


# --- failures PubSub must redeliver ------------------------------------------


@pytest.mark.parametrize(
    "error, expected_status",
    [
        (_provider_error(503), 500),
        (_provider_error(429), 500),
        (httpx.ConnectError("connection refused"), 500),
    ],
    ids=["provider_5xx", "provider_rate_limited", "provider_unreachable"],
)
def test_transient_handler_failure_is_not_acked(mocker, runner_mocks, error, expected_status):
    handler = _install_handler(mocker, AsyncMock(side_effect=error))

    response = api_client.post("/", json=_pubsub_message("pull_observations"))

    assert response.status_code == expected_status
    assert handler.called
    assert response.json()["detail"]["action_id"] == "pull_observations"


def test_runner_timeout_is_not_acked(mocker, runner_mocks):
    async def slow_handler(**kwargs):
        await asyncio.sleep(1)

    _install_handler(mocker, slow_handler)
    mocker.patch.object(settings, "MAX_ACTION_EXECUTION_TIME", 0.01)

    response = api_client.post("/", json=_pubsub_message("pull_observations"))

    assert response.status_code == 504
    assert response.json()["detail"]["error_type"] == "timeout"


# --- failures PubSub must ack (retrying cannot help) --------------------------


@pytest.mark.parametrize(
    "error",
    [_provider_error(401), _provider_error(400), Exception("a bug in the handler")],
    ids=["provider_auth_rejected", "provider_bad_request", "unclassified"],
)
def test_permanent_handler_failure_is_acked(mocker, runner_mocks, error):
    handler = _install_handler(mocker, AsyncMock(side_effect=error))

    response = api_client.post("/", json=_pubsub_message("pull_observations"))

    assert response.status_code == 200
    assert response.json() == {}
    assert handler.called


def test_invalid_config_is_acked(mocker, runner_mocks):
    # An internal action with no overrides has nothing to run with: 422 from
    # the runner, which PubSub must not see as a reason to redeliver.
    handler = _install_handler(
        mocker, AsyncMock(), action_id="pull_observations_by_date", config_model=MockSubActionConfiguration,
    )

    response = api_client.post("/", json=_pubsub_message("pull_observations_by_date"))

    assert response.status_code == 200
    assert not handler.called


def test_successful_run_is_acked(mocker, runner_mocks):
    _install_handler(mocker, AsyncMock(return_value={"ok": True}))

    response = api_client.post("/", json=_pubsub_message("pull_observations"))

    assert response.status_code == 200
    assert response.json() == {}


def test_background_mode_acks_before_the_run_finishes(mocker, runner_mocks):
    # Documented trade-off: with PROCESS_PUBSUB_MESSAGES_IN_BACKGROUND the
    # message is acked on receipt, so a failure cannot be redelivered.
    mocker.patch.object(settings, "PROCESS_PUBSUB_MESSAGES_IN_BACKGROUND", True)
    handler = _install_handler(mocker, AsyncMock(side_effect=_provider_error(503)))

    response = api_client.post("/", json=_pubsub_message("pull_observations"))

    assert response.status_code == 200
    assert handler.called  # TestClient runs background tasks before returning


@pytest.mark.parametrize(
    "error_type, status_code, expected",
    [
        ("connectivity", None, True),
        ("bad_response", 502, True),
        ("rate_limit", 429, True),
        ("timeout", None, True),
        ("gundi", 503, True),
        ("gundi", None, True),
        ("gundi", 404, False),
        ("auth", 401, False),
        (None, None, False),
    ],
)
def test_is_retryable_failure(error_type, status_code, expected):
    assert is_retryable_failure(error_type, status_code) is expected


# --- internal actions run from their overrides alone --------------------------


def test_internal_action_runs_from_overrides_without_a_config_lookup(mocker, runner_mocks):
    handler = _install_handler(
        mocker, AsyncMock(return_value={}), action_id="pull_observations_by_date", config_model=MockSubActionConfiguration,
    )
    overrides = {"start_datetime": "2024-01-15T00:00:00+00:00", "end_datetime": "2024-01-16T00:00:00+00:00"}

    response = api_client.post("/", json=_pubsub_message("pull_observations_by_date", overrides))

    assert response.status_code == 200
    assert handler.called
    parsed = handler.call_args.kwargs["action_config"]
    assert isinstance(parsed, MockSubActionConfiguration)
    assert parsed.start_datetime.isoformat() == "2024-01-15T00:00:00+00:00"
    # Nothing in the portal describes an internal action: no stored-config
    # lookup, and therefore no reload of the integration on the miss.
    assert not runner_mocks.get_action_configuration.called
    assert not runner_mocks._fetch_integration_from_gundi.called


def test_internal_action_without_overrides_is_422(mocker, runner_mocks):
    handler = _install_handler(
        mocker, AsyncMock(), action_id="pull_observations_by_date", config_model=MockSubActionConfiguration,
    )

    response = api_client.post(
        "/v1/actions/execute/", json={"integration_id": INTEGRATION_ID, "action_id": "pull_observations_by_date"},
    )

    assert response.status_code == 422
    assert not handler.called
    assert not runner_mocks.get_action_configuration.called
