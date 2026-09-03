from unittest.mock import AsyncMock
import httpx
from pathlib import Path
import re
import pytest
from app.services.gundi import (
    send_events_to_gundi,
    send_observations_to_gundi,
    send_event_attachments_to_gundi,
    send_messages_to_gundi,
    _get_gundi_api_key,
    EphemeralWriteBlocked,
)
from app.services.activity_logger import ephemeral_run


@pytest.mark.asyncio
async def test_send_events_to_gundi(
        mocker, mock_gundi_client_v2_class, mock_gundi_sensors_client_class,
        mock_get_gundi_api_key, integration_v2
):
    mocker.patch("app.services.gundi.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("app.services.gundi.GundiDataSenderClient", mock_gundi_sensors_client_class)
    mocker.patch("app.services.gundi._get_gundi_api_key", mock_get_gundi_api_key)
    events = [
        {
            "title": "Animal Sighting",
            "event_type": "wildlife_sighting_rep",
            "recorded_at": "2024-01-08 21:51:10-03:00",
            "location": {
                "lat": -51.688645,
                "lon": -72.704421
            },
            "event_details": {
                "site_name": "MM Spot",
                "species": "lion"
            }
        },
        {
            "title": "Animal Sighting",
            "event_type": "wildlife_sighting_rep",
            "recorded_at": "2024-01-08 21:51:10-03:00",
            "location": {
                "lat": -51.688645,
                "lon": -72.704421
            },
            "event_details": {
                "site_name": "MM Spot",
                "species": "lion"
            }
        }
    ]
    response = await send_events_to_gundi(
        events=events,
        integration_id=integration_v2.id
    )

    # Data is sent to gundi using the REST API for now
    assert len(response) == 2
    assert mock_gundi_sensors_client_class.called
    mock_gundi_sensors_client_class.return_value.post_events.assert_called_once_with(data=events)


@pytest.mark.asyncio
async def test_send_event_attachments_to_gundi(
        mocker, mock_gundi_client_v2_class, mock_gundi_sensors_client_class,
        mock_get_gundi_api_key, integration_v2
):
    mocker.patch("app.services.gundi.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("app.services.gundi.GundiDataSenderClient", mock_gundi_sensors_client_class)
    mocker.patch("app.services.gundi._get_gundi_api_key", mock_get_gundi_api_key)
    attachments = [
        ("file1.png", b'\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x01\x00x\x00x\x00\x00\xff\xdb\x00C\x00\x02\x01\x01\x02'),
        ("file2.png", b'\xff\xd8\xff\xe0\x00\x10JFIF\x00\x06\x01\x01\x00x\x00x\x01\x00\xff\xd5\x00C\x00\x98\x01\x01\x56')
    ]
    response = await send_event_attachments_to_gundi(
        event_id="dummy-1234",
        attachments=attachments,
        integration_id=integration_v2.id
    )

    # Data is sent to gundi using the REST API for now
    assert len(response) == 2
    assert mock_gundi_sensors_client_class.called
    mock_gundi_sensors_client_class.return_value.post_event_attachments.assert_called_once_with(
        event_id="dummy-1234",
        attachments=attachments
    )


@pytest.mark.asyncio
async def test_send_observations_to_gundi(
        mocker, mock_gundi_client_v2_class, mock_gundi_sensors_client_class,
        mock_get_gundi_api_key, integration_v2
):
    mocker.patch("app.services.gundi.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("app.services.gundi.GundiDataSenderClient", mock_gundi_sensors_client_class)
    mocker.patch("app.services.gundi._get_gundi_api_key", mock_get_gundi_api_key)
    observations = [
        {
            "source": "device-xy123",
            "type": "tracking-device",
            "subject_type": "puma",
            "recorded_at": "2024-01-24 09:03:00-0300",
            "location": {
                "lat": -51.748,
                "lon": -72.720
            },
            "additional": {
                "speed_kmph": 5
            }
        },
        {
            "source": "test-device-mariano",
            "type": "tracking-device",
            "subject_type": "puma",
            "recorded_at": "2024-01-24 09:05:00-0300",
            "location": {
                "lat": -51.755,
                "lon": -72.755
            },
            "additional": {
                "speed_kmph": 5
            }
        }
    ]
    response = await send_observations_to_gundi(
        observations=observations,
        integration_id=integration_v2.id
    )

    # Data is sent to gundi using the REST API for now
    assert len(response) == 2
    assert mock_gundi_sensors_client_class.called
    mock_gundi_sensors_client_class.return_value.post_observations.assert_called_once_with(data=observations)


@pytest.mark.parametrize("fn_and_args", [
    (send_events_to_gundi, {"events": [], "integration_id": "id"}),
    (send_observations_to_gundi, {"observations": [], "integration_id": "id"}),
    (send_event_attachments_to_gundi, {"event_id": "e", "attachments": [], "integration_id": "id"}),
    (send_messages_to_gundi, {"messages": [], "integration_id": "id"}),
    (_get_gundi_api_key, {"integration_id": "id"}),
])
@pytest.mark.asyncio
async def test_gundi_helpers_block_on_ephemeral_run(fn_and_args):
    # A reference handler running against a draft integration must not be
    # able to move data through Gundi (design invariant: reference actions
    # are read-only). Each entry point checks the contextvar and raises
    # before doing any I/O.
    fn, args = fn_and_args
    token = ephemeral_run.set(True)
    try:
        with pytest.raises(EphemeralWriteBlocked):
            await fn(**args)
    finally:
        ephemeral_run.reset(token)


def _worst_case_waits(policy: dict) -> list:
    """The sleeps tenacity performs between attempts when every jitter draw is
    maximal: min(initial * 2**n + jitter, max) for n = 0 .. attempts-2."""
    return [
        min(policy["wait_initial"] * 2 ** n + policy["wait_jitter"], policy["wait_max"])
        for n in range(policy["attempts"] - 1)
    ]


def test_gundi_api_retry_policy_reaches_every_declared_attempt():
    """stamina combines `attempts` and `timeout` with stop_any(), so the tighter
    one wins. A policy whose waits add up to more than its timeout declares
    attempts (and a wait_max) it can never reach; the effective policy is then
    whatever the timeout happens to allow, which is how the old 10-20-40 s
    curve silently ran three attempts under stamina's default 45 s budget."""
    from app.services.gundi import GUNDI_API_RETRY

    assert GUNDI_API_RETRY["attempts"] and GUNDI_API_RETRY["timeout"], "both stops must be explicit"
    waits = _worst_case_waits(GUNDI_API_RETRY)

    # Even with maximal jitter and instant calls, every declared attempt runs.
    assert sum(waits) <= GUNDI_API_RETRY["timeout"], (
        f"waits total {sum(waits)}s, more than the {GUNDI_API_RETRY['timeout']}s budget: "
        f"only part of the declared {GUNDI_API_RETRY['attempts']} attempts can ever run"
    )
    # wait_max is a real cap on the curve, not a decorative number.
    assert waits[-1] == GUNDI_API_RETRY["wait_max"]


def test_gundi_api_retry_loop_overhead_is_bounded():
    """tenacity's stop_after_delay is checked after a failed attempt and then the
    full wait is slept, so the retry loop's own overhead for one failing call is
    bounded by timeout + wait_max. This does not include the requests: the
    sensors client's httpx timeout is 120 s per attempt, so a hanging API adds
    up to that per attempt on top. PubSub messages are processed inline in the
    push request by default (PROCESS_PUBSUB_MESSAGES_IN_BACKGROUND is False),
    so the loop's share of the budget has to stay small."""
    from app.services.gundi import GUNDI_API_RETRY

    overhead = GUNDI_API_RETRY["timeout"] + GUNDI_API_RETRY["wait_max"]
    assert overhead <= 150, f"the retry loop alone may hold a request for {overhead}s before any request time"


_SERVICES_DIR = Path(__file__).resolve().parents[1]


def test_all_gundi_api_helpers_share_one_retry_policy():
    """The helpers used to repeat the same decorator by hand, which is how the
    wait curve and the stop condition drifted apart in the first place. Source
    inspection is the only way to check decorators applied at import time; the
    config-manager reload is checked behaviourally below."""
    source = (_SERVICES_DIR / "gundi.py").read_text()
    decorators = re.findall(r"@stamina\.retry\(\s*([^)]*?)\s*\)", source)
    assert decorators, "expected retry-decorated functions in gundi.py"
    assert all(d == "**GUNDI_API_RETRY" for d in decorators), decorators


def test_api_key_lookup_is_not_retried_on_its_own():
    """_get_gundi_api_key runs inside the send helpers' retry; a decorator of
    its own would nest a second GUNDI_API_RETRY inside the first and restart
    the inner attempts on every outer one, so a failing portal would cost up to
    36 calls instead of 6 and blow the loop-overhead bound the tests above pin."""
    assert not hasattr(_get_gundi_api_key, "__wrapped__")
    for helper in (send_events_to_gundi, send_observations_to_gundi, send_event_attachments_to_gundi, send_messages_to_gundi):
        assert hasattr(helper, "__wrapped__"), helper.__name__


@pytest.mark.asyncio
async def test_config_manager_reload_uses_the_shared_gundi_retry_policy(
        mocker, mock_redis_empty, mock_gundi_client_v2_class, integration_v2,
):
    """_reload_integration_from_gundi is a Gundi API call like the helpers; its
    retry must be the same policy, checked by spying on the call rather than
    grepping source."""
    import stamina
    from app.services.config_manager import IntegrationConfigurationManager
    from app.services.gundi import GUNDI_API_RETRY

    mocker.patch("app.services.config_manager.redis", mock_redis_empty)
    mocker.patch("app.services.config_manager.GundiClient", mock_gundi_client_v2_class)
    spy = mocker.spy(stamina, "retry_context")

    await IntegrationConfigurationManager().get_integration(str(integration_v2.id))

    http_policies = [c.kwargs for c in spy.call_args_list if c.kwargs.get("on") is httpx.HTTPError]
    assert http_policies, "expected the reload to retry on httpx.HTTPError"
    assert all(kw == GUNDI_API_RETRY for kw in http_policies), http_policies


@pytest.mark.asyncio
async def test_webhook_integration_lookup_does_not_add_its_own_retry_loop(mocker):
    """get_integration used to wrap get_integration_details in a second retry
    loop. The reload inside already retries the same call, so the two nested
    multiplicatively; worse, the outer loop iterated stamina synchronously
    inside a coroutine, sleeping the whole event loop between attempts. One
    call to get_integration_details per lookup, and the failure is reported."""
    from unittest.mock import MagicMock
    import app.services.webhooks as webhooks
    from gundi_core.events import IntegrationWebhookFailed

    lookup = mocker.patch.object(
        webhooks.config_manager, "get_integration_details",
        AsyncMock(side_effect=httpx.ConnectError("portal unreachable")),
    )
    publish = mocker.patch.object(webhooks, "publish_event", AsyncMock())
    request = MagicMock()
    request.headers = {"x-gundi-integration-id": "abc-123"}
    request.query_params = {}

    integration = await webhooks.get_integration(request)

    assert integration is None
    assert lookup.await_count == 1
    assert isinstance(publish.call_args.kwargs["event"], IntegrationWebhookFailed)
