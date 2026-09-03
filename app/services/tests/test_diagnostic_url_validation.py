import asyncio
from unittest.mock import AsyncMock

import pytest

from app.services.webhooks import _validate_diagnostic_url


def _addrinfo(ip):
    # Matches the (family, type, proto, canonname, sockaddr) shape returned by getaddrinfo.
    return [(None, None, None, None, (ip, 443))]


def _mock_resolution(mocker, ip):
    mock_loop = mocker.MagicMock()
    mock_loop.getaddrinfo = AsyncMock(return_value=_addrinfo(ip))
    mocker.patch("app.services.webhooks.asyncio.get_running_loop", return_value=mock_loop)


@pytest.mark.asyncio
async def test_ipv4_mapped_ipv6_loopback_is_blocked(mocker):
    # ::ffff:127.0.0.1 parses as IPv6, so the plain IPv4 blocklist alone misses it.
    _mock_resolution(mocker, "::ffff:127.0.0.1")
    with pytest.raises(ValueError, match="private or reserved"):
        await _validate_diagnostic_url("https://example.com/hook")


@pytest.mark.asyncio
async def test_ipv4_mapped_ipv6_private_is_blocked(mocker):
    _mock_resolution(mocker, "::ffff:169.254.169.254")
    with pytest.raises(ValueError, match="private or reserved"):
        await _validate_diagnostic_url("https://example.com/hook")


@pytest.mark.asyncio
async def test_ipv6_multicast_is_blocked(mocker):
    _mock_resolution(mocker, "ff02::1")
    with pytest.raises(ValueError, match="private or reserved"):
        await _validate_diagnostic_url("https://example.com/hook")


@pytest.mark.asyncio
async def test_public_address_is_allowed(mocker):
    _mock_resolution(mocker, "93.184.216.34")
    await _validate_diagnostic_url("https://example.com/hook")


@pytest.mark.asyncio
async def test_background_forward_task_is_strongly_referenced(mocker):
    """asyncio holds only a weak reference to a running task, so a bare
    ensure_future() can be garbage-collected mid-flight and disappear silently."""
    import app.services.webhooks as webhooks

    started = asyncio.Event()
    release = asyncio.Event()

    async def _slow():
        started.set()
        await release.wait()

    task = webhooks._spawn_background_task(_slow())
    await started.wait()
    assert task in webhooks._background_tasks

    release.set()
    await task
    # The done callback must drop the reference so the set can't grow forever.
    assert task not in webhooks._background_tasks


@pytest.mark.asyncio
async def test_close_diagnostic_client_drains_in_flight_forwards(mocker):
    """Closing the shared client out from under an in-flight forward would fail
    every request still on the wire."""
    import app.services.webhooks as webhooks

    finished = []
    release = asyncio.Event()

    async def _slow():
        await release.wait()
        finished.append(True)

    mock_client = mocker.MagicMock()
    mock_client.aclose = mocker.AsyncMock(
        side_effect=lambda: finished.append("closed")
    )
    mocker.patch.object(webhooks, "_diagnostic_client", mock_client)

    webhooks._spawn_background_task(_slow())
    closing = asyncio.ensure_future(webhooks.close_diagnostic_client())
    await asyncio.sleep(0)  # let close() reach the drain
    assert not finished, "client closed before the in-flight forward finished"

    release.set()
    await closing
    assert finished[0] is True and finished[1] == "closed"
    assert mock_client.aclose.called


@pytest.mark.asyncio
async def test_close_diagnostic_client_cancels_forwards_that_outlive_the_drain_budget(mocker):
    """An unbounded drain would hold lifespan shutdown until the platform's
    SIGKILL, so the aclose() it protects would never run anyway."""
    import app.services.webhooks as webhooks

    async def _stuck():
        await asyncio.Event().wait()

    mock_client = mocker.MagicMock()
    mock_client.aclose = mocker.AsyncMock()
    mocker.patch.object(webhooks, "_diagnostic_client", mock_client)
    mocker.patch.object(webhooks, "_SHUTDOWN_DRAIN_TIMEOUT_SECONDS", 0.05)
    task = webhooks._spawn_background_task(_stuck())

    await asyncio.wait_for(webhooks.close_diagnostic_client(), timeout=2)

    assert task.cancelled()
    assert mock_client.aclose.called
    assert task not in webhooks._background_tasks


@pytest.mark.asyncio
async def test_hung_dns_resolution_is_rejected_not_awaited_forever(mocker):
    """httpx's timeout does not cover getaddrinfo, which runs in the default
    executor with no deadline; a hung resolver would pin the forward task (and
    the shutdown drain) indefinitely."""
    import app.services.webhooks as webhooks

    async def _never(*args, **kwargs):
        await asyncio.Event().wait()

    mock_loop = mocker.MagicMock()
    mock_loop.getaddrinfo = _never
    mocker.patch("app.services.webhooks.asyncio.get_running_loop", return_value=mock_loop)
    mocker.patch.object(webhooks, "_DNS_RESOLUTION_TIMEOUT_SECONDS", 0.05)

    with pytest.raises(ValueError, match="Timed out resolving"):
        await asyncio.wait_for(_validate_diagnostic_url("https://slow.example/hook"), timeout=2)


@pytest.mark.parametrize(
    "url, secret",
    [
        ("https://hooks.slack.com/services/T00000000/B00000000/abcdefSECRET", "abcdefSECRET"),
        ("https://discord.com/api/webhooks/123456789/xyzSECRETtoken", "xyzSECRETtoken"),
        ("https://user:pw@example.com/hook?token=querySECRET", "querySECRET"),
    ],
)
def test_redact_url_drops_path_userinfo_and_query(url, secret):
    """Slack, Discord and Teams incoming webhooks put the shared secret in the
    URL *path*, so keeping the path would leak the credential this function
    exists to protect."""
    from app.services.webhooks import _redact_url

    redacted = _redact_url(url)

    assert secret not in redacted
    assert "pw" not in redacted
    # The host is still there, which is what makes the log line useful.
    assert redacted in ("hooks.slack.com", "discord.com", "example.com")


def test_redact_url_keeps_host_and_port():
    from app.services.webhooks import _redact_url

    assert _redact_url("https://example.com:8443/a/b?c=d") == "example.com:8443"


def test_redact_url_survives_garbage():
    from app.services.webhooks import _redact_url

    assert _redact_url("not a url at all") == "<no-host>"


@pytest.mark.asyncio
async def test_forward_failure_log_does_not_carry_the_url_secret(mocker, caplog):
    """httpx embeds the request URL in its error text; logging str(e) would put
    the path secret _redact_url hides straight back into the WARNING."""
    import logging
    import httpx
    import app.services.webhooks as webhooks

    secret_url = "https://hooks.slack.com/services/T000/B000/XXXXSECRET"
    _mock_resolution(mocker, "93.184.216.34")
    request = httpx.Request("POST", secret_url)
    client = mocker.MagicMock()
    client.post = AsyncMock(side_effect=httpx.HTTPStatusError(
        f"Client error '404 Not Found' for url '{secret_url}'", request=request, response=httpx.Response(404, request=request),
    ))
    mocker.patch.object(webhooks, "_get_diagnostic_client", return_value=client)
    caplog.set_level(logging.WARNING, logger="app.services.webhooks")

    await webhooks.forward_payload_to_diagnostic_url(destination_url=secret_url, integration_id="abc", json_content={"a": 1})

    assert "XXXXSECRET" not in caplog.text
    assert "hooks.slack.com" in caplog.text and "HTTP 404" in caplog.text
