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
