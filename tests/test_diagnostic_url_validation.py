from unittest.mock import AsyncMock

import pytest

from gundi_action_runner.services.webhooks import _validate_diagnostic_url


def _addrinfo(ip):
    # Matches the (family, type, proto, canonname, sockaddr) shape returned by getaddrinfo.
    return [(None, None, None, None, (ip, 443))]


def _mock_resolution(mocker, ip):
    mock_loop = mocker.MagicMock()
    mock_loop.getaddrinfo = AsyncMock(return_value=_addrinfo(ip))
    mocker.patch("gundi_action_runner.services.webhooks.asyncio.get_running_loop", return_value=mock_loop)


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
