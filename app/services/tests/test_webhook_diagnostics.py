"""
Tests for diagnostic-URL forwarding in app/services/webhooks.py:
SSRF validation (_validate_diagnostic_url) and payload forwarding
(forward_payload_to_diagnostic_url).
"""
import asyncio
import socket
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.services import webhooks


PUBLIC_ADDRINFO = [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 0))]
PRIVATE_ADDRINFO = [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("10.1.2.3", 0))]


def _patch_dns(mocker, addrinfo):
    """Patch getaddrinfo on the running loop to return a fixed resolution."""
    loop = asyncio.get_event_loop()
    return mocker.patch.object(loop, "getaddrinfo", AsyncMock(return_value=addrinfo))


def _patch_client(mocker):
    """Patch the shared diagnostic httpx client; returns the mock client."""
    response = MagicMock()
    response.status_code = 200
    response.raise_for_status = MagicMock()
    client = MagicMock()
    client.post = AsyncMock(return_value=response)
    mocker.patch.object(webhooks, "_get_diagnostic_client", return_value=client)
    return client


# ── _validate_diagnostic_url ──────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_validate_rejects_non_https_scheme(mocker):
    with pytest.raises(ValueError, match="only 'https' is permitted"):
        await webhooks._validate_diagnostic_url("http://diagnostics.example.com/hook")


@pytest.mark.asyncio
async def test_validate_rejects_missing_hostname():
    with pytest.raises(ValueError, match="no hostname"):
        await webhooks._validate_diagnostic_url("https:///path-only")


@pytest.mark.asyncio
async def test_validate_rejects_hostname_not_in_allowlist(mocker):
    mocker.patch.object(webhooks.settings, "DIAGNOSTIC_URL_ALLOWLIST", ["diagnostics.example.com"])
    with pytest.raises(ValueError, match="not in the configured allowlist"):
        await webhooks._validate_diagnostic_url("https://evil.example.org/hook")


@pytest.mark.asyncio
async def test_validate_trims_whitespace_in_allowlist_entries(mocker):
    # env.list("A, B") yields entries with leading spaces; they must still match.
    mocker.patch.object(
        webhooks.settings,
        "DIAGNOSTIC_URL_ALLOWLIST",
        ["other.example.com", " diagnostics.example.com "],
    )
    _patch_dns(mocker, PUBLIC_ADDRINFO)
    await webhooks._validate_diagnostic_url("https://diagnostics.example.com/hook")


@pytest.mark.asyncio
async def test_validate_rejects_private_address_resolution(mocker):
    mocker.patch.object(webhooks.settings, "DIAGNOSTIC_URL_ALLOWLIST", [])
    _patch_dns(mocker, PRIVATE_ADDRINFO)
    with pytest.raises(ValueError, match="private or reserved address"):
        await webhooks._validate_diagnostic_url("https://diagnostics.example.com/hook")


@pytest.mark.asyncio
async def test_validate_rejects_unresolvable_hostname(mocker):
    mocker.patch.object(webhooks.settings, "DIAGNOSTIC_URL_ALLOWLIST", [])
    loop = asyncio.get_event_loop()
    mocker.patch.object(loop, "getaddrinfo", AsyncMock(side_effect=OSError("no such host")))
    with pytest.raises(ValueError, match="Cannot resolve"):
        await webhooks._validate_diagnostic_url("https://diagnostics.example.com/hook")


@pytest.mark.asyncio
async def test_validate_accepts_public_address(mocker):
    mocker.patch.object(webhooks.settings, "DIAGNOSTIC_URL_ALLOWLIST", [])
    _patch_dns(mocker, PUBLIC_ADDRINFO)
    await webhooks._validate_diagnostic_url("https://diagnostics.example.com/hook")


# ── forward_payload_to_diagnostic_url ─────────────────────────────────────────

@pytest.mark.asyncio
async def test_forward_posts_dict_payload_with_metadata(mocker):
    mocker.patch.object(webhooks.settings, "DIAGNOSTIC_URL_ALLOWLIST", [])
    _patch_dns(mocker, PUBLIC_ADDRINFO)
    client = _patch_client(mocker)

    await webhooks.forward_payload_to_diagnostic_url(
        destination_url="https://diagnostics.example.com/hook",
        integration_id="test-integration-id",
        json_content={"device": "abc", "value": 1},
    )

    client.post.assert_called_once()
    body = client.post.call_args.kwargs["json"]
    assert body["device"] == "abc"
    metadata = body["__gundi_diagnostic_metadata"]
    assert metadata["integration_id"] == "test-integration-id"
    # received_at must be a tz-aware UTC timestamp in RFC 3339 "Z" form (the
    # template replaces the +00:00 offset with Z). Python 3.10's fromisoformat
    # cannot parse "Z", so normalise before parsing.
    received_at = metadata["received_at"]
    assert received_at.endswith("Z")
    assert "+00:00" not in received_at
    assert datetime.fromisoformat(received_at.replace("Z", "+00:00")).tzinfo is not None


@pytest.mark.asyncio
async def test_forward_wraps_non_dict_payload(mocker):
    mocker.patch.object(webhooks.settings, "DIAGNOSTIC_URL_ALLOWLIST", [])
    _patch_dns(mocker, PUBLIC_ADDRINFO)
    client = _patch_client(mocker)

    await webhooks.forward_payload_to_diagnostic_url(
        destination_url="https://diagnostics.example.com/hook",
        integration_id="test-integration-id",
        json_content=[{"a": 1}, {"b": 2}],
    )

    body = client.post.call_args.kwargs["json"]
    assert body["payload"] == [{"a": 1}, {"b": 2}]
    assert "__gundi_diagnostic_metadata" in body


@pytest.mark.asyncio
async def test_forward_swallows_validation_failure_without_posting(mocker):
    client = _patch_client(mocker)

    # http scheme fails validation; the error must be logged, not raised.
    await webhooks.forward_payload_to_diagnostic_url(
        destination_url="http://diagnostics.example.com/hook",
        integration_id="test-integration-id",
        json_content={"a": 1},
    )

    client.post.assert_not_called()


@pytest.mark.asyncio
async def test_forward_swallows_http_errors(mocker):
    import httpx

    mocker.patch.object(webhooks.settings, "DIAGNOSTIC_URL_ALLOWLIST", [])
    _patch_dns(mocker, PUBLIC_ADDRINFO)
    client = _patch_client(mocker)
    client.post.return_value.raise_for_status.side_effect = httpx.HTTPStatusError(
        "502", request=MagicMock(), response=MagicMock(status_code=502)
    )

    # Must not raise — diagnostic forwarding is best-effort.
    await webhooks.forward_payload_to_diagnostic_url(
        destination_url="https://diagnostics.example.com/hook",
        integration_id="test-integration-id",
        json_content={"a": 1},
    )
