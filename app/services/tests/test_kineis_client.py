"""Tests for Kineis API client (CONNECTORS-836)."""

import pytest
from unittest.mock import AsyncMock, MagicMock

import httpx

from app.services.kineis_client import (
    get_access_token,
    get_cached_token,
    clear_token_cache,
    retrieve_bulk_telemetry,
    retrieve_realtime_telemetry,
    retrieve_device_list,
    fetch_device_list,
    fetch_telemetry,
    fetch_telemetry_realtime,
)


@pytest.mark.asyncio
async def test_get_access_token_success(mocker):
    """Auth endpoint returns access_token and expires_in."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "access_token": "test-token-123",
        "expires_in": 300,
        "refresh_token": "refresh-xyz",
        "token_type": "Bearer",
    }
    mock_response.raise_for_status = MagicMock()

    mock_post = AsyncMock(return_value=mock_response)
    mocker.patch("app.services.kineis_client.settings.KINEIS_AUTH_BASE_URL", "https://account.example.com")
    mocker.patch("app.services.kineis_client._auth_path", return_value="/a")
    mock_client = MagicMock()
    mock_client.post = mock_post
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)
    mocker.patch("app.services.kineis_client.httpx.AsyncClient", return_value=mock_client)

    result = await get_access_token(username="u", password="p")

    assert result["access_token"] == "test-token-123"
    assert result["expires_in"] == 300
    assert mock_post.called


@pytest.mark.asyncio
async def test_get_cached_token_uses_cache(mocker):
    """Cached token is returned when not expired."""
    clear_token_cache("int-1")
    mocker.patch("app.services.kineis_client.get_access_token", AsyncMock(side_effect=AssertionError("should not call")))
    # Pre-populate cache
    from app.services.kineis_client import _token_cache, _token_cache_key
    import time
    _token_cache[_token_cache_key("int-1")] = ("cached-token", time.time() + 120)

    token = await get_cached_token("int-1", "u", "p", min_ttl_seconds=60)
    assert token == "cached-token"
    clear_token_cache("int-1")


@pytest.mark.asyncio
async def test_retrieve_bulk_telemetry_single_page(mocker):
    """Bulk endpoint returns single page of messages."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "contents": [
            {"deviceRef": "D1", "recordedAt": "2024-01-15T10:00:00.000Z", "gps": {"lat": -1.5, "lon": 30.2}},
        ],
        "pageInfo": {"hasNextPage": False},
    }
    mock_response.raise_for_status = MagicMock()

    mock_post = AsyncMock(return_value=mock_response)
    mocker.patch("app.services.kineis_client.settings.KINEIS_API_BASE_URL", "https://api.example.com")

    mock_client = MagicMock()
    mock_client.post = mock_post
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)
    mocker.patch("app.services.kineis_client.httpx.AsyncClient", return_value=mock_client)

    result = await retrieve_bulk_telemetry(
        access_token="token",
        from_datetime="2024-01-15T00:00:00.000Z",
        to_datetime="2024-01-15T12:00:00.000Z",
        page_size=100,
    )

    assert len(result) == 1
    assert result[0]["deviceRef"] == "D1"
    assert result[0]["gps"]["lat"] == -1.5
    call_args = mock_post.call_args
    assert call_args[1]["json"]["fromDatetime"] == "2024-01-15T00:00:00.000Z"
    assert call_args[1]["json"]["pagination"]["first"] == 100
    assert call_args[1]["headers"]["Authorization"] == "Bearer token"


@pytest.mark.asyncio
async def test_retrieve_bulk_telemetry_paginated(mocker):
    """Bulk endpoint paginates until hasNextPage is false."""
    mock_post = AsyncMock()
    mocker.patch("app.services.kineis_client.settings.KINEIS_API_BASE_URL", "https://api.example.com")

    def side_effect(*args, **kwargs):
        body = kwargs.get("json", {})
        after = body.get("pagination", {}).get("after")
        if after is None:
            return MagicMock(
                status_code=200,
                json=lambda: {
                    "contents": [{"deviceRef": "A", "recordedAt": "2024-01-15T10:00:00.000Z", "gps": {"lat": 0, "lon": 0}}],
                    "pageInfo": {"hasNextPage": True, "endCursor": "cursor1"},
                },
                raise_for_status=MagicMock(),
            )
        return MagicMock(
            status_code=200,
            json=lambda: {
                "contents": [{"deviceRef": "B", "recordedAt": "2024-01-15T11:00:00.000Z", "gps": {"lat": 1, "lon": 1}}],
                "pageInfo": {"hasNextPage": False},
            },
            raise_for_status=MagicMock(),
        )

    mock_post.side_effect = side_effect

    mock_client = MagicMock()
    mock_client.post = mock_post
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)
    mocker.patch("app.services.kineis_client.httpx.AsyncClient", return_value=mock_client)

    result = await retrieve_bulk_telemetry(
        access_token="t",
        from_datetime="2024-01-15T00:00:00.000Z",
        to_datetime="2024-01-15T12:00:00.000Z",
        page_size=1,
    )

    assert len(result) == 2
    assert result[0]["deviceRef"] == "A"
    assert result[1]["deviceRef"] == "B"
    assert mock_post.call_count == 2


@pytest.mark.asyncio
async def test_fetch_telemetry_clears_cache_on_401(mocker):
    """On 401 from bulk endpoint, token cache is cleared."""
    import app.services.kineis_client as kineis_client
    clear_token_cache("int-401")
    mocker.patch.object(kineis_client, "get_cached_token", AsyncMock(return_value="bad-token"))
    mocker.patch.object(
        kineis_client,
        "retrieve_bulk_telemetry",
        AsyncMock(side_effect=httpx.HTTPStatusError("Unauthorized", request=MagicMock(), response=MagicMock(status_code=401))),
    )
    # Call the inner function to avoid stamina retries (which would hang)
    inner = kineis_client.fetch_telemetry
    while hasattr(inner, "__wrapped__"):
        inner = inner.__wrapped__

    with pytest.raises(httpx.HTTPStatusError):
        await inner(
            integration_id="int-401",
            username="u",
            password="p",
            from_datetime="2024-01-15T00:00:00.000Z",
            to_datetime="2024-01-15T12:00:00.000Z",
        )

    assert kineis_client._token_cache_key("int-401") not in kineis_client._token_cache


@pytest.mark.asyncio
async def test_retrieve_realtime_telemetry_returns_messages_and_checkpoint(mocker):
    """Realtime endpoint returns contents and new checkpoint."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "contents": [
            {"deviceRef": "D1", "msgTs": 1705312800000, "gpsLocLat": -1.0, "gpsLocLon": 30.0},
        ],
        "checkpoint": 1727798490000,
    }
    mock_response.raise_for_status = MagicMock()

    mock_post = AsyncMock(return_value=mock_response)
    mocker.patch("app.services.kineis_client.settings.KINEIS_API_BASE_URL", "https://api.example.com")

    mock_client = MagicMock()
    mock_client.post = mock_post
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)
    mocker.patch("app.services.kineis_client.httpx.AsyncClient", return_value=mock_client)

    messages, new_checkpoint = await retrieve_realtime_telemetry(
        access_token="token",
        checkpoint=0,
    )

    assert len(messages) == 1
    assert messages[0]["deviceRef"] == "D1"
    assert new_checkpoint == 1727798490000
    call_args = mock_post.call_args
    assert call_args[1]["json"]["fromCheckpoint"] == 0
    assert call_args[1]["json"]["retrieveGpsLoc"] is True
    assert "retrieve-realtime" in call_args[0][0]


@pytest.mark.asyncio
async def test_retrieve_device_list_returns_contents(mocker):
    """Device list endpoint returns contents list."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "contents": [
            {"deviceUid": 67899, "deviceRef": "7896", "customerName": "WILDLIFE COMPUTER"},
        ],
    }
    mock_response.raise_for_status = MagicMock()

    mock_post = AsyncMock(return_value=mock_response)
    mocker.patch("app.services.kineis_client.settings.KINEIS_API_BASE_URL", "https://api.example.com")

    mock_client = MagicMock()
    mock_client.post = mock_post
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)
    mocker.patch("app.services.kineis_client.httpx.AsyncClient", return_value=mock_client)

    result = await retrieve_device_list(access_token="token")

    assert len(result) == 1
    assert result[0]["deviceUid"] == 67899
    assert result[0]["customerName"] == "WILDLIFE COMPUTER"
    call_args = mock_post.call_args
    assert "retrieve-device-list" in call_args[0][0]
    assert call_args[1]["headers"]["Authorization"] == "Bearer token"
