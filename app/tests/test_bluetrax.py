import pytest
import httpx
from datetime import datetime
from unittest.mock import AsyncMock
from pydantic import ValidationError
from app.bluetrax import get_asset_history, HistoryResult


@pytest.mark.asyncio
async def test_get_asset_history_success(mocker, a_good_history_result):
    """Test get_asset_history with a successful API response."""

    # Define test input
    unit_id = "12345"
    start_time = datetime(2024, 1, 1, 12, 0, 0)
    end_time = datetime(2024, 1, 1, 14, 0, 0)
    
    # Mock API response
    mock_response_data = a_good_history_result
    mock_history_result = HistoryResult(**mock_response_data)

    async def mock_post(*args, **kwargs):
        return httpx.Response(200, json=mock_response_data)

    mocker.patch("httpx.AsyncClient.post", new=mock_post)

    result = await get_asset_history(unit_id, start_time, end_time)

    assert isinstance(result, HistoryResult)
    assert result == mock_history_result


@pytest.mark.asyncio
async def test_get_asset_history_validation_error(mocker, a_bad_history_result):
    """Test get_asset_history when API response fails pydantic validation."""

    unit_id = "12345"
    start_time = datetime(2024, 1, 1, 12, 0, 0)
    end_time = datetime(2024, 1, 1, 14, 0, 0)
    
    async def mock_post(*args, **kwargs):
        return httpx.Response(200, json=a_bad_history_result)

    mocker.patch("httpx.AsyncClient.post", new=mock_post)

    with pytest.raises(ValidationError):
        await get_asset_history(unit_id, start_time, end_time)


@pytest.mark.asyncio
async def test_get_asset_history_http_error(mocker):
    """Test get_asset_history when API returns an HTTP error."""

    unit_id = "12345"
    start_time = datetime(2024, 1, 1, 12, 0, 0)
    end_time = datetime(2024, 1, 1, 14, 0, 0)

    async def mock_post(*args, **kwargs):
        return httpx.Response(500, json={"error": "Internal Server Error"}, request=AsyncMock())

    mocker.patch("httpx.AsyncClient.post", new=mock_post)

    with pytest.raises(httpx.HTTPStatusError):
        await get_asset_history(unit_id, start_time, end_time)