from datetime import datetime, timezone
import httpx
import pytest
from unittest.mock import AsyncMock
from app.actions.handlers import (
    get_observations,
)

@pytest.mark.asyncio
async def test_get_observations(mocker, a_good_history_result):

    mocker.patch('app.bluetrax.httpx.AsyncClient.post', return_value=httpx.Response(200, json=a_good_history_result))

    result = await get_observations("user1", "unit1", datetime(2025, 2, 1, tzinfo=timezone.utc), None)

    assert result.response == "success"


@pytest.mark.asyncio
async def test_get_observations_negative(mocker):
    # Mimic a 405 for the actual case where the API has been changed from GET to POST.
    mocker.patch('app.bluetrax.httpx.AsyncClient.post', return_value=httpx.Response(405, json={"response": "error"}, request=AsyncMock()))

    with pytest.raises(httpx.HTTPStatusError):
        result = await get_observations("user1", "unit1", datetime(2025, 2, 1, tzinfo=timezone.utc), None)


