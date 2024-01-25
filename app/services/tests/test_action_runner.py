from unittest.mock import MagicMock
import pytest

from app.conftest import async_return, AsyncMock
from app.services.action_runner import execute_action
from fastapi.testclient import TestClient
from app.main import app


api_client = TestClient(app)


@pytest.mark.asyncio
async def test_execute_action_from_pubsub(
        mocker, mock_gundi_client_v2, event_v2_cloud_event_headers, event_v2_cloud_event_payload
):
    mock_pull_observations = AsyncMock()
    mock_action_handlers = MagicMock()
    mock_action_handlers.__getitem__.return_value = mock_pull_observations
    mock_publish_event = MagicMock()
    mocker.patch("app.services.action_runner.action_handlers", mock_action_handlers)
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    response = api_client.post(
        "/",
        headers=event_v2_cloud_event_headers,
        json=event_v2_cloud_event_payload,
    )
    assert response.status_code == 200
    assert mock_gundi_client_v2.get_integration_details.called
    assert mock_action_handlers["pull_observations"].called
