from datetime import datetime
import json
import pytest

from app.actions.rmwhub import RmwHubAdapter
from requests.models import Response
from app.actions.rmwhub import RmwHubClient


@pytest.mark.asyncio
# TODO: Rewrite test
async def test_rmwhub_download_data(mocker, mock_rmwhub_response, a_good_configuration):
    """
    Test rmwhub.download_data
    """

    # Setup mock_rmwhub_client
    mocker.patch(
        "app.actions.rmwhub.RmwHubClient.search_hub",
        return_value=json.dumps(mock_rmwhub_response),
    )

    from app.actions.rmwhub import RmwHubAdapter

    rmwadapter = RmwHubAdapter(
        a_good_configuration.api_key, a_good_configuration.rmw_url
    )
    updates, deletes = rmwadapter.download_data(str(datetime.now().isoformat()))

    assert len(updates) == 5
    assert len(deletes) == 5


@pytest.mark.asyncio
# TODO: rewrite test
async def test_rmwhub_process_updates(mock_rmwhub_items, a_good_configuration):
    """
    Test rmwhub.process_updates
    """

    rmwadapter = RmwHubAdapter(
        a_good_configuration.api_key, a_good_configuration.rmw_url
    )
    updates, _ = mock_rmwhub_items
    observations = rmwadapter.process_updates_search_others(updates)

    assert len(observations) == 7


@pytest.mark.asyncio
async def test_rmwhub_search_hub(mocker, a_good_configuration):
    """
    Test rmwhub.search_hub
    """

    # Setup mock response
    mock_response = {
        "sets": [
            {
                "set_id": "set1",
                "deployment_type": "trawl",
                "traps": [{"sequence": 0, "latitude": 10.0, "longitude": 20.0}],
            },
            {
                "set_id": "set2",
                "deployment_type": "trawl",
                "traps": [{"sequence": 0, "latitude": 30.0, "longitude": 40.0}],
            },
        ]
    }

    mock_response_text = json.dumps(mock_response)

    # Mock requests.post to return the mock response
    def mock_post(*args, **kwargs):
        response = Response()
        response.status_code = 200
        response._content = str.encode(mock_response_text)
        return response

    mocker.patch("requests.post", side_effect=mock_post)

    rmw_client = RmwHubClient(
        a_good_configuration.api_key, a_good_configuration.rmw_url
    )
    start_datetime_str = datetime.now().isoformat()
    minute_interval = 60
    response = rmw_client.search_hub(start_datetime_str, minute_interval)

    assert response == mock_response_text


@pytest.mark.asyncio
async def test_rmwhub_search_hub_failure(mocker, a_good_configuration):
    """
    Test rmwhub.search_hub failure
    """

    # Mock requests.post to return a failed response
    def mock_post(*args, **kwargs):
        response = Response()
        response.status_code = 500
        response._content = b"Internal Server Error"
        return response

    mocker.patch("requests.post", side_effect=mock_post)

    rmw_client = RmwHubClient(
        a_good_configuration.api_key, a_good_configuration.rmw_url
    )
    start_datetime_str = datetime.now().isoformat()
    minute_interval = 60
    response = rmw_client.search_hub(start_datetime_str, minute_interval)

    assert response == "Internal Server Error"
