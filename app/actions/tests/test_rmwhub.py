from datetime import datetime
import json
import pytest

from app.actions.rmwhub import RmwHubAdapter, RmwSets
from app.actions.rmwhub import RmwHubClient


@pytest.mark.asyncio
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
        a_good_configuration.api_key,
        a_good_configuration.rmw_url,
        "super_secret_token",
        "er.destination.com",
    )
    start_datetime_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    minute_interval = 5
    rmw_sets = await rmwadapter.download_data(start_datetime_str, minute_interval)

    assert len(rmw_sets.sets) == 5


@pytest.mark.asyncio
# TODO: rewrite test
async def test_process_rmw_download(mocker, mock_rmwhub_items, a_good_configuration):
    """
    Test rmwhub.process_updates
    """

    # Setup mock_rmwhub_client
    mocker.patch(
        "app.actions.buoy.BuoyClient.get_er_subjects",
        return_value=[],
    )

    mocker.patch(
        "app.actions.buoy.BuoyClient.patch_er_subject_status",
        return_value=json.dumps(None),
    )

    rmwadapter = RmwHubAdapter(
        a_good_configuration.api_key,
        a_good_configuration.rmw_url,
        "super_secret_token",
        "er.destination.com",
    )

    rmw_sets = RmwSets(sets=mock_rmwhub_items)
    start_datetime_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    minute_interval = 5
    observations = await rmwadapter.process_rmw_download(
        rmw_sets, start_datetime_str, minute_interval
    )

    assert len(observations) == 9


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

    mocker.patch(
        "app.actions.rmwhub.RmwHubClient.search_hub",
        return_value=mock_response_text,
    )

    rmw_client = RmwHubClient(
        a_good_configuration.api_key, a_good_configuration.rmw_url
    )
    start_datetime_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    minute_interval = 5
    response = await rmw_client.search_hub(start_datetime_str, minute_interval)

    assert response == mock_response_text


@pytest.mark.asyncio
async def test_rmwhub_search_hub_failure(mocker, a_good_configuration):
    """
    Test rmwhub.search_hub failure
    """

    mocker.patch(
        "app.actions.rmwhub.RmwHubClient.search_hub",
        return_value="Internal Server Error",
    )

    rmw_client = RmwHubClient(
        a_good_configuration.api_key, a_good_configuration.rmw_url
    )
    start_datetime_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    minute_interval = 60
    response = await rmw_client.search_hub(start_datetime_str, minute_interval)

    assert response == "Internal Server Error"
