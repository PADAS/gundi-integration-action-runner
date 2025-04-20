from datetime import datetime, timezone, timedelta
import dateparser
import time
from app.actions.buoy import BuoyClient
import json
import pytest

from app.actions.rmwhub import RmwHubAdapter
from app.actions.rmwhub import RmwHubClient
from app.conftest import AsyncMock


@pytest.mark.asyncio
async def test_rmwhub_adapter_download_data(
    mocker, get_mock_rmwhub_data, a_good_configuration, a_good_integration
):
    """
    Test rmwhub.download_data
    """

    # Setup mock_rmwhub_client
    mocker.patch(
        "app.actions.rmwhub.RmwHubClient.search_hub",
        return_value=json.dumps(get_mock_rmwhub_data),
    )

    from app.actions.rmwhub import RmwHubAdapter

    rmwadapter = RmwHubAdapter(
        a_good_integration.id,
        a_good_configuration.api_key,
        a_good_configuration.rmw_url,
        "super_secret_token",
        "er.destination.com",
    )
    start_datetime_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    minute_interval = 5
    rmw_sets = await rmwadapter.download_data(start_datetime_str, minute_interval)

    assert len(rmw_sets) == 5


@pytest.mark.asyncio
# TODO: rewrite test
async def test_rmw_adapter_process_download(
    mocker, mock_rmwhub_items, a_good_configuration, a_good_integration
):
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
        a_good_integration.id,
        a_good_configuration.api_key,
        a_good_configuration.rmw_url,
        "super_secret_token",
        "er.destination.com",
    )

    rmw_sets = mock_rmwhub_items
    start_datetime_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    minute_interval = 5
    observations = await rmwadapter.process_download(
        rmw_sets, start_datetime_str, minute_interval
    )

    assert len(observations) == 9


@pytest.mark.asyncio
async def test_rmwhub_adapter_search_hub(mocker, a_good_configuration):
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
    response = await rmw_client.search_hub(start_datetime_str)

    assert response == mock_response_text


@pytest.mark.asyncio
async def test_rmwhub_adapter_search_hub_failure(mocker, a_good_configuration):
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
    response = await rmw_client.search_hub(start_datetime_str)

    assert response == "Internal Server Error"


@pytest.mark.asyncio
async def test_rmwhub_adapter_process_upload_insert_success(
    mocker,
    a_good_configuration,
    a_good_integration,
    mock_rmwhub_items,
    mock_rmw_upload_response,
    mock_er_subjects,
    mock_er_subjects_from_rmw,
    mock_get_latest_observations,
):
    """
    Test RmwHubAdapter.process_upload insert operations
    """

    rmw_adapter = RmwHubAdapter(
        a_good_integration.id,
        a_good_configuration.api_key,
        a_good_configuration.rmw_url,
        "super_secret_token",
        "er.destination.com",
    )
    start_datetime_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    mock_log_activity = AsyncMock()
    mocker.patch("app.actions.rmwhub.log_action_activity", mock_log_activity)

    # Test handle 0 rmw_sets and 0 ER subjects
    data = []
    mocker.patch(
        "app.actions.buoy.BuoyClient.get_er_subjects",
        return_value=data,
    )
    result = {}
    mocker.patch(
        "app.actions.rmwhub.RmwHubAdapter._upload_data",
        return_value=result,
    )
    mocker.patch(
        "app.actions.rmwhub.RmwHubAdapter.search_own",
        return_value=[],
    )
    mocker.patch(
        "app.actions.buoy.BuoyClient.get_gear",
        return_value=[],
    )
    mocker.patch(
        "app.actions.buoy.BuoyClient.get_source_provider",
        return_value="rmw",
    )
    mocker.patch(
        "app.actions.buoy.BuoyClient.create_v1_observation",
        return_value=1,
    )

    observations, rmw_response = await rmw_adapter.process_upload(start_datetime_str)

    assert observations == 0

    # Test handle ER upload success
    data = mock_er_subjects
    mocker.patch(
        "app.actions.buoy.BuoyClient.get_er_subjects",
        return_value=data,
    )
    mocker.patch(
        "app.actions.rmwhub.RmwHubAdapter._upload_data",
        return_value=mock_rmw_upload_response,
    )
    mocker.patch(
        "app.actions.rmwhub.RmwHubAdapter.search_own",
        return_value=[mock_rmwhub_items[0]],
    )
    mocker.patch(
        "app.actions.buoy.BuoyClient.get_latest_observations",
        new=mock_get_latest_observations,
    )
    mocker.patch(
        "app.actions.buoy.BuoyClient.get_gear",
        return_value=[],
    )

    observations, rmw_response = await rmw_adapter.process_upload(start_datetime_str)
    assert observations == 5
    assert rmw_response["trap_count"] == 5

    # Test handle ER upload success with ER Subjects from RMW
    data = mock_er_subjects_from_rmw
    mocker.patch(
        "app.actions.buoy.BuoyClient.get_er_subjects",
        return_value=data,
    )
    mock_rmw_upload_response["trap_count"] = 0
    mocker.patch(
        "app.actions.rmwhub.RmwHubAdapter._upload_data",
        return_value=mock_rmw_upload_response,
    )
    mocker.patch(
        "app.actions.rmwhub.RmwHubAdapter.search_own",
        return_value=[mock_rmwhub_items[0]],
    )
    mocker.patch(
        "app.actions.buoy.BuoyClient.get_gear",
        return_value=[],
    )

    observations, rmw_response = await rmw_adapter.process_upload(start_datetime_str)
    assert observations == 0
    assert rmw_response["trap_count"] == 0


@pytest.mark.asyncio
async def test_rmwhub_adapter_process_upload_update_success(
    mocker,
    a_good_configuration,
    a_good_integration,
    mock_rmwhub_items_update,
    mock_er_subjects_update,
    mock_rmw_upload_response,
    mock_get_latest_observations,
):
    """
    Test RmwHubAdapter.process_upload update operations
    """

    rmw_adapter = RmwHubAdapter(
        a_good_integration.id,
        a_good_configuration.api_key,
        a_good_configuration.rmw_url,
        "super_secret_token",
        "http://er.destination.com",
    )
    start_datetime_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    mock_log_activity = AsyncMock()
    mocker.patch("app.actions.rmwhub.log_action_activity", mock_log_activity)

    # Test handle ER upload success with updates
    data = mock_er_subjects_update
    mocker.patch(
        "app.actions.buoy.BuoyClient.get_er_subjects",
        return_value=data,
    )
    mock_rmw_upload_response["trap_count"] = 3
    mocker.patch(
        "app.actions.rmwhub.RmwHubAdapter._upload_data",
        return_value=mock_rmw_upload_response,
    )
    mocker.patch(
        "app.actions.rmwhub.RmwHubAdapter.search_own",
        return_value=mock_rmwhub_items_update,
    )
    mocker.patch(
        "app.actions.buoy.BuoyClient.get_gear",
        return_value=[],
    )
    mocker.patch(
        "app.actions.buoy.BuoyClient.get_latest_observations",
        new=mock_get_latest_observations,
    )
    mocker.patch(
        "app.actions.buoy.BuoyClient.get_source_provider",
        return_value={"source_provider": "rmwhub"},
    )
    mocker.patch(
        "app.actions.buoy.BuoyClient.create_v1_observation",
        return_value=0,
    )

    observations, rmw_response = await rmw_adapter.process_upload(start_datetime_str)

    # Should make 4 and skip the 5th
    assert observations == 4
    assert rmw_response


@pytest.mark.asyncio
async def test_rmwhub_adapter_process_upload_failure(
    mocker, a_good_configuration, a_good_integration, mock_rmwhub_items
):
    """
    Test rmwhub.search_hub no sets
    """

    rmw_adapter = RmwHubAdapter(
        a_good_integration.id,
        a_good_configuration.api_key,
        a_good_configuration.rmw_url,
        "super_secret_token",
        "er.destination.com",
    )
    start_datetime_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    mock_log_activity = AsyncMock()
    mocker.patch("app.actions.rmwhub.log_action_activity", mock_log_activity)

    # Test handle ER upload failure
    data = []
    mocker.patch(
        "app.actions.buoy.BuoyClient.get_er_subjects",
        return_value=data,
    )
    mocker.patch(
        "app.actions.rmwhub.RmwHubAdapter._upload_data",
        return_value={},
    )
    mocker.patch(
        "app.actions.rmwhub.RmwHubAdapter.search_own",
        return_value=[mock_rmwhub_items[0]],
    )
    mocker.patch(
        "app.actions.buoy.BuoyClient.get_gear",
        return_value=[],
    )

    observations, rmw_response = await rmw_adapter.process_upload(start_datetime_str)
    assert observations == 0
    assert rmw_response == {}


@pytest.mark.asyncio
async def test_rmwhub_adapter_create_rmw_update_from_er_subject(
    mocker,
    a_good_integration,
    a_good_configuration,
    mock_er_subjects,
    mock_rmwhub_items,
    mock_latest_observations,
):
    rmwadapter = RmwHubAdapter(
        a_good_integration.id,
        a_good_configuration.api_key,
        a_good_configuration.rmw_url,
        "super_secret_token",
        "er.destination.com",
    )

    mocker.patch(
        "app.actions.buoy.BuoyClient.get_latest_observations",
        return_value=mock_latest_observations,
    )

    # Test create INSERT update (no existing rmwHub gearset)
    gearset_insert = await rmwadapter._create_rmw_update_from_er_subject(
        mock_er_subjects[0]
    )

    assert gearset_insert
    assert gearset_insert.traps[0].id
    assert len(gearset_insert.traps[0].id) >= 32

    # Test create UPDATE update (existing rmwHub gearset)
    gearset_update = await rmwadapter._create_rmw_update_from_er_subject(
        mock_er_subjects[0], rmw_gearset=mock_rmwhub_items[0]
    )

    assert gearset_update
    assert gearset_update.traps[0].id
    assert len(gearset_update.traps[0].id) >= 32