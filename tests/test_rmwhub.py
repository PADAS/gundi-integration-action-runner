import pytest
from unittest.mock import patch


@pytest.mark.asyncio
async def test_rmwhub_download_data(mocker, mock_rmwhub_response, a_good_configuration):
    """
    Test rmwhub.download_data
    """

    # Setup mock_rmwhub_client
    mock_rmwclient_search_others = mocker.patch("rmwhub.RmwHubClient.search_others")
    mock_rmwclient_search_others.return_value = mock_rmwhub_response

    from rmwhub import RmwHubAdapter

    rmwadapter = RmwHubAdapter(
        a_good_configuration.api_key, a_good_configuration.rmw_url
    )
    updates, deletes = rmwadapter.download_data(str(datetime.now().isoformat()))

    assert len(updates) == 5
    assert len(deletes) == 5


async def test_rmwhub_process_updates(mocker, mock_rmw_items, mock_rmw_observations):
    """
    Test rmwhub.process_updates
    """

    from rmwhub import RmwHubAdapter

    rmwadapter = RmwHubAdapter(
        a_good_configuration.api_key, a_good_configuration.rmw_url
    )
    updates, deletes = mock_rmw_items
    observations = rmwadapter.process_updates(updates)

    assert len(observation) == 7
