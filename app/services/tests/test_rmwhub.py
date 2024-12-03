from datetime import datetime
import json
import pytest


@pytest.mark.asyncio
async def test_rmwhub_download_data(mocker, mock_rmwhub_response, a_good_configuration):
    """
    Test rmwhub.download_data
    """

    # Setup mock_rmwhub_client
    mocker.patch("app.actions.rmwhub.RmwHubClient.search_others", return_value=json.dumps(mock_rmwhub_response))

    from app.actions.rmwhub import RmwHubAdapter

    rmwadapter = RmwHubAdapter(
        a_good_configuration.api_key, a_good_configuration.rmw_url
    )
    updates, deletes = rmwadapter.download_data(str(datetime.now().isoformat()))

    assert len(updates) == 5
    assert len(deletes) == 5

@pytest.mark.asyncio
async def test_rmwhub_process_updates(mock_rmwhub_items, a_good_configuration):
    """
    Test rmwhub.process_updates
    """

    from app.actions.rmwhub import RmwHubAdapter

    rmwadapter = RmwHubAdapter(
        a_good_configuration.api_key, a_good_configuration.rmw_url
    )
    updates, _ = mock_rmwhub_items
    observations = rmwadapter.process_updates(updates)

    assert len(observations) == 7
