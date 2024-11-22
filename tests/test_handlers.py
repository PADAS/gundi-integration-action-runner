import pytest
from unittest.mock import patch


@pytest.mark.asyncio
async def test_handler_action_pull_observations(
    mocker,
    a_good_configuration,
    a_good_integration,
    mock_rmwhub_items,
    mock_rmw_observations,
):
    """
    Test handler.action_pull_observations
    """

    with patch("rmwhub.RmwHubAdapter") as mock_rmw_adapter:
        instance = mock_rmw_adapter.return_value

        instance.download_data.return_value = iter(mock_rmwhub_items)
        instance.process_updates.return_value = iter(mock_rmw_observations)

        from handlers import action_pull_observations

        action_response = action_pull_observations(
            a_good_integration, action_config=a_good_configuration
        )

        assert action_response.get("observations_extracted") == len(
            mock_rmw_observations
        )
