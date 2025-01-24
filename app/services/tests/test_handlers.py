import pytest
from app.actions.rmwhub import RmwSets


@pytest.mark.asyncio
async def test_handler_action_pull_observations(
    mocker,
    mock_gundi_client_v2,
    mock_publish_event,
    mock_action_handlers,
    mock_gundi_client_v2_class,
    mock_gundi_sensors_client_class,
    mock_get_gundi_api_key,
    a_good_configuration,
    a_good_integration,
    mock_rmwhub_items,
    mock_rmw_observations,
):
    """
    Test handler.action_pull_observations
    """

    items, _ = mock_rmwhub_items

    mocker.patch("app.services.action_runner.action_handlers", mock_action_handlers)
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.gundi.GundiClient", mock_gundi_client_v2_class)
    mocker.patch(
        "app.services.gundi.GundiDataSenderClient", mock_gundi_sensors_client_class
    )
    mocker.patch("app.services.gundi._get_gundi_api_key", mock_get_gundi_api_key)
    mocker.patch(
        "app.actions.rmwhub.RmwHubAdapter.download_data",
        return_value=(RmwSets(sets=items), []),
    )
    mocker.patch(
        "app.actions.rmwhub.RmwHubAdapter.process_updates",
        return_value=mock_rmw_observations,
    )

    from app.actions.handlers import action_pull_observations

    action_response = await action_pull_observations(
        a_good_integration, action_config=a_good_configuration
    )

    assert action_response.get("observations_extracted") == len(mock_rmw_observations)
