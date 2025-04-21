from datetime import datetime, timedelta, timezone
import pytz
import pytest

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
    a_good_connection,
    mock_rmwhub_items,
    mock_rmw_observations,
):
    """
    Test handler.action_pull_observations
    """
    fixed_now = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    items = mock_rmwhub_items

    mocker.patch("app.services.action_runner.action_handlers", mock_action_handlers)
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.actions.handlers.GundiClient", mock_gundi_client_v2_class)
    mocker.patch(
        "app.services.gundi.GundiDataSenderClient", mock_gundi_sensors_client_class
    )
    mocker.patch("app.services.gundi._get_gundi_api_key", mock_get_gundi_api_key)

    download_data_mock = mocker.patch(
        "app.actions.rmwhub.RmwHubAdapter.download_data",
        return_value=items,
    )
    mocker.patch(
        "app.actions.rmwhub.RmwHubAdapter.process_download",
        return_value=mock_rmw_observations,
    )
    mocker.patch(
        "app.actions.rmwhub.RmwHubAdapter.process_upload",
        return_value=(0, {}),
    )
    mocker.patch(
        "app.actions.rmwhub.RmwHubAdapter.push_status_updates",
        return_value=None,
    )
    mocker.patch(
        "app.actions.handlers.get_er_token_and_site",
        return_value=("super_secret_token", "er.destination.com"),
    )
    mock_datetime = mocker.patch("app.actions.handlers.datetime")
    mock_datetime.now.return_value = fixed_now

    from app.actions.handlers import action_pull_observations

    action_response = await action_pull_observations(
        a_good_integration, action_config=a_good_configuration
    )

    assert action_response.get("observations_extracted") == (
        len(mock_rmw_observations) * len(a_good_connection.destinations)
    )

    sync_interval_minutes = 30
    expected_start_datetime = (fixed_now - timedelta(minutes=sync_interval_minutes)).astimezone(pytz.utc)

    assert download_data_mock.call_count == len(a_good_connection.destinations)
    for call in download_data_mock.call_args_list:
        args, kwargs = call
        assert args[0] == expected_start_datetime
