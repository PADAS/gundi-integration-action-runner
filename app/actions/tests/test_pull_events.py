import pytest
from app.conftest import async_return
from app.services.action_runner import execute_action


@pytest.mark.asyncio
async def test_execute_pull_observations_action(
        mocker, mock_gundi_client_v2, mock_state_manager, inaturalist_integration_v2,
        mock_get_gundi_api_key, mock_gundi_sensors_client_class, mock_config_manager,
        mock_get_observations_v2, mock_publish_event, mock_gundi_client_v2_class
):
    mock_config_manager.get_integration_details.return_value = async_return(inaturalist_integration_v2)
    mock_config_manager.get_action_configuration.return_value = async_return(inaturalist_integration_v2.configurations[0])
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager)
    mock_state_manager.get_state.return_value = async_return({})
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)
    mocker.patch("app.services.gundi.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("app.services.gundi.GundiDataSenderClient", mock_gundi_sensors_client_class)
    mocker.patch("app.services.gundi._get_gundi_api_key", mock_get_gundi_api_key)
    mocker.patch("app.datasource.inaturalist.get_observations_v2", mock_get_observations_v2)

    response = await execute_action(
        integration_id=str(inaturalist_integration_v2.id),
        action_id="pull_events"
    )
    assert "result" in response
    assert response["result"].get("events_extracted") == 2
    assert response["result"].get("events_updated") == 0
    assert response["result"].get("photos_attached") == 5
    assert mock_get_observations_v2.called
    assert mock_get_observations_v2.call_count == 2


@pytest.mark.asyncio
async def test_execute_pull_observations_action_without_bounding_box(
        mocker, mock_gundi_client_v2, mock_state_manager, inaturalist_integration_v2_without_bounding_box,
        mock_get_gundi_api_key, mock_gundi_sensors_client_class, mock_config_manager,
        mock_get_observations_v2, mock_publish_event, mock_gundi_client_v2_class
):
    mock_config_manager.get_integration_details.return_value = async_return(inaturalist_integration_v2_without_bounding_box)
    mock_config_manager.get_action_configuration.return_value = async_return(inaturalist_integration_v2_without_bounding_box.configurations[0])
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager)
    mock_state_manager.get_state.return_value = async_return({})
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)
    mocker.patch("app.services.gundi.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("app.services.gundi.GundiDataSenderClient", mock_gundi_sensors_client_class)
    mocker.patch("app.services.gundi._get_gundi_api_key", mock_get_gundi_api_key)
    mocker.patch("app.datasource.inaturalist.get_observations_v2", mock_get_observations_v2)

    response = await execute_action(
        integration_id=str(inaturalist_integration_v2_without_bounding_box.id),
        action_id="pull_events"
    )
    assert "result" in response
    assert response["result"].get("events_extracted") == 2
    assert response["result"].get("events_updated") == 0
    assert response["result"].get("photos_attached") == 5
    assert mock_get_observations_v2.called
    assert mock_get_observations_v2.call_count == 2