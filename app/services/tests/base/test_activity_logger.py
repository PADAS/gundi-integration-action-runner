import pytest
from gundi_core.events import (
    LogLevel,
    IntegrationActionStarted,
    IntegrationActionComplete,
    IntegrationActionFailed,
    IntegrationActionCustomLog
)
from app import settings
from app.services.activity_logger import publish_event, activity_logger, log_activity


@pytest.mark.parametrize(
    "action",
    ["action_started_event", "action_complete_event", "action_failed_event", "custom_activity_log_event"]
)
@pytest.mark.asyncio
async def test_publish_event(
        mocker,
        action_runner_story,
        activity_logger_story,
        action
):
    given_pubsub_client_mock = action_runner_story.given_pubsub_client_mock()

    mocker.patch("app.services.activity_logger.pubsub", given_pubsub_client_mock)

    response = await publish_event(
        event=activity_logger_story.system_event(action),
        topic_name=settings.INTEGRATION_EVENTS_TOPIC
    )

    assert response == action_runner_story.given_gcp_pubsub_publish_response()
    assert given_pubsub_client_mock.PublisherClient.called
    assert given_pubsub_client_mock.PubsubMessage.called
    assert given_pubsub_client_mock.PublisherClient.called
    assert given_pubsub_client_mock.PublisherClient.return_value.publish.called
    given_pubsub_client_mock.PublisherClient.return_value.publish.assert_any_call(
        given_pubsub_client_mock.PublisherClient.return_value.topic_path.return_value,
        [given_pubsub_client_mock.PubsubMessage.return_value]
    )


@pytest.mark.asyncio
async def test_activity_logger_decorator(
        mocker,
        action_runner_story,
        gundi_api_story
):
    given_publish_event_mock = action_runner_story.given_publish_event_mock()

    mocker.patch("app.services.activity_logger.publish_event", given_publish_event_mock)

    @activity_logger()
    async def action_pull_observations(integration, action_config):
        return {"observations_extracted": 10}

    await action_pull_observations(
        integration=gundi_api_story.given_default_integration_v2_object(),
        action_config=gundi_api_story.given_pull_observations_config()
    )

    # Two events expected: One on start and one on completion
    assert given_publish_event_mock.call_count == 2
    assert isinstance(given_publish_event_mock.call_args_list[0].kwargs.get("event"), IntegrationActionStarted)
    assert isinstance(given_publish_event_mock.call_args_list[1].kwargs.get("event"), IntegrationActionComplete)


@pytest.mark.asyncio
async def test_activity_logger_decorator_with_arguments(
        mocker,
        action_runner_story,
        gundi_api_story
):
    given_publish_event_mock = action_runner_story.given_publish_event_mock()

    mocker.patch("app.services.activity_logger.publish_event", given_publish_event_mock)

    @activity_logger(on_start=False, on_completion=True, on_error=False)
    async def action_pull_observations(integration, action_config):
        return {"observations_extracted": 10}

    await action_pull_observations(
        integration=gundi_api_story.given_default_integration_v2_object(),
        action_config=gundi_api_story.given_pull_observations_config()
    )

    # Only one event expected, on completion
    assert given_publish_event_mock.call_count == 1
    assert isinstance(given_publish_event_mock.call_args_list[0].kwargs.get("event"), IntegrationActionComplete)


@pytest.mark.asyncio
async def test_activity_logger_decorator_on_error(
        mocker,
        action_runner_story,
        gundi_api_story
):
    given_publish_event_mock = action_runner_story.given_publish_event_mock()

    mocker.patch("app.services.activity_logger.publish_event", given_publish_event_mock)

    @activity_logger()
    async def action_pull_observations(integration, action_config):
        raise Exception("Something went wrong")

    with pytest.raises(Exception):
        await action_pull_observations(
            integration=gundi_api_story.given_default_integration_v2_object(),
            action_config=gundi_api_story.given_pull_observations_config()
        )

    # Two events expected: One on start and one on error
    assert given_publish_event_mock.call_count == 2
    assert isinstance(given_publish_event_mock.call_args_list[0].kwargs.get("event"), IntegrationActionStarted)
    assert isinstance(given_publish_event_mock.call_args_list[1].kwargs.get("event"), IntegrationActionFailed)


@pytest.mark.asyncio
async def test_log_activity_with_debug_level(
        mocker,
        action_runner_story,
        gundi_api_story
):
    given_publish_event_mock = action_runner_story.given_publish_event_mock()

    mocker.patch("app.services.activity_logger.publish_event", given_publish_event_mock)

    await log_activity(
        integration_id=gundi_api_story.given_default_integration_v2_object().id,
        action_id="pull_observations",
        level=LogLevel.DEBUG,
        title="Extracted 10 observations from 2 devices",
        data={"devices": ["deviceid1", "deviceid2"]},
        config_data=gundi_api_story.given_pull_observations_config().data
    )
    assert given_publish_event_mock.call_count == 1
    assert isinstance(given_publish_event_mock.call_args_list[0].kwargs.get("event"), IntegrationActionCustomLog)


@pytest.mark.asyncio
async def test_log_activity_with_info_level(
        mocker,
        action_runner_story,
        gundi_api_story
):
    given_publish_event_mock = action_runner_story.given_publish_event_mock()

    mocker.patch("app.services.activity_logger.publish_event", given_publish_event_mock)

    await log_activity(
        integration_id=gundi_api_story.given_default_integration_v2_object().id,
        action_id="pull_observations",
        level=LogLevel.INFO,
        title="Extracting observations with filter..",
        data={"start_date": "2024-01-01", "end_date": "2024-01-31"},
        config_data=gundi_api_story.given_pull_observations_config().data
    )
    assert given_publish_event_mock.call_count == 1
    assert isinstance(given_publish_event_mock.call_args_list[0].kwargs.get("event"), IntegrationActionCustomLog)


@pytest.mark.asyncio
async def test_log_activity_with_warning_level(
        mocker,
        action_runner_story,
        gundi_api_story
):
    given_publish_event_mock = action_runner_story.given_publish_event_mock()

    mocker.patch("app.services.activity_logger.publish_event", given_publish_event_mock)

    await log_activity(
        integration_id=gundi_api_story.given_default_integration_v2_object().id,
        action_id="pull_observations",
        level=LogLevel.WARNING,
        title="Skipping end_date because it's greater than today. Please review your configuration.",
        config_data=gundi_api_story.given_pull_observations_config().data
    )
    assert given_publish_event_mock.call_count == 1
    assert isinstance(given_publish_event_mock.call_args_list[0].kwargs.get("event"), IntegrationActionCustomLog)


@pytest.mark.asyncio
async def test_log_activity_with_error_level(
        mocker,
        action_runner_story,
        gundi_api_story
):
    given_publish_event_mock = action_runner_story.given_publish_event_mock()

    mocker.patch("app.services.activity_logger.publish_event", given_publish_event_mock)

    await log_activity(
        integration_id=gundi_api_story.given_default_integration_v2_object().id,
        action_id="pull_observations",
        level=LogLevel.ERROR,
        title="Error getting data from System X",
        data={"error": "Connection error with host 'systemx.com'"},
        config_data=gundi_api_story.given_pull_observations_config().data
    )
    assert given_publish_event_mock.call_count == 1
    assert isinstance(given_publish_event_mock.call_args_list[0].kwargs.get("event"), IntegrationActionCustomLog)
