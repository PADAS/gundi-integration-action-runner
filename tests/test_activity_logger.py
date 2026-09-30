import json
import pytest
from unittest.mock import ANY
from gundi_core.events import (
    LogLevel,
    IntegrationActionStarted,
    IntegrationActionComplete,
    IntegrationActionFailed,
    IntegrationActionCustomLog,
    IntegrationWebhookStarted,
    IntegrationWebhookComplete,
    IntegrationWebhookFailed
)
from gundi_action_runner import settings
from gundi_action_runner.testing.fixtures import async_return
from gundi_action_runner.services.activity_logger import (
    publish_event, publish_events, activity_logger, webhook_activity_logger, log_activity,
    log_action_activity, log_webhook_activity, PUBSUB_MAX_MESSAGES_PER_PUBLISH, PUBSUB_MAX_BYTES_PER_PUBLISH,
)
from gundi_action_runner.services.errors import IntegrationAuthError
from gundi_action_runner.webhooks import GenericJsonPayload, GenericJsonTransformConfig


@pytest.mark.parametrize(
    "system_event",
    [
        "action_started_event", "action_complete_event", "action_failed_event", "custom_activity_log_event",
        "webhook_started_event", "webhook_complete_event", "webhook_failed_event", "webhook_custom_activity_log_event"
    ],
    indirect=["system_event"])
@pytest.mark.asyncio
async def test_publish_event(
        mocker, mock_pubsub_client, integration_event_pubsub_message, gcp_pubsub_publish_response,
        system_event
):
    mocker.patch("gundi_action_runner.services.activity_logger.pubsub", mock_pubsub_client)

    response = await publish_event(
        event=system_event,
        topic_name=settings.INTEGRATION_EVENTS_TOPIC
    )

    assert response == gcp_pubsub_publish_response
    assert mock_pubsub_client.PublisherClient.called
    assert mock_pubsub_client.PubsubMessage.called
    assert mock_pubsub_client.PublisherClient.called
    assert mock_pubsub_client.PublisherClient.return_value.publish.called
    mock_pubsub_client.PublisherClient.return_value.publish.assert_any_call(
        f"projects/{settings.GCP_PROJECT_ID}/topics/{settings.INTEGRATION_EVENTS_TOPIC}",
        [integration_event_pubsub_message],
    )


@pytest.mark.asyncio
async def test_activity_logger_decorator(
        mocker, mock_publish_event, integration_v2, pull_observations_config
):

    mocker.patch("gundi_action_runner.services.activity_logger.publish_event", mock_publish_event)

    @activity_logger()
    async def action_pull_observations(integration, action_config):
        return {"observations_extracted": 10}

    await action_pull_observations(
        integration=integration_v2,
        action_config=pull_observations_config
    )

    # Two events expected: One on start and one on completion
    assert mock_publish_event.call_count == 2
    assert isinstance(mock_publish_event.call_args_list[0].kwargs.get("event"), IntegrationActionStarted)
    assert isinstance(mock_publish_event.call_args_list[1].kwargs.get("event"), IntegrationActionComplete)


@pytest.mark.asyncio
async def test_webhook_activity_logger(
        mocker, mock_publish_event, integration_v2_with_webhook_generic,
        mock_webhook_request_payload_for_dynamic_schema, mock_generic_webhook_config
):

    mocker.patch("gundi_action_runner.services.activity_logger.publish_event", mock_publish_event)

    @webhook_activity_logger()
    async def webhook_handler(payload: GenericJsonPayload, integration=None, webhook_config: GenericJsonTransformConfig = None):
        return {"observations_extracted": 10}

    await webhook_handler(
        payload=GenericJsonPayload(data=mock_webhook_request_payload_for_dynamic_schema),
        integration=integration_v2_with_webhook_generic,
        webhook_config=GenericJsonTransformConfig(**mock_generic_webhook_config)
    )

    # Two events expected: One on start and one on completion
    assert mock_publish_event.call_count == 2
    assert isinstance(mock_publish_event.call_args_list[0].kwargs.get("event"), IntegrationWebhookStarted)
    assert isinstance(mock_publish_event.call_args_list[1].kwargs.get("event"), IntegrationWebhookComplete)


@pytest.mark.asyncio
async def test_webhook_activity_logger_on_error(
        mocker, mock_publish_event, integration_v2_with_webhook_generic,
        mock_webhook_request_payload_for_dynamic_schema, mock_generic_webhook_config
):
    mocker.patch("gundi_action_runner.services.activity_logger.publish_event", mock_publish_event)

    @webhook_activity_logger()
    async def webhook_handler(payload: GenericJsonPayload, integration=None,
                              webhook_config: GenericJsonTransformConfig = None):
        raise Exception("Something went wrong")

    with pytest.raises(Exception):
        await webhook_handler(
            payload=GenericJsonPayload(data=mock_webhook_request_payload_for_dynamic_schema),
            integration=integration_v2_with_webhook_generic,
            webhook_config=GenericJsonTransformConfig(**mock_generic_webhook_config)
        )

    # Two events expected: One on start and one on error
    assert mock_publish_event.call_count == 2
    assert isinstance(mock_publish_event.call_args_list[0].kwargs.get("event"), IntegrationWebhookStarted)
    assert isinstance(mock_publish_event.call_args_list[1].kwargs.get("event"), IntegrationWebhookFailed)


@pytest.mark.asyncio
async def test_activity_logger_decorator_with_arguments(
        mocker, mock_publish_event, integration_v2, pull_observations_config
):

    mocker.patch("gundi_action_runner.services.activity_logger.publish_event", mock_publish_event)

    @activity_logger(on_start=False, on_completion=True, on_error=False)
    async def action_pull_observations(integration, action_config):
        return {"observations_extracted": 10}

    await action_pull_observations(
        integration=integration_v2,
        action_config=pull_observations_config
    )

    # Only one event expected, on completion
    assert mock_publish_event.call_count == 1
    assert isinstance(mock_publish_event.call_args_list[0].kwargs.get("event"), IntegrationActionComplete)


@pytest.mark.asyncio
async def test_activity_logger_decorator_on_error(
        mocker, mock_publish_event, integration_v2, pull_observations_config
):

    mocker.patch("gundi_action_runner.services.activity_logger.publish_event", mock_publish_event)

    @activity_logger()
    async def action_pull_observations(integration, action_config):
        raise Exception("Something went wrong")

    with pytest.raises(Exception):
        await action_pull_observations(
            integration=integration_v2,
            action_config=pull_observations_config
        )

    # Two events expected: One on start and one on error
    assert mock_publish_event.call_count == 2
    assert isinstance(mock_publish_event.call_args_list[0].kwargs.get("event"), IntegrationActionStarted)
    assert isinstance(mock_publish_event.call_args_list[1].kwargs.get("event"), IntegrationActionFailed)


@pytest.mark.asyncio
async def test_log_activity_with_debug_level(mocker, integration_v2, pull_observations_config, mock_publish_event):
    mocker.patch("gundi_action_runner.services.activity_logger.publish_event", mock_publish_event)
    await log_activity(
        integration_id=integration_v2.id,
        action_id="pull_observations",
        level=LogLevel.DEBUG,
        title="Extracted 10 observations from 2 devices",
        data={"devices": ["deviceid1", "deviceid2"]},
        config_data=pull_observations_config.dict()
    )
    assert mock_publish_event.call_count == 1
    assert isinstance(mock_publish_event.call_args_list[0].kwargs.get("event"), IntegrationActionCustomLog)


@pytest.mark.asyncio
async def test_log_activity_with_info_level(mocker, integration_v2, mock_publish_event, pull_observations_config):
    mocker.patch("gundi_action_runner.services.activity_logger.publish_event", mock_publish_event)
    await log_activity(
        integration_id=integration_v2.id,
        action_id="pull_observations",
        level=LogLevel.INFO,
        title="Extracting observations with filter..",
        data={"start_date": "2024-01-01", "end_date": "2024-01-31"},
        config_data=pull_observations_config.dict()
    )
    assert mock_publish_event.call_count == 1
    assert isinstance(mock_publish_event.call_args_list[0].kwargs.get("event"), IntegrationActionCustomLog)


@pytest.mark.asyncio
async def test_log_activity_with_warning_level(mocker, integration_v2, mock_publish_event, pull_observations_config):
    mocker.patch("gundi_action_runner.services.activity_logger.publish_event", mock_publish_event)
    await log_activity(
        integration_id=integration_v2.id,
        action_id="pull_observations",
        level=LogLevel.WARNING,
        title="Skipping end_date because it's greater than today. Please review your configuration.",
        config_data=pull_observations_config.dict()
    )
    assert mock_publish_event.call_count == 1
    assert isinstance(mock_publish_event.call_args_list[0].kwargs.get("event"), IntegrationActionCustomLog)


@pytest.mark.asyncio
async def test_log_activity_with_error_level(mocker, integration_v2, mock_publish_event, pull_observations_config):
    mocker.patch("gundi_action_runner.services.activity_logger.publish_event", mock_publish_event)
    await log_activity(
        integration_id=integration_v2.id,
        action_id="pull_observations",
        level=LogLevel.ERROR,
        title="Error getting data from System X",
        data={"error": "Connection error with host 'systemx.com'"},
        config_data=pull_observations_config.dict()
    )
    assert mock_publish_event.call_count == 1
    assert isinstance(mock_publish_event.call_args_list[0].kwargs.get("event"), IntegrationActionCustomLog)


@pytest.mark.asyncio
async def test_activity_logger_decorator_publishes_classified_error_text(
        mocker, mock_publish_event, integration_v2, pull_observations_config
):
    mocker.patch("gundi_action_runner.services.activity_logger.publish_event", mock_publish_event)

    @activity_logger()
    async def action_pull_observations(integration, action_config):
        raise IntegrationAuthError("TrackIt rejected the credentials", status_code=401)

    with pytest.raises(IntegrationAuthError):
        await action_pull_observations(
            integration=integration_v2, action_config=pull_observations_config
        )

    failed_events = [
        call.kwargs.get("event") or call.args[0]
        for call in mock_publish_event.mock_calls
        if call.kwargs.get("event") is not None or call.args
    ]
    failed_events = [e for e in failed_events if isinstance(e, IntegrationActionFailed)]
    assert len(failed_events) == 1
    assert failed_events[0].payload.error == (
        "Authentication failed — TrackIt rejected the credentials (HTTP 401)"
    )



@pytest.mark.asyncio
async def test_webhook_activity_logger_decorator_publishes_classified_error_text(
        mocker, mock_publish_event, integration_v2_with_webhook_generic,
        mock_generic_webhook_config, mock_webhook_request_payload_for_dynamic_schema
):
    mocker.patch("gundi_action_runner.services.activity_logger.publish_event", mock_publish_event)

    @webhook_activity_logger()
    async def webhook_handler(payload: GenericJsonPayload, integration=None,
                              webhook_config: GenericJsonTransformConfig = None):
        raise IntegrationAuthError("Provider rejected the credentials", status_code=401)

    with pytest.raises(IntegrationAuthError):
        await webhook_handler(
            payload=GenericJsonPayload(data=mock_webhook_request_payload_for_dynamic_schema),
            integration=integration_v2_with_webhook_generic,
            webhook_config=GenericJsonTransformConfig(**mock_generic_webhook_config)
        )

    failed_events = [
        call.kwargs.get("event") or call.args[0]
        for call in mock_publish_event.mock_calls
        if call.kwargs.get("event") is not None or call.args
    ]
    failed_events = [e for e in failed_events if isinstance(e, IntegrationWebhookFailed)]
    assert len(failed_events) == 1
    assert failed_events[0].payload.error == (
        "Authentication failed — Provider rejected the credentials (HTTP 401)"
    )


@pytest.mark.asyncio
async def test_log_activity_default_level_is_a_valid_log_level(mocker, integration_v2, mock_publish_event):
    """gundi-core's LogLevel is an IntEnum, so the string default "INFO" the
    helpers used to carry never validated: a connector that called
    log_action_activity without a level got a ValidationError instead of a
    log entry. The default must be the enum member."""
    mocker.patch("gundi_action_runner.services.activity_logger.publish_event", mock_publish_event)

    await log_action_activity(
        integration_id=str(integration_v2.id),
        action_id="pull_observations",
        title="Something worth telling the operator",
    )
    await log_webhook_activity(
        integration_id=str(integration_v2.id),
        title="Webhook received",
    )

    levels = [call.kwargs["event"].payload.level for call in mock_publish_event.call_args_list]
    assert levels == [LogLevel.INFO, LogLevel.INFO]


@pytest.mark.asyncio
async def test_publish_events_sends_all_events_in_a_single_publish_call(
        mocker, mock_pubsub_client, action_started_event, gcp_pubsub_publish_response
):
    mocker.patch("gundi_action_runner.services.activity_logger.pubsub", mock_pubsub_client)

    response = await publish_events([action_started_event] * 3, topic_name=settings.INTEGRATION_EVENTS_TOPIC)

    publisher = mock_pubsub_client.PublisherClient.return_value
    assert publisher.publish.call_count == 1
    topic, messages = publisher.publish.call_args.args
    assert topic == f"projects/{settings.GCP_PROJECT_ID}/topics/{settings.INTEGRATION_EVENTS_TOPIC}"
    assert len(messages) == 3
    assert response == gcp_pubsub_publish_response


@pytest.mark.asyncio
async def test_publish_events_splits_batches_at_the_pubsub_limit(
        mocker, mock_pubsub_client, action_started_event
):
    mocker.patch("gundi_action_runner.services.activity_logger.pubsub", mock_pubsub_client)
    publisher = mock_pubsub_client.PublisherClient.return_value
    publisher.publish.side_effect = lambda topic, messages: async_return(
        {"messageIds": [str(i) for i in range(len(messages))]}
    )
    events = [action_started_event] * (PUBSUB_MAX_MESSAGES_PER_PUBLISH + 5)

    response = await publish_events(events, topic_name=settings.INTEGRATION_EVENTS_TOPIC)

    assert [len(c.args[1]) for c in publisher.publish.call_args_list] == [PUBSUB_MAX_MESSAGES_PER_PUBLISH, 5]
    assert len(response["messageIds"]) == PUBSUB_MAX_MESSAGES_PER_PUBLISH + 5


@pytest.mark.asyncio
async def test_publish_events_is_a_no_op_on_ephemeral_run(mocker, mock_pubsub_client, action_started_event):
    from gundi_action_runner.services.activity_logger import ephemeral_run
    mocker.patch("gundi_action_runner.services.activity_logger.pubsub", mock_pubsub_client)

    token = ephemeral_run.set(True)
    try:
        response = await publish_events([action_started_event], topic_name=settings.INTEGRATION_EVENTS_TOPIC)
    finally:
        ephemeral_run.reset(token)

    assert response is None
    assert not mock_pubsub_client.PublisherClient.return_value.publish.called


def _serialized_request_size(messages):
    # What gcloud-aio's PublisherClient.publish puts on the wire: base64 data
    # plus JSON framing. Pub/Sub's 10 MB quota applies to this body.
    return len(json.dumps({"messages": [m.to_repr() for m in messages]}))


@pytest.fixture
def mock_publisher_client_only(mocker, gcp_pubsub_publish_response):
    """Mock the publisher but keep the real PubsubMessage, so batch sizes
    reflect the events actually being sent."""
    publisher = mocker.MagicMock()
    publisher.topic_path.return_value = f"projects/{settings.GCP_PROJECT_ID}/topics/{settings.INTEGRATION_EVENTS_TOPIC}"
    publisher.publish.side_effect = lambda topic, messages: async_return(
        {"messageIds": [str(i) for i in range(len(messages))]}
    )
    mocker.patch("gundi_action_runner.services.activity_logger.pubsub.PublisherClient", return_value=publisher)
    return publisher


@pytest.mark.asyncio
async def test_publish_events_splits_batches_at_the_byte_limit(
        mocker, mock_publisher_client_only, action_started_event
):
    # Review on #114: Pub/Sub also rejects publish requests over 10 MB, and a
    # count-only split assembled 1,000 x 12 KB commands into a 16 MB request
    # that every retry would resend. Split on serialized size as well.
    from gundi_action_runner.services import activity_logger
    one_event = _serialized_request_size(
        [activity_logger.pubsub.PubsubMessage(json.dumps(action_started_event.dict(), default=str).encode("utf-8"))]
    )
    limit = one_event * 3 + 16  # room for three, not four
    mocker.patch.object(activity_logger, "PUBSUB_MAX_BYTES_PER_PUBLISH", limit)

    response = await publish_events([action_started_event] * 7, topic_name=settings.INTEGRATION_EVENTS_TOPIC)

    sizes = [len(c.args[1]) for c in mock_publisher_client_only.publish.call_args_list]
    assert sizes == [3, 3, 1]
    for call in mock_publisher_client_only.publish.call_args_list:
        assert _serialized_request_size(call.args[1]) <= limit
    assert len(response["messageIds"]) == 7


@pytest.mark.asyncio
async def test_publish_events_sends_an_oversized_event_alone_instead_of_dropping_it(
        mocker, mock_publisher_client_only, action_started_event
):
    # A single event larger than the limit cannot be split; it goes out on
    # its own so Pub/Sub's rejection names it, and the loop still terminates.
    from gundi_action_runner.services import activity_logger
    mocker.patch.object(activity_logger, "PUBSUB_MAX_BYTES_PER_PUBLISH", 10)

    response = await publish_events([action_started_event] * 3, topic_name=settings.INTEGRATION_EVENTS_TOPIC)

    assert [len(c.args[1]) for c in mock_publisher_client_only.publish.call_args_list] == [1, 1, 1]
    assert len(response["messageIds"]) == 3


def test_publish_byte_limit_is_the_pubsub_quota():
    # 10 MB per publish request, per https://cloud.google.com/pubsub/quotas
    assert PUBSUB_MAX_BYTES_PER_PUBLISH <= 10 * 1000 * 1000
    assert PUBSUB_MAX_MESSAGES_PER_PUBLISH == 1000
