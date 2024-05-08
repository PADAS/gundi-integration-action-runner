import asyncio
import datetime
import json

import pytest
from unittest.mock import MagicMock
from app import settings
from gcloud.aio import pubsub
from gundi_core.schemas.v2 import Integration, IntegrationActionConfiguration, IntegrationActionSummery
from gundi_core.events import (
    SystemEventBaseModel,
    IntegrationActionCustomLog,
    CustomActivityLog,
    IntegrationActionStarted,
    ActionExecutionStarted,
    IntegrationActionFailed,
    ActionExecutionFailed,
    IntegrationActionComplete,
    ActionExecutionComplete,
    LogLevel
)

from app.actions import PullActionConfiguration


class AsyncMock(MagicMock):
    async def __call__(self, *args, **kwargs):
        return super(AsyncMock, self).__call__(*args, **kwargs)


def async_return(result):
    f = asyncio.Future()
    f.set_result(result)
    return f


@pytest.fixture
def mock_integration_state():
    return {"last_execution": "2024-01-29T11:20:00+0200"}


@pytest.fixture
def mock_redis(mocker, mock_integration_state):
    redis = MagicMock()
    redis_client = mocker.MagicMock()
    redis_client.set.return_value = async_return(MagicMock())
    redis_client.get.return_value = async_return(json.dumps(mock_integration_state, default=str))
    redis_client.delete.return_value = async_return(MagicMock())
    redis_client.setex.return_value = async_return(None)
    redis_client.incr.return_value = redis_client
    redis_client.decr.return_value = async_return(None)
    redis_client.expire.return_value = redis_client
    redis_client.execute.return_value = async_return((1, True))
    redis_client.__aenter__.return_value = redis_client
    redis_client.__aexit__.return_value = None
    redis_client.pipeline.return_value = redis_client
    redis.Redis.return_value = redis_client
    return redis


@pytest.fixture
def integration_v2():
    return Integration.parse_obj(
        {'id': '779ff3ab-5589-4f4c-9e0a-ae8d6c9edff0', 'name': 'Gundi X', 'base_url': 'https://gundi-er.pamdas.org',
         'enabled': True,
         'type': {'id': '50229e21-a9fe-4caa-862c-8592dfb2479b', 'name': 'EarthRanger', 'value': 'earth_ranger',
                  'description': 'Integration type for Integration X Sites', 'actions': [
                 {'id': '80448d1c-4696-4b32-a59f-f3494fc949ac', 'type': 'auth', 'name': 'Authenticate', 'value': 'auth',
                  'description': 'Authenticate against Integration X',
                  'schema': {'type': 'object', 'required': ['token'], 'properties': {'token': {'type': 'string'}}}},
                 {'id': '4b721b37-f4ca-4f20-b07c-2caadb095ecb', 'type': 'pull', 'name': 'Pull Events',
                  'value': 'pull_events', 'description': 'Extract events from EarthRanger sites',
                  'schema': {'type': 'object', 'title': 'PullObservationsConfig', 'required': ['start_datetime'],
                             'properties': {'start_datetime': {'type': 'string', 'title': 'Start Datetime'}}}},
                 {'id': '75b3040f-ab1f-42e7-b39f-8965c088b154', 'type': 'pull', 'name': 'Pull Observations',
                  'value': 'pull_observations', 'description': 'Extract observations from an EarthRanger Site',
                  'schema': {'type': 'object', 'title': 'PullObservationsConfig', 'required': ['start_datetime'],
                             'properties': {'start_datetime': {'type': 'string', 'title': 'Start Datetime'}}}},
                 {'id': '425a2e2f-ae71-44fb-9314-bc0116638e4f', 'type': 'push', 'name': 'Push Event Attachments',
                  'value': 'push_event_attachments',
                  'description': 'EarthRanger sites support adding attachments to events', 'schema': {}},
                 {'id': '8e101f31-e693-404c-b6ee-20fde6019f16', 'type': 'push', 'name': 'Push Events',
                  'value': 'push_events', 'description': 'EarthRanger sites support sending Events (a.k.a Reports)',
                  'schema': {}}]},
         'owner': {'id': 'a91b400b-482a-4546-8fcb-ee42b01deeb6', 'name': 'Test Org', 'description': ''},
         'configurations': [
             {'id': '5577c323-b961-4277-9047-b1f27fd6a1b7', 'integration': '779ff3ab-5589-4f4c-9e0a-ae8d6c9edff0',
              'action': {'id': '75b3040f-ab1f-42e7-b39f-8965c088b154', 'type': 'pull', 'name': 'Pull Observations',
                         'value': 'pull_observations'},
              'data': {'end_datetime': '2023-11-10T06:00:00-00:00', 'start_datetime': '2023-11-10T05:30:00-00:00',
                       'force_run_since_start': False}},
             {'id': '431af42b-c431-40af-8b57-a349253e15df', 'integration': '779ff3ab-5589-4f4c-9e0a-ae8d6c9edff0',
              'action': {'id': '4b721b37-f4ca-4f20-b07c-2caadb095ecb', 'type': 'pull', 'name': 'Pull Events',
                         'value': 'pull_events'}, 'data': {'start_datetime': '2023-11-16T00:00:00-03:00'}},
             {'id': '30f8878c-4a98-4c95-88eb-79f73c40fb2f', 'integration': '779ff3ab-5589-4f4c-9e0a-ae8d6c9edff0',
              'action': {'id': '80448d1c-4696-4b32-a59f-f3494fc949ac', 'type': 'auth', 'name': 'Authenticate',
                         'value': 'auth'}, 'data': {'token': 'testtoken2a97022f21732461ee103a08fac8a35'}}],
         'additional': {},
         'default_route': {'id': '5abf3845-7c9f-478a-bc0f-b24d87038c4b', 'name': 'Gundi X Provider - Default Route'},
         'status': {'id': 'mockid-b16a-4dbd-ad32-197c58aeef59', 'is_healthy': True,
                    'details': 'Last observation has been delivered with success.',
                    'observation_delivered_24hrs': 50231, 'last_observation_delivered_at': '2023-03-31T11:20:00+0200'}}
    )


@pytest.fixture
def pull_observations_config():
    return MockPullActionConfiguration(lookback_days=30)


@pytest.fixture
def mock_gundi_client_v2(
        mocker,
        integration_v2,
        mock_get_gundi_api_key
):
    mock_client = mocker.MagicMock()
    mock_client.get_integration_api_key.return_value = async_return(mock_get_gundi_api_key),
    mock_client.get_integration_details.return_value = async_return(
        integration_v2
    )
    mock_client.register_integration_type = AsyncMock()
    mock_client.__aenter__.return_value = mock_client
    return mock_client


@pytest.fixture
def mock_gundi_client_v2_class(mocker, mock_gundi_client_v2):
    mock_gundi_client_v2_class = mocker.MagicMock()
    mock_gundi_client_v2_class.return_value = mock_gundi_client_v2
    return mock_gundi_client_v2_class


@pytest.fixture
def mock_gundi_sensors_client_class(mocker, events_created_response, observations_created_response):
    mock_gundi_sensors_client_class = mocker.MagicMock()
    mock_gundi_sensors_client = mocker.MagicMock()
    mock_gundi_sensors_client.post_events.return_value = async_return(
        events_created_response
    )
    mock_gundi_sensors_client.post_observations.return_value = async_return(
        observations_created_response
    )
    mock_gundi_sensors_client_class.return_value = mock_gundi_sensors_client
    return mock_gundi_sensors_client_class


@pytest.fixture
def events_created_response():
    return [
        {
            "id": "e1e1e1e1-e1e1-e1e1-e1e1-e1e1e1e1e1e1",
            "title": "Animal Sighting",
            "event_type": "wildlife_sighting_rep",
            "recorded_at": "2024-01-29 20:51:10-03:00",
            "location": {
                "lat": -51.688645,
                "lon": -72.704421
            },
            "event_details": {
                "site_name": "MM Spot",
                "species": "lion"
            }
        },
        {
            "id": "e1e1e1e1-e1e1-e1e1-e1e1-e1e1e1e1e1e2",
            "title": "Animal Sighting",
            "event_type": "wildlife_sighting_rep",
            "recorded_at": "2024-01-29 20:51:25-03:00",
            "location": {
                "lat": -51.688646,
                "lon": -72.704421
            },
            "event_details": {
                "site_name": "MM Spot",
                "species": "lion"
            }
        }
    ]


@pytest.fixture
def observations_created_response():
    return [
        {
            "id": "e1e1e1e1-e1e1-e1e1-e1e1-e1e1e1e1e1e1",
            "source": "device-xy123",
            "type": "tracking-device",
            "subject_type": "puma",
            "recorded_at": "2024-01-24 09:03:00-0300",
            "location": {
                "lat": -51.748,
                "lon": -72.720
            },
            "additional": {
                "speed_kmph": 5
            }
        },
        {
            "id": "e1e1e1e1-e1e1-e1e1-e1e1-e1e1e1e1e1e2",
            "source": "test-device-mariano",
            "type": "tracking-device",
            "subject_type": "puma",
            "recorded_at": "2024-01-24 09:05:00-0300",
            "location": {
                "lat": -51.755,
                "lon": -72.755
            },
            "additional": {
                "speed_kmph": 5
            }
        }
    ]


@pytest.fixture
def mock_state_manager(mocker):
    mock_state_manager = mocker.MagicMock()
    mock_state_manager.get_state.return_value = async_return(
        {'last_execution': '2023-11-17T11:20:00+0200'}
    )
    mock_state_manager.set_state.return_value = async_return(None)
    return mock_state_manager


@pytest.fixture
def mock_pubsub_client(
        mocker, integration_event_pubsub_message, gcp_pubsub_publish_response
):
    mock_client = mocker.MagicMock()
    mock_publisher = mocker.MagicMock()
    mock_publisher.publish.return_value = async_return(gcp_pubsub_publish_response)
    mock_publisher.topic_path.return_value = (
        f"projects/{settings.GCP_PROJECT_ID}/topics/{settings.INTEGRATION_EVENTS_TOPIC}"
    )
    mock_client.PublisherClient.return_value = mock_publisher
    mock_client.PubsubMessage.return_value = integration_event_pubsub_message
    return mock_client


@pytest.fixture
def integration_event_pubsub_message():
    return pubsub.PubsubMessage(
        b'{"event_id": "6214c049-f786-45eb-9877-2efb2c2cf8e9", "timestamp": "2024-01-26 14:03:46.199385+00:00", "schema_version": "v1", "payload": {"integration_id": "779ff3ab-5589-4f4c-9e0a-ae8d6c9edff0", "action_id": "pull_observations", "config_data": {"end_datetime": "2024-01-01T00:00:00-00:00", "start_datetime": "2024-01-10T23:59:59-00:00", "force_run_since_start": true}}, "event_type": "IntegrationActionStarted"}'
    )


@pytest.fixture
def gcp_pubsub_publish_response():
    return {"messageIds": ["7061707768812258"]}


@pytest.fixture
def mock_publish_event(gcp_pubsub_publish_response):
    mock_publish_event = AsyncMock()
    mock_publish_event.return_value = gcp_pubsub_publish_response
    return mock_publish_event


class MockPullActionConfiguration(PullActionConfiguration):
    lookback_days: int = 10


@pytest.fixture
def mock_action_handlers(mocker):
    mock_action_handler = AsyncMock()
    mock_action_handler.return_value = {"observations_extracted": 10}
    mock_action_handlers = {"pull_observations": (mock_action_handler, MockPullActionConfiguration)}
    return mock_action_handlers


@pytest.fixture
def auth_headers_response():
    return {
        'Accept-Type': 'application/json',
        'Authorization': 'Bearer testtoken2a97022f21732461ee103a08fac8a35'
    }


@pytest.fixture
def mock_get_gundi_api_key(mocker, mock_api_key):
    mock = mocker.MagicMock()
    mock.return_value = async_return(mock_api_key)
    return mock


@pytest.fixture
def mock_api_key():
    return "MockAP1K3y"


@pytest.fixture
def event_v2_cloud_event_payload():
    timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return {
        "message": {
            "data": "eyJpbnRlZ3JhdGlvbl9pZCI6ICI4NDNlMDgwMS1lODFhLTQ3ZTUtOWNlMi1iMTc2ZTQ3MzZhODUiLCAiYWN0aW9uX2lkIjogInB1bGxfb2JzZXJ2YXRpb25zIn0=",
            "messageId": "10298788169291041", "message_id": "10298788169291041",
            "publishTime": timestamp,
            "publish_time": timestamp
        },
        "subscription": "projects/cdip-stage-78ca/subscriptions/integrationx-actions-sub"
    }


@pytest.fixture
def event_v2_cloud_event_payload_with_config_overrides():
    timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return {
        "message": {
            "data": "eyJpbnRlZ3JhdGlvbl9pZCI6ICI4NDNlMDgwMS1lODFhLTQ3ZTUtOWNlMi1iMTc2ZTQ3MzZhODUiLCAiYWN0aW9uX2lkIjogInB1bGxfb2JzZXJ2YXRpb25zIiwgImNvbmZpZ19vdmVycmlkZXMiOiB7Imxvb2tiYWNrX2RheXMiOiAzfX0=",
            "messageId": "10298788169291041", "message_id": "10298788169291041",
            "publishTime": timestamp,
            "publish_time": timestamp
        },
        "subscription": "projects/cdip-stage-78ca/subscriptions/integrationx-actions-sub"
    }


@pytest.fixture
def event_v2_cloud_event_headers():
    timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return {
        "host": "integrationx-actions-runner-jabcutl7za-uc.a.run.app",
        "content-type": "application/json",
        "authorization": "Bearer fake-token",
        "content-length": "2057",
        "accept": "application/json",
        "from": "noreply@google.com",
        "user-agent": "APIs-Google; (+https://developers.google.com/webmasters/APIs-Google.html)",
        "x-cloud-trace-context": "",
        "traceparent": "",
        "x-forwarded-for": "64.233.172.137",
        "x-forwarded-proto": "https",
        "forwarded": 'for="64.233.172.137";proto=https',
        "accept-encoding": "gzip, deflate, br",
        "ce-id": "20090163454824831",
        "ce-source": "//pubsub.googleapis.com/projects/cdip-stage-78ca/topics/integrationx-actions-topic-test",
        "ce-specversion": "1.0",
        "ce-type": "google.cloud.pubsub.topic.v1.messagePublished",
        "ce-time": timestamp,
    }


@pytest.fixture
def action_started_event():
    return IntegrationActionStarted(
        payload=ActionExecutionStarted(
            integration_id="779ff3ab-5589-4f4c-9e0a-ae8d6c9edff0",
            action_id="pull_observations",
            config_data={
                "end_datetime": "2024-01-10T00:00:00-00:00",
                "start_datetime": "2024-01-01T23:59:59-00:00",
                "force_run_since_start": True
            },
        )
    )


@pytest.fixture
def action_complete_event():
    return IntegrationActionComplete(
        payload=ActionExecutionComplete(
            integration_id="779ff3ab-5589-4f4c-9e0a-ae8d6c9edff0",
            action_id="pull_observations",
            config_data={
                "end_datetime": "2024-01-10T00:00:00-00:00",
                "start_datetime": "2024-01-01T23:59:59-00:00",
                "force_run_since_start": True
            },
            result={"observations_extracted": 10}
        )
    )

@pytest.fixture
def action_failed_event():
    return IntegrationActionFailed(
        payload=ActionExecutionFailed(
            integration_id="779ff3ab-5589-4f4c-9e0a-ae8d6c9edff0",
            action_id="pull_observations",
            config_data={
                "end_datetime": "2024-01-10T00:00:00-00:00",
                "start_datetime": "2024-01-01T23:59:59-00:00",
                "force_run_since_start": True
            },
            error="ConnectionError: Error connecting to X system"
        )
    )


@pytest.fixture
def custom_activity_log_event():
    return IntegrationActionCustomLog(
        payload=CustomActivityLog(
            integration_id="779ff3ab-5589-4f4c-9e0a-ae8d6c9edff0",
            action_id="pull_observations",
            config_data={
                "end_datetime": "2024-01-01T00:00:00-00:00",
                "start_datetime": "2024-01-10T23:59:59-00:00",
                "force_run_since_start": True
            },
            title="Invalid start_datetime for action pull_observations",
            level=LogLevel.ERROR,
            data={
                "details": "start_datetime cannot be grater than end_datetime. Please fix the configuration."
            }
        )
    )


@pytest.fixture
def system_event(request, action_started_event, action_complete_event, action_failed_event, custom_activity_log_event):
    if request.param == "action_started_event":
        return action_started_event
    if request.param == "action_complete_event":
        return action_complete_event
    if request.param == "action_failed_event":
        return action_failed_event
    if request.param == "custom_activity_log_event":
        return custom_activity_log_event
    return None
