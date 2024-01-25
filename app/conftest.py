import asyncio
import datetime
import pytest
from unittest.mock import MagicMock
from gundi_core.schemas.v2 import Integration


class AsyncMock(MagicMock):
    async def __call__(self, *args, **kwargs):
        return super(AsyncMock, self).__call__(*args, **kwargs)


def async_return(result):
    f = asyncio.Future()
    f.set_result(result)
    return f


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
def mock_gundi_client_v2(
        mocker,
        integration_v2,
):
    mock_client = mocker.MagicMock()
    mock_client.get_integration_details.return_value = async_return(
        integration_v2
    )
    mock_client.__aenter__.return_value = mock_client
    return mock_client


@pytest.fixture
def mock_state_manager(mocker):
    mock_state_manager = mocker.MagicMock()
    mock_state_manager.get_state.return_value = async_return(
        {'last_execution': '2023-11-17T11:20:00+0200'}
    )
    mock_state_manager.set_state.return_value = async_return(None)
    return mock_state_manager


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
            "object_id": "abebe106-3c50-446b-9c98-0b9b503fc900",
            "created_at": "2023-11-16T19:59:50.612864Z"
        },
        {
            "object_id": "cdebe106-3c50-446b-9c98-0b9b503fc911",
            "created_at": "2023-11-16T19:59:50.612864Z"
        }
    ]


@pytest.fixture
def observations_created_response():
    return [
        {
            "object_id": "efebe106-3c50-446b-9c98-0b9b503fc922",
            "created_at": "2023-11-16T19:59:55.612864Z"
        },
        {
            "object_id": "ghebe106-3c50-446b-9c98-0b9b503fc933",
            "created_at": "2023-11-16T19:59:56.612864Z"
        }
    ]


@pytest.fixture
def auth_headers_response():
    return {
        'Accept-Type': 'application/json',
        'Authorization': 'Bearer testtoken2a97022f21732461ee103a08fac8a35'
    }


@pytest.fixture
def get_events_response(events_batch_one, events_batch_two):
    return [
        events_batch_one,
        events_batch_two
    ]


@pytest.fixture
def events_batch_one():
    return [
        {'id': 'a7a5e9ad-e157-4f95-a824-1d59dbb56c3d', 'location': None, 'time': '2023-11-17T14:14:34.480590-06:00',
         'end_time': None, 'serial_number': 433, 'message': '', 'provenance': '',
         'event_type': 'silence_source_provider_rep', 'priority': 0, 'priority_label': 'Gray', 'attributes': {},
         'comment': None, 'title': 'A7a9e1ab-44e2-4585-8d4f-7770ca0b36e2 integration disrupted', 'reported_by': None,
         'state': 'new', 'is_contained_in': [], 'sort_at': '2023-11-17T14:14:34.482529-06:00', 'patrol_segments': [],
         'geometry': None, 'updated_at': '2023-11-17T14:14:34.482529-06:00',
         'created_at': '2023-11-17T14:14:34.482823-06:00', 'icon_id': 'silence_source_provider_rep',
         'event_details': {'report_time': '2023-11-17 20:14:34', 'silence_threshold': '00:01',
                           'last_device_reported_at': '2023-10-23 00:44:32', 'updates': []}, 'files': [],
         'related_subjects': [], 'event_category': 'analyzer_event',
         'url': 'https://gundi-er.pamdas.org/api/v1.0/activity/event/a7a5e9ad-e157-4f95-a824-1d59dbb56c3d',
         'image_url': 'https://gundi-er.pamdas.org/static/generic-gray.svg', 'geojson': None, 'is_collection': False,
         'updates': [{'message': 'Created', 'time': '2023-11-17T20:14:34.524434+00:00',
                      'user': {'first_name': '', 'last_name': '', 'username': ''}, 'type': 'add_event'}],
         'patrols': []},
        {'id': '72448da2-8e80-48d3-81c4-fc6d86c275a8', 'location': None, 'time': '2023-11-17T13:14:35.122386-06:00',
         'end_time': None, 'serial_number': 432, 'message': '', 'provenance': '',
         'event_type': 'silence_source_provider_rep', 'priority': 0, 'priority_label': 'Gray', 'attributes': {},
         'comment': None, 'title': '265de4c0-07b8-4e30-b136-5d5a75ff5912 integration disrupted', 'reported_by': None,
         'state': 'new', 'is_contained_in': [], 'sort_at': '2023-11-17T13:14:35.124145-06:00', 'patrol_segments': [],
         'geometry': None, 'updated_at': '2023-11-17T13:14:35.124145-06:00',
         'created_at': '2023-11-17T13:14:35.124427-06:00', 'icon_id': 'silence_source_provider_rep',
         'event_details': {'report_time': '2023-11-17 19:14:34', 'silence_threshold': '00:00',
                           'last_device_reported_at': '2023-10-26 21:24:02', 'updates': []}, 'files': [],
         'related_subjects': [], 'event_category': 'analyzer_event',
         'url': 'https://gundi-er.pamdas.org/api/v1.0/activity/event/72448da2-8e80-48d3-81c4-fc6d86c275a8',
         'image_url': 'https://gundi-er.pamdas.org/static/generic-gray.svg', 'geojson': None, 'is_collection': False,
         'updates': [{'message': 'Created', 'time': '2023-11-17T19:14:35.130037+00:00',
                      'user': {'first_name': '', 'last_name': '', 'username': ''}, 'type': 'add_event'}],
         'patrols': []}
    ]


@pytest.fixture
def events_batch_two():
    return [
        {'id': '2d3a5877-475a-423e-97c5-5eead34010e2', 'location': None, 'time': '2023-11-17T13:14:34.481295-06:00',
         'end_time': None, 'serial_number': 431, 'message': '', 'provenance': '',
         'event_type': 'silence_source_provider_rep', 'priority': 0, 'priority_label': 'Gray', 'attributes': {},
         'comment': None, 'title': 'A7a9e1ab-44e2-4585-8d4f-7770ca0b36e2 integration disrupted', 'reported_by': None,
         'state': 'new', 'is_contained_in': [], 'sort_at': '2023-11-17T13:14:34.483293-06:00', 'patrol_segments': [],
         'geometry': None, 'updated_at': '2023-11-17T13:14:34.483293-06:00',
         'created_at': '2023-11-17T13:14:34.483539-06:00', 'icon_id': 'silence_source_provider_rep',
         'event_details': {'report_time': '2023-11-17 19:14:34', 'silence_threshold': '00:01',
                           'last_device_reported_at': '2023-10-23 00:44:32', 'updates': []}, 'files': [],
         'related_subjects': [], 'event_category': 'analyzer_event',
         'url': 'https://gundi-er.pamdas.org/api/v1.0/activity/event/2d3a5877-475a-423e-97c5-5eead34010e2',
         'image_url': 'https://gundi-er.pamdas.org/static/generic-gray.svg', 'geojson': None, 'is_collection': False,
         'updates': [{'message': 'Created', 'time': '2023-11-17T19:14:34.528430+00:00',
                      'user': {'first_name': '', 'last_name': '', 'username': ''}, 'type': 'add_event'}],
         'patrols': []},
        {'id': '950401f9-5a14-4dd7-be53-e90168c9474a', 'location': {'latitude': 39.963, 'longitude': -77.152},
         'time': '2023-11-17T12:29:43.477991-06:00', 'end_time': None, 'serial_number': 430, 'message': '',
         'provenance': '', 'event_type': 'trailguard_rep', 'priority': 200, 'priority_label': 'Amber', 'attributes': {},
         'comment': None, 'title': 'Trailguard Trap', 'reported_by': None, 'state': 'active', 'is_contained_in': [],
         'sort_at': '2023-11-17T12:30:08.818727-06:00', 'patrol_segments': [], 'geometry': None,
         'updated_at': '2023-11-17T12:30:08.818727-06:00', 'created_at': '2023-11-17T12:29:53.171274-06:00',
         'icon_id': 'cameratrap_rep',
         'event_details': {'labels': ['adult', 'poacher'], 'species': 'human', 'animal_count': 1, 'updates': []},
         'files': [{'id': '67888221-3e2a-4ba4-9cc7-ea0539f1e5f3', 'comment': '',
                    'created_at': '2023-11-17T12:30:08.802202-06:00', 'updated_at': '2023-11-17T12:30:08.802222-06:00',
                    'updates': [{'message': 'File Added: a7dcd2bc-703b-4324-bfd3-13ea130e7395_poacher.jpg',
                                 'time': '2023-11-17T18:30:08.810486+00:00', 'text': '',
                                 'user': {'username': 'gundi_serviceaccout', 'first_name': 'Gundi',
                                          'last_name': 'Service Account', 'id': 'ddc888bb-d642-455a-a422-7393b4f172be',
                                          'content_type': 'accounts.user'}, 'type': 'add_eventfile'}],
                    'url': 'https://gundi-er.pamdas.org/api/v1.0/activity/event/950401f9-5a14-4dd7-be53-e90168c9474a/file/67888221-3e2a-4ba4-9cc7-ea0539f1e5f3/',
                    'images': {
                        'original': 'https://gundi-er.pamdas.org/api/v1.0/activity/event/950401f9-5a14-4dd7-be53-e90168c9474a/file/67888221-3e2a-4ba4-9cc7-ea0539f1e5f3/original/a7dcd2bc-703b-4324-bfd3-13ea130e7395_poacher.jpg',
                        'icon': 'https://gundi-er.pamdas.org/api/v1.0/activity/event/950401f9-5a14-4dd7-be53-e90168c9474a/file/67888221-3e2a-4ba4-9cc7-ea0539f1e5f3/icon/a7dcd2bc-703b-4324-bfd3-13ea130e7395_poacher.jpg',
                        'thumbnail': 'https://gundi-er.pamdas.org/api/v1.0/activity/event/950401f9-5a14-4dd7-be53-e90168c9474a/file/67888221-3e2a-4ba4-9cc7-ea0539f1e5f3/thumbnail/a7dcd2bc-703b-4324-bfd3-13ea130e7395_poacher.jpg',
                        'large': 'https://gundi-er.pamdas.org/api/v1.0/activity/event/950401f9-5a14-4dd7-be53-e90168c9474a/file/67888221-3e2a-4ba4-9cc7-ea0539f1e5f3/large/a7dcd2bc-703b-4324-bfd3-13ea130e7395_poacher.jpg',
                        'xlarge': 'https://gundi-er.pamdas.org/api/v1.0/activity/event/950401f9-5a14-4dd7-be53-e90168c9474a/file/67888221-3e2a-4ba4-9cc7-ea0539f1e5f3/xlarge/a7dcd2bc-703b-4324-bfd3-13ea130e7395_poacher.jpg'},
                    'filename': 'a7dcd2bc-703b-4324-bfd3-13ea130e7395_poacher.jpg', 'file_type': 'image',
                    'icon_url': 'https://gundi-er.pamdas.org/api/v1.0/activity/event/950401f9-5a14-4dd7-be53-e90168c9474a/file/67888221-3e2a-4ba4-9cc7-ea0539f1e5f3/icon/a7dcd2bc-703b-4324-bfd3-13ea130e7395_poacher.jpg'}],
         'related_subjects': [], 'event_category': 'monitoring',
         'url': 'https://gundi-er.pamdas.org/api/v1.0/activity/event/950401f9-5a14-4dd7-be53-e90168c9474a',
         'image_url': 'https://gundi-er.pamdas.org/static/cameratrap-black.svg',
         'geojson': {'type': 'Feature', 'geometry': {'type': 'Point', 'coordinates': [-77.152, 39.963]},
                     'properties': {'message': '', 'datetime': '2023-11-17T18:29:43.477991+00:00',
                                    'image': 'https://gundi-er.pamdas.org/static/cameratrap-black.svg',
                                    'icon': {'iconUrl': 'https://gundi-er.pamdas.org/static/cameratrap-black.svg',
                                             'iconSize': [25, 25], 'iconAncor': [12, 12], 'popupAncor': [0, -13],
                                             'className': 'dot'}}}, 'is_collection': False, 'updates': [
            {'message': 'Changed State: new → active', 'time': '2023-11-17T18:30:08.831311+00:00',
             'user': {'username': 'gundi_serviceaccout', 'first_name': 'Gundi', 'last_name': 'Service Account',
                      'id': 'ddc888bb-d642-455a-a422-7393b4f172be', 'content_type': 'accounts.user'}, 'type': 'read'},
            {'message': 'File Added: a7dcd2bc-703b-4324-bfd3-13ea130e7395_poacher.jpg',
             'time': '2023-11-17T18:30:08.810486+00:00', 'text': '',
             'user': {'username': 'gundi_serviceaccout', 'first_name': 'Gundi', 'last_name': 'Service Account',
                      'id': 'ddc888bb-d642-455a-a422-7393b4f172be', 'content_type': 'accounts.user'},
             'type': 'add_eventfile'}, {'message': 'Created', 'time': '2023-11-17T18:29:53.207751+00:00',
                                        'user': {'username': 'gundi_serviceaccout', 'first_name': 'Gundi',
                                                 'last_name': 'Service Account',
                                                 'id': 'ddc888bb-d642-455a-a422-7393b4f172be',
                                                 'content_type': 'accounts.user'}, 'type': 'add_event'}], 'patrols': []}
    ]


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
