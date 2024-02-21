from unittest.mock import MagicMock

from gundi_core.schemas.v2 import (
    Integration,
    IntegrationActionConfiguration,
    IntegrationActionSummery
)

from app.services.tests.storyboards.common import async_return


class GundiApiStoryboard:
    def __init__(self, mocker):
        self.mocker = mocker

    @staticmethod
    def given_default_integration_v2_object():
        return Integration.parse_obj(
            {'id': '779ff3ab-5589-4f4c-9e0a-ae8d6c9edff0', 'name': 'Gundi X', 'base_url': 'https://gundi-er.pamdas.org',
             'enabled': True,
             'type': {'id': '50229e21-a9fe-4caa-862c-8592dfb2479b', 'name': 'EarthRanger', 'value': 'earth_ranger',
                      'description': 'Integration type for Integration X Sites', 'actions': [
                     {'id': '80448d1c-4696-4b32-a59f-f3494fc949ac', 'type': 'auth', 'name': 'Authenticate',
                      'value': 'auth',
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
             'default_route': {'id': '5abf3845-7c9f-478a-bc0f-b24d87038c4b',
                               'name': 'Gundi X Provider - Default Route'},
             'status': {'id': 'mockid-b16a-4dbd-ad32-197c58aeef59', 'is_healthy': True,
                        'details': 'Last observation has been delivered with success.',
                        'observation_delivered_24hrs': 50231,
                        'last_observation_delivered_at': '2023-03-31T11:20:00+0200'}}
        )

    @staticmethod
    def given_default_observations_created_response():
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

    @staticmethod
    def given_default_events_created_response():
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

    @staticmethod
    def given_pull_observations_config():
        return IntegrationActionConfiguration(
            id='b3cdc6b2-b247-4fbd-8f86-53079b5860e5',
            integration='779ff3ab-5589-4f4c-9e0a-ae8d6c9edff0',
            action=IntegrationActionSummery(
                id='2e52c0ba-1723-4510-8702-496c232b2012',
                type='pull',
                name='Pull Observations',
                value='pull_observations'
            ),
            data={},
        )

    @staticmethod
    def given_api_key_mock():
        return "MockAP1K3y"

    def given_gundi_api_key_mock(self):
        mock = self.mocker.MagicMock()
        mock.return_value = async_return(self.given_api_key_mock())
        return mock

    def given_gundi_client_v2_mock(self):
        mock_client = self.mocker.MagicMock()
        mock_client.get_integration_api_key.return_value = async_return(self.given_gundi_api_key_mock()),
        mock_client.get_integration_details.return_value = async_return(
            self.given_default_integration_v2_object()
        )
        mock_client.__aenter__.return_value = mock_client
        return mock_client

    def given_gundi_client_v2_class_mock(self):
        mock_gundi_client_v2_class = self.mocker.MagicMock()
        mock_gundi_client_v2_class.return_value = self.given_gundi_client_v2_mock()
        return mock_gundi_client_v2_class

    def given_gundi_sensors_client_class_mock(self):
        mock_gundi_sensors_client_class = self.mocker.MagicMock()
        mock_gundi_sensors_client = self.mocker.MagicMock()
        mock_gundi_sensors_client.post_events.return_value = async_return(
            self.given_default_events_created_response()
        )
        mock_gundi_sensors_client.post_observations.return_value = async_return(
            self.given_default_observations_created_response()
        )
        mock_gundi_sensors_client_class.return_value = mock_gundi_sensors_client
        return mock_gundi_sensors_client_class
