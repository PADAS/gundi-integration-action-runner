import datetime
from unittest.mock import MagicMock

from gcloud.aio import pubsub

from app import settings
from app.services.tests.storyboards.common import AsyncMock, async_return


class ActionRunnerStoryboard:
    def __init__(self, mocker):
        self.mocker = mocker

    @staticmethod
    def given_integration_event_pubsub_message():
        return pubsub.PubsubMessage(
            b'{"event_id": "6214c049-f786-45eb-9877-2efb2c2cf8e9", "timestamp": "2024-01-26 14:03:46.199385+00:00", "schema_version": "v1", "payload": {"integration_id": "779ff3ab-5589-4f4c-9e0a-ae8d6c9edff0", "action_id": "pull_observations", "config_data": {"end_datetime": "2024-01-01T00:00:00-00:00", "start_datetime": "2024-01-10T23:59:59-00:00", "force_run_since_start": true}}, "event_type": "IntegrationActionStarted"}'
        )

    @staticmethod
    def given_gcp_pubsub_publish_response():
        return {"messageIds": ["7061707768812258"]}

    @staticmethod
    def given_auth_headers_response():
        return {
            'Accept-Type': 'application/json',
            'Authorization': 'Bearer testtoken2a97022f21732461ee103a08fac8a35'
        }

    @staticmethod
    def given_event_v2_cloud_event_payload():
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

    @staticmethod
    def given_event_v2_cloud_event_headers():
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

    def given_pubsub_client_mock(self):
        mock_client = self.mocker.MagicMock()
        mock_publisher = self.mocker.MagicMock()
        mock_publisher.publish.return_value = async_return(self.given_gcp_pubsub_publish_response())
        mock_publisher.topic_path.return_value = (
            f"projects/{settings.GCP_PROJECT_ID}/topics/{settings.INTEGRATION_EVENTS_TOPIC}"
        )
        mock_client.PublisherClient.return_value = mock_publisher
        mock_client.PubsubMessage.return_value = self.given_integration_event_pubsub_message()
        return mock_client

    def given_publish_event_mock(self):
        mock_publish_event = AsyncMock()
        mock_publish_event.return_value = self.given_gcp_pubsub_publish_response()
        return mock_publish_event

    def given_action_handlers_mock(self):
        mock_action_handler = AsyncMock()
        mock_action_handler.return_value = {"observations_extracted": 10}
        mock_action_handlers = self.mocker.MagicMock()
        mock_action_handlers.__getitem__.return_value = mock_action_handler
        return mock_action_handlers
