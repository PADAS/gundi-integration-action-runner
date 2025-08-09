import datetime
from typing import List
import httpx
import stamina
from gundi_client_v2.client import GundiClient, GundiDataSenderClient


@stamina.retry(on=httpx.HTTPError, wait_initial=1.0, wait_jitter=5.0, wait_max=32.0)
async def _get_gundi_api_key(integration_id):
    async with GundiClient() as gundi_client:
        return await gundi_client.get_integration_api_key(
            integration_id=integration_id
        )


async def _get_sensors_api_client(integration_id):
    gundi_api_key = await _get_gundi_api_key(integration_id=integration_id)
    assert gundi_api_key, f"Cannot get a valid API Key for integration {integration_id}"
    sensors_api_client = GundiDataSenderClient(
        integration_api_key=gundi_api_key
    )
    return sensors_api_client


@stamina.retry(on=httpx.HTTPError, wait_initial=1.0, wait_jitter=5.0, wait_max=32.0)
async def send_events_to_gundi(events: List[dict], **kwargs) -> dict:
    """
    Send Events to Gundi using the REST API v2
    :param events: A list of events in the following format:
    [
        {
        "title": "Animal Sighting",
        "event_type": "wildlife_sighting_rep",
        "recorded_at":"2024-01-08 21:51:10-03:00",
        "location":{
            "lat":-51.688645,
            "lon":-72.704421
        },
        "event_details":{
            "site_name":"MM Spot",
            "species":"lion"
        },
        ...
    ]
    :param kwargs: integration_id: The UUID of the related integration
    :return: A dict with the response from the API
    """
    integration_id = kwargs.get("integration_id")
    assert integration_id, "integration_id is required"
    sensors_api_client = await _get_sensors_api_client(integration_id=str(integration_id))
    return await sensors_api_client.post_events(data=events)


@stamina.retry(on=httpx.HTTPError, wait_initial=1.0, wait_jitter=5.0, wait_max=32.0)
async def send_event_attachments_to_gundi(event_id: str, attachments: List[tuple], **kwargs) -> dict:
    """
    Send Event Attachments to Gundi using the REST API v2
    :param event_id: Created event in which the attachments are going to be linked
    :param attachments: A list of attachments (tuples with filename, file in bytes). Example:
    filename = 'example.png'
    file_in_bytes = open(filename, 'rb')
    attachments = [(filename, file_in_bytes)]
    :param kwargs: integration_id: The UUID of the related integration
    :return: A dict with the response from the API
    """
    integration_id = kwargs.get("integration_id")
    assert integration_id, "integration_id is required"
    sensors_api_client = await _get_sensors_api_client(integration_id=str(integration_id))
    return await sensors_api_client.post_event_attachments(event_id=event_id, attachments=attachments)


@stamina.retry(on=httpx.HTTPError, wait_initial=1.0, wait_jitter=5.0, wait_max=32.0)
async def send_observations_to_gundi(observations: List[dict], **kwargs) -> dict:
    """
    Send Observations to Gundi using the REST API v2
    :param observations: A list of observations in the following format:
    [
        {
            "source": "collar-xy123",
            "type": "tracking-device",
            "subject_type": "puma",
            "recorded_at": "2024-01-24 09:03:00-0300",
            "location": {
                "lat": -51.748,
                "lon": -72.720
            },
            "additional": {
                "speed_kmph": 10
            }
        },
        ...
    ]
    :param kwargs: integration_id: The UUID of the related integration
    :return: A dict with the response from the API
    """
    integration_id = kwargs.get("integration_id")
    assert integration_id, "integration_id is required"
    sensors_api_client = await _get_sensors_api_client(integration_id=str(integration_id))
    return await sensors_api_client.post_observations(data=observations)


@stamina.retry(on=httpx.HTTPError, wait_initial=1.0, wait_jitter=5.0, wait_max=32.0)
async def send_messages_to_gundi(messages: List[dict], **kwargs) -> dict:
    """
    Send Messages to Gundi using the REST API v2
    :param messages: A list of messages in the following format:
    [
        {
            "sender": "2075752244",
            "recipients": ["admin@sitex.pamdas.org"],
            "text": "Help! I need assistance.",
            "recorded_at": "2025-08-09 09:54:10-0300",
            "location": {
                "latitude": -51.689,
                "longitude": -72.705
            },
            "additional": {
                "gpsFix": 2,
                "course": 45,
                "speed": 50,
                "status": {
                    "autonomous": 0,
                    "lowBattery": 1,
                    "intervalChange": 0,
                    "resetDetected": 0
                }
            }
        },
        ...
    ]
    :param kwargs: integration_id: The UUID of the related integration
    :return: A dict with the response from the API
    """
    integration_id = kwargs.get("integration_id")
    assert integration_id, "integration_id is required"
    sensors_api_client = await _get_sensors_api_client(integration_id=str(integration_id))
    return await sensors_api_client.post_messages(data=messages)
