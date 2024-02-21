import pytest
from app.services.gundi import send_events_to_gundi, send_observations_to_gundi


def set_mocks(gundi_api_story, action_runner_story):
    return {
        "given_gundi_client_v2_class_mock": gundi_api_story.given_gundi_client_v2_class_mock(),
        "given_gundi_sensors_client_class_mock": gundi_api_story.given_gundi_sensors_client_class_mock(),
        "given_publish_event_mock": action_runner_story.given_publish_event_mock()
    }


@pytest.mark.asyncio
async def test_send_events_to_gundi(
        mocker,
        action_runner_story,
        gundi_api_story
):
    mocks = set_mocks(gundi_api_story, action_runner_story)
    mocker.patch("app.services.gundi.GundiClient", mocks["given_gundi_client_v2_class_mock"])
    mocker.patch("app.services.gundi.GundiDataSenderClient", mocks["given_gundi_sensors_client_class_mock"])
    mocker.patch("app.services.gundi._get_gundi_api_key", mocks["given_publish_event_mock"])
    events = [
        {
            "title": "Animal Sighting",
            "event_type": "wildlife_sighting_rep",
            "recorded_at": "2024-01-08 21:51:10-03:00",
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
            "title": "Animal Sighting",
            "event_type": "wildlife_sighting_rep",
            "recorded_at": "2024-01-08 21:51:10-03:00",
            "location": {
                "lat": -51.688645,
                "lon": -72.704421
            },
            "event_details": {
                "site_name": "MM Spot",
                "species": "lion"
            }
        }
    ]
    response = await send_events_to_gundi(
        events=events,
        integration_id=gundi_api_story.given_default_integration_v2_object().id
    )

    # Data is sent to gundi using the REST API for now
    assert len(response) == 2
    assert mocks["given_gundi_sensors_client_class_mock"].called
    mocks["given_gundi_sensors_client_class_mock"].return_value.post_events.assert_called_once_with(data=events)


@pytest.mark.asyncio
async def test_send_observations_to_gundi(
        mocker,
        action_runner_story,
        gundi_api_story
):
    mocks = set_mocks(gundi_api_story, action_runner_story)
    mocker.patch("app.services.gundi.GundiClient", mocks["given_gundi_client_v2_class_mock"])
    mocker.patch("app.services.gundi.GundiDataSenderClient", mocks["given_gundi_sensors_client_class_mock"])
    mocker.patch("app.services.gundi._get_gundi_api_key", mocks["given_publish_event_mock"])
    observations = [
        {
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
    response = await send_observations_to_gundi(
        observations=observations,
        integration_id=gundi_api_story.given_default_integration_v2_object().id
    )

    # Data is sent to gundi using the REST API for now
    assert len(response) == 2
    assert mocks["given_gundi_sensors_client_class_mock"].called
    mocks["given_gundi_sensors_client_class_mock"].return_value.post_observations.assert_called_once_with(data=observations)
