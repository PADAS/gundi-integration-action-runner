import pytest
from app.services.gundi import send_events_to_gundi, send_observations_to_gundi, send_event_attachments_to_gundi


@pytest.mark.asyncio
async def test_send_events_to_gundi(
        mocker, mock_gundi_client_v2_class, mock_gundi_sensors_client_class,
        mock_get_gundi_api_key, integration_v2
):
    mocker.patch("app.services.gundi.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("app.services.gundi.GundiDataSenderClient", mock_gundi_sensors_client_class)
    mocker.patch("app.services.gundi._get_gundi_api_key", mock_get_gundi_api_key)
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
        integration_id=integration_v2.id
    )

    # Data is sent to gundi using the REST API for now
    assert len(response) == 2
    assert mock_gundi_sensors_client_class.called
    mock_gundi_sensors_client_class.return_value.post_events.assert_called_once_with(data=events)


@pytest.mark.asyncio
async def test_send_event_attachments_to_gundi(
        mocker, mock_gundi_client_v2_class, mock_gundi_sensors_client_class,
        mock_get_gundi_api_key, integration_v2
):
    mocker.patch("app.services.gundi.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("app.services.gundi.GundiDataSenderClient", mock_gundi_sensors_client_class)
    mocker.patch("app.services.gundi._get_gundi_api_key", mock_get_gundi_api_key)
    attachments = [
        b'\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x01\x00x\x00x\x00\x00\xff\xdb\x00C\x00\x02\x01\x01\x02',
        b'\xff\xd8\xff\xe0\x00\x10JFIF\x00\x06\x01\x01\x00x\x00x\x01\x00\xff\xd5\x00C\x00\x98\x01\x01\x56'
    ]
    response = await send_event_attachments_to_gundi(
        event_id="dummy-1234",
        attachments=attachments,
        integration_id=integration_v2.id
    )

    # Data is sent to gundi using the REST API for now
    assert len(response) == 2
    assert mock_gundi_sensors_client_class.called
    mock_gundi_sensors_client_class.return_value.post_event_attachments.assert_called_once_with(
        event_id="dummy-1234",
        attachments=attachments
    )


@pytest.mark.asyncio
async def test_send_observations_to_gundi(
        mocker, mock_gundi_client_v2_class, mock_gundi_sensors_client_class,
        mock_get_gundi_api_key, integration_v2
):
    mocker.patch("app.services.gundi.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("app.services.gundi.GundiDataSenderClient", mock_gundi_sensors_client_class)
    mocker.patch("app.services.gundi._get_gundi_api_key", mock_get_gundi_api_key)
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
        integration_id=integration_v2.id
    )

    # Data is sent to gundi using the REST API for now
    assert len(response) == 2
    assert mock_gundi_sensors_client_class.called
    mock_gundi_sensors_client_class.return_value.post_observations.assert_called_once_with(data=observations)
