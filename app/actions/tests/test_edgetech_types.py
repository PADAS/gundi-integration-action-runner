from ast import List
from datetime import datetime, timedelta, timezone

import pytest

from app.actions.edgetech.types import Buoy


@pytest.mark.usefixtures
def test_create_observations(get_mock_edgetech_data):
    """
    Test the create_observations method of the Buoy class.

    - Test using the latest location from previous record when newer record without location exists.
    - Test creating observations from multiple buoys.
    - Test creating observations from both changeRecords and currentState.
    """
    # Arrange
    buoys: List[Buoy] = [Buoy.parse_obj(record) for record in get_mock_edgetech_data]

    # Act
    observations = []
    for buoy in buoys:
        buoy_observations, _, _ = buoy.create_observations("edgetech_")
        observations.extend(buoy_observations)

    expected_observations = [
        {
            "name": "edgetech_88CE9978AE_A",
            "source": "edgetech_88CE9978AE_A",
            "type": "ropeless_buoy",
            "subject_type": "ropeless_buoy_device",
            "is_active": True,
            "recorded_at": "2024-10-22T16:34:46+00:00",
            "location": {"lat": 41.82907459248435, "lon": -71.41540430869928},
            "additional": {
                "subject_name": "edgetech_88CE9978AE_A",
                "edgetech_serial_number": "88CE9978AE",
                "display_id": "f1ab617fc777",
                "event_type": "gear_deployed",
                "devices": [
                    {
                        "label": "a",
                        "location": {
                            "latitude": 41.82907459248435,
                            "longitude": -71.41540430869928,
                        },
                        "device_id": "edgetech_88CE9978AE_A",
                        "last_updated": "2024-10-22T16:34:46+00:00",
                    }
                ],
            },
        },
        {
            "name": "edgetech_88CE99C99A_A",
            "source": "edgetech_88CE99C99A_A",
            "type": "ropeless_buoy",
            "subject_type": "ropeless_buoy_device",
            "is_active": False,
            "recorded_at": "2024-12-03T16:04:55+00:00",
            "location": {"lat": 41.5740898, "lon": -70.8831463},
            "additional": {
                "subject_name": "edgetech_88CE99C99A_A",
                "edgetech_serial_number": "88CE99C99A",
                "display_id": "e1f4d34d79f2",
                "event_type": "gear_retrieved",
                "devices": [
                    {
                        "label": "a",
                        "location": {"latitude": 41.5740898, "longitude": -70.8831463},
                        "device_id": "edgetech_88CE99C99A_A",
                        "last_updated": "2024-12-03T16:04:55+00:00",
                    }
                ],
            },
        },
        {
            "name": "edgetech_88CE99C99A_A",
            "source": "edgetech_88CE99C99A_A",
            "type": "ropeless_buoy",
            "subject_type": "ropeless_buoy_device",
            "is_active": False,
            "recorded_at": "2025-03-14T12:07:27+00:00",
            "location": {"lat": 41.7832483, "lon": -70.7527803},
            "additional": {
                "subject_name": "edgetech_88CE99C99A_A",
                "edgetech_serial_number": "88CE99C99A",
                "display_id": "e1f4d34d79f2",
                "event_type": "gear_retrieved",
                "devices": [
                    {
                        "label": "a",
                        "location": {"latitude": 41.7832483, "longitude": -70.7527803},
                        "device_id": "edgetech_88CE99C99A_A",
                        "last_updated": "2025-03-14T12:07:27+00:00",
                    }
                ],
            },
        },
        {
            "name": "edgetech_88CE999763_A",
            "source": "edgetech_88CE999763_A",
            "type": "ropeless_buoy",
            "subject_type": "ropeless_buoy_device",
            "is_active": True,
            "recorded_at": "2025-03-17T16:26:12+00:00",
            "location": {"lat": 41.52546746182916, "lon": -70.67401171221228},
            "additional": {
                "subject_name": "edgetech_88CE999763_A",
                "edgetech_serial_number": "88CE999763",
                "display_id": "dc5103663894",
                "event_type": "gear_deployed",
                "devices": [
                    {
                        "label": "a",
                        "location": {
                            "latitude": 41.52546746182916,
                            "longitude": -70.67401171221228,
                        },
                        "device_id": "edgetech_88CE999763_A",
                        "last_updated": "2025-03-17T16:26:12+00:00",
                    }
                ],
            },
        },
        {
            "name": "edgetech_88CE999763_A",
            "source": "edgetech_88CE999763_A",
            "type": "ropeless_buoy",
            "subject_type": "ropeless_buoy_device",
            "is_active": False,
            "recorded_at": "2025-03-17T16:41:22+00:00",
            "location": {"lat": 41.52546746182916, "lon": -70.67401171221228},
            "additional": {
                "subject_name": "edgetech_88CE999763_A",
                "edgetech_serial_number": "88CE999763",
                "display_id": "dc5103663894",
                "event_type": "gear_retrieved",
                "devices": [
                    {
                        "label": "a",
                        "location": {
                            "latitude": 41.52546746182916,
                            "longitude": -70.67401171221228,
                        },
                        "device_id": "edgetech_88CE999763_A",
                        "last_updated": "2025-03-17T16:41:22+00:00",
                    }
                ],
            },
        },
        {
            "name": "edgetech_88CE999763_A",
            "source": "edgetech_88CE999763_A",
            "type": "ropeless_buoy",
            "subject_type": "ropeless_buoy_device",
            "is_active": True,
            "recorded_at": "2025-03-17T16:43:40+00:00",
            "location": {"lat": 41.52546746182916, "lon": -70.67401171221228},
            "additional": {
                "subject_name": "edgetech_88CE999763_A",
                "edgetech_serial_number": "88CE999763",
                "display_id": "5eb353fb0f49",
                "event_type": "gear_deployed",
                "devices": [
                    {
                        "label": "a",
                        "location": {
                            "latitude": 41.52546746182916,
                            "longitude": -70.67401171221228,
                        },
                        "device_id": "edgetech_88CE999763_A",
                        "last_updated": "2025-03-17T16:43:40+00:00",
                    },
                    {
                        "label": "b",
                        "location": {
                            "latitude": 41.52537796592242,
                            "longitude": -70.6738777899687,
                        },
                        "device_id": "edgetech_88CE999763_B",
                        "last_updated": "2025-03-17T16:43:40+00:00",
                    },
                ],
            },
        },
        {
            "name": "edgetech_88CE999763_B",
            "source": "edgetech_88CE999763_B",
            "type": "ropeless_buoy",
            "subject_type": "ropeless_buoy_device",
            "is_active": True,
            "recorded_at": "2025-03-17T16:43:40+00:00",
            "location": {"lat": 41.52537796592242, "lon": -70.6738777899687},
            "additional": {
                "subject_name": "edgetech_88CE999763_B",
                "edgetech_serial_number": "88CE999763",
                "display_id": "5eb353fb0f49",
                "event_type": "gear_deployed",
                "devices": [
                    {
                        "label": "a",
                        "location": {
                            "latitude": 41.52546746182916,
                            "longitude": -70.67401171221228,
                        },
                        "device_id": "edgetech_88CE999763_A",
                        "last_updated": "2025-03-17T16:43:40+00:00",
                    },
                    {
                        "label": "b",
                        "location": {
                            "latitude": 41.52537796592242,
                            "longitude": -70.6738777899687,
                        },
                        "device_id": "edgetech_88CE999763_B",
                        "last_updated": "2025-03-17T16:43:40+00:00",
                    },
                ],
            },
        },
    ]

    expected_observations.sort(key=lambda x: x["recorded_at"])
    observations.sort(key=lambda x: x["recorded_at"])
    # Assert
    assert observations == expected_observations

@pytest.mark.usefixtures
def test_create_observations_recored_at_same_as_current_state(get_mock_edgetech_data):
    """
    Test the create_observations method of the Buoy class with a changeRecord with recorded_at
    at the same time as the currentState.
    """

    # Arrange
    buoys: List[Buoy] = [Buoy.parse_obj(record) for record in get_mock_edgetech_data]
    last_observation_timestamp = datetime.fromisoformat("2025-03-17T16:43:40").replace(
        tzinfo=timezone.utc
    )
    # Act
    observations = []
    for buoy in buoys:
        buoy_observations, _, _ = buoy.create_observations(
            "edgetech_", last_observation_timestamp
        )
        observations.extend(buoy_observations)

    # Assert
    assert observations == []


@pytest.mark.usefixtures
def test_create_observations_with_last_updated_timestamp(get_mock_edgetech_data):
    """
    Test the create_observations method of the Buoy class with a last
    updated timestamp (last_observation_timestamp) provided.

    - Test creating observations from multiple buoys.
    - Test creating observations from both changeRecords and
      currentState.
    - Test creating observations with a last updated timestamp
        - Prevents creating observations with a recorded_at timestamp
          earlier than the last updated timestamp.
    """

    # Arrange
    buoys: List[Buoy] = [Buoy.parse_obj(record) for record in get_mock_edgetech_data]
    last_observation_timestamp = datetime.fromisoformat("2025-03-17T16:43:40").replace(
        tzinfo=timezone.utc
    )

    buoys[12].changeRecords[0].changes[1].newValue = (
        buoys[12].currentState.dateRecovered - timedelta(minutes=2)
    ).isoformat()

    # Act
    observations = []
    for buoy in buoys:
        buoy_observations, _, _ = buoy.create_observations(
            "edgetech_", last_observation_timestamp
        )
        observations.extend(buoy_observations)

    # Assert
    expected_observations = [
        {
            "name": "edgetech_88CE999763_A",
            "source": "edgetech_88CE999763_A",
            "type": "ropeless_buoy",
            "subject_type": "ropeless_buoy_device",
            "is_active": False,
            "recorded_at": "2025-03-17T17:34:32+00:00",
            "location": {"lat": 41.52546746182916, "lon": -70.67401171221228},
            "additional": {
                "subject_name": "edgetech_88CE999763_A",
                "edgetech_serial_number": "88CE999763",
                "display_id": "5eb353fb0f49",
                "event_type": "gear_retrieved",
                "devices": [
                    {
                        "label": "a",
                        "location": {
                            "latitude": 41.52546746182916,
                            "longitude": -70.67401171221228,
                        },
                        "device_id": "edgetech_88CE999763_A",
                        "last_updated": "2025-03-17T17:34:32+00:00",
                    },
                    {
                        "label": "b",
                        "location": {
                            "latitude": 41.52537796592242,
                            "longitude": -70.6738777899687,
                        },
                        "device_id": "edgetech_88CE999763_B",
                        "last_updated": "2025-03-17T17:34:32+00:00",
                    },
                ],
            },
        },
        {
            "name": "edgetech_88CE999763_B",
            "source": "edgetech_88CE999763_B",
            "type": "ropeless_buoy",
            "subject_type": "ropeless_buoy_device",
            "is_active": False,
            "recorded_at": "2025-03-17T17:34:32+00:00",
            "location": {"lat": 41.52537796592242, "lon": -70.6738777899687},
            "additional": {
                "subject_name": "edgetech_88CE999763_B",
                "edgetech_serial_number": "88CE999763",
                "display_id": "5eb353fb0f49",
                "event_type": "gear_retrieved",
                "devices": [
                    {
                        "label": "a",
                        "location": {
                            "latitude": 41.52546746182916,
                            "longitude": -70.67401171221228,
                        },
                        "device_id": "edgetech_88CE999763_A",
                        "last_updated": "2025-03-17T17:34:32+00:00",
                    },
                    {
                        "label": "b",
                        "location": {
                            "latitude": 41.52537796592242,
                            "longitude": -70.6738777899687,
                        },
                        "device_id": "edgetech_88CE999763_B",
                        "last_updated": "2025-03-17T17:34:32+00:00",
                    },
                ],
            },
        },
    ]
    assert observations == expected_observations
