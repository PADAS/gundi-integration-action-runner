import hashlib
from ast import List
from datetime import datetime, timezone

import pytest

from app.actions.buoy import ObservationSubject
from app.actions.edgetech import EdgeTechProcessor
from app.actions.edgetech.types import Buoy


@pytest.mark.asyncio
async def test_edgetech_processor_updated_buoys(
    mocker, get_mock_edgetech_data, get_er_subjects_updated_data
):
    """
    Test if the EdgeTechProcessor returns the correct updated buoys
    when the buoys are already in the newest state in the ER site.

    - The EdgeTechProcessor should return an empty list of observations
    - The EdgeTechProcessor should return an empty set of inserts_buoys
    - The EdgeTechProcessor should return an empty set of update_buoys
    """
    # Arrange
    buoys = [Buoy.parse_obj(buoy) for buoy in get_mock_edgetech_data]
    subjects = [
        ObservationSubject.parse_obj(subject)
        for subject in get_er_subjects_updated_data
    ]

    mocker.patch("app.actions.buoy.BuoyClient.get_er_subjects", return_value=subjects)
    # Act
    processor = EdgeTechProcessor(buoys, "er_token", "er_token")
    observations, inserts_buoys, update_buoys = await processor.process()

    assert observations == []
    assert inserts_buoys == set()
    assert update_buoys == set()


@pytest.mark.asyncio
async def test_edgetech_processor_inserting_buoys(mocker, get_mock_edgetech_data):
    """ "
    Test if the EdgeTechProcessor returns the correct observations, inserts_buoys and update_buoys
    when the buoys are not in the ER site.

    - The EdgeTechProcessor should return the correct observations
    - The EdgeTechProcessor should return the correct set of inserts_buoys
    - The EdgeTechProcessor should return an empty set of update_buoys
    """

    # Arrange
    buoys = [Buoy.parse_obj(buoy) for buoy in get_mock_edgetech_data]
    subjects = []

    mocker.patch("app.actions.buoy.BuoyClient.get_er_subjects", return_value=subjects)

    # Act
    processor = EdgeTechProcessor(buoys, "er_token", "er_token")
    observations, inserts_buoys, update_buoys = await processor.process()

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
            "name": "edgetech_88CE9978AE_A",
            "source": "edgetech_88CE9978AE_A",
            "type": "ropeless_buoy",
            "subject_type": "ropeless_buoy_device",
            "is_active": False,
            "recorded_at": "2024-12-10T14:51:59+00:00",
            "location": {"lat": 41.82907459248435, "lon": -71.41540430869928},
            "additional": {
                "subject_name": "edgetech_88CE9978AE_A",
                "edgetech_serial_number": "88CE9978AE",
                "display_id": "f1ab617fc777",
                "event_type": "gear_retrieved",
                "devices": [
                    {
                        "label": "a",
                        "location": {
                            "latitude": 41.82907459248435,
                            "longitude": -71.41540430869928,
                        },
                        "device_id": "edgetech_88CE9978AE_A",
                        "last_updated": "2024-12-10T14:51:59+00:00",
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
            "recorded_at": "2024-12-10T14:52:06+00:00",
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
                        "last_updated": "2024-12-10T14:52:06+00:00",
                    }
                ],
            },
        },
        {
            "name": "edgetech_88CE9978AE_A",
            "source": "edgetech_88CE9978AE_A",
            "type": "ropeless_buoy",
            "subject_type": "ropeless_buoy_device",
            "is_active": False,
            "recorded_at": "2025-02-13T15:30:42+00:00",
            "location": {"lat": 41.82907459248435, "lon": -71.41540430869928},
            "additional": {
                "subject_name": "edgetech_88CE9978AE_A",
                "edgetech_serial_number": "88CE9978AE",
                "display_id": "f1ab617fc777",
                "event_type": "gear_retrieved",
                "devices": [
                    {
                        "label": "a",
                        "location": {
                            "latitude": 41.82907459248435,
                            "longitude": -71.41540430869928,
                        },
                        "device_id": "edgetech_88CE9978AE_A",
                        "last_updated": "2025-02-13T15:30:42+00:00",
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
    expected_inserts_buoys = {buoy.serialNumber for buoy in buoys}

    # Assert
    observations.sort(key=lambda x: x["recorded_at"])
    expected_observations.sort(key=lambda x: x["recorded_at"])
    assert observations == expected_observations
    assert inserts_buoys == expected_inserts_buoys
    assert update_buoys == set()
