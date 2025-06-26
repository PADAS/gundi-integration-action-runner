import json
from datetime import datetime

import pytest
from gundi_core.schemas import IntegrationInformation
from gundi_core.schemas.v2 import Connection, ConnectionIntegration
from pydantic import SecretStr

from app.actions.configurations import EdgeTechAuthConfiguration, EdgeTechConfiguration
from app.actions.edgetech.types import Buoy
from ropeless_utils import State


@pytest.fixture
def a_new_edgetech_trawl_record():
    """Fixture to create a sample EdgeTech record."""
    return {
        "serialNumber": "8899CEDAAA",
        "currentState": {
            "etag": "1748195599731",
            "isDeleted": False,
            "positionSetByCapri": False,
            "serialNumber": "8899CEDAAA",
            "releaseCommand": "C8AB8C75AA",
            "statusCommand": "8899CEDAAA",
            "idCommand": "CCCCCCCCCC",
            "isNfcTag": False,
            "latDeg": 44.358265,
            "lonDeg": -68.16757,
            "endLatDeg": 44.3591792,
            "endLonDeg": -68.167191,
            "modelNumber": "",
            "isDeployed": True,
            "dateDeployed": "2025-05-25T17:53:19.517Z",
            "lastUpdated": "2025-05-25T17:53:19.731Z",
        },
        "changeRecords": [
            {
                "type": "MODIFY",
                "timestamp": "2025-05-25T17:53:19.000Z",
                "changes": [
                    {
                        "key": "dateDeployed",
                        "oldValue": None,
                        "newValue": "2025-05-25T17:53:19.517Z",
                    },
                    {
                        "key": "dateRecovered",
                        "oldValue": "2025-05-25T17:40:26.022Z",
                        "newValue": None,
                    },
                    {
                        "key": "endLatDeg",
                        "oldValue": None,
                        "newValue": 44.3591792,
                    },
                    {
                        "key": "endLonDeg",
                        "oldValue": None,
                        "newValue": -68.167191,
                    },
                    {
                        "key": "geoHash",
                        "oldValue": "X",
                        "newValue": "4caeb",
                    },
                    {
                        "key": "isDeployed",
                        "oldValue": False,
                        "newValue": True,
                    },
                    {
                        "key": "lastUpdated",
                        "oldValue": "2025-05-25T17:42:30.350Z",
                        "newValue": "2025-05-25T17:53:19.731Z",
                    },
                    {
                        "key": "latDeg",
                        "oldValue": None,
                        "newValue": 44.358265,
                    },
                    {
                        "key": "lonDeg",
                        "oldValue": None,
                        "newValue": -68.16757,
                    },
                    {
                        "key": "recoveredLatDeg",
                        "oldValue": 44.3673757,
                        "newValue": None,
                    },
                    {
                        "key": "recoveredLonDeg",
                        "oldValue": -68.1953659,
                        "newValue": None,
                    },
                    {
                        "key": "recoveredRangeM",
                        "oldValue": 107.856,
                        "newValue": None,
                    },
                    {
                        "key": "recoveredTemperatureC",
                        "oldValue": 6,
                        "newValue": None,
                    },
                ],
            },
        ],
    }

@pytest.fixture
def a_deployed_earthranger_subject():
    return {
        "content_type": "observations.subject",
        "id": "0ac4fdf0-172e-467e-91a0-f7395847417d",
        "name": "edgetech_88CE999CAA_A",
        "subject_type": "unassigned",
        "subject_subtype": "ropeless_buoy_device",
        "common_name": None,
        "additional": {
            "devices": [
                {
                    "label": "a",
                    "location": {"latitude": 37.4802635, "longitude": -122.5286185},
                    "device_id": "edgetech_88CE999CAA_A",
                    "last_updated": "2023-10-13T18:42:33+00:00",
                }
            ],
            "display_id": "aa1b1aefc7d0",
            "event_type": "gear_retrieved",
            "subject_name": "edgetech_88CE999CAA_A",
            "subject_is_active": False,
            "edgetech_serial_number": "88CE999CAA",
        },
        "created_at": "2025-04-02T11:27:11.666704-07:00",
        "updated_at": "2025-06-07T19:28:40.829042-07:00",
        "is_active": True,
        "user": None,
        "tracks_available": True,
        "image_url": "/static/pin-black.svg",
        "last_position_status": {
            "last_voice_call_start_at": None,
            "radio_state_at": None,
            "radio_state": "na",
        },
        "last_position_date": "2025-06-07T04:09:34+00:00",
        "last_position": {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [-122.9417194, 37.997533]},
            "properties": {
                "title": "edgetech_88CE999CAA_A",
                "subject_type": "unassigned",
                "subject_subtype": "ropeless_buoy_device",
                "id": "0ac4fdf0-172e-467e-91a0-f7395847417d",
                "stroke": "#FFFF00",
                "stroke-opacity": 1.0,
                "stroke-width": 2,
                "image": "https://buoy.pamdas.org/static/pin-black.svg",
                "last_voice_call_start_at": None,
                "location_requested_at": None,
                "radio_state_at": "1970-01-01T00:00:00+00:00",
                "radio_state": "na",
                "coordinateProperties": {"time": "2025-06-07T04:09:34+00:00"},
                "DateTime": "2025-06-07T04:09:34+00:00",
            },
        },
        "device_status_properties": None,
        "url": "https://buoy.pamdas.org/api/v1.0/subject/0ac4fdf0-172e-467e-91a0-f7395847417d",
    }