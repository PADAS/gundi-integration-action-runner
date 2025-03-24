import json
import logging
from typing import List, Optional

import aiohttp

from .types import ObservationSubject

logger = logging.getLogger(__name__)


class BuoyClient:
    def __init__(self, er_token: str, er_site: str):
        self.er_token = er_token
        self.er_site = er_site
        self.headers = {
            "Authorization": f"Bearer {self.er_token}",
        }

    async def get_er_subjects(
        self, start_datetime: Optional[str] = None
    ) -> List[ObservationSubject]:
        return [
            ObservationSubject.parse_obj(r)
            for r in [
                {
                    "content_type": "observations.subject",
                    "id": "015afed5-bc84-4872-b9fa-0e6b2f67b72c",
                    "name": "edgetech_88CE999763_A",
                    "subject_type": "ropeless_buoy",
                    "subject_subtype": "ropeless_buoy_device",
                    "common_name": None,
                    "additional": {
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
                        "display_id": "88CE999763",
                        "event_type": "gear_deployed",
                        "subject_name": "edgetech_88CE999763_A",
                        "edgetech_serial_number": "88CE999763",
                    },
                    "created_at": "2025-03-21T13:33:10.537494-07:00",
                    "updated_at": "2025-03-21T13:33:10.537528-07:00",
                    "is_active": False,
                    "user": None,
                    "tracks_available": True,
                    "image_url": "/static/pin-black.svg",
                    "last_position_status": {
                        "last_voice_call_start_at": None,
                        "radio_state_at": None,
                        "radio_state": "na",
                    },
                    "last_position_date": "2025-03-17T17:36:32+00:00",
                    "last_position": {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [-70.67401171221228, 41.52546746182916],
                        },
                        "properties": {
                            "title": "edgetech_88CE999763_A",
                            "subject_type": "ropeless_buoy",
                            "subject_subtype": "ropeless_buoy_device",
                            "id": "015afed5-bc84-4872-b9fa-0e6b2f67b72c",
                            "stroke": "#FFFF00",
                            "stroke-opacity": 1.0,
                            "stroke-width": 2,
                            "image": "https://buoy.dev.pamdas.org/static/pin-black.svg",
                            "last_voice_call_start_at": None,
                            "location_requested_at": None,
                            "radio_state_at": "1970-01-01T00:00:00+00:00",
                            "radio_state": "na",
                            "coordinateProperties": {
                                "time": "2025-03-17T17:36:32+00:00"
                            },
                            "DateTime": "2025-03-17T17:36:32+00:00",
                        },
                    },
                    "device_status_properties": None,
                    "url": "https://buoy.dev.pamdas.org/api/v1.0/subject/015afed5-bc84-4872-b9fa-0e6b2f67b72c",
                },
                {
                    "content_type": "observations.subject",
                    "id": "48626952-ff1a-40fe-8d4d-1427084bec24",
                    "name": "edgetech_88CE999763_B",
                    "subject_type": "ropeless_buoy",
                    "subject_subtype": "ropeless_buoy_device",
                    "common_name": None,
                    "additional": {
                        "devices": [
                            {
                                "label": "a",
                                "location": {
                                    "latitude": 41.52546746182916,
                                    "longitude": -70.67401171221228,
                                },
                                "device_id": "edgetech_88CE999763_A",
                                "last_updated": "2025-03-17T17:36:32+00:00",
                            },
                            {
                                "label": "b",
                                "location": {
                                    "latitude": 41.52537796592242,
                                    "longitude": -70.6738777899687,
                                },
                                "device_id": "edgetech_88CE999763_B",
                                "last_updated": "2025-03-17T17:36:32+00:00",
                            },
                        ],
                        "display_id": "88CE999763",
                        "event_type": "gear_retrieved",
                        "subject_name": "edgetech_88CE999763_B",
                        "edgetech_serial_number": "88CE999763",
                    },
                    "created_at": "2025-03-21T13:33:12.033995-07:00",
                    "updated_at": "2025-03-21T13:33:12.034028-07:00",
                    "is_active": False,
                    "user": None,
                    "tracks_available": True,
                    "image_url": "/static/pin-black.svg",
                    "last_position_status": {
                        "last_voice_call_start_at": None,
                        "radio_state_at": None,
                        "radio_state": "na",
                    },
                    "last_position_date": "2025-03-17T17:36:32+00:00",
                    "last_position": {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [-70.6738777899687, 41.52537796592242],
                        },
                        "properties": {
                            "title": "edgetech_88CE999763_B",
                            "subject_type": "ropeless_buoy",
                            "subject_subtype": "ropeless_buoy_device",
                            "id": "48626952-ff1a-40fe-8d4d-1427084bec24",
                            "stroke": "#FFFF00",
                            "stroke-opacity": 1.0,
                            "stroke-width": 2,
                            "image": "https://buoy.dev.pamdas.org/static/pin-black.svg",
                            "last_voice_call_start_at": None,
                            "location_requested_at": None,
                            "radio_state_at": "1970-01-01T00:00:00+00:00",
                            "radio_state": "na",
                            "coordinateProperties": {
                                "time": "2025-03-17T17:36:32+00:00"
                            },
                            "DateTime": "2025-03-17T17:36:32+00:00",
                        },
                    },
                    "device_status_properties": None,
                    "url": "https://buoy.dev.pamdas.org/api/v1.0/subject/48626952-ff1a-40fe-8d4d-1427084bec24",
                },
                {
                    "content_type": "observations.subject",
                    "id": "8f8dcf49-0b73-4a29-81ce-d3bb0f293f98",
                    "name": "edgetech_88CE9978AE_A",
                    "subject_type": "ropeless_buoy",
                    "subject_subtype": "ropeless_buoy_device",
                    "common_name": None,
                    "additional": {
                        "devices": [
                            {
                                "label": "a",
                                "location": {
                                    "latitude": 41.82907459248435,
                                    "longitude": -71.41540430869928,
                                },
                                "device_id": "edgetech_88CE9978AE_A",
                                "last_updated": "2024-12-09T14:51:59+00:00",
                            }
                        ],
                        "display_id": "88CE9978AE",
                        "event_type": "gear_deployed",
                        "subject_name": "edgetech_88CE9978AE_A",
                        "edgetech_serial_number": "88CE9978AE",
                    },
                    "created_at": "2025-03-21T13:33:24.351567-07:00",
                    "updated_at": "2025-03-21T13:33:24.351595-07:00",
                    "is_active": True,
                    "user": None,
                    "tracks_available": True,
                    "image_url": "/static/pin-black.svg",
                    "last_position_status": {
                        "last_voice_call_start_at": None,
                        "radio_state_at": None,
                        "radio_state": "na",
                    },
                    "last_position_date": "2025-02-12T15:30:42+00:00",
                    "last_position": {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [-71.41540430869928, 41.82907459248435],
                        },
                        "properties": {
                            "title": "edgetech_88CE9978AE_A",
                            "subject_type": "ropeless_buoy",
                            "subject_subtype": "ropeless_buoy_device",
                            "id": "8f8dcf49-0b73-4a29-81ce-d3bb0f293f98",
                            "stroke": "#FFFF00",
                            "stroke-opacity": 1.0,
                            "stroke-width": 2,
                            "image": "https://buoy.dev.pamdas.org/static/pin-black.svg",
                            "last_voice_call_start_at": None,
                            "location_requested_at": None,
                            "radio_state_at": "1970-01-01T00:00:00+00:00",
                            "radio_state": "na",
                            "coordinateProperties": {
                                "time": "2025-02-13T15:30:42+00:00"
                            },
                            "DateTime": "2025-02-13T15:30:42+00:00",
                        },
                    },
                    "device_status_properties": None,
                    "url": "https://buoy.dev.pamdas.org/api/v1.0/subject/8f8dcf49-0b73-4a29-81ce-d3bb0f293f98",
                },
                {
                    "content_type": "observations.subject",
                    "id": "a90fba94-0868-4af7-a6ac-8e61df94976c",
                    "name": "edgetech_88CE99C99A_A",
                    "subject_type": "ropeless_buoy",
                    "subject_subtype": "ropeless_buoy_device",
                    "common_name": None,
                    "additional": {
                        "devices": [
                            {
                                "label": "a",
                                "location": {
                                    "latitude": 41.5740898,
                                    "longitude": -70.8831463,
                                },
                                "device_id": "edgetech_88CE99C99A_A",
                                "last_updated": "2024-12-10T14:52:06+00:00",
                            }
                        ],
                        "display_id": "88CE99C99A",
                        "event_type": "gear_retrieved",
                        "subject_name": "edgetech_88CE99C99A_A",
                        "edgetech_serial_number": "88CE99C99A",
                    },
                    "created_at": "2025-03-21T13:33:17.862024-07:00",
                    "updated_at": "2025-03-21T13:33:17.862049-07:00",
                    "is_active": False,
                    "user": None,
                    "tracks_available": True,
                    "image_url": "/static/pin-black.svg",
                    "last_position_status": {
                        "last_voice_call_start_at": None,
                        "radio_state_at": None,
                        "radio_state": "na",
                    },
                    "last_position_date": "2025-03-14T12:07:27+00:00",
                    "last_position": {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [-70.7527803, 41.7832483],
                        },
                        "properties": {
                            "title": "edgetech_88CE99C99A_A",
                            "subject_type": "ropeless_buoy",
                            "subject_subtype": "ropeless_buoy_device",
                            "id": "a90fba94-0868-4af7-a6ac-8e61df94976c",
                            "stroke": "#FFFF00",
                            "stroke-opacity": 1.0,
                            "stroke-width": 2,
                            "image": "https://buoy.dev.pamdas.org/static/pin-black.svg",
                            "last_voice_call_start_at": None,
                            "location_requested_at": None,
                            "radio_state_at": "1970-01-01T00:00:00+00:00",
                            "radio_state": "na",
                            "coordinateProperties": {
                                "time": "2025-03-14T12:07:27+00:00"
                            },
                            "DateTime": "2025-03-14T12:07:27+00:00",
                        },
                    },
                    "device_status_properties": None,
                    "url": "https://buoy.dev.pamdas.org/api/v1.0/subject/a90fba94-0868-4af7-a6ac-8e61df94976c",
                },
            ]
        ]
        return [
            ObservationSubject.parse_obj(obj)
            for obj in [
                {
                    "content_type": "observations.subject",
                    "id": "015afed5-bc84-4872-b9fa-0e6b2f67b72c",
                    "name": "edgetech_88CE999763_A",
                    "subject_type": "ropeless_buoy",
                    "subject_subtype": "ropeless_buoy_device",
                    "common_name": None,
                    "additional": {
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
                        "display_id": "88CE999763",
                        "event_type": "gear_deployed",
                        "subject_name": "edgetech_88CE999763_A",
                        "edgetech_serial_number": "88CE999763",
                    },
                    "created_at": "2025-03-21T13:33:10.537494-07:00",
                    "updated_at": "2025-03-21T13:33:10.537528-07:00",
                    "is_active": False,
                    "user": None,
                    "tracks_available": True,
                    "image_url": "/static/pin-black.svg",
                    "last_position_status": {
                        "last_voice_call_start_at": None,
                        "radio_state_at": None,
                        "radio_state": "na",
                    },
                    "last_position_date": "2025-03-17T17:36:32+00:00",
                    "last_position": {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [-70.67401171221228, 41.52546746182916],
                        },
                        "properties": {
                            "title": "edgetech_88CE999763_A",
                            "subject_type": "ropeless_buoy",
                            "subject_subtype": "ropeless_buoy_device",
                            "id": "015afed5-bc84-4872-b9fa-0e6b2f67b72c",
                            "stroke": "#FFFF00",
                            "stroke-opacity": 1.0,
                            "stroke-width": 2,
                            "image": "https://buoy.dev.pamdas.org/static/pin-black.svg",
                            "last_voice_call_start_at": None,
                            "location_requested_at": None,
                            "radio_state_at": "1970-01-01T00:00:00+00:00",
                            "radio_state": "na",
                            "coordinateProperties": {
                                "time": "2025-03-17T17:36:32+00:00"
                            },
                            "DateTime": "2025-03-17T17:36:32+00:00",
                        },
                    },
                    "device_status_properties": None,
                    "url": "https://buoy.dev.pamdas.org/api/v1.0/subject/015afed5-bc84-4872-b9fa-0e6b2f67b72c",
                },
                {
                    "content_type": "observations.subject",
                    "id": "48626952-ff1a-40fe-8d4d-1427084bec24",
                    "name": "edgetech_88CE999763_B",
                    "subject_type": "ropeless_buoy",
                    "subject_subtype": "ropeless_buoy_device",
                    "common_name": None,
                    "additional": {
                        "devices": [
                            {
                                "label": "a",
                                "location": {
                                    "latitude": 41.52546746182916,
                                    "longitude": -70.67401171221228,
                                },
                                "device_id": "edgetech_88CE999763_A",
                                "last_updated": "2025-03-17T17:36:32+00:00",
                            },
                            {
                                "label": "b",
                                "location": {
                                    "latitude": 41.52537796592242,
                                    "longitude": -70.6738777899687,
                                },
                                "device_id": "edgetech_88CE999763_B",
                                "last_updated": "2025-03-17T17:36:32+00:00",
                            },
                        ],
                        "display_id": "88CE999763",
                        "event_type": "gear_retrieved",
                        "subject_name": "edgetech_88CE999763_B",
                        "edgetech_serial_number": "88CE999763",
                    },
                    "created_at": "2025-03-21T13:33:12.033995-07:00",
                    "updated_at": "2025-03-21T13:33:12.034028-07:00",
                    "is_active": False,
                    "user": None,
                    "tracks_available": True,
                    "image_url": "/static/pin-black.svg",
                    "last_position_status": {
                        "last_voice_call_start_at": None,
                        "radio_state_at": None,
                        "radio_state": "na",
                    },
                    "last_position_date": "2025-03-17T17:36:32+00:00",
                    "last_position": {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [-70.6738777899687, 41.52537796592242],
                        },
                        "properties": {
                            "title": "edgetech_88CE999763_B",
                            "subject_type": "ropeless_buoy",
                            "subject_subtype": "ropeless_buoy_device",
                            "id": "48626952-ff1a-40fe-8d4d-1427084bec24",
                            "stroke": "#FFFF00",
                            "stroke-opacity": 1.0,
                            "stroke-width": 2,
                            "image": "https://buoy.dev.pamdas.org/static/pin-black.svg",
                            "last_voice_call_start_at": None,
                            "location_requested_at": None,
                            "radio_state_at": "1970-01-01T00:00:00+00:00",
                            "radio_state": "na",
                            "coordinateProperties": {
                                "time": "2025-03-17T17:36:32+00:00"
                            },
                            "DateTime": "2025-03-17T17:36:32+00:00",
                        },
                    },
                    "device_status_properties": None,
                    "url": "https://buoy.dev.pamdas.org/api/v1.0/subject/48626952-ff1a-40fe-8d4d-1427084bec24",
                },
                {
                    "content_type": "observations.subject",
                    "id": "8f8dcf49-0b73-4a29-81ce-d3bb0f293f98",
                    "name": "edgetech_88CE9978AE_A",
                    "subject_type": "ropeless_buoy",
                    "subject_subtype": "ropeless_buoy_device",
                    "common_name": None,
                    "additional": {
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
                        "display_id": "88CE9978AE",
                        "event_type": "gear_retrieved",
                        "subject_name": "edgetech_88CE9978AE_A",
                        "edgetech_serial_number": "88CE9978AE",
                    },
                    "created_at": "2025-03-21T13:33:24.351567-07:00",
                    "updated_at": "2025-03-21T13:33:24.351595-07:00",
                    "is_active": False,
                    "user": None,
                    "tracks_available": True,
                    "image_url": "/static/pin-black.svg",
                    "last_position_status": {
                        "last_voice_call_start_at": None,
                        "radio_state_at": None,
                        "radio_state": "na",
                    },
                    "last_position_date": "2025-02-13T15:30:42+00:00",
                    "last_position": {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [-71.41540430869928, 41.82907459248435],
                        },
                        "properties": {
                            "title": "edgetech_88CE9978AE_A",
                            "subject_type": "ropeless_buoy",
                            "subject_subtype": "ropeless_buoy_device",
                            "id": "8f8dcf49-0b73-4a29-81ce-d3bb0f293f98",
                            "stroke": "#FFFF00",
                            "stroke-opacity": 1.0,
                            "stroke-width": 2,
                            "image": "https://buoy.dev.pamdas.org/static/pin-black.svg",
                            "last_voice_call_start_at": None,
                            "location_requested_at": None,
                            "radio_state_at": "1970-01-01T00:00:00+00:00",
                            "radio_state": "na",
                            "coordinateProperties": {
                                "time": "2025-02-13T15:30:42+00:00"
                            },
                            "DateTime": "2025-02-13T15:30:42+00:00",
                        },
                    },
                    "device_status_properties": None,
                    "url": "https://buoy.dev.pamdas.org/api/v1.0/subject/8f8dcf49-0b73-4a29-81ce-d3bb0f293f98",
                },
                {
                    "content_type": "observations.subject",
                    "id": "a90fba94-0868-4af7-a6ac-8e61df94976c",
                    "name": "edgetech_88CE99C99A_A",
                    "subject_type": "ropeless_buoy",
                    "subject_subtype": "ropeless_buoy_device",
                    "common_name": None,
                    "additional": {
                        "devices": [
                            {
                                "label": "a",
                                "location": {
                                    "latitude": 41.5740898,
                                    "longitude": -70.8831463,
                                },
                                "device_id": "edgetech_88CE99C99A_A",
                                "last_updated": "2024-12-10T14:52:06+00:00",
                            }
                        ],
                        "display_id": "88CE99C99A",
                        "event_type": "gear_retrieved",
                        "subject_name": "edgetech_88CE99C99A_A",
                        "edgetech_serial_number": "88CE99C99A",
                    },
                    "created_at": "2025-03-21T13:33:17.862024-07:00",
                    "updated_at": "2025-03-21T13:33:17.862049-07:00",
                    "is_active": False,
                    "user": None,
                    "tracks_available": True,
                    "image_url": "/static/pin-black.svg",
                    "last_position_status": {
                        "last_voice_call_start_at": None,
                        "radio_state_at": None,
                        "radio_state": "na",
                    },
                    "last_position_date": "2025-03-14T12:07:27+00:00",
                    "last_position": {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [-70.7527803, 41.7832483],
                        },
                        "properties": {
                            "title": "edgetech_88CE99C99A_A",
                            "subject_type": "ropeless_buoy",
                            "subject_subtype": "ropeless_buoy_device",
                            "id": "a90fba94-0868-4af7-a6ac-8e61df94976c",
                            "stroke": "#FFFF00",
                            "stroke-opacity": 1.0,
                            "stroke-width": 2,
                            "image": "https://buoy.dev.pamdas.org/static/pin-black.svg",
                            "last_voice_call_start_at": None,
                            "location_requested_at": None,
                            "radio_state_at": "1970-01-01T00:00:00+00:00",
                            "radio_state": "na",
                            "coordinateProperties": {
                                "time": "2025-03-14T12:07:27+00:00"
                            },
                            "DateTime": "2025-03-14T12:07:27+00:00",
                        },
                    },
                    "device_status_properties": None,
                    "url": "https://buoy.dev.pamdas.org/api/v1.0/subject/a90fba94-0868-4af7-a6ac-8e61df94976c",
                },
                {
                    "content_type": "observations.subject",
                    "id": "bd29dbde-7138-48bf-b612-ae236506cefe",
                    "name": "edgetech_88CE999763_A",
                    "subject_type": "ropeless_buoy",
                    "subject_subtype": "ropeless_buoy_device",
                    "common_name": None,
                    "additional": {
                        "devices": [
                            {
                                "label": "a",
                                "location": {
                                    "latitude": 41.52546746182916,
                                    "longitude": -70.67401171221228,
                                },
                                "device_id": "edgetech_88CE999763_A",
                                "last_updated": "2025-03-17T17:36:32+00:00",
                            },
                            {
                                "label": "b",
                                "location": {
                                    "latitude": 41.52537796592242,
                                    "longitude": -70.6738777899687,
                                },
                                "device_id": "edgetech_88CE999763_B",
                                "last_updated": "2025-03-17T17:36:32+00:00",
                            },
                        ],
                        "display_id": "a30695bbab45",
                        "event_type": "gear_retrieved",
                        "subject_name": "edgetech_88CE999763_A",
                        "rmwhub_set_id": "e_333e6ad9-88a2-4c68-a631-af5c70e4b727",
                    },
                    "created_at": "2025-03-21T15:21:41.890043-07:00",
                    "updated_at": "2025-03-21T15:21:41.890067-07:00",
                    "is_active": False,
                    "user": None,
                    "tracks_available": True,
                    "image_url": "/static/pin-black.svg",
                    "last_position_status": {
                        "last_voice_call_start_at": None,
                        "radio_state_at": None,
                        "radio_state": "na",
                    },
                    "last_position_date": "2025-03-21T20:33:12+00:00",
                    "last_position": {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [-70.67401171221228, 41.52546746182916],
                        },
                        "properties": {
                            "title": "edgetech_88CE999763_A",
                            "subject_type": "ropeless_buoy",
                            "subject_subtype": "ropeless_buoy_device",
                            "id": "bd29dbde-7138-48bf-b612-ae236506cefe",
                            "stroke": "#FFFF00",
                            "stroke-opacity": 1.0,
                            "stroke-width": 2,
                            "image": "https://buoy.dev.pamdas.org/static/pin-black.svg",
                            "last_voice_call_start_at": None,
                            "location_requested_at": None,
                            "radio_state_at": "1970-01-01T00:00:00+00:00",
                            "radio_state": "na",
                            "coordinateProperties": {
                                "time": "2025-03-21T20:33:12+00:00"
                            },
                            "DateTime": "2025-03-21T20:33:12+00:00",
                        },
                    },
                    "device_status_properties": None,
                    "url": "https://buoy.dev.pamdas.org/api/v1.0/subject/bd29dbde-7138-48bf-b612-ae236506cefe",
                },
                {
                    "content_type": "observations.subject",
                    "id": "c1c67282-0d58-4a6c-ba82-9593ee8f7056",
                    "name": "edgetech_88CE9978AE_A",
                    "subject_type": "ropeless_buoy",
                    "subject_subtype": "ropeless_buoy_device",
                    "common_name": None,
                    "additional": {
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
                        "display_id": "bd758697f348",
                        "event_type": "gear_retrieved",
                        "subject_name": "edgetech_88CE9978AE_A",
                        "rmwhub_set_id": "e_402a17d1-dbe3-4ace-a861-dec1d7191f5e",
                    },
                    "created_at": "2025-03-21T15:21:42.190554-07:00",
                    "updated_at": "2025-03-21T15:21:42.190591-07:00",
                    "is_active": False,
                    "user": None,
                    "tracks_available": True,
                    "image_url": "/static/pin-black.svg",
                    "last_position_status": {
                        "last_voice_call_start_at": None,
                        "radio_state_at": None,
                        "radio_state": "na",
                    },
                    "last_position_date": "2025-03-21T20:33:24+00:00",
                    "last_position": {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [-71.41540430869928, 41.82907459248435],
                        },
                        "properties": {
                            "title": "edgetech_88CE9978AE_A",
                            "subject_type": "ropeless_buoy",
                            "subject_subtype": "ropeless_buoy_device",
                            "id": "c1c67282-0d58-4a6c-ba82-9593ee8f7056",
                            "stroke": "#FFFF00",
                            "stroke-opacity": 1.0,
                            "stroke-width": 2,
                            "image": "https://buoy.dev.pamdas.org/static/pin-black.svg",
                            "last_voice_call_start_at": None,
                            "location_requested_at": None,
                            "radio_state_at": "1970-01-01T00:00:00+00:00",
                            "radio_state": "na",
                            "coordinateProperties": {
                                "time": "2025-03-21T20:33:24+00:00"
                            },
                            "DateTime": "2025-03-21T20:33:24+00:00",
                        },
                    },
                    "device_status_properties": None,
                    "url": "https://buoy.dev.pamdas.org/api/v1.0/subject/c1c67282-0d58-4a6c-ba82-9593ee8f7056",
                },
                {
                    "content_type": "observations.subject",
                    "id": "cbd865ab-b0ed-4faf-9169-88fe097d0ece",
                    "name": "edgetech_88CE999763_B",
                    "subject_type": "ropeless_buoy",
                    "subject_subtype": "ropeless_buoy_device",
                    "common_name": None,
                    "additional": {
                        "devices": [
                            {
                                "label": "a",
                                "location": {
                                    "latitude": 41.52546746182916,
                                    "longitude": -70.67401171221228,
                                },
                                "device_id": "edgetech_88CE999763_A",
                                "last_updated": "2025-03-17T17:36:32+00:00",
                            },
                            {
                                "label": "b",
                                "location": {
                                    "latitude": 41.52537796592242,
                                    "longitude": -70.6738777899687,
                                },
                                "device_id": "edgetech_88CE999763_B",
                                "last_updated": "2025-03-17T17:36:32+00:00",
                            },
                        ],
                        "display_id": "a30695bbab45",
                        "event_type": "gear_retrieved",
                        "subject_name": "edgetech_88CE999763_B",
                        "rmwhub_set_id": "e_333e6ad9-88a2-4c68-a631-af5c70e4b727",
                    },
                    "created_at": "2025-03-21T15:21:41.659776-07:00",
                    "updated_at": "2025-03-21T15:21:41.659801-07:00",
                    "is_active": False,
                    "user": None,
                    "tracks_available": True,
                    "image_url": "/static/pin-black.svg",
                    "last_position_status": {
                        "last_voice_call_start_at": None,
                        "radio_state_at": None,
                        "radio_state": "na",
                    },
                    "last_position_date": "2025-03-21T20:33:12+00:00",
                    "last_position": {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [-70.6738777899687, 41.52537796592242],
                        },
                        "properties": {
                            "title": "edgetech_88CE999763_B",
                            "subject_type": "ropeless_buoy",
                            "subject_subtype": "ropeless_buoy_device",
                            "id": "cbd865ab-b0ed-4faf-9169-88fe097d0ece",
                            "stroke": "#FFFF00",
                            "stroke-opacity": 1.0,
                            "stroke-width": 2,
                            "image": "https://buoy.dev.pamdas.org/static/pin-black.svg",
                            "last_voice_call_start_at": None,
                            "location_requested_at": None,
                            "radio_state_at": "1970-01-01T00:00:00+00:00",
                            "radio_state": "na",
                            "coordinateProperties": {
                                "time": "2025-03-21T20:33:12+00:00"
                            },
                            "DateTime": "2025-03-21T20:33:12+00:00",
                        },
                    },
                    "device_status_properties": None,
                    "url": "https://buoy.dev.pamdas.org/api/v1.0/subject/cbd865ab-b0ed-4faf-9169-88fe097d0ece",
                },
                {
                    "content_type": "observations.subject",
                    "id": "f78ffc4f-a91a-4057-b8ae-2e7a1657ac1f",
                    "name": "edgetech_88CE99C99A_A",
                    "subject_type": "ropeless_buoy",
                    "subject_subtype": "ropeless_buoy_device",
                    "common_name": None,
                    "additional": {
                        "devices": [
                            {
                                "label": "a",
                                "location": {
                                    "latitude": 41.5740898,
                                    "longitude": -70.8831463,
                                },
                                "device_id": "edgetech_88CE99C99A_A",
                                "last_updated": "2024-12-10T14:52:06+00:00",
                            }
                        ],
                        "display_id": "1573bba14133",
                        "event_type": "gear_retrieved",
                        "subject_name": "edgetech_88CE99C99A_A",
                        "rmwhub_set_id": "e_fc439758-ee70-4514-847a-4eeee3456cf8",
                    },
                    "created_at": "2025-03-21T15:21:36.810455-07:00",
                    "updated_at": "2025-03-21T15:21:36.810475-07:00",
                    "is_active": False,
                    "user": None,
                    "tracks_available": True,
                    "image_url": "/static/pin-black.svg",
                    "last_position_status": {
                        "last_voice_call_start_at": None,
                        "radio_state_at": None,
                        "radio_state": "na",
                    },
                    "last_position_date": "2025-03-21T20:33:17+00:00",
                    "last_position": {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [-70.8831463, 41.5740898],
                        },
                        "properties": {
                            "title": "edgetech_88CE99C99A_A",
                            "subject_type": "ropeless_buoy",
                            "subject_subtype": "ropeless_buoy_device",
                            "id": "f78ffc4f-a91a-4057-b8ae-2e7a1657ac1f",
                            "stroke": "#FFFF00",
                            "stroke-opacity": 1.0,
                            "stroke-width": 2,
                            "image": "https://buoy.dev.pamdas.org/static/pin-black.svg",
                            "last_voice_call_start_at": None,
                            "location_requested_at": None,
                            "radio_state_at": "1970-01-01T00:00:00+00:00",
                            "radio_state": "na",
                            "coordinateProperties": {
                                "time": "2025-03-21T20:33:17+00:00"
                            },
                            "DateTime": "2025-03-21T20:33:17+00:00",
                        },
                    },
                    "device_status_properties": None,
                    "url": "https://buoy.dev.pamdas.org/api/v1.0/subject/f78ffc4f-a91a-4057-b8ae-2e7a1657ac1f",
                },
            ]
        ]

        query_params = {
            "include_details": "true",
            "include_inactive": "true",
        }
        if start_datetime:
            query_params["updated_since"] = start_datetime

        url = f"{self.er_site}api/v1.0/subjects/"

        async with aiohttp.ClientSession() as session:
            async with session.get(
                url, headers=self.headers, params=query_params
            ) as response:
                if response.status != 200:
                    logger.error(
                        f"Failed to make request. Status code: {response.status}"
                    )
                    return []
                data = await response.json()
                if len(data["data"]) == 0:
                    logger.error("No subjects found")
                    return []
                data = data["data"]
                items = []
                for item in data:
                    try:
                        items.append(ObservationSubject.parse_obj(item))
                    except Exception as e:
                        logger.error(f"Error parsing subject: {e}\n{json.dumps(item)})")
                return items