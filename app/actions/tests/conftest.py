import json
import pytest

from app.actions.configurations import PullRmwHubObservationsConfiguration
from app.actions.rmwhub import GearSet, Trap
from gundi_core.schemas.v2 import Connection, ConnectionIntegration
from gundi_core.schemas import IntegrationInformation

from app.conftest import AsyncMock
from ropeless_utils import State


@pytest.fixture
def a_good_state():
    return State(
        er_token="super_secret_token",
        er_site="fishing.pamdas.org",
        event_source="fancy_buoy_company",
        er_event_type="gear_position",
        er_buoy_config=State(
            er_token="an_er_buoy_site_token",
            er_site="https://somewhere.buoyranger.org",
            event_source="some_other_buoy_company",
            er_event_type="gear_position",
        ),
    )


@pytest.fixture
def a_good_integration(a_good_state):
    return IntegrationInformation(
        id="00000000-0000-0000-0000-000000000000",
        state=a_good_state.dict(),
        enabled=True,
        name="Test Integration",
        endpoint="https://someplace.pamdas.orfg/api/v1.0",
        login="test_login",
        password="test_password",
        token="test_token",
    )


@pytest.fixture
def a_good_configuration():
    return PullRmwHubObservationsConfiguration(
        api_key="anApiKey", rmw_url="https://somermwhub.url"
    )


@pytest.fixture
def a_good_connection():
    connection = Connection(
        provider=ConnectionIntegration(
            id="00000000-0000-0000-0000-000000000000",
        ),
        destinations=[
            ConnectionIntegration(
                id="00000000-0000-0000-0000-000000000001",
                name="Buoy Dev",
            ),
            ConnectionIntegration(
                id="00000000-0000-0000-0000-000000000002",
                name="Buoy Staging",
            ),
        ],
    )
    return connection


@pytest.fixture
def get_mock_rmwhub_data():
    return {
        "format_version": 0.1,
        "as_of_utc": "2025-01-03T02:21:57Z",
        "api_key": "018ffe73-baf6-7140-ab65-8a243c0ee02d",
        "sets": [
            {
                "vessel_id": "test_vessel_id_0",
                "set_id": "test_set_id_0",
                "deployment_type": "trawl",
                "traps_in_set": 2,
                "trawl_path": None,
                "share_with": ["Earth_Ranger"],
                "when_updated_utc": "2025-03-14T16:38:12Z",
                "traps": [
                    {
                        "trap_id": "test_trap_id_0",
                        "sequence": 1,
                        "latitude": 44.63648046,
                        "longitude": -63.58040926,
                        "deploy_datetime_utc": "2024-09-25T13:22:32",
                        "surface_datetime_utc": "2024-09-25T13:22:32",
                        "retrieved_datetime_utc": "2024-09-25T13:23:44",
                        "status": "retrieved",
                        "accuracy": "gps",
                        "release_type": "timed",
                        "is_on_end": True,
                    },
                    {
                        "trap_id": "test_trap_id_1",
                        "sequence": 2,
                        "latitude": 44.63648713,
                        "longitude": -63.58044069,
                        "deploy_datetime_utc": "2024-09-25T13:22:38",
                        "surface_datetime_utc": "2024-09-25T13:22:38",
                        "retrieved_datetime_utc": "2024-09-25T13:23:44",
                        "status": "retrieved",
                        "accuracy": "gps",
                        "release_type": "timed",
                        "is_on_end": True,
                    },
                ],
            },
            {
                "vessel_id": "test_vessel_id_1",
                "set_id": "test_set_id_1",
                "deployment_type": "trawl",
                "traps_in_set": 2,
                "trawl_path": None,
                "share_with": ["Earth_Ranger"],
                "when_updated_utc": "2025-03-14T16:38:12Z",
                "traps": [
                    {
                        "trap_id": "test_trap_id_2",
                        "sequence": 1,
                        "latitude": 44.3748774,
                        "longitude": -68.1630351,
                        "deploy_datetime_utc": "2024-06-10T18:24:46",
                        "surface_datetime_utc": "2024-06-10T18:24:46",
                        "retrieved_datetime_utc": "2024-11-02T12:53:38",
                        "status": "retrieved",
                        "accuracy": "gps",
                        "release_type": "timed",
                        "is_on_end": True,
                    },
                    {
                        "trap_id": "test_trap_id_3",
                        "sequence": 2,
                        "latitude": 44.3754398,
                        "longitude": -68.1630321,
                        "deploy_datetime_utc": "2024-06-10T18:25:08",
                        "surface_datetime_utc": "2024-06-10T18:25:08",
                        "retrieved_datetime_utc": "2024-11-02T12:53:38",
                        "status": "retrieved",
                        "accuracy": "gps",
                        "release_type": "timed",
                        "is_on_end": True,
                    },
                ],
            },
            {
                "vessel_id": "test_vessel_id_2",
                "set_id": "test_set_id_2",
                "deployment_type": "trawl",
                "traps_in_set": 2,
                "trawl_path": None,
                "share_with": ["Earth_Ranger"],
                "when_updated_utc": "2025-03-14T16:38:12Z",
                "traps": [
                    {
                        "trap_id": "test_trap_id_4",
                        "sequence": 1,
                        "latitude": 41.4414271,
                        "longitude": -70.9058206,
                        "deploy_datetime_utc": "2024-10-09T15:32:39",
                        "surface_datetime_utc": "2024-10-09T15:32:39",
                        "retrieved_datetime_utc": "2024-10-13T17:06:16",
                        "status": "retrieved",
                        "accuracy": "gps",
                        "release_type": "timed",
                        "is_on_end": True,
                    },
                    {
                        "trap_id": "test_trap_id_5",
                        "sequence": 2,
                        "latitude": 41.4383309,
                        "longitude": -70.9043825,
                        "deploy_datetime_utc": "2024-10-09T15:34:33",
                        "surface_datetime_utc": "2024-10-09T15:34:33",
                        "retrieved_datetime_utc": "2024-10-13T17:06:16",
                        "status": "retrieved",
                        "accuracy": "gps",
                        "release_type": "timed",
                        "is_on_end": True,
                    },
                ],
            },
            {
                "vessel_id": "test_vessel_id_3",
                "set_id": "test_set_id_3",
                "deployment_type": "trawl",
                "traps_in_set": 2,
                "trawl_path": None,
                "share_with": ["Earth_Ranger"],
                "when_updated_utc": "2025-03-14T16:38:12Z",
                "traps": [
                    {
                        "trap_id": "test_trap_id_6",
                        "sequence": 1,
                        "latitude": 42.0471565,
                        "longitude": -70.6253929,
                        "deploy_datetime_utc": "2024-09-15T11:53:29",
                        "surface_datetime_utc": "2024-09-15T11:53:29",
                        "retrieved_datetime_utc": "2024-09-19T12:11:11",
                        "status": "retrieved",
                        "accuracy": "gps",
                        "release_type": "timed",
                        "is_on_end": True,
                    },
                    {
                        "trap_id": "test_trap_id_7",
                        "sequence": 2,
                        "latitude": 42.0474643,
                        "longitude": -70.625706,
                        "deploy_datetime_utc": "2024-09-15T11:53:43",
                        "surface_datetime_utc": "2024-09-15T11:53:43",
                        "retrieved_datetime_utc": "2024-09-19T12:11:11",
                        "status": "retrieved",
                        "accuracy": "gps",
                        "release_type": "timed",
                        "is_on_end": True,
                    },
                ],
            },
            {
                "vessel_id": "test_vessel_id_4",
                "set_id": "test_set_id_4",
                "deployment_type": "single",
                "traps_in_set": 1,
                "trawl_path": None,
                "share_with": ["Earth_Ranger"],
                "when_updated_utc": "2025-03-14T16:38:12Z",
                "traps": [
                    {
                        "trap_id": "test_trap_id_8",
                        "sequence": 1,
                        "latitude": 44.02942909,
                        "longitude": -68.12754665,
                        "deploy_datetime_utc": "2024-09-17T16:55:20",
                        "surface_datetime_utc": "2024-09-17T16:55:20",
                        "retrieved_datetime_utc": "2024-09-19T16:39:45",
                        "status": "retrieved",
                        "accuracy": "gps",
                        "release_type": "timed",
                        "is_on_end": True,
                    }
                ],
            },
        ],
    }


@pytest.fixture
def mock_rmwhub_items():
    return [
        GearSet(
            vessel_id="test_vessel_id_0",
            traps_in_set=2,
            trawl_path=None,
            share_with=["Earth_Ranger"],
            id="test_set_id_0",
            deployment_type="trawl",
            when_updated_utc="2025-03-14T16:38:12Z",
            traps=[
                Trap(
                    id="test_trap_id_0",
                    sequence=1,
                    latitude=-5.19816,
                    longitude=122.8113,
                    deploy_datetime_utc="2024-09-25T13:22:32",
                    surface_datetime_utc="2024-09-25T13:22:32",
                    retrieved_datetime_utc="2024-09-25T13:23:44",
                    status="retrieved",
                    accuracy="gps",
                    release_type="timed",
                    is_on_end=True,
                ),
                Trap(
                    id="test_trap_id_1",
                    sequence=2,
                    latitude=44.63648713,
                    longitude=-63.58044069,
                    deploy_datetime_utc="2024-09-25T13:22:38",
                    surface_datetime_utc="2024-09-25T13:22:38",
                    retrieved_datetime_utc="2024-09-25T13:23:44",
                    status="retrieved",
                    accuracy="gps",
                    release_type="timed",
                    is_on_end=True,
                ),
            ],
        ),
        GearSet(
            vessel_id="test_vessel_id_1",
            traps_in_set=2,
            trawl_path=None,
            share_with=["Earth_Ranger"],
            id="test_set_id_1",
            deployment_type="trawl",
            when_updated_utc="2025-03-14T16:38:12Z",
            traps=[
                Trap(
                    id="test_trap_id_2",
                    sequence=1,
                    latitude=44.3748774,
                    longitude=-68.1630351,
                    deploy_datetime_utc="2024-06-10T18:24:46",
                    surface_datetime_utc="2024-06-10T18:24:46",
                    retrieved_datetime_utc="2024-11-02T12:53:38",
                    status="retrieved",
                    accuracy="gps",
                    release_type="timed",
                    is_on_end=True,
                ),
                Trap(
                    id="test_trap_id_3",
                    sequence=2,
                    latitude=44.3754398,
                    longitude=-68.1630321,
                    deploy_datetime_utc="2024-06-10T18:25:08",
                    surface_datetime_utc="2024-06-10T18:25:08",
                    retrieved_datetime_utc="2024-11-02T12:53:38",
                    status="retrieved",
                    accuracy="gps",
                    release_type="timed",
                    is_on_end=True,
                ),
            ],
        ),
        GearSet(
            vessel_id="test_vessel_id_2",
            traps_in_set=2,
            trawl_path=None,
            share_with=["Earth_Ranger"],
            id="test_set_id_2",
            deployment_type="trawl",
            when_updated_utc="2025-03-14T16:38:12Z",
            traps=[
                Trap(
                    id="test_trap_id_4",
                    sequence=1,
                    latitude=41.4414271,
                    longitude=-70.9058206,
                    deploy_datetime_utc="2024-10-09T15:32:39",
                    surface_datetime_utc="2024-10-09T15:32:39",
                    retrieved_datetime_utc="2024-10-13T17:06:16",
                    status="retrieved",
                    accuracy="gps",
                    release_type="timed",
                    is_on_end=True,
                ),
                Trap(
                    id="test_trap_id_5",
                    sequence=2,
                    latitude=41.4383309,
                    longitude=-70.9043825,
                    deploy_datetime_utc="2024-10-09T15:34:33",
                    surface_datetime_utc="2024-10-09T15:34:33",
                    retrieved_datetime_utc="2024-10-13T17:06:16",
                    status="retrieved",
                    accuracy="gps",
                    release_type="timed",
                    is_on_end=True,
                ),
            ],
        ),
        GearSet(
            vessel_id="test_vessel_id_3",
            traps_in_set=2,
            trawl_path=None,
            share_with=["Earth_Ranger"],
            id="test_set_id_3",
            deployment_type="trawl",
            when_updated_utc="2025-03-14T16:38:12Z",
            traps=[
                Trap(
                    id="test_trap_id_6",
                    sequence=1,
                    latitude=42.0471565,
                    longitude=-70.6253929,
                    deploy_datetime_utc="2024-09-15T11:53:29",
                    surface_datetime_utc="2024-09-15T11:53:29",
                    retrieved_datetime_utc="2024-09-19T12:11:11",
                    status="retrieved",
                    accuracy="gps",
                    release_type="timed",
                    is_on_end=True,
                ),
                Trap(
                    id="test_trap_id_7",
                    sequence=2,
                    latitude=42.0474643,
                    longitude=-70.625706,
                    deploy_datetime_utc="2024-09-15T11:53:43",
                    surface_datetime_utc="2024-09-15T11:53:43",
                    retrieved_datetime_utc="2024-09-19T12:11:11",
                    status="retrieved",
                    accuracy="gps",
                    release_type="timed",
                    is_on_end=True,
                ),
            ],
        ),
        GearSet(
            vessel_id="test_vessel_id_4",
            traps_in_set=1,
            trawl_path=None,
            share_with=["Earth_Ranger"],
            id="test_set_id_4",
            deployment_type="single",
            when_updated_utc="2025-03-14T16:38:12Z",
            traps=[
                Trap(
                    id="test_trap_id_8",
                    sequence=1,
                    latitude=41.4414271,
                    longitude=-70.9058206,
                    deploy_datetime_utc="2024-10-09T15:32:39",
                    surface_datetime_utc="2024-10-09T15:32:39",
                    retrieved_datetime_utc="2024-10-13T17:06:16",
                    status="retrieved",
                    accuracy="gps",
                    release_type="timed",
                    is_on_end=True,
                ),
            ],
        ),
    ]


@pytest.fixture
def mock_rmwhub_items_update():
    return [
        GearSet(
            vessel_id="test_vessel_id_0",
            traps_in_set=2,
            trawl_path=None,
            share_with=["Earth_Ranger"],
            id="test_set_id_0",
            deployment_type="trawl",
            when_updated_utc="2025-03-14T16:38:12Z",
            traps=[
                Trap(
                    id="e_100###########################",
                    sequence=1,
                    latitude=-5.19816,
                    longitude=122.8113,
                    deploy_datetime_utc="2024-09-25T13:22:32",
                    surface_datetime_utc="2024-09-25T13:22:32",
                    retrieved_datetime_utc="2024-09-25T13:23:44",
                    status="retrieved",
                    accuracy="gps",
                    release_type="timed",
                    is_on_end=True,
                )
            ],
        ),
        GearSet(
            vessel_id="test_vessel_id_1",
            traps_in_set=2,
            trawl_path=None,
            share_with=["Earth_Ranger"],
            id="test_set_id_1",
            deployment_type="trawl",
            when_updated_utc="2025-03-14T16:38:12Z",
            traps=[
                Trap(
                    id="test_trap_id_1",
                    sequence=1,
                    latitude=44.3748774,
                    longitude=-68.1630351,
                    deploy_datetime_utc="2024-06-10T18:24:46",
                    surface_datetime_utc="2024-06-10T18:24:46",
                    retrieved_datetime_utc="2024-11-02T12:53:38",
                    status="retrieved",
                    accuracy="gps",
                    release_type="timed",
                    is_on_end=True,
                ),
                Trap(
                    id="test_trap_id_2",
                    sequence=2,
                    latitude=44.3754398,
                    longitude=-68.1630321,
                    deploy_datetime_utc="2024-06-10T18:25:08",
                    surface_datetime_utc="2024-06-10T18:25:08",
                    retrieved_datetime_utc="2024-11-02T12:53:38",
                    status="retrieved",
                    accuracy="gps",
                    release_type="timed",
                    is_on_end=True,
                ),
            ],
        ),
    ]


@pytest.fixture
def mock_rmw_upload_response():
    return {
        "description": "Update confirmation",
        "acknowledged": True,
        "datetime_utc": "2025-01-28T22:04:57Z",
        "trap_count": 5,
        "failed_sets": [],
    }


@pytest.fixture
# TODO: Add an observation for each subject (Trap), currently only 1 per set
def mock_rmw_observations():
    return [
        {
            "name": "test_trap_id_0",
            "source": "rmwhub_test_trap_id_0",
            "type": "ropeless_buoy",
            "subject_type": "ropeless_buoy_device",
            "recorded_at": "2024-10-22T13:33:15.081704-07:00",
            "location": {"lat": -5.19816, "lon": 122.8113},
            "additional": {
                "subject_name": "test_trap_id_0",
                "rmwHub_set_id": "test_set_id_0",
                "vessel_id": "test_vessel_id_0",
                "display_id": "test_display_hash_0",
                "event_type": "gear_retrieved",
                "devices": [
                    {
                        "label": "a",
                        "location": {"latitude": "-5.19816", "longitude": "122.8113"},
                        "device_id": "test_trap_id_0",
                        "last_updated": "2024-10-22T13:33:15.081704-07:00",
                    },
                    {
                        "label": "b",
                        "location": {
                            "latitude": "44.63648713",
                            "longitude": "-63.58044069",
                        },
                        "device_id": "test_trap_id_1",
                        "last_updated": "2024-10-22T13:33:15.081704-07:00",
                    },
                ],
            },
        },
        {
            "name": "test_trap_id_2",
            "source": "rmwhub_test_trap_id_2",
            "type": "ropeless_buoy",
            "subject_type": "ropeless_buoy_device",
            "recorded_at": "2024-10-22T13:33:15.081704-07:00",
            "location": {"lat": 44.3748774, "lon": -68.1630351},
            "additional": {
                "subject_name": "test_trap_id_2",
                "rmwHub_set_id": "test_set_id_1",
                "vessel_id": "test_vessel_id_1",
                "display_id": "test_display_hash_1",
                "event_type": "gear_retrieved",
                "devices": [
                    {
                        "label": "a",
                        "location": {
                            "latitude": "44.3748774",
                            "longitude": "-68.1630351",
                        },
                        "device_id": "test_trap_id_2",
                        "last_updated": "2024-10-22T13:33:15.081704-07:00",
                    },
                    {
                        "label": "b",
                        "location": {
                            "latitude": "44.3754398",
                            "longitude": "-68.1630321",
                        },
                        "device_id": "test_trap_id_3",
                        "last_updated": "2024-10-22T13:33:15.081704-07:00",
                    },
                ],
            },
        },
        {
            "name": "test_trap_id_4",
            "source": "rmwhub_test_trap_id_4",
            "type": "ropeless_buoy",
            "subject_type": "ropeless_buoy_device",
            "recorded_at": "2024-10-22T13:33:15.081704-07:00",
            "location": {"lat": 41.4414271, "lon": -70.9058206},
            "additional": {
                "subject_name": "test_trap_id_4",
                "rmwHub_set_id": "test_set_id_2",
                "vessel_id": "test_vessel_id_2",
                "display_id": "test_display_hash_2",
                "event_type": "gear_retrieved",
                "devices": [
                    {
                        "label": "a",
                        "location": {
                            "latitude": "41.4414271",
                            "longitude": "-70.9058206",
                        },
                        "device_id": "test_trap_id_4",
                        "last_updated": "2024-10-22T13:33:15.081704-07:00",
                    },
                    {
                        "label": "b",
                        "location": {
                            "latitude": "41.4383309",
                            "longitude": "-70.9043825",
                        },
                        "device_id": "test_trap_id_5",
                        "last_updated": "2024-10-22T13:33:15.081704-07:00",
                    },
                ],
            },
        },
        {
            "name": "test_trap_id_6",
            "source": "rmwhub_test_set_id_2_0",
            "type": "ropeless_buoy",
            "subject_type": "ropeless_buoy_device",
            "recorded_at": "2024-10-22T13:33:15.081704-07:00",
            "location": {"lat": 42.0471565, "lon": -70.6253929},
            "additional": {
                "subject_name": "test_trap_id_6",
                "rmwHub_set_id": "test_set_id_3",
                "vessel_id": "test_vessel_id_3",
                "display_id": "test_display_hash_3",
                "event_type": "gear_deployed",
                "devices": [
                    {
                        "label": "a",
                        "location": {
                            "latitude": "42.0471565",
                            "longitude": "-70.6253929",
                        },
                        "device_id": "test_trap_id_6",
                        "last_updated": "2024-10-22T13:33:15.081704-07:00",
                    },
                    {
                        "label": "b",
                        "location": {
                            "latitude": "42.0474643",
                            "longitude": "-70.625706",
                        },
                        "device_id": "test_trap_id_7",
                        "last_updated": "2024-10-22T13:33:15.081704-07:00",
                    },
                ],
            },
        },
        {
            "name": "test_trap_id_8",
            "source": "rmwhub_test_set_id_3_0",
            "type": "ropeless_buoy",
            "subject_type": "ropeless_buoy_device",
            "recorded_at": "2024-10-22T13:33:15.081704-07:00",
            "location": {"lat": 41.4414271, "lon": -70.9058206},
            "additional": {
                "subject_name": "test_trap_id_8",
                "rmwHub_set_id": "test_set_id_4",
                "vessel_id": "test_vessel_id_4",
                "display_id": "test_display_hash_4",
                "event_type": "gear_deployed",
                "devices": [
                    {
                        "label": "a",
                        "location": {
                            "latitude": "41.4414271",
                            "longitude": "-70.9058206",
                        },
                        "device_id": "test_trap_id_8",
                        "last_updated": "2024-10-22T13:33:15.081704-07:00",
                    }
                ],
            },
        },
    ]


@pytest.fixture
def mock_er_subjects():
    return [
        {
            "content_type": "observations.subject",
            "id": "0302a774-1971-4a64-8264-1d7f17969442",
            "name": "short_name",
            "subject_type": "ropeless_buoy",
            "subject_subtype": "ropeless_buoy_device",
            "common_name": None,
            "additional": {
                "devices": [
                    {
                        "label": "a",
                        "location": {"latitude": 42.6762, "longitude": -70.6255043},
                        "device_id": "FBB01895-0BC3-4498-ACAC-BCBCE12F1363",
                        "last_updated": "2025-01-26T03:20:57+00:00",
                    }
                ],
                "display_id": "30548f5def46",
                "event_type": "gear_position_rmwhub",
                "subject_name": "FBB01895-0BC3-4498-ACAC-BCBCE12F1363",
            },
            "created_at": "2025-01-28T14:51:02.996570-08:00",
            "updated_at": "2025-01-28T14:51:02.996595-08:00",
            "is_active": True,
            "user": None,
            "tracks_available": False,
            "image_url": "/static/pin-black.svg",
            "last_position_date": "2025-01-16T17:33:21+00:00",
            "last_position": {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [-70.443459307605, 41.83290438292462],
                },
                "properties": {
                    "title": "edgetech_88CE99D36A_A",
                    "subject_type": "ropeless_buoy",
                    "subject_subtype": "ropeless_buoy_device",
                    "id": "0006a86a-9a99-4112-94b7-f72190ff178f",
                    "stroke": "#FFFF00",
                    "stroke-opacity": 1.0,
                    "stroke-width": 2,
                    "image": "https://buoy.dev.pamdas.org/static/pin-black.svg",
                    "radio_state_at": "1970-01-01T00:00:00+00:00",
                    "radio_state": "na",
                    "coordinateProperties": {"time": "2025-01-16T17:33:21+00:00"},
                    "DateTime": "2025-01-16T17:33:21+00:00",
                },
            },
            "url": "https://buoy.dev.pamdas.org/api/v1.0/subject/0302a774-1971-4a64-8264-1d7f17969442",
        },
        {
            "content_type": "observations.subject",
            "id": "081bfce1-e977-46ad-b948-aa90c9283304",
            "name": "BB1ABEBC-13BF-4110-A4A3-DE6C4F7022D4",
            "subject_type": "ropeless_buoy",
            "subject_subtype": "ropeless_buoy_device",
            "common_name": None,
            "additional": {
                "devices": [
                    {
                        "label": "a",
                        "location": {"latitude": 20.629892, "longitude": -105.318998},
                        "device_id": "F6528E48-39B9-49A8-8F24-0023CF5EE3D7",
                        "last_updated": "2025-01-25T13:22:32+00:00",
                    },
                    {
                        "label": "b",
                        "location": {"latitude": 20.624751, "longitude": -105.310673},
                        "device_id": "BB1ABEBC-13BF-4110-A4A3-DE6C4F7022D4",
                        "last_updated": "2025-01-25T13:22:32+00:00",
                    },
                ],
                "display_id": "84f360b0a8a5",
                "event_type": "gear_deployed",
                "subject_name": "BB1ABEBC-13BF-4110-A4A3-DE6C4F7022D4",
            },
            "created_at": "2025-02-11T13:49:50.852032-08:00",
            "updated_at": "2025-02-11T13:49:50.852056-08:00",
            "is_active": True,
            "user": None,
            "tracks_available": False,
            "image_url": "/static/pin-black.svg",
            "last_position_date": "2025-01-16T17:33:21+00:00",
            "last_position": {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [-70.443459307605, 41.83290438292462],
                },
                "properties": {
                    "title": "edgetech_88CE99D36A_A",
                    "subject_type": "ropeless_buoy",
                    "subject_subtype": "ropeless_buoy_device",
                    "id": "0006a86a-9a99-4112-94b7-f72190ff178f",
                    "stroke": "#FFFF00",
                    "stroke-opacity": 1.0,
                    "stroke-width": 2,
                    "image": "https://buoy.dev.pamdas.org/static/pin-black.svg",
                    "radio_state_at": "1970-01-01T00:00:00+00:00",
                    "radio_state": "na",
                    "coordinateProperties": {"time": "2025-01-16T17:33:21+00:00"},
                    "DateTime": "2025-01-16T17:33:21+00:00",
                },
            },
            "url": "https://buoy.dev.pamdas.org/api/v1.0/subject/081bfce1-e977-46ad-b948-aa90c9283304",
        },
        {
            "content_type": "observations.subject",
            "id": "0931ddaa-770c-4bb4-9d66-b8106c17e043",
            "name": "296A5748-7F7A-4A6B-B055-BFFD4CE6E48F",
            "subject_type": "ropeless_buoy",
            "subject_subtype": "ropeless_buoy_device",
            "common_name": None,
            "additional": {
                "devices": [
                    {
                        "label": "a",
                        "location": {"latitude": 42.6762032, "longitude": -70.6253728},
                        "device_id": "296A5748-7F7A-4A6B-B055-BFFD4CE6E48F",
                        "last_updated": "2025-01-24T14:43:45+00:00",
                    },
                    {
                        "label": "b",
                        "location": {"latitude": 42.6762032, "longitude": -70.6253728},
                        "device_id": "CD7F5F30-B498-4F4A-A2CE-81E9FCF3CB46",
                        "last_updated": "2025-01-24T14:43:45+00:00",
                    },
                ],
                "display_id": "71be27a7ed7e",
                "event_type": "gear_position_rmwhub",
                "subject_name": "296A5748-7F7A-4A6B-B055-BFFD4CE6E48F",
            },
            "created_at": "2025-01-28T14:50:57.323144-08:00",
            "updated_at": "2025-01-28T14:50:57.323169-08:00",
            "is_active": True,
            "user": None,
            "tracks_available": False,
            "image_url": "/static/pin-black.svg",
            "last_position_date": "2025-01-16T17:33:21+00:00",
            "last_position": {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [-70.443459307605, 41.83290438292462],
                },
                "properties": {
                    "title": "edgetech_88CE99D36A_A",
                    "subject_type": "ropeless_buoy",
                    "subject_subtype": "ropeless_buoy_device",
                    "id": "0006a86a-9a99-4112-94b7-f72190ff178f",
                    "stroke": "#FFFF00",
                    "stroke-opacity": 1.0,
                    "stroke-width": 2,
                    "image": "https://buoy.dev.pamdas.org/static/pin-black.svg",
                    "radio_state_at": "1970-01-01T00:00:00+00:00",
                    "radio_state": "na",
                    "coordinateProperties": {"time": "2025-01-16T17:33:21+00:00"},
                    "DateTime": "2025-01-16T17:33:21+00:00",
                },
            },
            "url": "https://buoy.dev.pamdas.org/api/v1.0/subject/0931ddaa-770c-4bb4-9d66-b8106c17e043",
        },
    ]
    
@pytest.fixture
def mock_get_latest_observations():
    """
    Fixture that simulates the behavior of get_latest_observations.
    """
    async def _get_latest_observations(self, subject_id: str, page_size: int):
        # You can ignore page_size for this fake implementation
        if subject_id == "0302a774-1971-4a64-8264-1d7f17969442":
            return [{
                'id': '0302a774-1971-4a64-8264-1d7f17969442',
                'location': {'latitude': 42.6762, 'longitude': -70.6255043},
                'created_at': '2025-01-26T03:20:57+00:00',
                'recorded_at': '2025-04-20T14:51:02.996570-08:00',
                'source': 'random-string',
                'exclusion_flags': 0,
                'observation_details':
                    {
                        'devices': [
                            {
                                "label": "a",
                                "location": {"latitude": 42.6762, "longitude": -70.6255043},
                                "device_id": "100",
                                "last_updated": "2025-01-26T03:20:57+00:00",
                            }
                        ],
                        'display_id': '30548f5def46',
                        'event_type': 'gear_position_rmwhub',
                        'subject_is_active': True
                    }
                }]
        elif subject_id == "081bfce1-e977-46ad-b948-aa90c9283304":
            return [{
                'id': '081bfce1-e977-46ad-b948-aa90c9283304',
                'location': {"latitude": 20.624751, "longitude": -105.310673},
                'created_at': '2025-01-28T14:51:02.996570-08:00',
                'recorded_at': '2025-01-26T03:20:57+00:00',
                'source': 'random-string',
                'exclusion_flags': 0,
                'observation_details':
                    {
                        'devices': [
                            {
                                "label": "a",
                                "location": {"latitude": 20.629892, "longitude": -105.318998},
                                "device_id": "F6528E48-39B9-49A8-8F24-0023CF5EE3D7",
                                "last_updated": "2025-01-25T13:22:32+00:00",
                            },
                            {
                                "label": "b",
                                "location": {"latitude": 20.624751, "longitude": -105.310673},
                                "device_id": "BB1ABEBC-13BF-4110-A4A3-DE6C4F7022D4",
                                "last_updated": "2025-01-25T13:22:32+00:00",
                            },
                        ],
                        'display_id': '84f360b0a8a5',
                        'event_type': 'gear_deployed',
                        'subject_is_active': True
                    }
                }]
        elif subject_id == "0931ddaa-770c-4bb4-9d66-b8106c17e043":
            return [{
                'id': '0931ddaa-770c-4bb4-9d66-b8106c17e043',
                'location': {"latitude": 42.6762032, "longitude": -70.6253728},
                'created_at': '2025-01-28T14:51:02.996570-08:00',
                'recorded_at': '2025-04-26T03:20:57+00:00',
                'source': 'random-string',
                'exclusion_flags': 0,
                'observation_details':
                    {
                        'devices': [
                            {
                                "label": "a",
                                "location": {"latitude": 20.629892, "longitude": -105.318998},
                                "device_id": "F6528E48-39B9-49A8-8F24-0023CF5EE3D7",
                                "last_updated": "2025-01-25T13:22:32+00:00",
                            },
                            {
                                "label": "b",
                                "location": {"latitude": 20.624751, "longitude": -105.310673},
                                "device_id": "BB1ABEBC-13BF-4110-A4A3-DE6C4F7022D4",
                                "last_updated": "2025-01-25T13:22:32+00:00",
                            },
                        ],
                        'display_id': '71be27a7ed7e',
                        'event_type': 'gear_position_rmwhub',
                        'subject_is_active': True
                    }
                }]
    return _get_latest_observations


@pytest.fixture
def mock_er_subjects_update():
    return [
        {
            "content_type": "observations.subject",
            "id": "0302a774-1971-4a64-8264-1d7f17969442",
            "name": "test_trap_id_0",
            "subject_type": "ropeless_buoy",
            "subject_subtype": "ropeless_buoy_device",
            "common_name": None,
            "additional": {
                "devices": [
                    {
                        "label": "a",
                        "location": {"latitude": 42.6762, "longitude": -70.6255043},
                        "device_id": "test_trap_id_0",
                        "last_updated": "2025-01-26T03:20:57+00:00",
                    }
                ],
                "display_id": "000000000000",
                "event_type": "gear_position_rmwhub",
                "subject_name": "test_trap_id_0",
                "rmwhub_set_id": "test_set_id_0",
            },
            "created_at": "2025-01-28T14:51:02.996570-08:00",
            "updated_at": "2025-01-28T14:51:02.996595-08:00",
            "is_active": True,
            "user": None,
            "tracks_available": False,
            "image_url": "/static/pin-black.svg",
            "url": "https://buoy.dev.pamdas.org/api/v1.0/subject/0302a774-1971-4a64-8264-1d7f17969442",
        },
        {
            "content_type": "observations.subject",
            "id": "081bfce1-e977-46ad-b948-aa90c9283304",
            "name": "test_trap_id_1",
            "subject_type": "ropeless_buoy",
            "subject_subtype": "ropeless_buoy_device",
            "common_name": None,
            "additional": {
                "devices": [
                    {
                        "label": "a",
                        "location": {"latitude": 20.629892, "longitude": -105.318998},
                        "device_id": "test_trap_id_1",
                        "last_updated": "2025-01-25T13:22:32+00:00",
                    },
                    {
                        "label": "b",
                        "location": {"latitude": 20.624751, "longitude": -105.310673},
                        "device_id": "test_trap_id_2",
                        "last_updated": "2025-01-25T13:22:32+00:00",
                    },
                ],
                "display_id": "111111111111",
                "event_type": "gear_deployed",
                "subject_name": "test_trap_id_1",
                "rmwhub_set_id": "test_set_id_1",
            },
            "created_at": "2025-02-11T13:49:50.852032-08:00",
            "updated_at": "2025-02-11T13:49:50.852056-08:00",
            "is_active": True,
            "user": None,
            "tracks_available": False,
            "image_url": "/static/pin-black.svg",
            "url": "https://buoy.dev.pamdas.org/api/v1.0/subject/081bfce1-e977-46ad-b948-aa90c9283304",
        },
        {
            "content_type": "observations.subject",
            "id": "0931ddaa-770c-4bb4-9d66-b8106c17e043",
            "name": "test_trap_id_2",
            "subject_type": "ropeless_buoy",
            "subject_subtype": "ropeless_buoy_device",
            "common_name": None,
            "additional": {
                "devices": [
                    {
                        "label": "a",
                        "location": {"latitude": 20.629892, "longitude": -105.318998},
                        "device_id": "test_trap_id_1",
                        "last_updated": "2025-01-25T13:22:32+00:00",
                    },
                    {
                        "label": "b",
                        "location": {"latitude": 20.624751, "longitude": -105.310673},
                        "device_id": "test_trap_id_2",
                        "last_updated": "2025-01-25T13:22:32+00:00",
                    },
                ],
                "display_id": "111111111111",
                "event_type": "gear_position_rmwhub",
                "subject_name": "test_trap_id_2",
                "rmwhub_set_id": "test_set_id_1",
            },
            "created_at": "2025-01-28T14:50:57.323144-08:00",
            "updated_at": "2025-01-28T14:50:57.323169-08:00",
            "is_active": True,
            "user": None,
            "tracks_available": False,
            "image_url": "/static/pin-black.svg",
            "url": "https://buoy.dev.pamdas.org/api/v1.0/subject/0931ddaa-770c-4bb4-9d66-b8106c17e043",
        },
    ]


@pytest.fixture
def mock_er_subjects_from_rmw():
    return [
        {
            "content_type": "observations.subject",
            "id": "0302a774-1971-4a64-8264-1d7f17969442",
            "name": "rmwhub_FBB01895-0BC3-4498-ACAC-BCBCE12F1363",
            "subject_type": "ropeless_buoy",
            "subject_subtype": "ropeless_buoy_device",
            "common_name": None,
            "additional": {
                "devices": [
                    {
                        "label": "a",
                        "location": {"latitude": 42.6762, "longitude": -70.6255043},
                        "device_id": "rmwhub_FBB01895-0BC3-4498-ACAC-BCBCE12F1363",
                        "last_updated": "2025-01-26T03:20:57+00:00",
                    }
                ],
                "display_id": "30548f5def46",
                "event_type": "gear_position_rmwhub",
                "subject_name": "rmwhub_FBB01895-0BC3-4498-ACAC-BCBCE12F1363",
                "rmwhub_set_id": "FBB01895-0BC3-4498-ACAC-BCBCE12F1363",
            },
            "created_at": "2025-01-28T14:51:02.996570-08:00",
            "updated_at": "2025-01-28T14:51:02.996595-08:00",
            "is_active": True,
            "user": None,
            "tracks_available": False,
            "image_url": "/static/pin-black.svg",
            "url": "https://buoy.dev.pamdas.org/api/v1.0/subject/0302a774-1971-4a64-8264-1d7f17969442",
        },
        {
            "content_type": "observations.subject",
            "id": "081bfce1-e977-46ad-b948-aa90c9283304",
            "name": "rmwhub_BB1ABEBC-13BF-4110-A4A3-DE6C4F7022D4",
            "subject_type": "ropeless_buoy",
            "subject_subtype": "ropeless_buoy_device",
            "common_name": None,
            "additional": {
                "devices": [
                    {
                        "label": "a",
                        "location": {"latitude": 20.629892, "longitude": -105.318998},
                        "device_id": "rmwhub_F6528E48-39B9-49A8-8F24-0023CF5EE3D7",
                        "last_updated": "2025-01-25T13:22:32+00:00",
                    },
                    {
                        "label": "b",
                        "location": {"latitude": 20.624751, "longitude": -105.310673},
                        "device_id": "rmwhub_BB1ABEBC-13BF-4110-A4A3-DE6C4F7022D4",
                        "last_updated": "2025-01-25T13:22:32+00:00",
                    },
                ],
                "display_id": "84f360b0a8a5",
                "event_type": "gear_deployed",
                "subject_name": "rmwhub_BB1ABEBC-13BF-4110-A4A3-DE6C4F7022D4",
                "rmwhub_set_id": "000F8EA8-EFBC-4B47-8359-A91CC7843D9H",
            },
            "created_at": "2025-02-11T13:49:50.852032-08:00",
            "updated_at": "2025-02-11T13:49:50.852056-08:00",
            "is_active": True,
            "user": None,
            "tracks_available": False,
            "image_url": "/static/pin-black.svg",
            "url": "https://buoy.dev.pamdas.org/api/v1.0/subject/081bfce1-e977-46ad-b948-aa90c9283304",
        },
        {
            "content_type": "observations.subject",
            "id": "0931ddaa-770c-4bb4-9d66-b8106c17e043",
            "name": "rmwhub_296A5748-7F7A-4A6B-B055-BFFD4CE6E48F",
            "subject_type": "ropeless_buoy",
            "subject_subtype": "ropeless_buoy_device",
            "common_name": None,
            "additional": {
                "devices": [
                    {
                        "label": "a",
                        "location": {"latitude": 42.6762032, "longitude": -70.6253728},
                        "device_id": "rmwhub_296A5748-7F7A-4A6B-B055-BFFD4CE6E48F",
                        "last_updated": "2025-01-24T14:43:45+00:00",
                    },
                    {
                        "label": "b",
                        "location": {"latitude": 42.6762032, "longitude": -70.6253728},
                        "device_id": "CD7F5F30-B498-4F4A-A2CE-81E9FCF3CB46",
                        "last_updated": "2025-01-24T14:43:45+00:00",
                    },
                ],
                "display_id": "71be27a7ed7e",
                "event_type": "gear_position_rmwhub",
                "subject_name": "rmwhub_296A5748-7F7A-4A6B-B055-BFFD4CE6E48F",
            },
            "created_at": "2025-01-28T14:50:57.323144-08:00",
            "updated_at": "2025-01-28T14:50:57.323169-08:00",
            "is_active": True,
            "user": None,
            "tracks_available": False,
            "image_url": "/static/pin-black.svg",
            "url": "https://buoy.dev.pamdas.org/api/v1.0/subject/0931ddaa-770c-4bb4-9d66-b8106c17e043",
        },
    ]


@pytest.fixture
def mock_latest_observations():
    return [
        {
            "id": "82d599d7-62f1-4620-bab4-0415fa8e6b4b",
            "location": {"latitude": 34.472032, "longitude": -123.10975},
            "created_at": "2024-01-09T17:00:54.182801-07:00",
            "recorded_at": "2020-07-31T11:58:33-07:00",
            "source": "7107445c-d883-423b-84d9-3525e7a37c8c",
            "exclusion_flags": 0,
            "observation_details": {
                "devices": [
                    {
                        "label": "a",
                        "location": {"latitude": 34.472032, "longitude": -123.10975},
                        "device_id": "100",
                        "last_updated": "2020-07-31T18:58:33+00:00",
                    }
                ],
                "display_id": "9f9a88df5fbc",
                "event_type": "smelts_buoy_deployment",
                "radio_state": "online-gps",
            },
        },
        {
            "id": "207b5901-b3d7-4a04-8b2a-e3121b452ad3",
            "location": {"latitude": 34.472032, "longitude": -123.10975},
            "created_at": "2024-10-14T20:55:11.392267-07:00",
            "recorded_at": "2020-07-31T11:58:33-07:00",
            "source": "34ba3be1-b25e-411a-8d39-2a4b777bcbb1",
            "exclusion_flags": 0,
            "observation_details": {
                "devices": [
                    {
                        "label": "a",
                        "location": {"latitude": 34.472032, "longitude": -123.10975},
                        "device_id": "device_100",
                        "last_updated": "2020-07-31T18:58:33+00:00",
                    }
                ],
                "display_id": "c242b9e4c9d9",
                "event_type": "smelts_buoy_deployment",
                "radio_state": "online-gps",
            },
        },
        {
            "id": "b404c5b9-bdd2-49f9-a036-80fab8393b6c",
            "location": {"latitude": 41.876497, "longitude": -70.273052},
            "created_at": "2024-10-14T20:55:00.331752-07:00",
            "recorded_at": "2022-05-20T11:18:17-07:00",
            "source": "a8db9e65-280b-4c5c-8d1d-caf568761e28",
            "exclusion_flags": 0,
            "observation_details": {
                "devices": [
                    {
                        "label": "a",
                        "location": {"latitude": 41.876497, "longitude": -70.273052},
                        "device_id": "device_9909",
                        "last_updated": "2022-05-20T18:18:17+00:00",
                    }
                ],
                "display_id": "28d27646abdf",
                "event_type": "smelts_buoy_subsea_data",
                "radio_state": "online-gps",
            },
        },
        {
            "id": "112c4f7e-c17b-4e5a-8e3d-de576d1ad121",
            "location": {"latitude": 41.876497, "longitude": -70.273052},
            "created_at": "2024-10-09T17:00:50.031504-07:00",
            "recorded_at": "2022-05-20T11:18:17-07:00",
            "source": "823baa3b-47ea-4b08-bc55-cb6b666d220f",
            "exclusion_flags": 0,
            "observation_details": {
                "devices": [
                    {
                        "label": "a",
                        "location": {"latitude": 41.876497, "longitude": -70.273052},
                        "device_id": "9909",
                        "last_updated": "2022-05-20T18:18:17+00:00",
                    }
                ],
                "display_id": "394f4fc71b46",
                "event_type": "smelts_buoy_subsea_data",
                "radio_state": "online-gps",
            },
        },
    ]