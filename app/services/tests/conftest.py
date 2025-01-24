import pytest

from app.actions.configurations import PullRmwHubObservationsConfiguration
from app.actions.rmwhub import GearSet, Trap
from ropeless_utils import State
from gundi_core.schemas import IntegrationInformation


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
def a_state_without_er_buoy_config():
    return State(
        er_token="super_secret_token",
        er_site="fishing.pamdas.org",
        event_source="fancy_buoy_company",
        er_event_type="gear_position",
        er_buoy_config=None,
    )


@pytest.fixture
def an_integration_without_er_buoy_config(a_state_without_er_buoy_config):
    return IntegrationInformation(
        id="00000000-0000-0000-0000-000000000000",
        state=a_state_without_er_buoy_config.dict(),
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
def mock_rmwhub_response():
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
