import pytest

from app.actions.configurations import PullRmwHubObservationsConfiguration
from app.actions.rmwhub import GearSetSearchOthers, TrapSearchOthers
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
        "as_of_utc": "2024-11-06T20:53:21Z",
        "updates": {
            "description": 'New or changed locations of "others" gear',
            "sets": [
                {
                    "set_id": "test_set_id_0",
                    "deployment_type": "single",
                    "traps": [
                        {"sequence": 0, "latitude": -5.19816, "longitude": 122.8113}
                    ],
                },
                {
                    "set_id": "test_set_id_1",
                    "deployment_type": "trawl",
                    "traps": [
                        {
                            "sequence": 0,
                            "latitude": 41.2576527,
                            "longitude": -71.2768455,
                        },
                        {
                            "sequence": 1,
                            "latitude": 41.2544098,
                            "longitude": -71.2793166,
                        },
                    ],
                },
                {
                    "set_id": "test_set_id_2",
                    "deployment_type": "single",
                    "traps": [
                        {
                            "sequence": 0,
                            "latitude": 41.3830183,
                            "longitude": -71.5085983,
                        }
                    ],
                },
                {
                    "set_id": "test_set_id_3",
                    "deployment_type": "single",
                    "traps": [
                        {
                            "sequence": 0,
                            "latitude": 41.3830183,
                            "longitude": -71.5085983,
                        }
                    ],
                },
                {
                    "set_id": "test_set_id_4",
                    "deployment_type": "trawl",
                    "traps": [
                        {
                            "sequence": 0,
                            "latitude": 41.1966817,
                            "longitude": -71.3199314,
                        },
                        {
                            "sequence": 1,
                            "latitude": 41.193145,
                            "longitude": -71.3223467,
                        },
                    ],
                },
            ],
        },
        "deletes": {
            "sets": [
                {"set_id": "test_delete_set_id_1"},
                {"set_id": "test_delete_set_id_2"},
                {"set_id": "test_delete_set_id_3"},
                {"set_id": "test_delete_set_id_4"},
                {"set_id": "test_delete_set_id_5"},
            ]
        },
    }


@pytest.fixture
def mock_rmwhub_items():
    return [
        GearSetSearchOthers(
            set_id="test_set_id_0",
            deployment_type="single",
            traps=[TrapSearchOthers(sequence=0, latitude=-5.19816, longitude=122.8113)],
        ),
        GearSetSearchOthers(
            set_id="test_set_id_1",
            deployment_type="trawl",
            traps=[
                TrapSearchOthers(
                    sequence=0, latitude=41.2576527, longitude=-71.2768455
                ),
                TrapSearchOthers(
                    sequence=1, latitude=41.2544098, longitude=-71.2793166
                ),
            ],
        ),
        GearSetSearchOthers(
            set_id="test_set_id_2",
            deployment_type="single",
            traps=[
                TrapSearchOthers(sequence=0, latitude=41.3830183, longitude=-71.508598)
            ],
        ),
        GearSetSearchOthers(
            set_id="test_set_id_3",
            deployment_type="single",
            traps=[
                TrapSearchOthers(sequence=0, latitude=41.3830183, longitude=-71.508598)
            ],
        ),
        GearSetSearchOthers(
            set_id="test_set_id_4",
            deployment_type="trawl",
            traps=[
                TrapSearchOthers(
                    sequence=0, latitude=41.1966817, longitude=-71.3199314
                ),
                TrapSearchOthers(sequence=1, latitude=41.193145, longitude=-71.3223467),
            ],
        ),
    ], [
        {"set_id": "test_delete_set_id_1"},
        {"set_id": "test_delete_set_id_2"},
        {"set_id": "test_delete_set_id_3"},
        {"set_id": "test_delete_set_id_4"},
        {"set_id": "test_delete_set_id_5"},
    ]


@pytest.fixture
def mock_rmw_observations():
    return [
        {
            "name": "test_set_id_0_0",
            "source": "rmwhub_test_set_id_0_0",
            "type": "ropeless_buoy",
            "subject_type": "ropeless_buoy_device",
            "recorded_at": "2024-10-22T13:33:15.081704-07:00",
            "location": {"lat": -5.19816, "lon": 122.8113},
            "additional": {
                "subject_name": "test_set_id_0_0",
                "rmwHub_id": "test_set_id_0",
                "display_id": "test_display_hash_1",
                "event_type": "gear_deployed",
                "devices": [
                    {
                        "label": "a",
                        "location": {"latitude": "-5.19816", "longitude": "122.8113"},
                        "device_id": "rmwhub_test_set_id_1_1",
                        "last_updated": "2024-10-22T13:33:15.081704-07:00",
                    }
                ],
            },
        },
        {
            "name": "test_set_id_1_0",
            "source": "rmwhub_test_set_id_1_0",
            "type": "ropeless_buoy",
            "subject_type": "ropeless_buoy_device",
            "recorded_at": "2024-10-22T13:33:15.081704-07:00",
            "location": {"lat": 41.2576527, "lon": -71.2768455},
            "additional": {
                "subject_name": "test_set_id_2_0",
                "rmwHub_id": "test_set_id_1",
                "display_id": "test_display_hash_1",
                "event_type": "gear_deployed",
                "devices": [
                    {
                        "label": "a",
                        "location": {
                            "latitude": "41.2576527",
                            "longitude": "-71.2768455",
                        },
                        "device_id": "rmwhub_test_set_id_1_0",
                        "last_updated": "2024-10-22T13:33:15.081704-07:00",
                    },
                    {
                        "label": "b",
                        "location": {
                            "latitude": "41.2544098",
                            "longitude": "-71.2793166",
                        },
                        "device_id": "rmwhub_test_set_id_1_1",
                        "last_updated": "2024-10-22T13:33:15.081704-07:00",
                    },
                ],
            },
        },
        {
            "name": "test_set_id_1_1",
            "source": "rmwhub_test_set_id_1_1",
            "type": "ropeless_buoy",
            "subject_type": "ropeless_buoy_device",
            "recorded_at": "2024-10-22T13:33:15.081704-07:00",
            "location": {"lat": 41.2544098, "lon": -71.2793166},
            "additional": {
                "subject_name": "test_set_id_2_1",
                "rmwHub_id": "test_set_id_1",
                "display_id": "test_display_hash_1",
                "event_type": "gear_deployed",
                "devices": [
                    {
                        "label": "a",
                        "location": {
                            "latitude": "41.2576527",
                            "longitude": "-71.2768455",
                        },
                        "device_id": "rmwhub_test_set_id_1_0",
                        "last_updated": "2024-10-22T13:33:15.081704-07:00",
                    },
                    {
                        "label": "b",
                        "location": {
                            "latitude": "41.2544098",
                            "longitude": "-71.2793166",
                        },
                        "device_id": "rmwhub_test_set_id_1_1",
                        "last_updated": "2024-10-22T13:33:15.081704-07:00",
                    },
                ],
            },
        },
        {
            "name": "test_set_id_2_0",
            "source": "rmwhub_test_set_id_2_0",
            "type": "ropeless_buoy",
            "subject_type": "ropeless_buoy_device",
            "recorded_at": "2024-10-22T13:33:15.081704-07:00",
            "location": {"lat": 41.3830183, "lon": -71.5085983},
            "additional": {
                "subject_name": "test_set_id_2_0",
                "rmwHub_id": "test_set_id_2",
                "display_id": "test_display_hash_2",
                "event_type": "gear_deployed",
                "devices": [
                    {
                        "label": "a",
                        "location": {
                            "latitude": "41.3830183",
                            "longitude": "-71.5085983",
                        },
                        "device_id": "rmwhub_test_set_id_2_1",
                        "last_updated": "2024-10-22T13:33:15.081704-07:00",
                    }
                ],
            },
        },
        {
            "name": "test_set_id_3_0",
            "source": "rmwhub_test_set_id_3_0",
            "type": "ropeless_buoy",
            "subject_type": "ropeless_buoy_device",
            "recorded_at": "2024-10-22T13:33:15.081704-07:00",
            "location": {"lat": 41.3830183, "lon": -71.5085983},
            "additional": {
                "subject_name": "test_set_id_3_0",
                "rmwHub_id": "test_set_id_3",
                "display_id": "test_display_hash_3",
                "event_type": "gear_deployed",
                "devices": [
                    {
                        "label": "a",
                        "location": {
                            "latitude": "41.3830183",
                            "longitude": "-71.5085983",
                        },
                        "device_id": "rmwhub_test_set_id_3_1",
                        "last_updated": "2024-10-22T13:33:15.081704-07:00",
                    }
                ],
            },
        },
        {
            "name": "test_set_id_4_0",
            "source": "rmwhub_test_set_id_4_0",
            "type": "ropeless_buoy",
            "subject_type": "ropeless_buoy_device",
            "recorded_at": "2024-10-22T13:33:15.081704-07:00",
            "location": {"lat": 41.1966817, "lon": -71.3199314},
            "additional": {
                "subject_name": "test_set_id_4_0",
                "rmwHub_id": "test_set_id_4",
                "display_id": "test_display_hash_4",
                "event_type": "gear_deployed",
                "devices": [
                    {
                        "label": "a",
                        "location": {
                            "latitude": "41.1966817",
                            "longitude": "-71.3199314",
                        },
                        "device_id": "rmwhub_test_set_id_4_0",
                        "last_updated": "2024-10-22T13:33:15.081704-07:00",
                    },
                    {
                        "label": "b",
                        "location": {
                            "latitude": "41.193145",
                            "longitude": "-71.3223467",
                        },
                        "device_id": "rmwhub_test_set_id_4_1",
                        "last_updated": "2024-10-22T13:33:15.081704-07:00",
                    },
                ],
            },
        },
        {
            "name": "test_set_id_4_1",
            "source": "rmwhub_test_set_id_4_1",
            "type": "ropeless_buoy",
            "subject_type": "ropeless_buoy_device",
            "recorded_at": "2024-10-22T13:33:15.081704-07:00",
            "location": {"lat": 41.193145, "lon": -71.3223467},
            "additional": {
                "subject_name": "test_set_id_4_1",
                "rmwHub_id": "test_set_id_4",
                "display_id": "test_display_hash_4",
                "event_type": "gear_deployed",
                "devices": [
                    {
                        "label": "a",
                        "location": {
                            "latitude": "41.1966817",
                            "longitude": "-71.3199314",
                        },
                        "device_id": "rmwhub_test_set_id_4_0",
                        "last_updated": "2024-10-22T13:33:15.081704-07:00",
                    },
                    {
                        "label": "b",
                        "location": {
                            "latitude": "41.193145",
                            "longitude": "-71.3223467",
                        },
                        "device_id": "rmwhub_test_set_id_4_1",
                        "last_updated": "2024-10-22T13:33:15.081704-07:00",
                    },
                ],
            },
        },
    ]
