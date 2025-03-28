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
def a_good_auth_configuration():
    token_data = {
        "access_token": "dummy_token",
        "refresh_token": "dummy_refresh",
        "expires_in": 86400,
        "expires_at": 1739552948.0993779,
    }
    return EdgeTechAuthConfiguration(
        token_json=SecretStr(json.dumps(token_data)),
        api_base_url="https://edgetech.api/",
        client_id="client123",
        redirect_uri="https://redirect.uri/",
        token_url="https://edgetech.api/token",
    )

@pytest.fixture
def a_good_pull_configuration():
    return EdgeTechConfiguration(
        api_base_url="https://edgetech.api/",
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
def get_mock_edgetech_data():
    return [
        {
            "serialNumber": "88CE9978AE",
            "currentState": {
                "etag": '"1733842319546"',
                "isDeleted": False,
                "positionSetByCapri": False,
                "serialNumber": "88CE9978AE",
                "releaseCommand": "C8AB8C73AE",
                "statusCommand": "88CE9978AE",
                "idCommand": "CCCCCCCCCC",
                "isNfcTag": False,
                "modelNumber": "5112",
                "dateStatus": "2024-10-07T19:31:24.469Z",
                "statusRangeM": 0,
                "statusIsTilted": False,
                "statusBatterySoC": 86,
                "lastUpdated": "2024-12-10T14:51:59.546Z",
            },
            "changeRecords": [],
        },
        {
            "serialNumber": "88CE999763",
            "currentState": {
                "etag": '"1733842320416"',
                "isDeleted": False,
                "positionSetByCapri": False,
                "serialNumber": "88CE999763",
                "releaseCommand": "C8AB8CCAE3",
                "statusCommand": "88CE999763",
                "idCommand": "CCCCCCCCCC",
                "isNfcTag": False,
                "modelNumber": "5112",
                "dateOfManufacture": "2021-12-07T05:00:00.000Z",
                "dateOfBatteryCharge": "2023-03-28T04:00:00.000Z",
                "isDeployed": False,
                "dateRecovered": "2024-05-28T14:22:54.151Z",
                "dateStatus": "2023-09-23T20:07:48.458Z",
                "statusRangeM": 257.656,
                "statusIsTilted": False,
                "statusBatterySoC": 82,
                "lastUpdated": "2024-12-10T14:52:00.416Z",
            },
            "changeRecords": [],
        },
        {
            "serialNumber": "88CE99C99A",
            "currentState": {
                "etag": '"1733842326885"',
                "isDeleted": False,
                "positionSetByCapri": False,
                "serialNumber": "88CE99C99A",
                "releaseCommand": "C8AB8CCC9A",
                "statusCommand": "88CE99C99A",
                "idCommand": "CCCCCCCCCC",
                "isNfcTag": False,
                "modelNumber": "5112",
                "dateStatus": "2023-08-01T18:15:44.563Z",
                "statusRangeM": 0,
                "statusIsTilted": False,
                "statusBatterySoC": 62,
                "lastUpdated": "2024-12-10T14:52:06.885Z",
            },
            "changeRecords": [],
        },
        {
            "serialNumber": "88CE9978AE",
            "currentState": {
                "etag": '"1729614890401"',
                "isDeleted": False,
                "positionSetByCapri": False,
                "serialNumber": "88CE9978AE",
                "releaseCommand": "C8AB8C73AE",
                "statusCommand": "88CE9978AE",
                "idCommand": "CCCCCCCCCC",
                "isNfcTag": False,
                "latDeg": 41.82907459248435,
                "lonDeg": -71.41540430869928,
                "modelNumber": "5112",
                "dateOfManufacture": "2024-10-07T15:26:33.362Z",
                "dateOfBatteryCharge": "2024-10-07T15:26:34.034Z",
                "isDeployed": True,
                "dateDeployed": "2024-10-22T16:34:46.981Z",
                "lastUpdated": "2024-10-22T16:34:50.401Z",
            },
            "changeRecords": [],
        },
        {
            "serialNumber": "88CE99C99A",
            "currentState": {
                "etag": '"1741954819256"',
                "isDeleted": False,
                "positionSetByCapri": False,
                "serialNumber": "88CE99C99A",
                "releaseCommand": "C8AB8CCC9A",
                "statusCommand": "88CE99C99A",
                "idCommand": "CCCCCCCCCC",
                "isNfcTag": True,
                "modelNumber": "5112",
                "dateOfManufacture": "2022-12-21T05:00:00.000Z",
                "isDeployed": False,
                "dateRecovered": "2025-03-14T12:07:27.199Z",
                "recoveredLatDeg": 41.7832483,
                "recoveredLonDeg": -70.7527803,
                "recoveredRangeM": 0,
                "recoveredTemperatureC": 24,
                "dateStatus": "2025-03-14T12:20:13.201Z",
                "statusRangeM": 0,
                "statusIsTilted": True,
                "statusBatterySoC": 3,
                "lastUpdated": "2025-03-14T12:20:19.256Z",
            },
            "changeRecords": [],
        },
        {
            "serialNumber": "88CE9978AE",
            "currentState": {
                "etag": '"1739896745003"',
                "isDeleted": True,
                "positionSetByCapri": False,
                "serialNumber": "88CE9978AE",
                "releaseCommand": "C8AB8C73AE",
                "statusCommand": "88CE9978AE",
                "idCommand": "CCCCCCCCCC",
                "isNfcTag": False,
                "modelNumber": "5112",
                "isDeployed": False,
                "dateRecovered": "2025-02-13T15:30:42.492Z",
                "recoveredRangeM": 0,
                "recoveredTemperatureC": 7,
                "dateStatus": "2025-02-11T17:14:15.690Z",
                "statusRangeM": 0,
                "statusIsTilted": False,
                "statusBatterySoC": 0,
                "lastUpdated": "2025-02-18T16:39:05.003Z",
            },
            "changeRecords": [],
        },
        {
            "serialNumber": "88CE9978AE",
            "currentState": {
                "etag": '"1717697928807"',
                "isDeleted": False,
                "positionSetByCapri": False,
                "serialNumber": "88CE9978AE",
                "releaseCommand": "C8AB8C73AE",
                "statusCommand": "88CE9978AE",
                "idCommand": "CCCCCCCCCC",
                "isNfcTag": False,
                "modelNumber": "5112",
                "dateStatus": "2024-06-06T18:16:22.470Z",
                "statusRangeM": 1.498,
                "statusIsTilted": True,
                "statusBatterySoC": 94,
                "lastUpdated": "2024-06-06T18:18:48.807Z",
            },
            "changeRecords": [],
        },
        {
            "serialNumber": "88CE99C99A",
            "currentState": {
                "etag": '"1682088076708"',
                "isDeleted": False,
                "positionSetByCapri": False,
                "serialNumber": "88CE99C99A",
                "releaseCommand": "C8AB8CCC9A",
                "statusCommand": "88CE99C99A",
                "idCommand": "CCCCCCCCCC",
                "isNfcTag": True,
                "modelNumber": "5112",
                "dateOfManufacture": "2022-12-21T05:00:00.000Z",
                "dateStatus": "2023-04-21T14:41:08.627Z",
                "statusRangeM": 1575.89599609375,
                "lastUpdated": "2023-04-21T14:41:16.708Z",
            },
            "changeRecords": [],
        },
        {
            "serialNumber": "88CE99C99A",
            "currentState": {
                "etag": '"1733241895379"',
                "isDeleted": False,
                "positionSetByCapri": False,
                "serialNumber": "88CE99C99A",
                "releaseCommand": "C8AB8CCC9A",
                "statusCommand": "88CE99C99A",
                "idCommand": "CCCCCCCCCC",
                "isNfcTag": False,
                "modelNumber": "5112",
                "dateOfManufacture": "2022-12-21T05:00:00.000Z",
                "isDeployed": False,
                "dateRecovered": "2024-12-03T16:04:55.230Z",
                "recoveredLatDeg": 41.5740898,
                "recoveredLonDeg": -70.8831463,
                "dateStatus": "2024-12-03T15:23:51.886Z",
                "statusRangeM": 58.422,
                "statusIsTilted": False,
                "statusBatterySoC": 104,
                "lastUpdated": "2024-12-03T16:04:55.379Z",
            },
            "changeRecords": [],
        },
        {
            "serialNumber": "88CE999763",
            "currentState": {
                "etag": '"1726594987576"',
                "isDeleted": False,
                "positionSetByCapri": False,
                "serialNumber": "88CE999763",
                "releaseCommand": "C8AB8CCAE3",
                "statusCommand": "88CE999763",
                "idCommand": "CCCCCCCCCC",
                "isNfcTag": False,
                "lastUpdated": "2024-09-17T17:43:07.576Z",
            },
            "changeRecords": [],
        },
        {
            "serialNumber": "88CE99C99A",
            "currentState": {
                "etag": '"1726083572993"',
                "isDeleted": False,
                "positionSetByCapri": False,
                "serialNumber": "88CE99C99A",
                "releaseCommand": "C8AB8CCC9A",
                "statusCommand": "88CE99C99A",
                "idCommand": "CCCCCCCCCC",
                "isNfcTag": False,
                "dateStatus": "2024-09-11T19:34:41.744Z",
                "statusRangeM": 0,
                "statusIsTilted": True,
                "statusBatterySoC": 69,
                "lastUpdated": "2024-09-11T19:39:32.993Z",
            },
            "changeRecords": [],
        },
        {
            "serialNumber": "88CE9978AE",
            "currentState": {
                "etag": '"1728330307444"',
                "isDeleted": False,
                "positionSetByCapri": False,
                "serialNumber": "88CE9978AE",
                "releaseCommand": "C8AB8C73AE",
                "statusCommand": "88CE9978AE",
                "idCommand": "CCCCCCCCCC",
                "isNfcTag": False,
                "modelNumber": "5112",
                "dateStatus": "2024-10-07T19:31:24.469Z",
                "statusRangeM": 0,
                "statusIsTilted": False,
                "statusBatterySoC": 86,
                "lastUpdated": "2024-10-07T19:45:07.444Z",
            },
            "changeRecords": [],
        },
        {
            "serialNumber": "88CE999763",
            "currentState": {
                "etag": '"1742232994823"',
                "isDeleted": False,
                "positionSetByCapri": False,
                "serialNumber": "88CE999763",
                "releaseCommand": "C8AB8CCAE3",
                "statusCommand": "88CE999763",
                "idCommand": "CCCCCCCCCC",
                "isNfcTag": False,
                "modelNumber": "5112",
                "dateOfManufacture": "2021-12-07T05:00:00.000Z",
                "dateOfBatteryCharge": "2023-03-28T04:00:00.000Z",
                "isDeployed": False,
                "dateRecovered": "2025-03-17T17:36:32.643Z",
                "dateStatus": "2023-09-23T20:07:48.458Z",
                "statusRangeM": 257.656,
                "statusIsTilted": False,
                "statusBatterySoC": 82,
                "lastUpdated": "2025-03-17T17:36:34.823Z",
            },
            "changeRecords": [
                {
                    "type": "MODIFY",
                    "timestamp": "2025-03-17T17:36:34.000Z",
                    "changes": [
                        {
                            "key": "dateDeployed",
                            "oldValue": "2025-03-17T16:43:40.228Z",
                            "newValue": None,
                        },
                        {
                            "key": "dateRecovered",
                            "oldValue": None,
                            "newValue": "2025-03-17T17:36:32.643Z",
                        },
                        {
                            "key": "endLatDeg",
                            "oldValue": 41.52537796592242,
                            "newValue": None,
                        },
                        {
                            "key": "endLonDeg",
                            "oldValue": -70.6738777899687,
                            "newValue": None,
                        },
                        {"key": "geoHash", "oldValue": "89e4d", "newValue": "X"},
                        {"key": "isDeployed", "oldValue": True, "newValue": False},
                        {
                            "key": "lastUpdated",
                            "oldValue": "2025-03-17T16:43:40.831Z",
                            "newValue": "2025-03-17T17:36:34.823Z",
                        },
                        {
                            "key": "latDeg",
                            "oldValue": 41.52546746182916,
                            "newValue": None,
                        },
                        {
                            "key": "lonDeg",
                            "oldValue": -70.67401171221228,
                            "newValue": None,
                        },
                    ],
                },
                {
                    "type": "MODIFY",
                    "timestamp": "2025-03-17T16:43:40.000Z",
                    "changes": [
                        {
                            "key": "dateDeployed",
                            "oldValue": None,
                            "newValue": "2025-03-17T16:43:40.228Z",
                        },
                        {
                            "key": "dateRecovered",
                            "oldValue": "2025-03-17T16:41:22.508Z",
                            "newValue": None,
                        },
                        {
                            "key": "endLatDeg",
                            "oldValue": None,
                            "newValue": 41.52537796592242,
                        },
                        {
                            "key": "endLonDeg",
                            "oldValue": None,
                            "newValue": -70.6738777899687,
                        },
                        {"key": "geoHash", "oldValue": "X", "newValue": "89e4d"},
                        {"key": "isDeployed", "oldValue": False, "newValue": True},
                        {
                            "key": "lastUpdated",
                            "oldValue": "2025-03-17T16:43:14.071Z",
                            "newValue": "2025-03-17T16:43:40.831Z",
                        },
                        {
                            "key": "latDeg",
                            "oldValue": None,
                            "newValue": 41.52546746182916,
                        },
                        {
                            "key": "lonDeg",
                            "oldValue": None,
                            "newValue": -70.67401171221228,
                        },
                    ],
                },
                {
                    "type": "MODIFY",
                    "timestamp": "2025-03-17T16:43:14.000Z",
                    "changes": [
                        {
                            "key": "dateDeployed",
                            "oldValue": "2025-03-17T16:26:12.059Z",
                            "newValue": None,
                        },
                        {
                            "key": "dateRecovered",
                            "oldValue": None,
                            "newValue": "2025-03-17T16:41:22.508Z",
                        },
                        {"key": "geoHash", "oldValue": "89e4d", "newValue": "X"},
                        {"key": "isDeployed", "oldValue": True, "newValue": False},
                        {
                            "key": "lastUpdated",
                            "oldValue": "2025-03-17T16:26:12.457Z",
                            "newValue": "2025-03-17T16:43:14.071Z",
                        },
                        {
                            "key": "latDeg",
                            "oldValue": 41.52546746182916,
                            "newValue": None,
                        },
                        {
                            "key": "lonDeg",
                            "oldValue": -70.67401171221228,
                            "newValue": None,
                        },
                    ],
                },
                {
                    "type": "MODIFY",
                    "timestamp": "2025-03-17T16:26:12.000Z",
                    "changes": [
                        {
                            "key": "dateDeployed",
                            "oldValue": None,
                            "newValue": "2025-03-17T16:26:12.059Z",
                        },
                        {
                            "key": "dateRecovered",
                            "oldValue": "2024-05-28T14:22:54.151Z",
                            "newValue": None,
                        },
                        {"key": "geoHash", "oldValue": "X", "newValue": "89e4d"},
                        {"key": "isDeployed", "oldValue": False, "newValue": True},
                        {
                            "key": "lastUpdated",
                            "oldValue": "2024-05-28T14:22:55.449Z",
                            "newValue": "2025-03-17T16:26:12.457Z",
                        },
                        {
                            "key": "latDeg",
                            "oldValue": None,
                            "newValue": 41.52546746182916,
                        },
                        {
                            "key": "lonDeg",
                            "oldValue": None,
                            "newValue": -70.67401171221228,
                        },
                    ],
                },
            ],
        },
        {
            "serialNumber": "88CE99C99A",
            "currentState": {
                "etag": '"1691005965618"',
                "isDeleted": False,
                "positionSetByCapri": False,
                "serialNumber": "88CE99C99A",
                "releaseCommand": "C8AB8CCC9A",
                "statusCommand": "88CE99C99A",
                "idCommand": "CCCCCCCCCC",
                "isNfcTag": False,
                "modelNumber": "5112",
                "dateStatus": "2023-08-01T18:15:44.563Z",
                "statusRangeM": 0,
                "statusIsTilted": False,
                "statusBatterySoC": 62,
                "lastUpdated": "2023-08-02T19:52:45.618Z",
            },
            "changeRecords": [],
        },
        {
            "serialNumber": "88CE99C99A",
            "currentState": {
                "etag": '"1692717424234"',
                "isDeleted": False,
                "positionSetByCapri": False,
                "serialNumber": "88CE99C99A",
                "releaseCommand": "C8AB8CCC9A",
                "statusCommand": "88CE99C99A",
                "idCommand": "CCCCCCCCCC",
                "isNfcTag": True,
                "modelNumber": "5112",
                "dateOfManufacture": "2022-12-21T05:00:00.000Z",
                "dateStatus": "2023-08-22T15:17:01.599Z",
                "statusRangeM": 7.489999771118164,
                "statusIsTilted": False,
                "statusBatterySoC": 71,
                "lastUpdated": "2023-08-22T15:17:04.234Z",
            },
            "changeRecords": [],
        },
    ]


# @pytest.fixture
# def get_er_subjects_updated_data():
#     """
#     This updated fixture includes every deploy/retrieve event timestamp
#     that appears in the EdgeTech data, so the processor sees no "new" events.
#     """
#     return [
#         #
#         # 1) edgetech_88CE9978AE_A
#         #
#         {
#             "content_type": "observations.subject",
#             "id": "00000000-0000-0000-0000-000000000001",
#             "name": "edgetech_88CE9978AE_A",
#             "subject_type": "ropeless_buoy",
#             "subject_subtype": "ropeless_buoy_device",
#             "common_name": None,
#             "last_position_data": datetime
#             "additional": {
#                 "display_id": "f1ab617fc777",  # Matches processor output
#                 "edgetech_serial_number": "88CE9978AE",
#                 "event_type": "gear_retrieved",  # Final event for this buoy is retrieved
#                 "subject_name": "edgetech_88CE9978AE_A",
#                 "devices": [
#                     # Deployed on 2024-10-22T16:34:46Z
#                     {
#                         "label": "a",
#                         "device_id": "edgetech_88CE9978AE_A",
#                         "last_updated": "2024-10-22T16:34:46+00:00",
#                         "location": {
#                             "latitude": 41.82907459248435,
#                             "longitude": -71.41540430869928,
#                         },
#                     },
#                     # Retrieved on 2024-12-10T14:51:59Z
#                     {
#                         "label": "a",
#                         "device_id": "edgetech_88CE9978AE_A",
#                         "last_updated": "2024-12-10T14:51:59+00:00",
#                         "location": {
#                             "latitude": 41.82907459248435,
#                             "longitude": -71.41540430869928,
#                         },
#                     },
#                     # Retrieved again on 2025-02-13T15:30:42Z
#                     {
#                         "label": "a",
#                         "device_id": "edgetech_88CE9978AE_A",
#                         "last_updated": "2025-02-13T15:30:42+00:00",
#                         "location": {
#                             "latitude": 41.82907459248435,
#                             "longitude": -71.41540430869928,
#                         },
#                     },
#                 ],
#             },
#             "created_at": "2025-03-21T00:00:00Z",
#             "updated_at": "2025-03-21T00:00:00Z",
#             "is_active": False,  # Matches final retrieved state
#         },
#         #
#         # 2) edgetech_88CE999763_A
#         #
#         {
#             "content_type": "observations.subject",
#             "id": "00000000-0000-0000-0000-000000000002",
#             "name": "edgetech_88CE999763_A",
#             "subject_type": "ropeless_buoy",
#             "subject_subtype": "ropeless_buoy_device",
#             "common_name": None,
#             "additional": {
#                 "display_id": "5eb353fb0f49",
#                 "edgetech_serial_number": "88CE999763",
#                 "event_type": "gear_deployed",  # Final event is deployed
#                 "subject_name": "edgetech_88CE999763_A",
#                 "devices": [
#                     # 2025-03-17T16:26:12Z => gear_deployed
#                     {
#                         "label": "a",
#                         "device_id": "edgetech_88CE999763_A",
#                         "last_updated": "2025-03-17T16:26:12+00:00",
#                         "location": {
#                             "latitude": 41.52546746182916,
#                             "longitude": -70.67401171221228,
#                         },
#                     },
#                     # 2025-03-17T16:41:22Z => gear_retrieved
#                     {
#                         "label": "a",
#                         "device_id": "edgetech_88CE999763_A",
#                         "last_updated": "2025-03-17T16:41:22+00:00",
#                         "location": {
#                             "latitude": 41.52546746182916,
#                             "longitude": -70.67401171221228,
#                         },
#                     },
#                     # 2025-03-17T16:43:40Z => gear_deployed
#                     {
#                         "label": "a",
#                         "device_id": "edgetech_88CE999763_A",
#                         "last_updated": "2025-03-17T16:43:40+00:00",
#                         "location": {
#                             "latitude": 41.52546746182916,
#                             "longitude": -70.67401171221228,
#                         },
#                     },
#                 ],
#             },
#             "created_at": "2025-03-21T00:00:00Z",
#             "updated_at": "2025-03-21T00:00:00Z",
#             "is_active": True,  # final state is deployed
#         },
#         #
#         # 3) edgetech_88CE999763_B
#         #
#         {
#             "content_type": "observations.subject",
#             "id": "00000000-0000-0000-0000-000000000003",
#             "name": "edgetech_88CE999763_B",
#             "subject_type": "ropeless_buoy",
#             "subject_subtype": "ropeless_buoy_device",
#             "common_name": None,
#             "additional": {
#                 "display_id": "5eb353fb0f49",
#                 "edgetech_serial_number": "88CE999763",
#                 "event_type": "gear_deployed",  # Only final "gear_deployed" for device B
#                 "subject_name": "edgetech_88CE999763_B",
#                 "devices": [
#                     {
#                         "label": "b",
#                         "device_id": "edgetech_88CE999763_B",
#                         "last_updated": "2025-03-17T16:43:40+00:00",
#                         "location": {
#                             "latitude": 41.52537796592242,
#                             "longitude": -70.6738777899687,
#                         },
#                     }
#                 ],
#             },
#             "created_at": "2025-03-21T00:00:00Z",
#             "updated_at": "2025-03-21T00:00:00Z",
#             "is_active": True,  # final event is deployed
#         },
#         #
#         # 4) edgetech_88CE99C99A_A
#         #
#         {
#             "content_type": "observations.subject",
#             "id": "00000000-0000-0000-0000-000000000004",
#             "name": "edgetech_88CE99C99A_A",
#             "subject_type": "ropeless_buoy",
#             "subject_subtype": "ropeless_buoy_device",
#             "common_name": None,
#             "additional": {
#                 "display_id": "e1f4d34d79f2",
#                 "edgetech_serial_number": "88CE99C99A",
#                 "event_type": "gear_retrieved",  # final event
#                 "subject_name": "edgetech_88CE99C99A_A",
#                 "devices": [
#                     # Recovered on 2024-12-03T16:04:55Z
#                     {
#                         "label": "a",
#                         "device_id": "edgetech_88CE99C99A_A",
#                         "last_updated": "2024-12-03T16:04:55+00:00",
#                         "location": {"latitude": 41.5740898, "longitude": -70.8831463},
#                     },
#                     # Recovered on 2024-12-10T14:52:06Z
#                     {
#                         "label": "a",
#                         "device_id": "edgetech_88CE99C99A_A",
#                         "last_updated": "2024-12-10T14:52:06+00:00",
#                         "location": {"latitude": 41.5740898, "longitude": -70.8831463},
#                     },
#                     # Recovered again on 2025-03-14T12:07:27Z
#                     {
#                         "label": "a",
#                         "device_id": "edgetech_88CE99C99A_A",
#                         "last_updated": "2025-03-14T12:07:27+00:00",
#                         "location": {"latitude": 41.7832483, "longitude": -70.7527803},
#                     },
#                 ],
#             },
#             "created_at": "2025-03-21T00:00:00Z",
#             "updated_at": "2025-03-21T00:00:00Z",
#             "is_active": False,  # final state is retrieved
#         },
#     ]


@pytest.fixture
def get_er_subjects_updated_data():
    return [
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
                    "coordinateProperties": {"time": "2025-03-17T17:36:32+00:00"},
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
                    "coordinateProperties": {"time": "2025-03-17T17:36:32+00:00"},
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
                    "coordinateProperties": {"time": "2025-02-13T15:30:42+00:00"},
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
                    "coordinateProperties": {"time": "2025-03-14T12:07:27+00:00"},
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
                    "coordinateProperties": {"time": "2025-03-21T20:33:12+00:00"},
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
                    "coordinateProperties": {"time": "2025-03-21T20:33:24+00:00"},
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
                    "coordinateProperties": {"time": "2025-03-21T20:33:12+00:00"},
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
                    "coordinateProperties": {"time": "2025-03-21T20:33:17+00:00"},
                    "DateTime": "2025-03-21T20:33:17+00:00",
                },
            },
            "device_status_properties": None,
            "url": "https://buoy.dev.pamdas.org/api/v1.0/subject/f78ffc4f-a91a-4057-b8ae-2e7a1657ac1f",
        },
    ]