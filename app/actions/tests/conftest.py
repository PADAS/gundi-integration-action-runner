# conftest.py
import json

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
            "serialNumber": "88CE99B3CC",
            "currentState": {
                "etag": '"1669654965656"',
                "isDeleted": False,
                "positionSetByCapri": False,
                "serialNumber": "88CE99B3CC",
                "releaseCommand": "88CE99B3CD",
                "statusCommand": "88CE99B3CC",
                "idCommand": "CCCCCCCCCC",
                "isNfcTag": False,
                "modelNumber": "5112",
                "isDeployed": False,
                "dateRecovered": "2022-10-12T14:51:56.561Z",
                "lastUpdated": "2022-11-28T17:02:45.656Z",
            },
            "changeRecords": [],
        },
        {
            "serialNumber": "88CE99C8AB",
            "currentState": {
                "etag": '"1718360079500"',
                "isDeleted": False,
                "positionSetByCapri": False,
                "serialNumber": "88CE99C8AB",
                "releaseCommand": "C8AB8CCBAB",
                "statusCommand": "88CE99C8AB",
                "idCommand": "CCCCCCCCCC",
                "isNfcTag": False,
                "latDeg": 41.3154712,
                "lonDeg": -68.9005241,
                "endLatDeg": 41.325429,
                "endLonDeg": -68.9098518,
                "modelNumber": "5112",
                "isDeployed": True,
                "dateDeployed": "2024-06-14T10:14:39.234Z",
                "dateStatus": "2023-10-17T14:51:52.826Z",
                "statusRangeM": 1.498,
                "statusIsTilted": False,
                "statusBatterySoC": 64,
                "lastUpdated": "2024-06-14T10:14:39.500Z",
            },
            "changeRecords": [],
        },
    ]


# Fixture for Buoy items used in synchronous tests.
@pytest.fixture
def mock_edgetech_items():
    return [
        Buoy(
            serialNumber="88CE99B3CC",
            currentState={
                "etag": '"1669654965656"',
                "isDeleted": False,
                "serialNumber": "88CE99B3CC",
                "releaseCommand": "88CE99B3CD",
                "statusCommand": "88CE99B3CC",
                "idCommand": "CCCCCCCCCC",
                "isNfcTag": False,
                "modelNumber": "5112",
                "isDeployed": False,
                "dateRecovered": "2022-10-12T14:51:56.561Z",
                "lastUpdated": "2022-11-28T17:02:45.656Z",
                "latDeg": 0.0,
                "lonDeg": 0.0,
            },
            changeRecords=[],
        ),
        Buoy(
            serialNumber="88CE99C8AB",
            currentState={
                "etag": '"1718360079500"',
                "isDeleted": False,
                "serialNumber": "88CE99C8AB",
                "releaseCommand": "C8AB8CCBAB",
                "statusCommand": "88CE99C8AB",
                "idCommand": "CCCCCCCCCC",
                "isNfcTag": False,
                "latDeg": 41.3154712,
                "lonDeg": -68.9005241,
                "endLatDeg": 41.325429,
                "endLonDeg": -68.9098518,
                "modelNumber": "5112",
                "isDeployed": True,
                "dateDeployed": "2024-06-14T10:14:39.234Z",
                "dateStatus": "2023-10-17T14:51:52.826Z",
                "statusRangeM": 1.498,
                "statusIsTilted": False,
                "statusBatterySoC": 64,
                "lastUpdated": "2024-06-14T10:14:39.500Z",
            },
            changeRecords=[],
        ),
    ]