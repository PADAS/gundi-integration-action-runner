import logging
from datetime import datetime
from uuid import UUID

import pytest

from app.actions.edgetech.types import (
    TRAP_DEPLOYMENT_EVENT,
    Buoy,
    CurrentState,
)


@pytest.fixture
def base_state():
    """Return a dict of minimal fields to construct a valid CurrentState."""
    return {
        "etag": '"abc123"',
        "isDeleted": False,
        "serialNumber": "S123",
        "releaseCommand": "rc",
        "statusCommand": "sc",
        "idCommand": "ic",
        "isNfcTag": False,
        "modelNumber": None,
        "dateOfManufacture": None,
        "dateOfBatteryChange": None,
        "dateDeployed": None,
        "isDeployed": None,
        "dateRecovered": None,
        "recoveredLatDeg": None,
        "recoveredLonDeg": None,
        "recoveredRangeM": None,
        "dateStatus": None,
        "statusRangeM": None,
        "statusIsTilted": None,
        "statusBatterySoC": None,
        # include a microsecond to ensure .create_observations strips it
        "lastUpdated": datetime(2025, 1, 1, 12, 0, 0, 123456),
        "latDeg": None,
        "lonDeg": None,
        "endLatDeg": None,
        "endLonDeg": None,
        "isTwoUnitLine": None,
        "endUnit": None,
        "startUnit": None,
    }


@pytest.mark.parametrize(
    "override,expected",
    [
        ({"recoveredLatDeg": 1.0, "recoveredLonDeg": 2.0}, True),
        ({"latDeg": 3.0, "lonDeg": 4.0}, True),
        ({"endLatDeg": 5.0, "endLonDeg": 6.0}, True),
        ({}, False),
    ],
)
def test_has_location_variants(base_state, override, expected):
    state_kwargs = {**base_state, **override}
    state = CurrentState(**state_kwargs)
    buoy = Buoy(
        userId="7889ad74-aab3-4044-bcf4-13d6f9586a82",
        currentState=state,
        serialNumber="S123",
        changeRecords=[],
    )
    assert buoy.has_location is expected

