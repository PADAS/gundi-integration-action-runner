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


def test_create_observations_skips_when_no_start_location(caplog, base_state):
    # Neither latDeg nor lonDeg set → no observations
    state_kwargs = {**base_state, "latDeg": None, "lonDeg": None}
    state = CurrentState(**state_kwargs)
    buoy = Buoy(
        userId="7889ad74-aab3-4044-bcf4-13d6f9586a82",
        currentState=state,
        serialNumber="S123",
        changeRecords=[],
    )

    caplog.set_level(logging.WARNING)
    obs = buoy.create_observations(is_deployed=True)

    assert obs == []
    assert "No valid location for buoy S123" in caplog.text


def test_create_observations_only_start_point(base_state):
    # Only start coords → one observation
    start = (10.0, 20.0)
    state_kwargs = {
        **base_state,
        "latDeg": start[0],
        "lonDeg": start[1],
        "dateDeployed": None,  # fallback to lastUpdated
    }
    state = CurrentState(**state_kwargs)
    buoy = Buoy(
        userId="7889ad74-aab3-4044-bcf4-13d6f9586a82",
        currentState=state,
        serialNumber="S123",
        changeRecords=[],
    )

    obs = buoy.create_observations(is_deployed=True)
    assert len(obs) == 1

    o = obs[0]
    iso = state.lastUpdated.replace(microsecond=0).isoformat()
    # device record embedded
    assert o["location"] == {"lat": start[0], "lon": start[1]}
    assert o["source"].startswith("S123_")
    assert o["recorded_at"] == iso

    # event type for deployed
    assert o["additional"]["event_type"] == TRAP_DEPLOYMENT_EVENT


def test_create_observations_with_two_unit_line_new_structure(base_state):
    """Test creating observations for a two-unit line using the new structure."""
    start = (11.0, 21.0)
    end = (12.0, 22.0)
    
    # Create start unit state
    start_state_kwargs = {
        **base_state,
        "latDeg": start[0],
        "lonDeg": start[1],
        "isTwoUnitLine": True,
        "endUnit": "S456",
    }
    start_state = CurrentState(**start_state_kwargs)
    start_buoy = Buoy(
        userId="7889ad74-aab3-4044-bcf4-13d6f9586a82",
        currentState=start_state,
        serialNumber="S123",
        changeRecords=[],
    )

    # Create end unit state
    end_state_kwargs = {
        **base_state,
        "latDeg": end[0],
        "lonDeg": end[1],
        "isTwoUnitLine": True,
        "startUnit": "S123",
    }
    end_state = CurrentState(**end_state_kwargs)
    end_buoy = Buoy(
        userId="7889ad74-aab3-4044-bcf4-13d6f9586a82",
        currentState=end_state,
        serialNumber="S456",
        changeRecords=[],
    )

    # Create observations using the new structure
    obs = start_buoy.create_observations(is_deployed=True, end_unit_buoy=end_buoy)
    assert len(obs) == 2

    # Check both observations use same source_name/subject_name
    assert obs[0]["source_name"] == obs[1]["source_name"]

    # Check that second observation has end unit location
    assert obs[1]["location"] == {"lat": end[0], "lon": end[1]}

    # Check that the source correspond to the expected ones (serial_hashed_user_id)
    assert obs[0]["source"].startswith("S123_")
    assert obs[1]["source"].startswith("S456_")

def test_create_observations_with_two_unit_line_missing_end_unit(caplog, base_state):
    """Test creating observations when end unit is missing."""
    start = (11.0, 21.0)
    state_kwargs = {
        **base_state,
        "latDeg": start[0],
        "lonDeg": start[1],
        "isTwoUnitLine": True,
        "endUnit": "S456",
    }
    state = CurrentState(**state_kwargs)
    buoy = Buoy(
        userId="7889ad74-aab3-4044-bcf4-13d6f9586a82",
        currentState=state,
        serialNumber="S123",
        changeRecords=[],
    )

    caplog.set_level(logging.WARNING)
    obs = buoy.create_observations(is_deployed=True, end_unit_buoy=None)
    
    # Should still create observation for start unit
    assert len(obs) == 1
    assert obs[0]["location"] == {"lat": start[0], "lon": start[1]}
    assert UUID(obs[0]["source_name"]).version == 4


def test_create_observations_with_two_unit_line_old_structure(base_state):
    """Test creating observations for a two-unit line using the old structure."""
    start = (11.0, 21.0)
    end = (12.0, 22.0)
    state_kwargs = {
        **base_state,
        "latDeg": start[0],
        "lonDeg": start[1],
        "endLatDeg": end[0],
        "endLonDeg": end[1],
        "isTwoUnitLine": False,
    }
    state = CurrentState(**state_kwargs)
    buoy = Buoy(
        userId="7889ad74-aab3-4044-bcf4-13d6f9586a82",
        currentState=state,
        serialNumber="S123",
        changeRecords=[],
    )

    obs = buoy.create_observations(is_deployed=True)
    assert len(obs) == 2

    # Check both observations use same source_name/subject_name
    assert len({o["source_name"] for o in obs}) == 1

    # Check that second observation has end unit location
    assert obs[1]["location"] == {"lat": end[0], "lon": end[1]}


def test_create_device_record(base_state):
    """Test the _create_device_record method."""
    state = CurrentState(**base_state)
    buoy = Buoy(
        userId="7889ad74-aab3-4044-bcf4-13d6f9586a82",
        currentState=state,
        serialNumber="S123",
        changeRecords=[],
    )

    # Test the _create_device_record method directly
    device_record = buoy._create_device_record(
        label="Test Device",
        latitude=42.123,
        longitude=-71.456,
        subject_name="test_subject_123",
        last_updated="2025-01-01T12:00:00Z",
    )

    expected_record = {
        "label": "Test Device",
        "location": {"latitude": 42.123, "longitude": -71.456},
        "device_id": "test_subject_123",
        "last_updated": "2025-01-01T12:00:00Z",
    }

    assert device_record == expected_record
