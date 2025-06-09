import hashlib
import logging
from datetime import datetime

import pytest

from app.actions.edgetech.types import (
    GEAR_DEPLOYED_EVENT,
    GEAR_RETRIEVED_EVENT,
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
    buoy = Buoy(currentState=state, serialNumber="S123", changeRecords=[])
    assert buoy.has_location is expected

def test_create_observations_skips_when_no_start_location(caplog, base_state):
    # Neither latDeg nor lonDeg set → no observations
    state_kwargs = {**base_state, "latDeg": None, "lonDeg": None}
    state = CurrentState(**state_kwargs)
    buoy = Buoy(currentState=state, serialNumber="S123", changeRecords=[])

    caplog.set_level(logging.WARNING)
    obs = buoy.create_observations(prefix="pre_", is_deployed=True)

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
    buoy = Buoy(currentState=state, serialNumber="S123", changeRecords=[])

    obs = buoy.create_observations(prefix="pf_", is_deployed=True)
    assert len(obs) == 1

    o = obs[0]
    iso = state.lastUpdated.replace(microsecond=0).isoformat()
    subj = "pf_S123_A"
    # device record embedded
    device = o["additional"]["devices"][0]
    assert device == {
        "label": "a",
        "location": {"latitude": start[0], "longitude": start[1]},
        "device_id": subj,
        "last_updated": iso,
    }
    # event type for deployed
    assert o["additional"]["event_type"] == GEAR_DEPLOYED_EVENT
    # display_id = first 12 chars of sha256 of concatenated device_ids
    expected_id = hashlib.sha256(subj.encode("utf-8")).hexdigest()[:12]
    assert o["additional"]["display_id"] == expected_id
    # recorded_at and is_active
    assert o["recorded_at"] == iso
    assert o["is_active"] is True

def test_create_observations_with_end_points(base_state):
    start = (11.0, 21.0)
    end = (12.0, 22.0)
    deployed_time = datetime(2025, 2, 2, 8, 30, 15, 999999)
    state_kwargs = {
        **base_state,
        "latDeg": start[0],
        "lonDeg": start[1],
        "endLatDeg": end[0],
        "endLonDeg": end[1],
        "dateDeployed": deployed_time,  # should use this over lastUpdated
    }
    state = CurrentState(**state_kwargs)
    buoy = Buoy(currentState=state, serialNumber="S123", changeRecords=[])

    obs = buoy.create_observations(prefix="XX_", is_deployed=False)
    assert len(obs) == 2

    iso = deployed_time.replace(microsecond=0).isoformat()
    subj_a = "XX_S123_A"
    subj_b = "XX_S123_B"
    # check devices list length
    devices = obs[0]["additional"]["devices"]
    assert len(devices) == 2
    # check both observations use same display_id
    concat = subj_a + subj_b
    exp_id = hashlib.sha256(concat.encode("utf-8")).hexdigest()[:12]
    for o in obs:
        assert o["recorded_at"] == iso
        assert o["is_active"] is False
        assert o["additional"]["event_type"] == GEAR_RETRIEVED_EVENT
        assert o["additional"]["display_id"] == exp_id

    # check that second obs has location=end
    assert obs[1]["location"] == {"lat": end[0], "lon": end[1]}
