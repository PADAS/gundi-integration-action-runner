# tests/test_observation_subject.py

import hashlib
from datetime import datetime

import pytest

from app.actions.buoy.types import ObservationSubject
from app.actions.edgetech.types import (
    GEAR_DEPLOYED_EVENT,
    GEAR_RETRIEVED_EVENT,
)


class DummyGeometry:
    def __init__(self, coords):
        self.coordinates = coords


class DummyPosition:
    def __init__(self, coords):
        self.geometry = DummyGeometry(coords)


@pytest.fixture
def base_subject():
    # Build an ObservationSubject without running Pydantic validation
    subj = ObservationSubject.construct()
    subj.name = "TestSubject"
    subj.subject_type = "buoy"
    subj.subject_subtype = "gps"
    subj.is_active = True
    # last_position_date, url, etc. are not used by create_observations
    return subj


def test_no_last_position_raises(base_subject):
    base_subject.last_position = None
    base_subject.additional = {"devices": [{"device_id": "d1"}]}
    with pytest.raises(ValueError) as exc:
        base_subject.create_observations(datetime.utcnow())
    assert "Last position is not available." in str(exc.value)


def test_last_position_without_geometry_raises(base_subject):
    class NoGeom:
        geometry = None

    base_subject.last_position = NoGeom()
    base_subject.additional = {"devices": [{"device_id": "d1"}]}
    with pytest.raises(ValueError) as exc:
        base_subject.create_observations(datetime.utcnow())
    assert "Last position is not available." in str(exc.value)


def test_no_devices_raises(base_subject):
    base_subject.last_position = DummyPosition([10.0, 20.0])
    base_subject.additional = {"devices": []}
    with pytest.raises(ValueError) as exc:
        base_subject.create_observations(datetime.utcnow())
    assert "No devices available in additional information." in str(exc.value)


@pytest.mark.parametrize(
    "is_active,event_type",
    [
        (True, GEAR_DEPLOYED_EVENT),
        (False, GEAR_RETRIEVED_EVENT),
    ],
)
def test_create_observations_happy_path(base_subject, is_active, event_type):
    # setup
    base_subject.is_active = is_active
    lon, lat = -122.5, 37.7
    base_subject.last_position = DummyPosition([lon, lat])
    device_ids = ["devA", "devB", "devC"]
    base_subject.additional = {
        "devices": [{"device_id": d} for d in device_ids],
        "edgetech_serial_number": "SN-1234",
    }
    recorded = datetime(2025, 6, 8, 12, 0, 0)

    # invoke
    obs = base_subject.create_observations(recorded)

    # verify top‐level keys
    assert obs["name"] == base_subject.name
    assert obs["source"] == base_subject.name
    assert obs["type"] == base_subject.subject_type
    assert obs["subject_type"] == base_subject.subject_subtype
    assert obs["recorded_at"] is recorded

    # location
    assert obs["location"] == {"lat": lat, "lon": lon}

    # compute expected display_id
    concat = "".join(device_ids)
    expected = hashlib.sha256(concat.encode("utf-8")).hexdigest()[:12]

    add = obs["additional"]
    assert add["subject_name"] == base_subject.name
    assert add["edgetech_serial_number"] == "SN-1234"
    assert add["display_id"] == expected
    assert add["subject_is_active"] == is_active
    assert add["event_type"] == event_type
    assert add["devices"] == [{"device_id": d} for d in device_ids]
