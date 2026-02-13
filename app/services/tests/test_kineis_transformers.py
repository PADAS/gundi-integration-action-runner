"""Tests for Kineis telemetry to Gundi observation mapping (CONNECTORS-836)."""

import pytest

from app.actions.transformers import (
    telemetry_to_observation,
    telemetry_batch_to_observations,
    GUNDI_OBSERVATION_TYPE,
)


def test_telemetry_to_observation_valid():
    """Map a valid telemetry message with deviceRef, gps, recordedAt."""
    msg = {
        "deviceRef": "238883",
        "recordedAt": "2024-01-15T10:30:00.000Z",
        "gps": {"lat": -1.5, "lon": 30.2},
    }
    obs = telemetry_to_observation(msg)
    assert obs is not None
    assert obs["source"] == "238883"
    assert obs["type"] == GUNDI_OBSERVATION_TYPE
    assert obs["recorded_at"] == "2024-01-15T10:30:00.000Z"
    assert obs["location"] == {"lat": -1.5, "lon": 30.2}
    assert "additional" in obs


def test_telemetry_to_observation_device_uid():
    """Use deviceUid when deviceRef is missing."""
    msg = {
        "deviceUid": 62533,
        "timestamp": "2024-01-15T12:00:00Z",
        "lat": -2.0,
        "lon": 31.0,
    }
    obs = telemetry_to_observation(msg)
    assert obs is not None
    assert obs["source"] == "62533"
    assert obs["location"] == {"lat": -2.0, "lon": 31.0}


def test_telemetry_to_observation_missing_source_returns_none():
    """Return None when deviceRef and deviceUid are missing."""
    msg = {
        "recordedAt": "2024-01-15T10:00:00Z",
        "gps": {"lat": 0, "lon": 0},
    }
    assert telemetry_to_observation(msg) is None


def test_telemetry_to_observation_missing_location_returns_none():
    """Return None when lat/lon are missing."""
    msg = {
        "deviceRef": "D1",
        "recordedAt": "2024-01-15T10:00:00Z",
    }
    assert telemetry_to_observation(msg) is None


def test_telemetry_to_observation_missing_timestamp_returns_none():
    """Return None when no timestamp field."""
    msg = {
        "deviceRef": "D1",
        "gps": {"lat": 0, "lon": 0},
    }
    assert telemetry_to_observation(msg) is None


def test_telemetry_to_observation_additional_fields():
    """Extra fields appear in additional."""
    msg = {
        "deviceRef": "D1",
        "recordedAt": "2024-01-15T10:00:00.000Z",
        "gps": {"lat": 0, "lon": 0, "speed": 5.2, "course": 90},
    }
    obs = telemetry_to_observation(msg)
    assert obs is not None
    assert obs["additional"].get("speed") == 5.2
    assert obs["additional"].get("course") == 90


def test_telemetry_batch_to_observations_skips_invalid():
    """Invalid messages are skipped; valid ones are returned."""
    messages = [
        {"deviceRef": "A", "recordedAt": "2024-01-15T10:00:00Z", "gps": {"lat": 0, "lon": 0}},
        {"deviceRef": "B"},  # missing location and timestamp
        {"deviceUid": 1, "timestamp": "2024-01-15T11:00:00Z", "lat": 1, "lon": 1},
    ]
    result = telemetry_batch_to_observations(messages)
    assert len(result) == 2
    assert result[0]["source"] == "A"
    assert result[1]["source"] == "1"


def test_telemetry_to_observation_api_shape_msg_ts_gps_loc():
    """Map API-shaped message with msgTs (epoch ms) and gpsLocLat/gpsLocLon (bulk/realtime)."""
    msg = {
        "deviceRef": "238883",
        "msgTs": 1705312800000,  # 2024-01-15 10:00:00 UTC
        "gpsLocLat": -1.5,
        "gpsLocLon": 30.2,
    }
    obs = telemetry_to_observation(msg)
    assert obs is not None
    assert obs["source"] == "238883"
    assert obs["location"] == {"lat": -1.5, "lon": 30.2}
    assert "2024-01-15" in obs["recorded_at"] and ("Z" in obs["recorded_at"] or "+00:00" in obs["recorded_at"])


def test_telemetry_to_observation_api_shape_doppler_loc():
    """Map API-shaped message with dopplerLocLat/dopplerLocLon when GPS missing."""
    msg = {
        "deviceUid": 1788,
        "acqTs": 1705316400000,
        "dopplerLocLat": 0.0,
        "dopplerLocLon": 0.0,
    }
    obs = telemetry_to_observation(msg)
    assert obs is not None
    assert obs["source"] == "1788"
    assert obs["location"] == {"lat": 0.0, "lon": 0.0}


def test_telemetry_to_observation_sample_response_gps_fix():
    """Map sample response shape (docs/kineis-api-samples): GPS fix time and extra fields."""
    # Matches retrieve-bulk-response / retrieve-realtime-response sample message
    msg = {
        "deviceMsgUid": 59220647342112780,
        "deviceUid": 67899,
        "deviceRef": "7896",
        "modemRef": "7896",
        "msgType": "operation-mo-pdrgroup",
        "msgDatetime": "2024-10-01T15:56:19.001Z",
        "acqDatetime": "2024-10-01T15:56:25.001Z",
        "gpsLocDatetime": "2024-10-01T15:56:18.001Z",
        "gpsLocLat": 20.45123,
        "gpsLocLon": 58.77856,
        "gpsLocAlt": 0,
        "gpsLocSpeed": 2.78,
        "gpsLocHeading": 67.45,
    }
    obs = telemetry_to_observation(msg)
    assert obs is not None
    assert obs["source"] == "7896"
    assert obs["source_name"] == "7896"  # API has no device display name; we use deviceRef
    assert obs["location"] == {"lat": 20.45123, "lon": 58.77856}
    # Prefer GPS fix time for recorded_at
    assert obs["recorded_at"] == "2024-10-01T15:56:18.001Z"
    assert obs["additional"].get("speed") == 2.78
    assert obs["additional"].get("heading") == 67.45
    assert obs["additional"].get("altitude") == 0
    assert obs["additional"].get("msgType") == "operation-mo-pdrgroup"


def test_telemetry_to_observation_source_name_from_device_list():
    """When device_uid_to_customer_name is provided and deviceUid is in map, source_name is 'deviceUid (customerName)'."""
    msg = {
        "deviceUid": 67899,
        "deviceRef": "7896",
        "gpsLocDatetime": "2024-10-01T15:56:18.001Z",
        "gpsLocLat": 20.45123,
        "gpsLocLon": 58.77856,
    }
    device_uid_to_customer_name = {67899: "WILDLIFE COMPUTER"}
    obs = telemetry_to_observation(msg, device_uid_to_customer_name=device_uid_to_customer_name)
    assert obs is not None
    assert obs["source"] == "7896"
    assert obs["source_name"] == "67899 (WILDLIFE COMPUTER)"


def test_telemetry_to_observation_source_name_fallback_when_not_in_map():
    """When deviceUid is not in device_uid_to_customer_name, source_name equals source."""
    msg = {
        "deviceUid": 99999,
        "deviceRef": "ref99",
        "gpsLocLat": 0,
        "gpsLocLon": 0,
        "msgTs": 1705312800000,
    }
    device_uid_to_customer_name = {67899: "WILDLIFE COMPUTER"}
    obs = telemetry_to_observation(msg, device_uid_to_customer_name=device_uid_to_customer_name)
    assert obs is not None
    assert obs["source"] == "ref99"
    assert obs["source_name"] == "ref99"


def test_telemetry_batch_to_observations_passes_device_map():
    """telemetry_batch_to_observations passes device_uid_to_customer_name to each message."""
    messages = [
        {
            "deviceUid": 67899,
            "deviceRef": "7896",
            "gpsLocLat": 20.45,
            "gpsLocLon": 58.77,
            "msgTs": 1705312800000,
        },
    ]
    device_uid_to_customer_name = {67899: "WILDLIFE COMPUTER"}
    result = telemetry_batch_to_observations(
        messages,
        device_uid_to_customer_name=device_uid_to_customer_name,
    )
    assert len(result) == 1
    assert result[0]["source_name"] == "67899 (WILDLIFE COMPUTER)"
