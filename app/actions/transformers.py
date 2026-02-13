"""
Map Kineis/CLS telemetry messages to Gundi observation schema (CONNECTORS-836).

Uses docs/kineis-api-reference.md and docs/kineis-api-samples/ for field names.
Goal: read GPS fixes (gpsLocLat/Lon, gpsLocDatetime), transform, and send as observations.
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

GUNDI_OBSERVATION_TYPE = "tracking-device"


def _normalize_recorded_at(value: Any) -> Optional[str]:
    """Convert timestamp to ISO string with Z (UTC)."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value / 1000.0 if value > 1e12 else value, tz=timezone.utc).isoformat()
    s = str(value).strip()
    if not s:
        return None
    if not s.endswith("Z") and "+" not in s and (len(s) < 6 or s[-6] not in "+-"):
        s = s + "Z"
    return s


def telemetry_to_observation(
    message: Dict[str, Any],
    device_uid_to_customer_name: Optional[Dict[int, str]] = None,
) -> Optional[Dict[str, Any]]:
    """
    Map a single Kineis telemetry message to a Gundi observation (GPS fix).

    Prefers GPS location (gpsLocLat/Lon) and GPS fix time (gpsLocDatetime) when present;
    falls back to Doppler location and message timestamps. Returns None if location
    or required fields are missing. When device_uid_to_customer_name is provided and
    the message's deviceUid is in the map, source_name is set to "deviceUid (customerName)".
    """
    # Source: deviceRef (string) or deviceUid (int) – API core fields
    source = None
    if message.get("deviceRef") is not None:
        source = str(message["deviceRef"])
    elif message.get("deviceUid") is not None:
        source = str(message["deviceUid"])
    if not source:
        logger.debug("Telemetry message missing deviceRef/deviceUid, skipping")
        return None

    # Timestamp: for GPS fixes prefer gpsLocDatetime (fix time), then msgDatetime/acqDatetime, then msgTs/acqTs
    recorded_at = (
        message.get("gpsLocDatetime")  # GPS fix timestamp when using GPS location
        or message.get("recordedAt")
        or message.get("msgDatetime")
        or message.get("acqDatetime")
        or message.get("timestamp")
        or message.get("receivedAt")
        or message.get("date")
    )
    if recorded_at is None:
        msg_ts = message.get("msgTs") or message.get("acqTs") or message.get("gpsLocTs")
        if msg_ts is not None:
            recorded_at = datetime.fromtimestamp(msg_ts / 1000.0, tz=timezone.utc).isoformat()
    recorded_at = _normalize_recorded_at(recorded_at)
    if not recorded_at:
        logger.debug("Telemetry message missing timestamp, skipping: %s", source)
        return None

    # Location: prefer GPS (gpsLocLat/Lon), then Doppler (dopplerLocLat/Lon), then legacy shapes
    lat = None
    lon = None
    gps = message.get("gps") or message.get("location") or message.get("position") or {}
    if isinstance(gps, dict):
        lat = gps.get("lat") if gps.get("lat") is not None else gps.get("latitude")
        lon = gps.get("lon") if gps.get("lon") is not None else (gps.get("longitude") or gps.get("lng"))
    if lat is None:
        lat = message.get("gpsLocLat") if message.get("gpsLocLat") is not None else message.get("lat") or message.get("latitude")
    if lon is None:
        lon = message.get("gpsLocLon") if message.get("gpsLocLon") is not None else message.get("lon") or message.get("longitude")
    if lat is None:
        lat = message.get("dopplerLocLat")
    if lon is None:
        lon = message.get("dopplerLocLon")

    if lat is None or lon is None:
        logger.debug("Telemetry message missing lat/lon, skipping: %s", source)
        return None

    try:
        lat_f = float(lat)
        lon_f = float(lon)
    except (TypeError, ValueError):
        logger.debug("Invalid lat/lon for %s: %s, %s", source, lat, lon)
        return None

    # Additional: API uses gpsLocSpeed, gpsLocHeading, gpsLocAlt; map to common names + keep API names
    additional: Dict[str, Any] = {}
    for key in ("speed", "course", "altitude", "gpsFix", "fixQuality", "modemType", "modemRef"):
        if message.get(key) is not None:
            additional[key] = message[key]
    if message.get("gpsLocSpeed") is not None:
        additional["speed"] = message["gpsLocSpeed"]
    if message.get("gpsLocHeading") is not None:
        additional["heading"] = message["gpsLocHeading"]
        if "course" not in additional:
            additional["course"] = message["gpsLocHeading"]
    if message.get("gpsLocAlt") is not None:
        additional["altitude"] = message["gpsLocAlt"]
    if message.get("msgType") is not None:
        additional["msgType"] = message["msgType"]
    if gps and isinstance(gps, dict):
        for key in ("speed", "course", "altitude", "accuracy"):
            if gps.get(key) is not None and key not in additional:
                additional[key] = gps[key]

    # source_name: "deviceUid (customerName)" when device list provides customerName; else source
    device_uid = message.get("deviceUid")
    if (
        device_uid_to_customer_name
        and device_uid is not None
        and device_uid in device_uid_to_customer_name
        and device_uid_to_customer_name[device_uid]
    ):
        source_name = f"{device_uid} ({device_uid_to_customer_name[device_uid]})"
    else:
        source_name = source

    return {
        "source": source,
        "source_name": source_name,
        "type": GUNDI_OBSERVATION_TYPE,
        "subject_type": "unassigned",
        "recorded_at": recorded_at,
        "location": {"lat": lat_f, "lon": lon_f},
        "additional": additional or {},
    }


def telemetry_batch_to_observations(
    messages: List[Dict[str, Any]],
    device_uid_to_customer_name: Optional[Dict[int, str]] = None,
) -> List[Dict[str, Any]]:
    """
    Map a list of telemetry messages to Gundi observations. Skips invalid messages.
    When device_uid_to_customer_name is provided, source_name uses "deviceUid (customerName)" when available.
    """
    observations = []
    for msg in messages:
        obs = telemetry_to_observation(
            msg,
            device_uid_to_customer_name=device_uid_to_customer_name,
        )
        if obs:
            observations.append(obs)
    return observations
