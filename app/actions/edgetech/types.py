import hashlib
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pydantic

logger = logging.getLogger(__name__)

SOURCE_TYPE = "ropeless_buoy"
SUBJECT_SUBTYPE = "ropeless_buoy_device"
GEAR_DEPLOYED_EVENT = "gear_deployed"
GEAR_RETRIEVED_EVENT = "gear_retrieved"


class GeoLocation(pydantic.BaseModel):
    latitude: float
    longitude: float


class ChangeRecordEntry(pydantic.BaseModel):
    key: str
    oldValue: Any
    newValue: Any


class ChangeRecord(pydantic.BaseModel):
    type: str
    timestamp: datetime
    changes: List[ChangeRecordEntry]


class CurrentState(pydantic.BaseModel):
    etag: str
    isDeleted: bool
    serialNumber: str
    releaseCommand: str
    statusCommand: str
    idCommand: str
    isNfcTag: Optional[bool]
    modelNumber: Optional[str]
    dateOfManufacture: Optional[datetime]
    dateOfBatteryChange: Optional[datetime]
    dateDeployed: Optional[datetime]
    isDeployed: Optional[bool]
    dateRecovered: Optional[datetime]
    recoveredLatDeg: Optional[float]
    recoveredLonDeg: Optional[float]
    recoveredRangeM: Optional[float]
    dateStatus: Optional[datetime]
    statusRangeM: Optional[float]
    statusIsTilted: Optional[bool]
    statusBatterySoC: Optional[int]
    lastUpdated: datetime
    latDeg: Optional[float]
    lonDeg: Optional[float]
    endLatDeg: Optional[float]
    endLonDeg: Optional[float]

    class Config:
        json_encoders = {datetime: lambda val: val.isoformat()}

    @pydantic.validator("etag")
    def sanitize_etag(cls, val):
        return val.strip('"') if val else val


class Buoy(pydantic.BaseModel):
    """Represents a ropeless buoy device and its change records."""

    currentState: CurrentState
    serialNumber: str
    changeRecords: List[ChangeRecord]

    @property
    def has_location(self) -> bool:
        """Check if the buoy has a valid location."""

        has_retrieved_location = (
            self.currentState.recoveredLatDeg is not None
            and self.currentState.recoveredLonDeg is not None
        )

        has_deployed_location = (
            self.currentState.latDeg is not None
            and self.currentState.lonDeg is not None
        )

        has_deployed_end_location = (
            self.currentState.endLatDeg is not None
            and self.currentState.endLonDeg is not None
        )

        return (
            has_retrieved_location or has_deployed_location or has_deployed_end_location
        )

    def _create_device_record(
        self,
        label: str,
        latitude: float,
        longitude: float,
        subject_name: str,
        last_updated: str,
    ) -> Dict[str, Any]:
        """Return a device record with the given parameters."""

        return {
            "label": label,
            "location": {"latitude": latitude, "longitude": longitude},
            "device_id": subject_name,
            "last_updated": last_updated,
        }

    def _create_observation_record(
        self,
        subject_name: str,
        lat: float,
        lon: float,
        devices: List[Dict[str, Any]],
        is_active: bool,
        last_updated: str,
    ) -> Dict[str, Any]:
        """Return an observation record with the given parameters."""
        devices_names = [device["device_id"] for device in devices]
        concatenated = "".join(devices_names)
        display_id = hashlib.sha256(concatenated.encode("utf-8")).hexdigest()[:12]

        return {
            "name": subject_name,
            "source": subject_name,
            "type": SOURCE_TYPE,
            "subject_type": SUBJECT_SUBTYPE,
            "is_active": is_active,
            "recorded_at": last_updated,
            "location": {"lat": lat, "lon": lon},
            "additional": {
                "subject_name": subject_name,
                "edgetech_serial_number": self.serialNumber,
                "display_id": display_id,
                "subject_is_active": is_active,
                "event_type": GEAR_DEPLOYED_EVENT
                if is_active
                else GEAR_RETRIEVED_EVENT,
                "devices": devices,
            },
        }

    def _build_observations(
        self,
        is_deployed: bool,
        recorded_at: datetime,
        prefix: str,
        start_lat: float,
        start_lon: float,
        end_lat: Optional[float],
        end_lon: Optional[float],
    ) -> List[Dict[str, Any]]:
        """
        Return one or two observations for deployment or retrieval events,
        including associated device records.
        """
        observations = []
        iso_time = recorded_at.isoformat()

        devices = []
        subject_name_a = f"{prefix}{self.serialNumber}_A"
        subject_name_b = f"{prefix}{self.serialNumber}_B"
        device_a = self._create_device_record(
            "a", start_lat, start_lon, subject_name_a, iso_time
        )
        devices.append(device_a)

        if end_lat is not None and end_lon is not None:
            device_b = self._create_device_record(
                "b", end_lat, end_lon, subject_name_b, iso_time
            )
            devices.append(device_b)

        observation_a = self._create_observation_record(
            subject_name=subject_name_a,
            lat=start_lat,
            lon=start_lon,
            devices=devices,
            is_active=is_deployed,
            last_updated=iso_time,
        )
        observations.append(observation_a)

        if end_lat is not None and end_lon is not None:
            observation_b = self._create_observation_record(
                subject_name=subject_name_b,
                lat=end_lat,
                lon=end_lon,
                devices=devices,
                is_active=is_deployed,
                last_updated=iso_time,
            )
            observations.append(observation_b)

        return observations

    def create_observations(
        self,
        prefix: str,
        is_deployed: bool,
    ) -> List[Dict[str, Any]]:
        """
        Return observations from the current state or from changeRecords if available.
        Skips if the event is older than or equal to last_observation_timestamp when provided.
        """
        observations: List[Dict[str, Any]] = []

        state = self.currentState

        recorded_at = (state.dateDeployed or state.lastUpdated).replace(microsecond=0)

        start_lat = state.latDeg
        start_lon = state.lonDeg
        end_lat = state.endLatDeg
        end_lon = state.endLonDeg

        if start_lat is None or start_lon is None:
            logger.warning(
                "No valid location for buoy %s, skipping observation creation.",
                self.serialNumber,
            )
            return observations

        observations.extend(
            self._build_observations(
                is_deployed=is_deployed,
                recorded_at=recorded_at,
                prefix=prefix,
                start_lat=start_lat,
                start_lon=start_lon,
                end_lat=end_lat,
                end_lon=end_lon,
            )
        )

        return observations
