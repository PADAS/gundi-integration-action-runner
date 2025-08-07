import logging
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

import pydantic

from app.actions.utils import get_hashed_user_id

logger = logging.getLogger(__name__)

SOURCE_TYPE = "ropeless_buoy"
SUBJECT_SUBTYPE = "ropeless_buoy_gearset"
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
    isTwoUnitLine: Optional[bool]
    endUnit: Optional[str]
    startUnit: Optional[str]

    class Config:
        json_encoders = {datetime: lambda val: val.isoformat()}

    @pydantic.validator("etag")
    def sanitize_etag(cls, val):
        return val.strip('"') if val else val


class Buoy(pydantic.BaseModel):
    """Represents a ropeless buoy device and its change records."""

    currentState: CurrentState
    serialNumber: str
    userId: str
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
        source_name: str,
        lat: float,
        lon: float,
        is_active: bool,
        recorded_at: str,
    ) -> Dict[str, Any]:
        """Return an observation record with the given parameters."""
        raw = self.dict()
        raw.pop("changeRecords", None)
        return {
            "subject_name": subject_name,
            "subject_type": SUBJECT_SUBTYPE,
            "recorded_at": recorded_at,
            "source_type": SOURCE_TYPE,
            "manufacturer_id": source_name,
            "is_active": is_active,
            "location": {"lat": lat, "lon": lon},
            "source_additional": {"raw": raw},
        }

    def _build_observations(
        self,
        is_deployed: bool,
        recorded_at: datetime,
        start_lat: float,
        start_lon: float,
        end_lat: Optional[float],
        end_lon: Optional[float],
        end_unit_serial: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Return one or two observations for deployment or retrieval events,
        including associated device records.
        """
        observations = []
        recorded_at = recorded_at.isoformat()

        hashed_user_id = get_hashed_user_id(self.userId)

        if end_unit_serial:
            source_name_a = f"{self.serialNumber}_{hashed_user_id}"
            source_name_b = f"{end_unit_serial}_{hashed_user_id}"
        else:
            source_name_a = f"{self.serialNumber}_{hashed_user_id}_A"
            source_name_b = f"{self.serialNumber}_{hashed_user_id}_B"

        subject_name = uuid.uuid4()

        observation_a = self._create_observation_record(
            subject_name=subject_name,
            source_name=source_name_a,
            lat=start_lat,
            lon=start_lon,
            is_active=is_deployed,
            recorded_at=recorded_at,
        )
        observations.append(observation_a)
        if (end_lat, end_lon) != (None, None):
            observation_b = self._create_observation_record(
                subject_name=subject_name,
                source_name=source_name_b,
                lat=end_lat,
                lon=end_lon,
                is_active=is_deployed,
                recorded_at=recorded_at,
            )
            observations.append(observation_b)

        return observations

    def create_observations(
        self,
        is_deployed: bool,
        end_unit_buoy: Optional["Buoy"] = None,
    ) -> List[Dict[str, Any]]:
        """
        Return observations from the current state or from changeRecords if available.
        Skips if the event is older than or equal to last_observation_timestamp when provided.
        """
        observations: List[Dict[str, Any]] = []

        state = self.currentState
        recorded_at = state.lastUpdated.replace(microsecond=0)

        start_lat = state.latDeg
        start_lon = state.lonDeg
        if end_unit_buoy:
            end_lat = end_unit_buoy.currentState.latDeg
            end_lon = end_unit_buoy.currentState.lonDeg
        else:
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
                start_lat=start_lat,
                start_lon=start_lon,
                end_lat=end_lat,
                end_lon=end_lon,
                end_unit_serial=state.endUnit,
            )
        )

        return observations
