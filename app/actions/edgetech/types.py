import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import pydantic

logger = logging.getLogger(__name__)

SOURCE_TYPE = "ropeless_buoy"
SUBJECT_SUBTYPE = "ropeless_buoy_device"
GEAR_DEPLOYED_EVENT = "gear_deployed"
GEAR_RETRIEVED_EVENT = "gear_retrieved"


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
    def deployed(self) -> bool:
        isDeployed = self.currentState.isDeployed or False
        isDeleted = self.currentState.isDeleted or False
        return isDeployed and not isDeleted

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
        last_updated: str,
    ) -> Dict[str, Any]:
        """Return an observation record with the given parameters."""
        return {
            "name": subject_name,
            "source": subject_name,
            "type": SOURCE_TYPE,
            "subject_type": SUBJECT_SUBTYPE,
            "is_active": False,
            "recorded_at": last_updated,
            "location": {"lat": lat, "lon": lon},
            "additional": {
                "subject_name": subject_name,
                "edgetech_serial_number": self.serialNumber,
                "display_id": self.serialNumber,
                "event_type": None,
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
        event_type = GEAR_DEPLOYED_EVENT if is_deployed else GEAR_RETRIEVED_EVENT
        iso_time = recorded_at.isoformat()

        subject_name_a = f"{prefix}{self.serialNumber}_A"
        device_a = self._create_device_record(
            "a", start_lat, start_lon, subject_name_a, iso_time
        )
        devices = [device_a]
        observation_a = self._create_observation_record(
            subject_name=subject_name_a,
            lat=start_lat,
            lon=start_lon,
            devices=[],
            last_updated=iso_time,
        )
        observation_a["is_active"] = is_deployed
        observation_a["additional"]["event_type"] = event_type

        if end_lat is not None and end_lon is not None:
            subject_name_b = f"{prefix}{self.serialNumber}_B"
            device_b = self._create_device_record(
                "b", end_lat, end_lon, subject_name_b, iso_time
            )
            devices = [device_a, device_b]
            observation_b = self._create_observation_record(
                subject_name=subject_name_b,
                lat=end_lat,
                lon=end_lon,
                devices=devices,
                last_updated=iso_time,
            )
            observation_b["is_active"] = is_deployed
            observation_b["additional"]["event_type"] = event_type
            observations.append(observation_b)

        observation_a["additional"]["devices"] = devices
        observations.append(observation_a)

        return observations

    def generate_observations_from_change_records(
        self, prefix: str, last_observation_timestamp: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """
        Generate observations from deployment or retrieval changes in the changeRecords.
        Skips records that are older than or equal to last_observation_timestamp if provided.
        """
        observations = []

        for record in self.changeRecords:
            if record.type != "MODIFY":
                logger.debug(
                    "Skipping change record of type %s (Serial %s)",
                    record.type,
                    self.serialNumber,
                )
                continue

            changes = {
                change.key: {"old": change.oldValue, "new": change.newValue}
                for change in record.changes
            }

            wasDeployed = changes.get("dateDeployed", {}).get("new") is not None
            wasRecovered = changes.get("dateRecovered", {}).get("new") is not None
            if not (wasDeployed or wasRecovered):
                continue

            recorded_str = changes.get("dateDeployed", {}).get("new") or changes.get(
                "dateRecovered", {}
            ).get("new")
            if not recorded_str:
                continue

            recorded_str = recorded_str.replace("Z", "+00:00")
            recorded_at = datetime.fromisoformat(recorded_str).replace(microsecond=0)

            if last_observation_timestamp and recorded_at <= last_observation_timestamp:
                continue

            if wasRecovered:
                lat = changes.get("latDeg", {}).get("old")
                lon = changes.get("lonDeg", {}).get("old")
                end_lat = changes.get("endLatDeg", {}).get("old")
                end_lon = changes.get("endLonDeg", {}).get("old")
                is_deployed = False
            else:
                lat = changes.get("latDeg", {}).get("new")
                lon = changes.get("lonDeg", {}).get("new")
                end_lat = changes.get("endLatDeg", {}).get("new")
                end_lon = changes.get("endLonDeg", {}).get("new")
                is_deployed = True

            if lat is None or lon is None:
                continue

            event_observations = self._build_observations(
                is_deployed=is_deployed,
                recorded_at=recorded_at,
                prefix=prefix,
                start_lat=lat,
                start_lon=lon,
                end_lat=end_lat,
                end_lon=end_lon,
            )
            observations.extend(event_observations)

        return observations

    def create_observations(
        self, prefix: str, last_observation_timestamp: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """
        Return observations from the current state or from changeRecords if available.
        Skips if the event is older than or equal to last_observation_timestamp when provided.
        """
        if self.changeRecords:
            return self.generate_observations_from_change_records(
                prefix, last_observation_timestamp
            )

        current = self.currentState
        is_deployed = current.isDeployed or False

        recorded_at = (
            current.dateDeployed or current.dateRecovered or current.lastUpdated
        ).replace(microsecond=0)

        if last_observation_timestamp and recorded_at <= last_observation_timestamp:
            return []

        start_lat = current.latDeg or current.recoveredLatDeg
        start_lon = current.lonDeg or current.recoveredLonDeg
        end_lat = current.endLatDeg
        end_lon = current.endLonDeg

        if start_lat is None or start_lon is None:
            logger.warning(
                "Skipping buoy %s due to missing location data", self.serialNumber
            )
            return []

        return self._build_observations(
            is_deployed=is_deployed,
            recorded_at=recorded_at,
            prefix=prefix,
            start_lat=start_lat,
            start_lon=start_lon,
            end_lat=end_lat,
            end_lon=end_lon,
        )