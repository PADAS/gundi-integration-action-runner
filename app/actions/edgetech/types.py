from datetime import datetime
from typing import Any, Dict, List, Optional

import pydantic

SOURCE_TYPE = "ropeless_buoy"
SUBJECT_SUBTYPE = "ropeless_buoy_device"
GEAR_DEPLOYED_EVENT = "gear_deployed"
GEAR_RETRIEVED_EVENT = "gear_retrieved"


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
    def sanitize_etag(val):
        return val.strip('"') if val else val


class Buoy(pydantic.BaseModel):
    currentState: CurrentState
    serialNumber: str
    changeRecords: List

    @property
    def deployed(self) -> bool:
        return self.currentState.isDeployed or not self.currentState.isDeleted

    def _create_device_record(
        self,
        label: str,
        latitude: float,
        longitude: float,
        subject_name: str,
        last_updated: str,
    ) -> Dict[str, Any]:
        """
        Creates a device record with the provided parameters.
        """
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
        """
        Creates an observation with the provided parameters.
        """
        return {
            "name": subject_name,
            "source": subject_name,
            "type": SOURCE_TYPE,
            "subject_type": SUBJECT_SUBTYPE,
            "is_active": self.deployed,
            "recorded_at": last_updated,
            "location": {"lat": lat, "lon": lon},
            "additional": {
                "subject_name": subject_name,
                "edgetech_serial_number": self.serialNumber,
                "display_id": self.serialNumber,
                "event_type": GEAR_DEPLOYED_EVENT
                if self.deployed
                else GEAR_RETRIEVED_EVENT,
                "devices": devices,
            },
        }

    def create_observations(self, prefix: str) -> List[Dict[str, Any]]:
        """
        Creates a list of observations:
        - The first one (with suffix "_A") is always created.
        - The second one (with suffix "_B") is created if endLatDeg and endLonDeg are defined.

        Both records share the devices list, which contains the records of device A and, if exists, device B.
        """
        current_state = self.currentState
        last_updated = current_state.lastUpdated.isoformat().replace("Z", "+00:00")

        # Create record and observation for device A (always exists)
        subject_name_a = f"{prefix}{self.serialNumber}_A"
        device_a = self._create_device_record(
            "a",
            current_state.latDeg,
            current_state.lonDeg,
            subject_name_a,
            last_updated,
        )
        devices = [device_a]
        observation_a = self._create_observation_record(
            subject_name_a,
            current_state.latDeg,
            current_state.lonDeg,
            devices,
            last_updated,
        )

        observations = [observation_a]

        # Create record and observation for device B, if final coordinates exist
        if current_state.endLatDeg is not None and current_state.endLonDeg is not None:
            subject_name_b = f"{prefix}{self.serialNumber}_B"
            device_b = self._create_device_record(
                "b",
                current_state.endLatDeg,
                current_state.endLonDeg,
                subject_name_b,
                last_updated,
            )
            devices.append(device_b)
            observation_b = self._create_observation_record(
                subject_name_b,
                current_state.endLatDeg,
                current_state.endLonDeg,
                devices,
                last_updated,
            )
            observations.append(observation_b)

        return observations