from ast import Dict, List
from datetime import datetime
from typing import Optional

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

    def create_observation(self, prefix: str) -> Dict[str, any]:
        subject_name = f"edgetech_{self.serialNumber}"
        current_state = self.currentState
        observation = {
            "name": subject_name,
            "source": subject_name,
            "type": SOURCE_TYPE,
            "subject_type": SUBJECT_SUBTYPE,
            "is_active": self.deployed,
            "recorded_at": current_state.dateStatus.replace("Z", "+00:00"),
            "location": {"lat": current_state.latDeg, "lon": current_state.lonDeg},
            "additional": {
                "subject_name": subject_name,
                "edgetech_serial_number": self.serialNumber,
                "display_id": self.serialNumber,
                "event_type": GEAR_DEPLOYED_EVENT
                if self.deployed
                else GEAR_RETRIEVED_EVENT,
            },
        }
        return observation
