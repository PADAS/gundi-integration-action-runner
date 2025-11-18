from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import BaseModel

from app.actions.edgetech.types import TRAP_RETRIEVED_EVENT


class DeviceLocation(BaseModel):
    latitude: float
    longitude: float


class BuoyDevice(BaseModel):
    device_id: str
    mfr_device_id: str
    label: str
    location: DeviceLocation
    last_updated: datetime
    last_deployed: Optional[datetime]


class BuoyGear(BaseModel):
    id: UUID
    display_id: str
    status: str
    last_updated: datetime
    devices: List[BuoyDevice]
    type: str
    manufacturer: str
