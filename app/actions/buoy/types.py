import hashlib
from curses.ascii import SO
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from uuid import UUID

from pydantic import BaseModel, Field, HttpUrl


class DeviceLocation(BaseModel):
    latitude: float
    longitude: float


class BuoyDevice(BaseModel):
    device_id: str
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

    def create_haul_observation(self, recorded_at: datetime) -> Dict[str, Any]:
        """
        Create an observation record for the buoy gear.
        """
        from app.actions.edgetech.types import SOURCE_TYPE, SUBJECT_SUBTYPE

        return [
            {
                "subject_name": self.display_id,
                "manufacturer_id": device.device_id,
                "subject_is_active": False,
                "source_type": SOURCE_TYPE,
                "subject_subtype": SUBJECT_SUBTYPE,
                "location": {
                    "lat": device.location.latitude,
                    "lon": device.location.longitude,
                },
                "additional": {
                    "event_type": "trap_retrieved",
                },
                "recorded_at": recorded_at,
            }
            for device in self.devices
        ]


class LastPositionStatus(BaseModel):
    last_voice_call_start_at: Optional[datetime]
    radio_state_at: Optional[datetime]
    radio_state: str


class Geometry(BaseModel):
    type: str
    coordinates: List[float]


class CoordinateProperties(BaseModel):
    time: datetime


class FeatureProperties(BaseModel):
    title: str
    subject_type: str
    subject_subtype: str
    id: UUID
    stroke: str
    stroke_opacity: float = Field(..., alias="stroke-opacity")
    stroke_width: int = Field(..., alias="stroke-width")
    image: str
    last_voice_call_start_at: Optional[datetime]
    location_requested_at: Optional[datetime]
    radio_state_at: datetime
    radio_state: str
    coordinateProperties: CoordinateProperties
    DateTime: datetime


class Feature(BaseModel):
    type: str
    geometry: Geometry
    properties: FeatureProperties


class ObservationSubject(BaseModel):
    content_type: str
    id: UUID
    name: str
    subject_type: str
    subject_subtype: str
    common_name: Optional[str]
    additional: Dict[str, Any]
    created_at: datetime
    updated_at: datetime
    is_active: bool
    user: Optional[Any]
    tracks_available: bool
    image_url: str
    last_position_status: Optional[LastPositionStatus]
    last_position_date: Optional[datetime]
    last_position: Optional[Feature]
    device_status_properties: Optional[Any]
    url: HttpUrl

    @property
    def location(self) -> Tuple[float, float]:
        """
        Return the last known location as a tuple of (latitude, longitude).
        """
        return (self.latitude, self.longitude)

    @property
    def latitude(self) -> float:
        """
        Return the latitude of the last known location.
        """
        if not self.last_position or not self.last_position.geometry:
            raise ValueError("Last position is not available.")
        return self.last_position.geometry.coordinates[1]

    @property
    def longitude(self) -> float:
        """
        Return the longitude of the last known location.
        """
        if not self.last_position or not self.last_position.geometry:
            raise ValueError("Last position is not available.")
        return self.last_position.geometry.coordinates[0]

    def create_observation(
        self, recorded_at: Optional[datetime], is_active: Optional[bool] = None
    ) -> Dict[str, Any]:
        """
        Create observations based on the subject's last position and status.
        Returns a list of observation records.
        """
        from app.actions.edgetech.types import TRAP_DEPLOYMENT_EVENT, TRAP_RETRIEVED_EVENT

        if not self.last_position or not self.last_position.geometry:
            raise ValueError("Last position is not available.")

        devices = self.additional.get("devices", [])
        if not devices:
            raise ValueError("No devices available in additional information.")

        devices_names = [device["device_id"] for device in devices]
        concatenated = "".join(devices_names)
        display_id = hashlib.sha256(concatenated.encode("utf-8")).hexdigest()[:12]
        is_active = is_active if is_active is not None else self.is_active
        observation = {
            "name": self.name,
            "source": self.name,
            "type": self.subject_type,
            "subject_type": self.subject_subtype,
            "recorded_at": recorded_at.isoformat()
            or datetime.now(timezone.utc).isoformat(),
            "location": {"lat": self.latitude, "lon": self.longitude},
            "additional": {
                "subject_name": self.name,
                "edgetech_serial_number": self.additional.get("edgetech_serial_number"),
                "display_id": display_id,
                "subject_is_active": is_active,
                "event_type": (
                    TRAP_DEPLOYMENT_EVENT if is_active else TRAP_RETRIEVED_EVENT
                ),
                "devices": self.additional.get("devices", []),
            },
        }

        return observation
