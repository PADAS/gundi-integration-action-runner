from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import BaseModel, Field, HttpUrl


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
