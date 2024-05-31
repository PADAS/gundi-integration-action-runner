from __future__ import annotations
from typing import List
from pydantic import BaseModel, Field

from typing import Optional, List

from app.services.utils import StructHexString
from app.webhooks.core import WebhookConfiguration, WebhookPayload, HexStringPayload, DynamicSchemaConfig, \
    HexStringConfig, JQTransformConfig

##################################################################################
# LiquidTech Example: Using specific models with hex data support
class LiquidTechConfig(HexStringConfig, WebhookConfiguration):
    pass


class LiquidTechPayload(HexStringPayload, WebhookPayload):
    device: str
    time: str
    type: Optional[str]
    data: StructHexString



print(LiquidTechPayload.schema_json())
##################################################################################

##################################################################################
# Everywhere Example: Using generic models with dynamic schema support and JQ transformations


class Point(BaseModel):
    x: int
    y: int


class TrackPoint(BaseModel):
    point: Point
    time: int


class PayloadItem(BaseModel):
    device_id: int = Field(..., alias='deviceId')
    team_id: int = Field(..., alias='teamId')
    track_point: TrackPoint = Field(..., alias='trackPoint')
    source: str
    entity_id: int = Field(..., alias='entityId')
    device_type: str = Field(..., alias='deviceType')
    name: str






##################################################################################




