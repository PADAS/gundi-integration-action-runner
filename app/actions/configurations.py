from datetime import datetime
from typing import List, Optional
from .core import ActionConfiguration


class AuthenticateConfig(ActionConfiguration):
    endpoint: str = "oauth/token"
    username: str
    password: str
    grant_type: str = "password"
    refresh_token: str = "string"


class PullObservationsHeader(ActionConfiguration):
    Authorization: str


class PullObservationsConfig(ActionConfiguration):
    endpoint: str = "mobile/vehicles"


class VehiclesResponse(ActionConfiguration):
    deviceId: int
    vehicleId: Optional[int]
    x: float
    y: float
    name: str
    regNo: Optional[str]
    iconURL: Optional[str]
    address: Optional[str]
    alarm: Optional[str]
    unit_msisdn: Optional[str]
    speed: Optional[int]
    direction: Optional[int]
    time: Optional[int]
    timeStr: datetime
    ignOn: Optional[bool]


class PullObservationsResponse(ActionConfiguration):
    vehicles: List[VehiclesResponse]
