import logging
import pydantic

from datetime import datetime, timedelta, timezone
from pydantic import BaseModel
from app.services.state import IntegrationStateManager

DEFAULT_TIMEOUT = (3.1, 20)
DEFAULT_LOOKBACK_DAYS = 60

logger = logging.getLogger(__name__)
state_manager = IntegrationStateManager()


class OnyeshaPosition(BaseModel):
    ChannelStatus: str
    UploadTimeStamp: datetime
    Latitude: float
    Longitude: float
    Altitude: float
    ECEFx: int
    ECEFy: int
    ECEFz: int
    RxStatus: int
    PDOP: float
    MainV: float
    BkUpV: float
    Temperature: float
    FixDuration: int
    bHasTempVoltage: bool
    DevName: str
    DeltaTime: int
    FixType: int
    CEPRadius: int
    CRC: int
    DeviceID: int
    RecDateTime: datetime


def default_last_run():
    '''Default for a new configuration is to pretend the last run was 7 days ago'''
    return datetime.now(tz=timezone.utc) - timedelta(days=7)

class IntegrationState(pydantic.BaseModel):
    last_run: datetime = pydantic.Field(default_factory=default_last_run, alias='last_run')
    error: str = None

    @pydantic.validator("last_run")
    def clean_last_run(cls, v):
        if v is None:
            return default_last_run()
        if not v.tzinfo:
            return v.replace(tzinfo=timezone.utc)
        return v

async def get_token():
    response = {
        "grant_type": "token",
        "token": "dummy_token",
    }
    return response

async def get_devices(): # reutrn list of strings
    return []


async def get_positions():
    return []