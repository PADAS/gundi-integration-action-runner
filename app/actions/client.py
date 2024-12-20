import httpx
import logging
import pydantic

from datetime import datetime, timedelta, timezone
from pydantic import BaseModel
from app.services.state import IntegrationStateManager

DEFAULT_TIMEOUT = (3.1, 20)
DEFAULT_LOOKBACK_DAYS = 60

logger = logging.getLogger(__name__)
state_manager = IntegrationStateManager()

class LotekConnectionException(Exception):
    def __init__(self, error: Exception, message: str, status_code=403):
        self.status_code = status_code
        self.message = message
        self.error = error
        super().__init__(f"'{self.status_code}: {self.message}, Error: {self.error}'")

class LotekException(Exception):
    def __init__(self, error: Exception, message: str, status_code=500):
        self.status_code = status_code
        self.message = message
        self.error = error
        super().__init__(f"'{self.status_code}: {self.message}, Error: {self.error}'")

class LotekPosition(BaseModel):
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

class LotekDevice(BaseModel):
    nDeviceID: str
    strSpecialID: str
    dtCreated: datetime
    strSatellite: str

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

async def get_token(integration, auth):
    params = {
        "grant_type": "password",
        "username": auth.username,
        "password": auth.password.get_secret_value()
    }
    async with httpx.AsyncClient(timeout=120) as session:
        try:
            base_url = integration.base_url or 'https://webservice.lotek.com/API'
            response = await session.post(base_url + "/user/login", data=params)
            response.raise_for_status()
        except httpx.HTTPError as ex:
            msg = f'Lotek login failed for user {auth.username}. Caught exception: {ex}'
            raise LotekConnectionException(message=msg, error=ex)
        else:
            if not response:
                msg = f'Lotek login failed for user {auth.username}. Token response is: {response.text}'
                raise LotekConnectionException(message=msg)
            data = response.json()
            return data.get('access_token', None)

async def get_devices(integration, auth):
    try:
        saved_token = await state_manager.get_state(
            str(integration.id),
            "pull_observations",
            "token"
        )
        if not saved_token:
            token = await get_token(integration, auth)
            await state_manager.set_state(
                str(integration.id),
                "pull_observations",
                token
            )
        else:
            token = saved_token

        headers = {
            'Authorization': f"Bearer {token}",
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        async with httpx.AsyncClient(timeout=120) as session:
            base_url = integration.base_url or 'https://webservice.lotek.com/API'
            response = await session.get(base_url + "/devices", headers=headers)
            response.raise_for_status()
    except httpx.HTTPError as ex:
        msg = f'Lotek get_devices failed for user {auth.username}. Caught exception: {ex}'
        raise LotekException(status_code=response.status_code, message=msg, error=ex)
    else:
        data = response.json()
        devices = [LotekDevice(**device) for device in data]
        return devices

async def get_positions(device_id, auth, integration, start_datetime=None, end_datetime=None, geo_only=False):
    saved_token = await state_manager.get_state(
        str(integration.id),
        "pull_observations",
        "token"
    )
    if not saved_token:
        token = await get_token(integration, auth)
        await state_manager.set_state(
            str(integration.id),
            "pull_observations",
            token
        )
    else:
        token = saved_token

    headers = {
        'Authorization': f"Bearer {token}",
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }
    if not start_datetime:
        start_datetime = datetime.today() - timedelta(days=DEFAULT_LOOKBACK_DAYS)
    params = {
        'deviceId': device_id,
        'from': start_datetime.strftime('%Y-%m-%d')
    }
    if end_datetime:
        end_datetime = (end_datetime.date() if isinstance(end_datetime, datetime) else end_datetime) + timedelta(days=1)
        params['to'] = end_datetime.strftime('%Y-%m-%d')
    else:
        params['to'] = (datetime.today() + timedelta(days=1)).strftime('%Y-%m-%d')

    async with httpx.AsyncClient(timeout=120) as session:
        try:
            logger.debug('Getting positions for user: %s, params: %s', auth.username, params)
            base_url = integration.base_url or 'https://webservice.lotek.com/API'
            response = await session.get(base_url + "/positions/findByDate", params=params, headers=headers)
            response.raise_for_status()
        except httpx.HTTPError as e:
            if response.status_code == 400:
                logger.info("Received status code 400 - Lotek throws this when there are no data")
                return []
            if response.status_code == 401:
                logger.info("Received status code 401 - Token expired, fetching a new one...")
                await state_manager.delete_state(
                    str(integration.id),
                    "pull_observations"
                )
                return await get_positions(device_id, auth, integration, start_datetime, end_datetime, geo_only)
            logger.exception(
                f'Lotek get_positions failed for user {auth.username}. Caught exception: {e}',
                extra={
                    "attention_needed": True,
                    "device_id": str(device_id),
                    "integration_type": "lotek"
                }
            )
            raise e
        else:
            positions = response.json()
            logger.debug('Got %d positions using params=%s', len(positions), params)
            results = [LotekPosition(**position) for position in positions if not (geo_only and (position['Latitude'] == 0 or position['Longitude'] == 0))]
            return results
