import httpx
import json
import logging
import asyncio
import pydantic
import random
import re
import stamina
import backoff
import app.actions.utils as utils

from app.actions.configurations import AuthenticateConfig
from app.services.errors import ConfigurationNotFound
from app.services.state import IntegrationStateManager
from app.services.utils import find_config_for_action

from shapely.geometry import GeometryCollection, shape, mapping

from datetime import datetime, timedelta, timezone
from typing import Optional, List, Set, Tuple

logger = logging.getLogger(__name__)
state_manager = IntegrationStateManager()


DATASET_GFW_INTEGRATED_ALERTS = "gfw_integrated_alerts"
DATA_API_ROOT_URL = "https://data-api.globalforestwatch.org"


SQL_TEMPLATE = """SELECT pt.*
    FROM suomi_viirs_c2_global_7d pt
    where acq_date >= \'{start_date}\'
        AND acq_date <= \'{end_date}\'
        AND cartodb_id > {cartodb_id} 
        AND ST_INTERSECTS(ST_SetSRID(ST_GeomFromGeoJSON(\'{geometry}\'), 4326), the_geom)
"""


class DatasetStatus(pydantic.BaseModel):
    latest_updated_on: datetime = pydantic.Field(
        default_factory=lambda: datetime(1970, 1, 1, tzinfo=timezone.utc)
    )
    version: Optional[str] = ""
    dataset: Optional[str] = ""

    class Config:
        json_encoders = {datetime: lambda val: val.isoformat()}


class DataAPIToken(pydantic.BaseModel):
    access_token: str
    token_type: str

    # In case GFW's Oauth2 token does not provide expiration, we assume it's good for a day
    expires_in: int = 86400
    expires_at: datetime = None

    @pydantic.root_validator
    def calculator(cls, values):
        expires_at = values.get("expires_at")
        if not expires_at:
            values["expires_at"] = datetime.now(tz=timezone.utc) + timedelta(
                seconds=values["expires_in"]
            )
        return values


class DatasetMetadata(pydantic.BaseModel):
    title: str
    subtitle: str
    function: str
    resolution: str
    geographic_coverage: str
    source: str
    update_frequency: str


class DatasetResponseItem(pydantic.BaseModel):
    created_on: datetime
    updated_on: datetime
    dataset: str
    version: str
    is_latest: bool
    is_mutable: bool

    @pydantic.validator("created_on", "updated_on")
    def clean_timestamp(val):
        return val if val.tzinfo else val.replace(tzinfo=timezone.utc)


class DataAPIKey(pydantic.BaseModel):
    created_on: datetime
    updated_on: datetime
    alias: str
    user_id: str
    api_key: str
    organization: str
    email: str
    domains: List[str]
    expires_on: datetime

    @pydantic.validator("created_on", "updated_on", "expires_on")
    def sanitize_datetimes(val):
        if not val.tzinfo:
            return val.replace(tzinfo=timezone.utc)
        return val


class DataAPIKeyResponse(pydantic.BaseModel):
    data: DataAPIKey


class DataAPIKeysResponse(pydantic.BaseModel):
    data: List[DataAPIKey]


class DataAPIAuthException(Exception):
    pass


class DataAPIQueryException(Exception):
    pass

class GFWClientException(Exception):
    pass


def random_string(n=4):
    return "".join(random.sample([chr(x) for x in range(97, 97 + 26)], n))


class AOIAttributes(pydantic.BaseModel):
    name: Optional[str]
    application: Optional[str]
    geostore: Optional[str] = None
    createdAt: datetime
    updatedAt: datetime
    datasets: Optional[List[str]] = []
    use: dict
    env: str
    tags: Optional[List[str]]
    status: str
    public: bool
    fireAlerts: Optional[bool] = True
    deforestationAlerts: Optional[bool] = True
    webhookUrl: Optional[str]
    monthlySummary: Optional[bool] = False
    subscriptionId: Optional[str]
    email: Optional[str]
    language: Optional[str]
    confirmed: Optional[bool] = True


class AOIData(pydantic.BaseModel):
    type: str
    id: str
    attributes: AOIAttributes


class GeostoreAttributes(pydantic.BaseModel):
    geojson: dict
    hash: str
    provider: dict
    areaHa: float
    bbox: List[float]
    lock: bool
    info: dict


class Geostore(pydantic.BaseModel):
    type: str = pydantic.Field("geoStore", const=True)
    id: str
    attributes: GeostoreAttributes
    area: float = 0


class FireAlert(pydantic.BaseModel):
    cartodb_id: int
    the_geom: str
    the_geom_webmercator: str
    latitude: float
    longitude: float
    bright_ti4: float
    scan: float
    track: float
    acq_date: datetime
    acq_time: str
    satellite: str
    confidence: str
    version: str
    bright_ti5: float
    frp: float
    daynight: str


class Geometry(pydantic.BaseModel):
    type: str
    coordinates: List[List[List[float]]]


class CreatedGeostore(pydantic.BaseModel):
    created_on: datetime
    updated_on: datetime
    gfw_geostore_id: str
    gfw_geojson: Geometry
    gfw_area__ha: float
    gfw_bbox: List[float]

    @pydantic.validator("created_on", "updated_on")
    def clean_timestamp(val):
        return val if val.tzinfo else val.replace(tzinfo=timezone.utc)


class GeoStoreResponse(pydantic.BaseModel):
    data: CreatedGeostore
    status: str


@backoff.on_exception(backoff.constant, httpx.HTTPError, max_tries=3, interval=10)
async def create_api_key(auth):
    headers = await get_auth_header(auth)

    payload = {
        "alias": "-".join((auth.email, random_string())),
        "email": auth.email,
        "organization": "EarthRanger",
        "domains": [],
    }

    async with httpx.AsyncClient(timeout=httpx.Timeout(60.0, connect=10.0)) as client:
        response = await client.post(
            url=f"{DATA_API_ROOT_URL}/auth/apikey",
            headers=headers,
            json=payload,
            follow_redirects=True
        )
        response.raise_for_status()


@backoff.on_exception(backoff.constant, httpx.HTTPError, max_tries=3, interval=10)
async def get_access_token(auth):
    async with httpx.AsyncClient(timeout=httpx.Timeout(60.0, connect=5.0)) as client:
        response = await client.post(
            url=f"{DATA_API_ROOT_URL}/auth/token",
            data={"username": auth.email, "password": auth.password},
            follow_redirects=True
        )
        response.raise_for_status()
        response = response.json()

        dapitoken = DataAPIToken.parse_obj(response["data"])

        return dapitoken


async def auth_generator(auth):
    expire_at = datetime(1970, 1, 1, tzinfo=timezone.utc)

    while True:
        present = datetime.now(tz=timezone.utc)
        try:
            if expire_at <= present:
                token = await get_access_token(auth)
                ttl_seconds = token.expires_in - 5
                expire_at = present + timedelta(seconds=ttl_seconds)
            if logger.isEnabledFor(logging.DEBUG):
                ttl = (expire_at - present).total_seconds()
                logger.debug(f"Using cached auth, expires in {ttl} seconds.")

        except Exception as e:  # Catch all exceptions to avoid a fast, endless loop.
            logger.exception(f"Failed to authenticate with GFW Data API: {e}")
        else:
            yield token


async def get_auth_header(auth, _auth_gen=None, refresh=False):
    if not _auth_gen or refresh:
        _auth_gen = auth_generator(auth)

    try:
        token = await anext(_auth_gen)
    except StopIteration:
        _auth_gen = auth_generator(auth)
        token = await anext(_auth_gen)

    return {"authorization": f"{token.token_type} {token.access_token}"}


@backoff.on_exception(backoff.constant, httpx.HTTPError, max_tries=3, interval=10)
async def get_api_keys(auth, integration):
    current_state = await state_manager.get_state(
        str(integration.id),
        "get_api_keys",
        auth.email
    )

    if current_state:
        return DataAPIKey.parse_obj(current_state["api_keys"])

    headers = await get_auth_header(auth)

    for x in range(2):    
        async with httpx.AsyncClient(timeout=httpx.Timeout(60.0, connect=3.1)) as session:
            response = await session.get(
                f"{DATA_API_ROOT_URL}/auth/apikeys", headers=headers,
                follow_redirects=True
            )
            response.raise_for_status()
            response = response.json()

            data = DataAPIKeysResponse.parse_obj(response)
            if data.data:
                api_keys = data.data[0]
                state = {
                    "api_keys": api_keys.dict()
                }
                await state_manager.set_state(
                    str(integration.id),
                    "get_api_keys",
                    state,
                    auth.email,
                )
                break
            # Assume we need to create an API key.
            await create_api_key(auth)

    return api_keys


@backoff.on_exception(backoff.constant, httpx.HTTPError, max_tries=3, interval=10)
async def aoi_from_url(url) -> str:
    """
    Extracts the AOI ID from a GFW share link URL.
    """

    URL_PATTERN = ".*globalforestwatch.org.*aoi/([^/]+).*"
    if matches := re.match(URL_PATTERN, url):
        return matches[1]
    
    async with httpx.AsyncClient(timeout=httpx.Timeout(60.0, connect=5)) as client:
        head = await client.head(url, follow_redirects=True)

    try:
        matches = re.match(URL_PATTERN, str(head.url))
        return matches[1]
    except IndexError:
        logger.error("Unable to parse AOI from globalforestwatch URL: %s", url)


@backoff.on_exception(backoff.constant, httpx.HTTPError, max_tries=3, interval=10)
async def get_geostore(integration, geostore_id):
    async with httpx.AsyncClient(timeout=httpx.Timeout(60.0, connect=10.0)) as client:
        response = await client.get(
            url=f"{integration.base_url}/v1/geostore/{geostore_id}",
            follow_redirects=True
        )
        response.raise_for_status()
        response = response.json()

        try:
            return Geostore.parse_obj(response.get("data"))
        except pydantic.ValidationError as e:
            logger.exception(f"Unexpected error parsing Geostore data: {e}")

        logger.error(
            "Failed to get Geostore for id: %s. result is: %s", geostore_id, response.text[:250]
        )

        raise GFWClientException(f"Failed to get Geostore for id: '{geostore_id}'")


async def get_fire_alerts_response(integration, config, aoi_data, start_date, end_date):
    geostore = await get_geostore(integration, aoi_data.attributes.geostore)
    geojson_geometry = geostore.attributes.geojson.get("features")[0]["geometry"]

    latest_cartodb_id = 0

    geojson_geometry = json.dumps(geojson_geometry)

    sql = SQL_TEMPLATE.format(
        start_date=start_date,
        end_date=end_date,
        cartodb_id=latest_cartodb_id,
        geometry=geojson_geometry,
    )

    async for attempt in stamina.retry_context(
            on=httpx.HTTPError,
            attempts=3,
            wait_initial=timedelta(seconds=10),
            wait_max=timedelta(seconds=30),
            wait_jitter=timedelta(seconds=3)
    ):
        with attempt:
            async with httpx.AsyncClient(timeout=httpx.Timeout(60.0, connect=3.1)) as session:
                response = await session.post(
                    url=config.carto_url,
                    data={"format": "json", "q": sql}
                )
                response.raise_for_status()

    response = response.json()
    rows = response.get("rows")
    rows = [r for r in rows if r['confidence'] in ('high', 'highest')]

    return pydantic.parse_obj_as(List[FireAlert], rows) if rows else []


def get_auth_config(integration):
    # Look for the login credentials, needed for any action
    auth_config = find_config_for_action(
        configurations=integration.configurations,
        action_id="auth"
    )
    if not auth_config:
        raise ConfigurationNotFound(
            f"Authentication settings for integration {str(integration.id)} "
            f"are missing. Please fix the integration setup in the portal."
        )
    return AuthenticateConfig.parse_obj(auth_config.data)


async def get_token(integration, config):
    current_state = await state_manager.get_state(
        str(integration.id),
        "auth",
        config.email
    )

    if current_state:
        return current_state["token"]

    url = f"{integration.base_url}/auth/login"
    async for attempt in stamina.retry_context(
            on=httpx.HTTPError,
            attempts=3,
            wait_initial=timedelta(seconds=10),
            wait_max=timedelta(seconds=30),
            wait_jitter=timedelta(seconds=3)
    ):
        with attempt:
            async with httpx.AsyncClient(timeout=httpx.Timeout(60.0, connect=10.0)) as session:
                response = await session.post(
                    url=url,
                    json={
                        "email": config.email,
                        "password": config.password
                    }
                )
                response.raise_for_status()

    state = {
        "token": response.json()["data"]
    }
    await state_manager.set_state(
        str(integration.id),
        "auth",
        state,
        config.email,
    )

    return response.json()["data"]
    

async def get_fire_alerts(aoi_data, integration, config):
    auth = get_auth_config(integration)

    logger.info(
        f"Processing fire alerts for '{auth.email}' ",
        extra={
            "integration_id": str(integration.id),
            "endpoint": integration.base_url,
            "username": auth.email,
        },
    )

    try:
        end_date = datetime.now(tz=timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        start_date = end_date - timedelta(days=config.fire_lookback_days)

        alerts = await get_fire_alerts_response(integration, config, aoi_data, start_date, end_date)

    except pydantic.ValidationError as ve:
        message = f'Error while parsing GFW "get_fire_alerts" endpoint. {ve.json()}'
        logger.exception(
            message,
            extra={
                "integration_id": str(integration.id),
                "attention_needed": True
            }
        )
        raise ve

    except Exception as e:

        logger.exception(
            'Unexpected error reading fire alerts.',
            extra={
                "integration_id": str(integration.id),
                "attention_needed": True
            }
        )
        raise e
    else:
        logger.info(f"Got {len(alerts)} fire alerts")
        return alerts

