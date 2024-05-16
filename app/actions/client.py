import httpx
import json
import logging
import pydantic
import random
import re
import stamina
import os

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


class ClusteredIntegratedAlert(pydantic.BaseModel):
    latitude: float
    longitude: float
    confidence_label: str = pydantic.Field(
        ..., alias="gfw_integrated_alerts__confidence"
    )
    confidence: float = 0.0
    num_clustered_alerts: int = 1
    recorded_at: datetime = pydantic.Field(..., alias="gfw_integrated_alerts__date")

    @pydantic.validator(
        "recorded_at",
        pre=True,
    )
    def sanitized_date(cls, val) -> datetime:
        return datetime.strptime(val, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    @pydantic.root_validator
    def compute_confidence(cls, values):
        values["confidence"] = (
            1.0 if values.get("confidence_label", "") in {"high", "highest"} else 0.0
        )
        return values


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
    num_clustered_alerts: Optional[int] = 1


class GFWClientException(Exception):
    pass


async def get_alerts(
        *,
        auth,
        dataset: str,
        fields: Set[str],
        date_field: str,
        geometry: dict,
        daterange: Tuple[datetime, datetime],
        extra_where: str = "",
):
    alerts = []
    # 'coordinates' might be an empty array.
    if not geometry.get("coordinates"):
        return alerts

    api_keys = await get_api_keys(auth)
    headers = {"x-api-key": api_keys[0].api_key}

    fields = {"latitude", "longitude"} | fields or set()

    lower_bound = daterange[0]
    upper_bound = min(daterange[1], datetime.now(tz=timezone.utc))

    block_size = timedelta(days=2)
    while lower_bound < upper_bound:
        lower_date = lower_bound.strftime("%Y-%m-%d")
        upper_date = (lower_bound + block_size).strftime("%Y-%m-%d")
        sql_query = f"SELECT {','.join(fields)} FROM results WHERE ({date_field} >= '{lower_date}' AND {date_field} <= '{upper_date}')"
        if extra_where:
            sql_query += f" AND {extra_where}"

        logger.debug(f"Querying dataset with sql: {sql_query}")
        payload = {
            "geometry": geometry,
            "sql": sql_query,
        }
        async with httpx.AsyncClient(timeout=httpx.Timeout(300.0, connect=3.1)) as client:
            response = await client.post(
                f"{DATA_API_ROOT_URL}/dataset/{dataset}/latest/query/json",
                headers=headers,
                json=payload,
                follow_redirects=True
            )
            response.raise_for_status()
            response = response.json()

            data_len = len(response.get("data"))
            logger.info(f"Extracted {data_len} alerts for period {lower_date} - {upper_date}.")
            alerts.extend(response.get("data", []))

        lower_bound = lower_bound + block_size

    return alerts


async def get_gfw_integrated_alerts(auth, date_range, geometry):
    fields = {"gfw_integrated_alerts__date", "gfw_integrated_alerts__confidence"}
    async for attempt in stamina.retry_context(
            on=httpx.HTTPError,
            attempts=3,
            wait_initial=timedelta(seconds=10),
            wait_max=timedelta(seconds=10),
    ):
        with attempt:
            response = await get_alerts(
                auth=auth,
                dataset="gfw_integrated_alerts",
                geometry=geometry,
                date_field="gfw_integrated_alerts__date",
                daterange=date_range,
                fields=fields,
                extra_where="(gfw_integrated_alerts__confidence = 'highest')"
            )
            return response


async def create_api_key(auth):
    headers = await get_auth_header(auth)

    payload = {
        "alias": "-".join((auth.email, random_string())),
        "email": auth.email,
        "organization": "EarthRanger",
        "domains": [],
    }

    async with httpx.AsyncClient(timeout=httpx.Timeout(60.0, connect=3.1)) as client:
        response = await client.post(
            url=f"{DATA_API_ROOT_URL}/auth/apikey",
            headers=headers,
            json=payload,
            follow_redirects=True
        )
        response.raise_for_status()


async def get_access_token(auth):
    async with httpx.AsyncClient(timeout=httpx.Timeout(60.0, connect=3.1)) as client:
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


async def get_api_keys(auth):
    headers = await get_auth_header(auth)

    for _ in range(0, 2):
        async with httpx.AsyncClient(timeout=httpx.Timeout(60.0, connect=3.1)) as session:
            response = await session.get(
                f"{DATA_API_ROOT_URL}/auth/apikeys", headers=headers,
                follow_redirects=True
            )
            response.raise_for_status()
            response = response.json()

            data = DataAPIKeysResponse.parse_obj(response)
            if data.data:
                api_keys = data.data
                break
            # Assume we need to create an API key.
            await create_api_key(auth)

    return api_keys


def aoi_from_url(url) -> str:
    href = httpx.get(url, verify=True, timeout=10).url
    try:
        matches = re.match(".*globalforestwatch.org.*aoi/([^/]+).*", str(href))
        return matches[1]
    except IndexError as ie:
        logger.error("Unable to parse AOI from globalforestwatch URL: %s", url)


async def get_aoi(integration, auth, aoi_id: str, token):
    async with httpx.AsyncClient(timeout=httpx.Timeout(60.0, connect=3.1)) as session:
        response = await session.get(
            url=f"{integration.base_url}/v2/area/{aoi_id}",
            headers={"Authorization": f'Bearer {token["token"]}'},
            follow_redirects=True
        )
        response.raise_for_status()
        response = response.json()

        try:
            return AOIData.parse_obj(response.get("data"))
        except pydantic.ValidationError as e:
            logger.exception(f"Unexpected error parsing AOI data: {e}")

        logger.error(
            "Failed to get AOI for id: %s. result is: %s", aoi_id, response.text[:250]
        )

        await state_manager.delete_state(
            str(integration.id),
            "auth",
            auth.email
        )

        raise GFWClientException(f"Failed to get AOI for id: '{aoi_id}'")


async def get_geostore(integration, geostore_id):
    async with httpx.AsyncClient(timeout=httpx.Timeout(60.0, connect=3.1)) as session:
        response = await session.get(
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


async def get_aoi_geojson_geometry(integration, aoi_data):
    geostore = await get_geostore(integration, aoi_data.attributes.geostore)
    geojson_geometry = (
        geostore.attributes.geojson
    )
    return geojson_geometry


async def get_dataset_metadata(dataset, auth):
    api_keys = await get_api_keys(auth)
    headers = {"x-api-key": api_keys[0].api_key}

    async with httpx.AsyncClient(timeout=httpx.Timeout(60.0, connect=3.1)) as client:
        response = await client.get(
            f"{DATA_API_ROOT_URL}/dataset/{dataset}/latest",
            headers=headers,
            follow_redirects=True
        )
        response.raise_for_status()
        response = response.json()

        return DatasetResponseItem.parse_obj(response.get("data"))


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
    async with httpx.AsyncClient(timeout=120) as session:
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


async def get_aoi_data(integration, config):
    auth = get_auth_config(integration)
    try:
        token = await get_token(integration, auth)
        aoi_id = aoi_from_url(config.gfw_share_link_url)
        aoi_data = await get_aoi(integration, auth, aoi_id, token)
    except Exception as e:
        message = f"Unhandled exception occurred. Exception: {e}"
        logger.exception(
            message,
            extra={
                "integration_id": str(integration.id),
                "attention_needed": True
            }
        )
        raise e
    else:
        return aoi_data


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
        end_date = datetime.utcnow()
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
        message = f"Unhandled exception occurred. Exception: {e}"
        logger.exception(
            message,
            extra={
                "integration_id": str(integration.id),
                "attention_needed": True
            }
        )
        raise e
    else:
        logger.info(f"Got {len(alerts)} fire alerts")
        return alerts


async def get_integrated_alerts(aoi_data, integration, config):
    auth = get_auth_config(integration)
    integrated_alerts_response = []

    logger.info(
        f"Processing integrated alerts for '{auth.email}' ",
        extra={
            "integration_id": str(integration.id),
            "endpoint": integration.base_url,
            "username": auth.email,
        },
    )

    try:
        dataset_metadata = await get_dataset_metadata(
            DATASET_GFW_INTEGRATED_ALERTS,
            auth
        )

        dataset_status = await state_manager.get_state(
            str(integration.id),
            "pull_integrated_alerts",
            DATASET_GFW_INTEGRATED_ALERTS
        )

        if dataset_status:
            dataset_status = DatasetStatus.parse_obj(dataset_status)
        else:
            dataset_status = DatasetStatus(
                dataset=dataset_metadata.dataset,
                version=dataset_metadata.version,
            )

        # If I've saved a status for this dataset, compare 'updated_on' timestamp to avoid redundant queries.
        if dataset_status.latest_updated_on >= dataset_metadata.updated_on:
            logger.info(
                "No updates reported for dataset %s so skipping tree-loss queries",
                DATASET_GFW_INTEGRATED_ALERTS,
                extra={
                    "integration_id": str(integration.id),
                    "integration_login": auth.email,
                    "dataset_updated_on": dataset_metadata.updated_on.isoformat(),
                },
            )
        else:
            logger.info(
                "Found updated dataset %s with updated_on: %s",
                DATASET_GFW_INTEGRATED_ALERTS,
                dataset_metadata.updated_on.isoformat(),
                extra={
                    "integration_id": str(integration.id),
                    "integration_login": auth.email,
                    "dataset_updated_on": dataset_metadata.updated_on.isoformat(),
                },
            )

            geojson_geometry = await get_aoi_geojson_geometry(integration, aoi_data)

            end = datetime.now(tz=timezone.utc)
            start = end - timedelta(days=config.integrated_alerts_lookback_days)

            # NOTE: buffer(0) is a trick for fixing scenarios where polygons have overlapping coordinates
            geometry_collection = GeometryCollection(
                [
                    shape(feature["geometry"]).buffer(0)
                    for feature in geojson_geometry["features"]
                ]
            )
            for geometry_fragment in utils.generate_geometry_fragments(
                    geometry_collection=geometry_collection
            ):

                geojson_area = mapping(geometry_fragment)
                query_arguments = dict(
                    date_range=(start, end), geometry=geojson_area
                )

                integrated_alerts = await get_gfw_integrated_alerts(
                    auth,
                    **query_arguments
                )

                if integrated_alerts:
                    integrated_alerts_response.extend(
                        [ClusteredIntegratedAlert.parse_obj(x) for x in integrated_alerts]
                    )

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
        message = f"Unhandled exception occurred. Exception: {e}"
        logger.exception(
            message,
            extra={
                "integration_id": str(integration.id),
                "attention_needed": True
            }
        )
        raise e
    else:
        return integrated_alerts_response
