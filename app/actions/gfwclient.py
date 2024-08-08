import httpx
import json
import logging
import asyncio
import pydantic
import random
import re
import backoff
import uuid

import httpx
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Set, Tuple, Dict, Any


logger = logging.getLogger(__name__)

DATASET_GFW_INTEGRATED_ALERTS = "gfw_integrated_alerts"
DATASET_NASA_VIIRS_FIRE_ALERTS = "nasa_viirs_fire_alerts"

def random_string(n=4):
    return "".join(random.sample([chr(x) for x in range(97, 97 + 26)], n))


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


# class DatasetMetadata(pydantic.BaseModel):
#     title: str
#     subtitle: str
#     function: str
#     resolution: str
#     geographic_coverage: str
#     source: str
#     update_frequency: str


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

class GeostoreView(pydantic.BaseModel):
    view_link: str

class IntegratedAlert(pydantic.BaseModel):
    latitude: float
    longitude: float
    confidence_label: str = pydantic.Field(
        ..., alias="gfw_integrated_alerts__confidence"
    )
    confidence: float = 0.0
    recorded_at: datetime = pydantic.Field(..., alias="gfw_integrated_alerts__date")
    intensity: float = pydantic.Field(0.0, alias="gfw_integrated_alerts__intensity")

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

class NasaViirsFireAlert(pydantic.BaseModel):
    latitude: float
    longitude: float
    confidence: str = pydantic.Field(..., alias="confidence__cat")

    alert_date: datetime = pydantic.Field(..., alias="alert__date")

    @pydantic.validator("alert_date", pre=True)
    def sanitized_date(cls, val) -> datetime:
        return datetime.strptime(val, "%Y-%m-%d").replace(tzinfo=timezone.utc)


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

    @pydantic.validator("gfw_geostore_id")
    def simply_geostore_id(val):
        # when we get a Geostore ID back, it's in standard form, but 
        # when using it as a query parameter, it needs to in compact form.
        return val.replace('-', '').lower() if val else val


class GeoStoreResponse(pydantic.BaseModel):
    data: CreatedGeostore
    status: str

class DatasetMetadata(pydantic.BaseModel):
    created_on: Optional[datetime] = None
    updated_on: Optional[datetime] = None
    resolution: Optional[int] = None
    geographic_coverage: Optional[str] = None
    update_frequency: Optional[str] = None
    scale: Optional[str] = None
    citation: Optional[str] = None
    title: Optional[str] = None
    source: Optional[str] = None
    license: Optional[str] = None
    data_language: Optional[str] = None
    overview: Optional[str] = None
    function: Optional[str] = None
    cautions: Optional[str] = None
    key_restrictions: Optional[str] = None
    tags: Optional[List[str]] = pydantic.Field(default_factory=list)
    why_added: Optional[Any] = None
    learn_more: Optional[Any] = None
    id: Optional[str] = None

    @pydantic.validator("created_on", "updated_on")
    def clean_timestamp(val):
        return val if val.tzinfo else val.replace(tzinfo=timezone.utc)


class Dataset(pydantic.BaseModel):
    created_on: datetime
    updated_on: datetime
    dataset: str
    is_downloadable: bool
    metadata: DatasetMetadata
    versions: Optional[List[str]] = pydantic.Field(default_factory=list)

    @pydantic.validator("created_on", "updated_on")
    def clean_timestamp(val):
        return val if val.tzinfo else val.replace(tzinfo=timezone.utc)


class DatasetsResponse(pydantic.BaseModel):
    data: List[Dataset] = []
    status: Optional[str] = None

class DatasetResponse(pydantic.BaseModel):
    data: Optional[Dataset] = None
    status: Optional[str] = None

class DatasetField(pydantic.BaseModel):
    name: str
    alias: str
    description: Any
    data_type: str
    unit: Any
    is_feature_info: bool
    is_filter: bool


class DatasetFields(pydantic.BaseModel):
    data: Optional[List[DatasetField]] = None
    status: Optional[str] = None


def giveup_handler(details):
    d1, d2 = details['kwargs']['daterange']
    d1 = d1.strftime("%Y-%m-%d")
    d2 = d2.strftime("%Y-%m-%d")
    logger.error(f"Failed to get alerts for dataset: {details['kwargs']['dataset']}, geostore_id:{details['kwargs']['geostore_id']}, daterange: ({d1} - {d2})")

def backoff_hdlr(details):
    logger.warning("Backing off {wait:0.1f} seconds after {tries} tries "
           "calling function {target} with args {args} and kwargs "
           "{kwargs}".format(**details))
    

DEFAULT_REQUEST_TIMEOUT = httpx.Timeout(60.0, connect=3.1)
class DataAPI:

    DATA_API_URL = "https://data-api.globalforestwatch.org"
    RESOURCE_WATCH_URL = "https://api.resourcewatch.org"
    
    def __init__(self, *, username: str = None, password: str = None):

        self._username = username
        self._password = password
        self._auth_gen = None
        self._api_keys = []

    @backoff.on_exception(backoff.expo, (httpx.TimeoutException, httpx.HTTPStatusError), max_tries=3)
    async def get_access_token(self):

        async with httpx.AsyncClient(timeout=DEFAULT_REQUEST_TIMEOUT) as client:
            try:
                response = await client.post(
                    url=f"{self.DATA_API_URL}/auth/token",
                    data={"username": self._username, "password": self._password},
                    follow_redirects=True
                )
            except Exception as e:
                logger.exception(f"Failed to get an access token for username {self._username}. {e}")
            else:
                if httpx.codes.is_success(response.status_code):
                    dapitoken = DataAPIToken.parse_obj(response.json()["data"])
                    return dapitoken

            raise DataAPIAuthException("Failed to get an access token for username {self._username}.")

    async def auth_generator(self):
        """
        Simple generator to provide a header and keep it for a designated TTL.
        """
        expire_at = datetime(1970, 1, 1, tzinfo=timezone.utc)

        while True:
            present = datetime.now(tz=timezone.utc)
            try:
                if expire_at <= present:
                    token = await self.get_access_token()

                    ttl_seconds = token.expires_in - 5
                    expire_at = present + timedelta(seconds=ttl_seconds)
                if logger.isEnabledFor(logging.DEBUG):
                    ttl = (expire_at - present).total_seconds()
                    logger.debug(f"Using cached auth, expires in {ttl} seconds.")

            except DataAPIAuthException as e:
                logger.exception(f"Failed to authenticate with GFW Data API: {e}")
                raise e
            else:
                yield token

    async def get_auth_header(self, refresh=False) -> Dict[str, str]:
        if not self._auth_gen or refresh:
            self._auth_gen = self.auth_generator()
        try:
            token = await anext(self._auth_gen)
        except StopIteration:
            self._auth_gen = self.auth_generator()
            token = await anext(self._auth_gen)
        return {"authorization": f"{token.token_type} {token.access_token}"}

    @backoff.on_exception(backoff.expo, (httpx.TimeoutException, httpx.HTTPStatusError), max_tries=3)
    async def create_api_key(self):

        headers = await self.get_auth_header()

        payload = {
            "alias": "-".join((self._username, random_string())),
            "email": self._username,
            "organization": "EarthRanger",
            "domains": [],
        }

        async with httpx.AsyncClient(timeout=DEFAULT_REQUEST_TIMEOUT) as client:
            response = await client.post(
                url=f"{self.DATA_API_URL}/auth/apikey",
                headers=headers,
                json=payload,
                follow_redirects=True
            )

            if httpx.codes.is_success(response.status_code):
                return DataAPIKeyResponse.parse_obj(response.json())

    @backoff.on_exception(backoff.expo, (httpx.TimeoutException, httpx.HTTPStatusError), max_tries=3)
    async def get_api_keys(self) -> List[DataAPIKey]:

        if not self._api_keys:

            headers = await self.get_auth_header()

            for _ in range(0, 2):
                async with httpx.AsyncClient(timeout=DEFAULT_REQUEST_TIMEOUT) as client:
                    response = await client.get(
                        f"{self.DATA_API_URL}/auth/apikeys", headers=headers,
                        follow_redirects=True
                    )

                    if httpx.codes.is_success(response.status_code):
                        data = DataAPIKeysResponse.parse_obj(response.json())
                        if data.data:
                            self._api_keys = data.data
                            break
                    # Assume we need to create an API key.
                    data = await self.create_api_key()

        return self._api_keys

    @backoff.on_exception(backoff.constant, httpx.HTTPError, max_tries=3, interval=10)
    async def get_aoi(self, aoi_id: str):
        async with httpx.AsyncClient(timeout=DEFAULT_REQUEST_TIMEOUT) as client:
            response = await client.get(
                url=f"{self.RESOURCE_WATCH_URL}/v2/area/{aoi_id}",
                headers=await self.get_auth_header()
,
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

            raise GFWClientException(f"Failed to get AOI for id: '{aoi_id}'")


    @backoff.on_exception(backoff.constant, httpx.HTTPError, max_tries=3, interval=10)
    async def get_geostore(self, geostore_id=None):
        async with httpx.AsyncClient(timeout=DEFAULT_REQUEST_TIMEOUT) as client:
            response = await client.get(
                url=f"{self.RESOURCE_WATCH_URL}/v2/geostore/{geostore_id}",
                follow_redirects=True,
                headers=await self.get_auth_header()
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
        

    @backoff.on_exception(backoff.expo, (httpx.TimeoutException, httpx.HTTPStatusError), max_tries=3)
    async def create_geostore(self, geometry) -> CreatedGeostore:
        
        headers = await self.get_auth_header()

        async with httpx.AsyncClient(timeout=DEFAULT_REQUEST_TIMEOUT) as client:
            response = await client.post(
                # url=f"{integration.base_url}/geostore",
                url=f"{self.DATA_API_URL}/geostore/",
                headers=headers,
                json={'geometry': geometry},
                follow_redirects=True
            )
            response.raise_for_status()
            val = response.json()

            val = GeoStoreResponse.parse_obj(val)
            if val.status == 'success':
                return val.data

    @backoff.on_exception(backoff.constant, (httpx.TimeoutException, httpx.HTTPStatusError),
                          interval=5, max_tries=3, 
                          on_giveup=giveup_handler, raise_on_giveup=False,
                          on_backoff=backoff_hdlr)
    async def get_alerts(
        self,
        *,
        dataset: str,
        fields: Set[str],
        date_field: str,
        daterange: Tuple[datetime, datetime],
        extra_where: str = "",
        geostore_id: str
    ):

        api_keys = await self.get_api_keys()
        headers = {"x-api-key": api_keys[0].api_key}

        fields = {"latitude", "longitude"} | fields or set()

        lower_bound, upper_bound = daterange

        lower_date = lower_bound.strftime("%Y-%m-%d")
        upper_date = upper_bound.strftime("%Y-%m-%d")

        sql_query = f"SELECT {','.join(fields)} FROM results WHERE ({date_field} >= '{lower_date}' AND {date_field} <= '{upper_date}')"
        if extra_where:
            sql_query += f" AND {extra_where}"

        logger.debug(f"Querying dataset with sql: {sql_query}")
        payload = {
            'geostore_id': geostore_id,
            'sql': sql_query
        }

        async def fn():
            async with httpx.AsyncClient(timeout=DEFAULT_REQUEST_TIMEOUT) as client:

                response = await client.get(f"{self.DATA_API_URL}/dataset/{dataset}/latest/query/json",
                    headers=headers,
                    params=payload,
                    follow_redirects=True
                )

                if httpx.codes.is_success(response.status_code):
                    data = response.json()
                    data_len = len(data.get("data"))
                    logger.info(f"Extracted {data_len} alerts from dataset {dataset} for period {lower_date} - {upper_date}.")
                    return data.get("data", [])
                else:
                    logger.error(
                        f"Failed getting data for dataset {dataset}. status: {response.status_code}, text: {response.text}",
                        extra=payload,
                    )
                    response.raise_for_status() 

        return await fn()

    @backoff.on_exception(backoff.expo, (httpx.TimeoutException, httpx.HTTPStatusError), max_tries=3, on_backoff=backoff_hdlr)
    async def get_gfw_integrated_alerts(self, *, geostore_id: str,date_range: Tuple[datetime, datetime], semaphore: asyncio.Semaphore = None):

        async with semaphore:
            fields = {"gfw_integrated_alerts__date", "gfw_integrated_alerts__confidence"}
            alerts = await self.get_alerts(
                dataset="gfw_integrated_alerts",
                date_field="gfw_integrated_alerts__date",
                daterange=date_range,
                fields=fields,
                extra_where="(gfw_integrated_alerts__confidence = 'highest')",
                geostore_id=geostore_id
            )
        
        return [IntegratedAlert.parse_obj(alert) for alert in alerts] if alerts else []
        
    @backoff.on_exception(backoff.expo, (httpx.TimeoutException, httpx.HTTPStatusError), max_tries=3, on_backoff=backoff_hdlr)
    async def get_nasa_viirs_fire_alerts(self, *, geostore_id: str,date_range: Tuple[datetime, datetime], semaphore: asyncio.Semaphore = None):

        async with semaphore:
            fields = {"confidence__cat", "alert__date"}
            alerts = await self.get_alerts(
                dataset="nasa_viirs_fire_alerts",
                date_field="alert__date",
                daterange=date_range,
                fields=fields,
                extra_where="(confidence__cat = 'h')",
                geostore_id=geostore_id
            )
        
        return [NasaViirsFireAlert.parse_obj(alert) for alert in alerts] if alerts else []
        

    @backoff.on_exception(backoff.expo, (httpx.TimeoutException, httpx.HTTPStatusError), max_tries=3)
    async def get_dataset_metadata(self, dataset: str = "", version: str = "latest"):

        api_keys = await self.get_api_keys()
        headers = {"x-api-key": api_keys[0].api_key}

        async with httpx.AsyncClient(timeout=DEFAULT_REQUEST_TIMEOUT) as client:
            response = await client.get(
                f"{self.DATA_API_URL}/dataset/{dataset}/{version}",
                headers=headers,
                follow_redirects=True
            )

            if httpx.codes.is_success(response.status_code):
                data = response.json()
                return DatasetResponseItem.parse_obj(data.get("data"))


    @backoff.on_exception(backoff.constant, httpx.HTTPError, max_tries=3, interval=10)
    async def aoi_from_url(self, url) -> str:
        """
        Extracts the AOI ID from a GFW share link URL.
        """

        URL_PATTERN = ".*globalforestwatch.org.*aoi/([^/]+).*"
        if matches := re.match(URL_PATTERN, url):
            return matches[1]
        
        async with httpx.AsyncClient(timeout=DEFAULT_REQUEST_TIMEOUT) as client:
            head = await client.head(url, follow_redirects=True)

        try:
            matches = re.match(URL_PATTERN, str(head.url))
            return matches[1]
        except IndexError:
            logger.error("Unable to parse AOI from globalforestwatch URL: %s", url)


    @backoff.on_exception(backoff.constant, httpx.HTTPError, max_tries=3, interval=10)
    async def get_datasets(self):
        async with httpx.AsyncClient(timeout=DEFAULT_REQUEST_TIMEOUT) as client:
            response = await client.get(
                f"{self.DATA_API_URL}/datasets",
                follow_redirects=True
            )
            response.raise_for_status()
            content = response.json()
            datasets_response = DatasetsResponse.parse_obj(content)
            return datasets_response.data
        
    @backoff.on_exception(backoff.constant, httpx.HTTPError, max_tries=3, interval=10)
    async def get_dataset_fields(self, *, dataset:str, version:str="latest"):
        async with httpx.AsyncClient(timeout=DEFAULT_REQUEST_TIMEOUT) as client:
            response = await client.get(
                f"{self.DATA_API_URL}/dataset/{dataset}/{version}/fields",
                follow_redirects=True
            )
            response.raise_for_status()
            content = response.json()
            fields_response = DatasetFields.parse_obj(content)

            return fields_response.data