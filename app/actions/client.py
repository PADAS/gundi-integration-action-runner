"""HTTP client for the OlmoEarth prediction API.

Two endpoints carry this integration:

  POST /api/v1/predictions/search
      Finds prediction runs — filtered by model, project, organization and
      creation time — newest first.

  POST /api/v1/prediction-results/{prediction_result_id}/features/search
      Returns the GeoJSON features a prediction produced, filtered by
      geometry, observation window and arbitrary feature properties.

Every filter the API accepts is an object of comparison operators
(``{"eq": ...}``, ``{"gte": ...}``) and every operator is optional. The models
below mirror that shape, and requests are serialized with ``exclude_none=True``
so an operator we did not set is absent from the body rather than sent as an
empty string: the documented sample shows ``{"eq": ""}``, but sent literally
that asks for records whose field equals the empty string, which matches
nothing.
"""
import logging
from datetime import datetime
from typing import Any, AsyncIterator, Dict, List, Optional

import httpx
import pydantic
import stamina

from app.services.errors import (
    IntegrationAuthError,
    IntegrationBadResponseError,
    IntegrationConfigurationError,
    IntegrationConnectionError,
    IntegrationRateLimitError,
)
from app.services.utils import find_config_for_action

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = httpx.Timeout(60.0, connect=10.0)

# The API caps a page at some size we don't know yet; 50 is what the documented
# sample sends for features and 100 for predictions. Stay at those defaults.
DEFAULT_FEATURE_PAGE_SIZE = 50
DEFAULT_PREDICTION_PAGE_SIZE = 100

# A runaway query (an AOI covering a continent, a model that emits millions of
# detections) would otherwise page forever and blow the action's memory. Every
# paging helper stops here and says so.
MAX_PAGES = 200


# --------------------------------------------------------------------------
# Filter primitives
# --------------------------------------------------------------------------
class _Filter(pydantic.BaseModel):
    """Base for the operator objects. Unset operators are never serialized."""

    class Config:
        extra = "forbid"

    def is_empty(self) -> bool:
        return not self.dict(exclude_none=True)


class KeywordFilter(_Filter):
    """Exact-match filter over an identifier-like field."""

    eq: Optional[str] = None
    neq: Optional[str] = None
    inc: Optional[List[str]] = None
    ninc: Optional[List[str]] = None
    exists: Optional[bool] = None


class OrganizationFilter(_Filter):
    """Organization accepts only `eq` and `inc` (see the documented schema)."""

    eq: Optional[str] = None
    inc: Optional[List[str]] = None


class StringFilter(_Filter):
    """Keyword operators plus SQL-style `like` matching."""

    eq: Optional[str] = None
    neq: Optional[str] = None
    inc: Optional[List[str]] = None
    ninc: Optional[List[str]] = None
    like: Optional[str] = None
    nlike: Optional[str] = None


class NumericFilter(_Filter):
    eq: Optional[float] = None
    neq: Optional[float] = None
    gte: Optional[float] = None
    gt: Optional[float] = None
    lte: Optional[float] = None
    lt: Optional[float] = None


class DatetimeFilter(_Filter):
    gte: Optional[datetime] = None
    gt: Optional[datetime] = None
    lte: Optional[datetime] = None
    lt: Optional[datetime] = None
    exists: Optional[bool] = None


class PropertyFilter(pydantic.BaseModel):
    """A filter on one arbitrary feature property.

    The model that produced the features decides what properties exist, so
    these are the escape hatch for model-specific thresholds — the common case
    being a confidence score: ``{"property_name": "confidence",
    "numeric_filter": {"gte": 0.8}}``.
    """

    property_name: str
    keyword_filter: Optional[KeywordFilter] = None
    string_filter: Optional[StringFilter] = None
    numeric_filter: Optional[NumericFilter] = None
    datetime_filter: Optional[DatetimeFilter] = None


class Geometry(pydantic.BaseModel):
    """A GeoJSON geometry, kept loose on purpose.

    `coordinates` is nested to a depth that depends on `type`, so it is typed
    as Any rather than reproducing the GeoJSON grammar. Anything we send came
    from an operator's AOI; anything we receive came from the API.
    """

    type: str
    coordinates: Any = None
    bbox: Optional[List[float]] = None
    # A GeometryCollection carries `geometries` instead of `coordinates`.
    geometries: Optional[List[Dict[str, Any]]] = None

    class Config:
        extra = "allow"


# --------------------------------------------------------------------------
# Requests
# --------------------------------------------------------------------------
class _SearchRequest(pydantic.BaseModel):
    class Config:
        extra = "forbid"
        json_encoders = {datetime: lambda dt: dt.isoformat()}

    def as_body(self) -> dict:
        """The request body with every unset operator dropped.

        Goes through ``json()`` rather than ``dict()`` so datetimes are ISO
        strings and the body is JSON-serializable as-is.
        """
        import json

        return json.loads(self.json(exclude_none=True, exclude_defaults=False))


class PredictionSearchRequest(_SearchRequest):
    sort_by: str = "creation_time"
    sort_direction: str = "desc"
    limit: int = DEFAULT_PREDICTION_PAGE_SIZE
    offset: int = 0
    id: Optional[KeywordFilter] = None
    organization_id: Optional[OrganizationFilter] = None
    project_id: Optional[KeywordFilter] = None
    model_id: Optional[KeywordFilter] = None
    requester_id: Optional[KeywordFilter] = None
    name: Optional[StringFilter] = None
    status: Optional[KeywordFilter] = None
    workflow_type: Optional[KeywordFilter] = None
    creation_time: Optional[DatetimeFilter] = None
    deleted_time: Optional[DatetimeFilter] = None


class FeatureSearchRequest(_SearchRequest):
    limit: int = DEFAULT_FEATURE_PAGE_SIZE
    offset: int = 0
    sort_by: str = "oe_created_at"
    sort_direction: str = "desc"
    id: Optional[KeywordFilter] = None
    intersects_geometry: Optional[Geometry] = None
    oe_prediction_result_id: Optional[KeywordFilter] = None
    oe_prediction_result_file_id: Optional[KeywordFilter] = None
    oe_start_time: Optional[DatetimeFilter] = None
    oe_end_time: Optional[DatetimeFilter] = None
    oe_created_at: Optional[DatetimeFilter] = None
    property_filters: Optional[List[PropertyFilter]] = None


# --------------------------------------------------------------------------
# Responses
# --------------------------------------------------------------------------
class SearchMeta(pydantic.BaseModel):
    total: Optional[int] = None

    class Config:
        extra = "allow"


class ApiError(pydantic.BaseModel):
    code: Optional[str] = None
    message: Optional[str] = None

    class Config:
        extra = "allow"


class Prediction(pydantic.BaseModel):
    """One prediction run.

    Only `id` is required. The rest is what the documented filters imply the
    record carries; `extra = "allow"` keeps every field we did not model, which
    is what `result_ids_for` reads to find the prediction's result ids.
    """

    id: str
    name: Optional[str] = None
    status: Optional[str] = None
    model_id: Optional[str] = None
    project_id: Optional[str] = None
    organization_id: Optional[str] = None
    requester_id: Optional[str] = None
    workflow_type: Optional[str] = None
    creation_time: Optional[datetime] = None
    deleted_time: Optional[datetime] = None

    class Config:
        extra = "allow"


class FeatureProperties(pydantic.BaseModel):
    """The `oe_`-prefixed properties every feature carries, plus the model's own.

    The model-authored properties are the point of the record and vary per
    model, so they arrive as extras and are read back with `model_properties()`.
    """

    oe_start_time: Optional[datetime] = None
    oe_end_time: Optional[datetime] = None
    oe_created_at: Optional[datetime] = None
    oe_prediction_result_id: Optional[str] = None
    oe_prediction_result_file_id: Optional[str] = None

    class Config:
        extra = "allow"

    def model_properties(self) -> Dict[str, Any]:
        """Everything the model itself put on the feature, without the `oe_`
        system fields — those are provenance, and the handler reports them
        separately rather than burying them among the model's own outputs."""
        return {
            name: value
            for name, value in self.dict(exclude_none=True).items()
            if not name.startswith("oe_")
        }


class Feature(pydantic.BaseModel):
    """A GeoJSON Feature as returned by the features search.

    `id` is normalized to a string: the documented response returns an integer
    (`"id": 1`) while the documented request filters the same field as a
    keyword (`{"eq": ""}`). Picking one here means a feature's identity does not
    change type depending on which end of the API it came from — which matters,
    because it ends up in the event's `external_source_id`.
    """

    id: Optional[str] = None
    type: str = "Feature"
    geometry: Optional[Geometry] = None
    bbox: Optional[List[float]] = None
    properties: FeatureProperties = pydantic.Field(default_factory=FeatureProperties)

    class Config:
        extra = "allow"


class PredictionSearchResponse(pydantic.BaseModel):
    records: List[Prediction] = pydantic.Field(default_factory=list)
    meta: SearchMeta = pydantic.Field(default_factory=SearchMeta)
    errors: List[ApiError] = pydantic.Field(default_factory=list)

    class Config:
        extra = "allow"


class FeatureSearchResponse(pydantic.BaseModel):
    records: List[Feature] = pydantic.Field(default_factory=list)
    meta: SearchMeta = pydantic.Field(default_factory=SearchMeta)
    errors: List[ApiError] = pydantic.Field(default_factory=list)

    class Config:
        extra = "allow"


# --------------------------------------------------------------------------
# Resolving a prediction's result ids
# --------------------------------------------------------------------------
# Keys the prediction record might use to name its results. The predictions
# search response schema was not part of the API sample this connector was
# written from, so we probe rather than assume.
_RESULT_ID_KEYS = ("prediction_result_ids", "result_ids", "prediction_results", "results")


def result_ids_for(prediction: Prediction) -> List[str]:
    """The prediction-result ids whose features belong to `prediction`.

    The features endpoint is keyed by `prediction_result_id`, but the only
    documented way to *find* work is the predictions search, which returns
    predictions. Nothing in the documented sample links the two, so this is the
    one seam in the connector that is a guess:

      1. If the record names its results under any of `_RESULT_ID_KEYS` —
         either as bare ids or as objects with an `id` — use those.
      2. Otherwise fall back to the prediction's own id, which is correct if
         the two identifiers are the same value.

    TODO(confirm with the OlmoEarth team): whether a prediction has one result
    or many, and which field names them. When the answer arrives, this function
    is the only thing that changes — and if it turns out to need a third
    request (e.g. `GET /predictions/{id}/results`), it becomes a client method
    and `_collect_result_ids` in handlers.py awaits it instead.
    """
    extras = prediction.dict(exclude_none=True)
    for key in _RESULT_ID_KEYS:
        value = extras.get(key)
        if not value:
            continue
        if isinstance(value, str):
            return [value]
        if isinstance(value, list):
            ids = []
            for item in value:
                if isinstance(item, str):
                    ids.append(item)
                elif isinstance(item, dict) and item.get("id"):
                    ids.append(str(item["id"]))
            if ids:
                return ids
    logger.warning(
        "Prediction %s names no prediction-result ids under any of %s; assuming the "
        "prediction id is also its result id. If the features search 404s, this "
        "assumption is what is wrong.",
        prediction.id,
        ", ".join(_RESULT_ID_KEYS),
    )
    return [prediction.id]


# --------------------------------------------------------------------------
# Client
# --------------------------------------------------------------------------
class OlmoEarthClient:
    """Thin async wrapper over the two search endpoints.

    Use as an async context manager so the underlying connection pool is closed
    even when an action raises partway through paging.
    """

    def __init__(
        self,
        base_url: str,
        api_token: str,
        timeout: httpx.Timeout = DEFAULT_TIMEOUT,
        transport: Optional[httpx.AsyncBaseTransport] = None,
    ):
        if not base_url:
            raise IntegrationConfigurationError(
                "No base URL is set for this integration; set the site URL to the "
                "OlmoEarth API root."
            )
        if not api_token:
            raise IntegrationConfigurationError(
                "No API token is configured; run the Authenticate action first."
            )
        self.base_url = base_url.rstrip("/")
        self._api_token = api_token
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=timeout,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": f"Bearer {api_token}",
            },
            # Tests inject an httpx.MockTransport here; production passes none
            # and httpx builds its own.
            transport=transport,
        )

    async def __aenter__(self) -> "OlmoEarthClient":
        return self

    async def __aexit__(self, *exc_info) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._client.aclose()

    # -- transport ---------------------------------------------------------
    async def _post(self, path: str, body: dict) -> dict:
        """POST a search body and return the decoded response.

        Retries transport failures, 429 and 5xx; a 4xx is a definite answer and
        fails immediately. Failures come back as `IntegrationError` subclasses
        so the runner records a classified status on the integration rather
        than an opaque traceback.
        """
        async for attempt in stamina.retry_context(
            on=_is_transient,
            attempts=3,
            wait_initial=2.0,
            wait_max=20.0,
            wait_jitter=2.0,
        ):
            with attempt:
                try:
                    response = await self._client.post(path, json=body)
                    response.raise_for_status()
                except httpx.HTTPStatusError as e:
                    raise _classify_status_error(e) from e
                except httpx.HTTPError as e:
                    raise IntegrationConnectionError(
                        f"Could not reach the OlmoEarth API at {self.base_url}: {e}"
                    ) from e
        try:
            return response.json()
        except ValueError as e:
            raise IntegrationBadResponseError(
                f"OlmoEarth returned a non-JSON response from {path}."
            ) from e

    @staticmethod
    def _parse(model, payload: dict, path: str):
        try:
            parsed = model.parse_obj(payload)
        except pydantic.ValidationError as e:
            raise IntegrationBadResponseError(
                f"OlmoEarth returned an unexpected response shape from {path}: {e}"
            ) from e
        # A 200 can still carry errors alongside records. Records present means
        # a partial result worth keeping; an error with nothing to show for it
        # is a failure.
        if parsed.errors and not parsed.records:
            detail = "; ".join(
                filter(None, (f"{e.code or 'error'}: {e.message or ''}".strip(" :") for e in parsed.errors))
            )
            raise IntegrationBadResponseError(f"OlmoEarth rejected the search: {detail}")
        if parsed.errors:
            logger.warning(
                "OlmoEarth returned %s error(s) alongside %s record(s) from %s: %s",
                len(parsed.errors), len(parsed.records), path,
                [e.dict(exclude_none=True) for e in parsed.errors],
            )
        return parsed

    # -- endpoints ---------------------------------------------------------
    async def search_predictions(self, request: PredictionSearchRequest) -> PredictionSearchResponse:
        path = "/api/v1/predictions/search"
        payload = await self._post(path, request.as_body())
        return self._parse(PredictionSearchResponse, payload, path)

    async def search_features(
        self, prediction_result_id: str, request: FeatureSearchRequest
    ) -> FeatureSearchResponse:
        path = f"/api/v1/prediction-results/{prediction_result_id}/features/search"
        payload = await self._post(path, request.as_body())
        return self._parse(FeatureSearchResponse, payload, path)

    # -- paging ------------------------------------------------------------
    async def iter_features(
        self, prediction_result_id: str, request: FeatureSearchRequest, max_features: Optional[int] = None
    ) -> AsyncIterator[Feature]:
        """Yield every feature matching `request`, a page at a time.

        Paging is by offset, so it sorts ascending by `oe_created_at`: with the
        `desc` default a feature created between two requests shifts every
        later record one slot forward and the walk skips one. Ascending order
        only ever appends beyond the window already read.
        """
        request = request.copy(deep=True)
        request.sort_by = "oe_created_at"
        request.sort_direction = "asc"
        request.offset = request.offset or 0
        yielded = 0
        for page in range(MAX_PAGES):
            response = await self.search_features(prediction_result_id, request)
            if not response.records:
                return
            for feature in response.records:
                yield feature
                yielded += 1
                if max_features is not None and yielded >= max_features:
                    logger.info(
                        "Stopped at the %s-feature cap for prediction result %s; "
                        "the provider reported %s in total.",
                        max_features, prediction_result_id, response.meta.total,
                    )
                    return
            # A short page is the last page. `meta.total` is a second, cheaper
            # stop for a provider that always fills the page.
            if len(response.records) < request.limit:
                return
            request.offset += request.limit
            if response.meta.total is not None and request.offset >= response.meta.total:
                return
        logger.warning(
            "Stopped paging features for prediction result %s after %s pages "
            "(%s features). Narrow the query — by area, time window or a property "
            "filter — to see the rest.",
            prediction_result_id, MAX_PAGES, yielded,
        )


def _is_transient(exc: BaseException) -> bool:
    """Retry predicate: transport failures, 429 and 5xx are worth another try."""
    if isinstance(exc, (IntegrationConnectionError, IntegrationRateLimitError)):
        return True
    if isinstance(exc, IntegrationBadResponseError):
        return (getattr(exc, "status_code", None) or 0) >= 500
    return False


def _classify_status_error(exc: httpx.HTTPStatusError):
    """Turn an HTTP status into the runner's classified error for that status."""
    status = exc.response.status_code
    detail = _response_detail(exc.response)
    if status in (401, 403):
        return IntegrationAuthError(
            f"OlmoEarth rejected the API token ({status}). {detail}".strip(), status_code=status
        )
    if status == 429:
        return IntegrationRateLimitError(
            f"OlmoEarth rate-limited the request. {detail}".strip(), status_code=status
        )
    if status == 404:
        return IntegrationBadResponseError(
            f"OlmoEarth has no record at {exc.request.url.path}. {detail}".strip(), status_code=status
        )
    return IntegrationBadResponseError(
        f"OlmoEarth returned {status} for {exc.request.url.path}. {detail}".strip(), status_code=status
    )


def _response_detail(response: httpx.Response, limit: int = 500) -> str:
    """A short, safe excerpt of an error body for the activity log."""
    try:
        text = response.text or ""
    except Exception:  # a streamed or already-closed body
        return ""
    text = " ".join(text.split())
    return text[:limit] + ("…" if len(text) > limit else "")
