"""Action handlers for the OlmoEarth integration.

The ingest is two searches deep, because the API splits "what did the model
run on?" from "what did it find?":

    predictions/search                      -> the prediction runs for a model
      -> (a prediction's result ids)        -> see client.result_ids_for
        -> prediction-results/{id}/features/search  -> the GeoJSON detections
          -> Gundi events

A run is incremental at the *prediction* level rather than the feature level: a
prediction is the unit the provider publishes, so the watermark records which
predictions have been ingested and each new one is drained in full. That also
sidesteps the trap in a feature-level watermark — a bulk-inserted result gives
thousands of features the same `oe_created_at`, so a `>` cursor on that field
drops whatever shares the boundary second.
"""
import datetime
import logging
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pydantic

import app.actions.client as client
import app.services.gundi as gundi_tools
from app.actions.core import ReferenceDataResponse, ReferenceOption, action_title
from app.services.action_scheduler import crontab_schedule
from app.services.activity_logger import activity_logger, log_action_activity
from app.services.errors import IntegrationConfigurationError
from app.services.state import IntegrationStateManager
from app.services.utils import find_config_for_action

from .configurations import (
    AuthenticateConfig,
    ListPredictionsConfig,
    PredictionSelection,
    PullEventsConfig,
)

logger = logging.getLogger(__name__)

state_manager = IntegrationStateManager()

PULL_EVENTS_ACTION_ID = "pull_events"

# How many recently-ingested prediction ids to remember. The prediction search
# filters `creation_time >= watermark` (not `>`), so predictions sharing the
# watermark second come back every run; this list is what recognises them.
# Bounded so the state row cannot grow without limit.
PROCESSED_ID_MEMORY = 200


# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------
def get_auth_config(integration) -> AuthenticateConfig:
    auth_config = find_config_for_action(integration.configurations, "auth")
    if not auth_config:
        raise IntegrationConfigurationError(
            "This integration has no authentication configured. Add the OlmoEarth "
            "API token in the Authenticate action."
        )
    return AuthenticateConfig.parse_obj(auth_config.data)


def client_for(integration, auth_config: Optional[AuthenticateConfig] = None) -> client.OlmoEarthClient:
    """An API client bound to this integration's site URL and token."""
    auth_config = auth_config or get_auth_config(integration)
    return client.OlmoEarthClient(
        base_url=integration.base_url,
        api_token=auth_config.api_token.get_secret_value(),
    )


# --------------------------------------------------------------------------
# Query construction
# --------------------------------------------------------------------------
def build_prediction_query(
    config: PullEventsConfig, since: datetime.datetime, limit: Optional[int] = None
) -> client.PredictionSearchRequest:
    """The predictions search for this run.

    `creation_time` is bounded with `gte`, not `gt`: a prediction created in the
    same second as the watermark would otherwise be skipped. Re-seeing the
    boundary prediction is handled by the processed-id list instead.
    """
    request = client.PredictionSearchRequest(
        sort_by="creation_time",
        sort_direction="desc",
        limit=limit or client.DEFAULT_PREDICTION_PAGE_SIZE,
        model_id=client.KeywordFilter(eq=config.model_id),
        creation_time=client.DatetimeFilter(gte=since),
    )
    if config.project_id:
        request.project_id = client.KeywordFilter(eq=config.project_id)
    if config.organization_id:
        request.organization_id = client.OrganizationFilter(eq=config.organization_id)
    if config.prediction_status:
        request.status = client.KeywordFilter(eq=config.prediction_status)
    # TODO(area filtering): the predictions search is gaining geometry and time
    # filters. Once it has them, `config.area_of_interest` belongs here too, so
    # a prediction covering no part of the AOI is never opened at all. Until
    # then the AOI is applied one level down, on the features search, which
    # gives the same events for one extra round trip per prediction.
    return request


def build_feature_query(config: PullEventsConfig) -> client.FeatureSearchRequest:
    """The features search shared by every prediction result in this run.

    Sort order and offset are set by `iter_features`, which owns the paging.
    """
    request = client.FeatureSearchRequest(limit=config.feature_page_size)
    if config.area_of_interest:
        try:
            request.intersects_geometry = client.Geometry.parse_obj(config.area_of_interest)
        except pydantic.ValidationError as e:
            raise IntegrationConfigurationError(
                "The configured area of interest is not a valid GeoJSON geometry; "
                "it needs a `type` and matching `coordinates`."
            ) from e
    if config.min_confidence is not None:
        request.property_filters = [
            client.PropertyFilter(
                property_name=config.confidence_property,
                numeric_filter=client.NumericFilter(gte=config.min_confidence),
            )
        ]
    return request


# --------------------------------------------------------------------------
# Geometry
# --------------------------------------------------------------------------
def _flatten_positions(coordinates: Any) -> Iterable[Tuple[float, float]]:
    """Yield every (lon, lat) position in an arbitrarily nested coordinate array."""
    if not isinstance(coordinates, (list, tuple)) or not coordinates:
        return
    first = coordinates[0]
    if isinstance(first, (int, float)) and len(coordinates) >= 2:
        yield float(coordinates[0]), float(coordinates[1])
        return
    for item in coordinates:
        for position in _flatten_positions(item):
            yield position


def centroid_of(feature: client.Feature) -> Optional[Dict[str, float]]:
    """A single lat/lon for a feature of any geometry type.

    Gundi's event location is a point, but a detection is often a polygon, so
    one has to be derived. The feature's own `bbox` is preferred — the provider
    computed it over the real geometry — and averaging the positions is the
    fallback. Neither is a true centroid for a concave shape; the full geometry
    travels alongside for anything that needs the real footprint.
    """
    bbox = feature.bbox or (feature.geometry.bbox if feature.geometry else None)
    if bbox and len(bbox) >= 4:
        min_lon, min_lat, max_lon, max_lat = bbox[0], bbox[1], bbox[2], bbox[3]
        return {"lat": (min_lat + max_lat) / 2, "lon": (min_lon + max_lon) / 2}
    if not feature.geometry:
        return None
    positions = list(_flatten_positions(feature.geometry.coordinates))
    if not positions:
        return None
    return {
        "lat": sum(lat for _, lat in positions) / len(positions),
        "lon": sum(lon for lon, _ in positions) / len(positions),
    }


# --------------------------------------------------------------------------
# Transformation
# --------------------------------------------------------------------------
def transform_feature(
    feature: client.Feature,
    prediction: client.Prediction,
    prediction_result_id: str,
    config: PullEventsConfig,
) -> Optional[dict]:
    """One OlmoEarth feature as one Gundi event, or None if it cannot be placed.

    `recorded_at` is the observation's own `oe_start_time` — when the model saw
    the thing — falling back to `oe_created_at`, when the record was written.
    They differ by however long the imagery took to process, which for a
    detection feed is the difference between a useful timestamp and a useless
    one.
    """
    location = centroid_of(feature)
    if not location:
        logger.warning(
            "Skipping feature %s from prediction result %s: it carries no geometry "
            "or bounding box, so it cannot be placed on a map.",
            feature.id, prediction_result_id,
        )
        return None

    properties = feature.properties
    recorded_at = properties.oe_start_time or properties.oe_created_at
    if not recorded_at:
        logger.warning(
            "Skipping feature %s from prediction result %s: it has neither "
            "oe_start_time nor oe_created_at, and an event needs a timestamp.",
            feature.id, prediction_result_id,
        )
        return None

    event_details = properties.model_properties()
    event_details.update(
        {
            "prediction_id": prediction.id,
            "prediction_result_id": properties.oe_prediction_result_id or prediction_result_id,
            "feature_id": feature.id,
        }
    )
    if properties.oe_prediction_result_file_id:
        event_details["prediction_result_file_id"] = properties.oe_prediction_result_file_id
    if prediction.model_id:
        event_details["model_id"] = prediction.model_id
    if prediction.name:
        event_details["prediction_name"] = prediction.name
    if properties.oe_end_time:
        event_details["observation_end_time"] = properties.oe_end_time.isoformat()
    if properties.oe_created_at:
        event_details["detected_at"] = properties.oe_created_at.isoformat()

    event = {
        "title": _event_title(feature, prediction, config),
        "event_type": config.event_type,
        "recorded_at": recorded_at.isoformat(),
        "location": location,
        "event_details": event_details,
        # Stable across runs, so a redelivered feature is recognisable as the
        # same detection rather than a second one.
        "external_source_id": external_id_for(feature, prediction_result_id),
    }
    if config.include_geometry and feature.geometry:
        event["geometry"] = feature.geometry.dict(exclude_none=True)
    return event


def external_id_for(feature: client.Feature, prediction_result_id: str) -> str:
    """A feature id is only unique within its prediction result, so qualify it."""
    return f"{prediction_result_id}:{feature.id}"


def _event_title(feature: client.Feature, prediction: client.Prediction, config: PullEventsConfig) -> str:
    label = prediction.name or prediction.model_id or "OlmoEarth"
    return f"{label} detection {feature.id}" if feature.id is not None else f"{label} detection"


# --------------------------------------------------------------------------
# State
# --------------------------------------------------------------------------
async def _load_watermark(integration_id: str) -> Tuple[Optional[datetime.datetime], List[str]]:
    state = await state_manager.get_state(integration_id, PULL_EVENTS_ACTION_ID) or {}
    last_seen = state.get("last_prediction_creation_time")
    parsed = None
    if last_seen:
        try:
            parsed = _parse_datetime(last_seen)
        except ValueError:
            logger.warning(
                "Ignoring unparseable watermark %r for integration %s; falling back "
                "to the configured lookback.",
                last_seen, integration_id,
            )
    return parsed, list(state.get("processed_prediction_ids") or [])


async def _save_watermark(
    integration_id: str,
    watermark: Optional[datetime.datetime],
    processed_ids: List[str],
) -> None:
    await state_manager.set_state(
        integration_id,
        PULL_EVENTS_ACTION_ID,
        {
            "last_prediction_creation_time": watermark.isoformat() if watermark else None,
            "processed_prediction_ids": processed_ids[:PROCESSED_ID_MEMORY],
            "updated_at": datetime.datetime.now(tz=datetime.timezone.utc).isoformat(),
        },
    )


def _parse_datetime(value: str) -> datetime.datetime:
    parsed = pydantic.datetime_parse.parse_datetime(value)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=datetime.timezone.utc)


def _as_utc(value: datetime.datetime) -> datetime.datetime:
    """Compare watermarks in one timezone; a naive provider timestamp is UTC."""
    return value if value.tzinfo else value.replace(tzinfo=datetime.timezone.utc)


# --------------------------------------------------------------------------
# Actions
# --------------------------------------------------------------------------
@action_title("Authenticate")
async def action_auth(integration, action_config: AuthenticateConfig) -> dict:
    """Check that the token and site URL actually reach the OlmoEarth API.

    The cheapest request that proves both is a predictions search asking for a
    single record: it exercises the same host, path prefix and bearer token the
    pull action uses, and an empty result is still a successful answer.
    """
    logger.info(f"Executing auth action for integration {integration.id}...")
    async with client_for(integration, action_config) as api:
        response = await api.search_predictions(
            client.PredictionSearchRequest(limit=1, sort_by="creation_time", sort_direction="desc")
        )
    return {
        "valid_credentials": True,
        "predictions_visible": response.meta.total,
    }


@action_title("List Predictions")
async def action_list_predictions(integration, action_config: ListPredictionsConfig) -> dict:
    """Reference lookup: the predictions the portal offers as a dropdown.

    Stateless by contract — the query arrives as config overrides when a user
    opens the dropdown and nothing is stored.
    """
    request = client.PredictionSearchRequest(
        limit=action_config.limit, sort_by="creation_time", sort_direction="desc"
    )
    if action_config.model_id:
        request.model_id = client.KeywordFilter(eq=action_config.model_id)
    if action_config.project_id:
        request.project_id = client.KeywordFilter(eq=action_config.project_id)
    if action_config.status:
        request.status = client.KeywordFilter(eq=action_config.status)

    async with client_for(integration) as api:
        response = await api.search_predictions(request)

    options = [
        ReferenceOption(
            value=prediction.id,
            label=prediction.name or prediction.id,
            description=_prediction_description(prediction),
            group=prediction.model_id,
        )
        for prediction in response.records
    ]
    total = response.meta.total
    return ReferenceDataResponse(
        options=options,
        truncated=bool(total is not None and total > len(options)),
    ).dict()


def _prediction_description(prediction: client.Prediction) -> Optional[str]:
    parts = []
    if prediction.status:
        parts.append(prediction.status)
    if prediction.creation_time:
        parts.append(f"created {prediction.creation_time.isoformat()}")
    return " · ".join(parts) or None


@crontab_schedule("0 */4 * * *")  # Every four hours
@activity_logger()
async def action_pull_events(integration, action_config: PullEventsConfig) -> dict:
    """Ingest the features of every prediction this integration has not seen."""
    integration_id = str(integration.id)
    logger.info(
        f"Executing pull_events for integration {integration_id}, "
        f"model {action_config.model_id}..."
    )

    watermark, processed_ids = await _load_watermark(integration_id)
    since = watermark or (
        datetime.datetime.now(tz=datetime.timezone.utc)
        - datetime.timedelta(days=action_config.lookback_days)
    )

    async with client_for(integration) as api:
        predictions = await _select_predictions(api, action_config, since, processed_ids)

        if not predictions:
            await log_action_activity(
                integration_id=integration_id,
                action_id=PULL_EVENTS_ACTION_ID,
                level="INFO",
                title="No new predictions to ingest.",
                data={"since": since.isoformat(), "model_id": action_config.model_id},
                config_data=action_config.dict(),
            )
            return {
                "predictions_processed": 0,
                "features_extracted": 0,
                "events_sent": 0,
                "since": since.isoformat(),
            }

        await log_action_activity(
            integration_id=integration_id,
            action_id=PULL_EVENTS_ACTION_ID,
            level="INFO",
            title=f"Ingesting {len(predictions)} new prediction(s).",
            data={
                "since": since.isoformat(),
                "prediction_ids": [p.id for p in predictions],
            },
            config_data=action_config.dict(),
        )

        totals = {"predictions_processed": 0, "features_extracted": 0, "events_sent": 0}
        # Predictions arrive newest-first; ingest oldest-first so the watermark
        # only ever moves forward and a failure halfway leaves a consistent one.
        for prediction in sorted(predictions, key=_creation_time_key):
            extracted, sent = await _ingest_prediction(api, integration, prediction, action_config)
            totals["predictions_processed"] += 1
            totals["features_extracted"] += extracted
            totals["events_sent"] += sent

            processed_ids = [prediction.id] + [pid for pid in processed_ids if pid != prediction.id]
            if prediction.creation_time:
                created = _as_utc(prediction.creation_time)
                watermark = max(watermark, created) if watermark else created
            # Persisted per prediction, not once at the end: a failure on the
            # fifth of six predictions must not re-send the first four.
            await _save_watermark(integration_id, watermark, processed_ids)

    totals["since"] = since.isoformat()
    totals["watermark"] = watermark.isoformat() if watermark else None
    return totals


def _creation_time_key(prediction: client.Prediction) -> datetime.datetime:
    """Sort key that tolerates a prediction with no creation time (ingest it first)."""
    if prediction.creation_time:
        return _as_utc(prediction.creation_time)
    return datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)


async def _select_predictions(
    api: client.OlmoEarthClient,
    config: PullEventsConfig,
    since: datetime.datetime,
    processed_ids: List[str],
) -> List[client.Prediction]:
    """The predictions this run should ingest, newest first."""
    response = await api.search_predictions(build_prediction_query(config, since))
    already_seen = set(processed_ids)
    fresh = [p for p in response.records if p.id not in already_seen]
    if config.prediction_selection == PredictionSelection.LATEST_ONLY:
        return fresh[:1]
    return fresh


async def _ingest_prediction(
    api: client.OlmoEarthClient,
    integration,
    prediction: client.Prediction,
    config: PullEventsConfig,
) -> Tuple[int, int]:
    """Drain one prediction's features into Gundi. Returns (extracted, sent)."""
    integration_id = str(integration.id)
    feature_query = build_feature_query(config)
    extracted = 0
    sent = 0

    for prediction_result_id in client.result_ids_for(prediction):
        batch: List[dict] = []
        async for feature in api.iter_features(
            prediction_result_id,
            feature_query,
            max_features=config.max_features_per_prediction,
        ):
            extracted += 1
            event = transform_feature(feature, prediction, prediction_result_id, config)
            if not event:
                continue
            batch.append(event)
            if len(batch) >= config.events_per_request:
                sent += await _send_events(batch, integration_id)
                batch = []
        if batch:
            sent += await _send_events(batch, integration_id)

    logger.info(
        "Prediction %s: %s feature(s) read, %s event(s) sent for integration %s.",
        prediction.id, extracted, sent, integration_id,
    )
    return extracted, sent


async def _send_events(events: List[dict], integration_id: str) -> int:
    """Send one batch and report how many events it carried.

    `send_events_to_gundi` already retries transient Gundi failures; a failure
    that survives that propagates, which fails the action and leaves the
    watermark where it was so the next run retries this prediction.
    """
    logger.info(f"Sending {len(events)} event(s) to Gundi for integration {integration_id}...")
    await gundi_tools.send_events_to_gundi(events=events, integration_id=integration_id)
    return len(events)
