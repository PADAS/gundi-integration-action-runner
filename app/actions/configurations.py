"""Configuration models for the OlmoEarth integration.

Three actions, three configs:

  auth              the API token, validated against the predictions search
  pull_events       the scheduled ingest: predictions -> features -> Gundi
  list_predictions  a reference lookup the portal renders as a dropdown

The site URL is not a field here — it comes from the integration's own
`base_url`, which is where the portal already asks for it.
"""
from enum import Enum
from typing import Any, Dict, List, Optional

import pydantic

from app.services.utils import (
    FieldWithUIOptions,
    GlobalUISchemaOptions,
    OptionalStringType,
    UIOptions,
)

from .core import (
    AuthActionConfiguration,
    ExecutableActionMixin,
    PullActionConfiguration,
    ReferenceActionConfiguration,
)


class AuthenticateConfig(AuthActionConfiguration, ExecutableActionMixin):
    api_token: pydantic.SecretStr = FieldWithUIOptions(
        ...,
        title="API Token",
        description=(
            "Bearer token for the OlmoEarth API. Sent as "
            "`Authorization: Bearer <token>` on every request."
        ),
        format="password",
        ui_options=UIOptions(widget="password"),
    )
    ui_global_options = GlobalUISchemaOptions(order=["api_token"])


class PredictionSelection(str, Enum):
    """Which of the matching predictions a run ingests.

    `all_new` is the default because it cannot skip data: if two predictions
    land between runs, `latest_only` would ingest the newer and silently drop
    the older. In the ordinary case — one new prediction per run — the two
    behave identically.
    """

    ALL_NEW = "all_new"
    LATEST_ONLY = "latest_only"


class PullEventsConfig(PullActionConfiguration):
    model_id: str = FieldWithUIOptions(
        ...,
        title="Model ID",
        description="Ingest predictions produced by this OlmoEarth model.",
    )
    project_id: OptionalStringType = FieldWithUIOptions(
        None,
        title="Project ID",
        description="Optional. Narrow the search to one project.",
    )
    organization_id: OptionalStringType = FieldWithUIOptions(
        None,
        title="Organization ID",
        description="Optional. Narrow the search to one organization.",
    )
    prediction_status: OptionalStringType = FieldWithUIOptions(
        "completed",
        title="Prediction Status",
        description=(
            "Only ingest predictions in this status. Clear the field to ingest "
            "every status."
        ),
    )
    prediction_selection: PredictionSelection = FieldWithUIOptions(
        PredictionSelection.ALL_NEW,
        title="Predictions To Ingest",
        description=(
            "`all_new` ingests every prediction created since the last run; "
            "`latest_only` ingests just the newest one."
        ),
        ui_options=UIOptions(widget="select"),
    )
    lookback_days: int = FieldWithUIOptions(
        7,
        ge=1,
        le=365,
        title="Lookback Days",
        description=(
            "How far back the first run reaches. Later runs resume from where "
            "the last one finished and ignore this."
        ),
        ui_options=UIOptions(widget="range"),
    )
    area_of_interest: Optional[Dict[str, Any]] = FieldWithUIOptions(
        None,
        title="Area Of Interest",
        description=(
            "Optional GeoJSON geometry. Only features intersecting it are "
            'ingested, e.g. {"type": "Polygon", "coordinates": [[...]]}.'
        ),
        ui_options=UIOptions(widget="textarea"),
    )
    min_confidence: Optional[float] = FieldWithUIOptions(
        None,
        ge=0.0,
        le=1.0,
        title="Minimum Confidence",
        description=(
            "Optional. Drop features whose confidence property is below this "
            "value. The filter runs on the provider, not here."
        ),
    )
    confidence_property: str = FieldWithUIOptions(
        "confidence",
        title="Confidence Property",
        description=(
            "Name of the feature property the minimum-confidence filter reads. "
            "Models differ — some call it `score`."
        ),
    )
    event_type: str = FieldWithUIOptions(
        "olmoearth_detection",
        title="Gundi Event Type",
        description="Event type recorded in Gundi for every ingested feature.",
    )
    include_geometry: bool = FieldWithUIOptions(
        True,
        title="Include Full Geometry",
        description=(
            "Send the feature's full GeoJSON geometry alongside the point "
            "location, so a polygon detection stays a polygon downstream."
        ),
        ui_options=UIOptions(widget="radio"),
    )
    max_features_per_prediction: int = FieldWithUIOptions(
        10_000,
        ge=1,
        title="Max Features Per Prediction",
        description=(
            "Stop after this many features from a single prediction, so one "
            "oversized run cannot stall the schedule."
        ),
    )
    feature_page_size: int = FieldWithUIOptions(
        50,
        ge=1,
        le=1000,
        title="Feature Page Size",
        description="How many features to request per page.",
    )
    events_per_request: int = FieldWithUIOptions(
        200,
        ge=1,
        le=1000,
        title="Events Per Request",
        description="How many events to send to Gundi in one request.",
    )

    ui_global_options = GlobalUISchemaOptions(
        order=[
            "model_id",
            "project_id",
            "organization_id",
            "prediction_status",
            "prediction_selection",
            "lookback_days",
            "area_of_interest",
            "min_confidence",
            "confidence_property",
            "event_type",
            "include_geometry",
            "max_features_per_prediction",
            "feature_page_size",
            "events_per_request",
            "run_on_schedule",
        ],
    )


class ListPredictionsConfig(ReferenceActionConfiguration):
    """Query for the reference lookup: the config model *is* the query.

    Stateless — the portal sends these as overrides when a user opens the
    dropdown, and nothing is stored.
    """

    model_id: OptionalStringType = pydantic.Field(
        None,
        title="Model ID",
        description="Optional. List only predictions from this model.",
    )
    project_id: OptionalStringType = pydantic.Field(
        None,
        title="Project ID",
        description="Optional. List only predictions from this project.",
    )
    status: OptionalStringType = pydantic.Field(
        "completed",
        title="Status",
        description="Optional. List only predictions in this status.",
    )
    limit: int = pydantic.Field(
        100,
        ge=1,
        le=500,
        title="Limit",
        description="How many predictions to return, newest first.",
    )
