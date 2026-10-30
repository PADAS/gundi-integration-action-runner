"""One LCC monitor per saved Gundi integration."""

import json
from datetime import date
from enum import Enum
from typing import Any, Dict, List, Optional
from uuid import UUID

import pydantic
from shapely.geometry import shape
from shapely.geometry.base import BaseGeometry

from app.services.utils import FieldWithUIOptions, UIOptions

from .core import (
    AuthActionConfiguration,
    ExecutableActionMixin,
    PullActionConfiguration,
    ReferenceActionConfiguration,
)


class AuthenticateConfig(AuthActionConfiguration, ExecutableActionMixin):
    api_token: pydantic.SecretStr = FieldWithUIOptions(
        ..., title="OlmoEarth API token", ui_options=UIOptions(widget="password")
    )


class Cadence(str, Enum):
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    ANNUAL = "annual"


class PostChange(str, Enum):
    VEGETATION_GROWTH = "vegetation_growth"
    NEW_BUILDING = "new_building"
    NEW_ROAD = "new_road"
    NEW_INFRASTRUCTURE = "new_infrastructure"
    NEW_CROP_FIELD = "new_crop_field"
    NEW_AQUAFARM = "new_aquafarm"
    SITE_CLEARING = "site_clearing"
    WATER_EXPAND = "water_expand"
    MINING = "mining"
    NEW_CROP_STRUCTURE = "new_crop_structure"
    SELECTIVE_LOGGING = "selective_logging"
    LANDSLIDE = "landslide"
    SETTLEMENT = "settlement"


def polygon_geometry(value: dict) -> dict:
    polygon_shape(value)
    return value


def polygon_shape(value: dict) -> BaseGeometry:
    """The shapely geometry for a regional, valid WGS84 (Multi)Polygon."""
    try:
        geom = shape(value)
        if (
            geom.geom_type not in ("Polygon", "MultiPolygon")
            or geom.is_empty
            or not geom.is_valid
        ):
            raise ValueError("Expected a nonempty valid Polygon or MultiPolygon")
        xmin, ymin, xmax, ymax = geom.bounds
        if (
            not (-180 <= xmin <= xmax <= 180 and -90 <= ymin <= ymax <= 90)
            or xmax - xmin >= 180
        ):
            raise ValueError(
                "Use a regional WGS84 polygon that does not cross the antimeridian"
            )
    except (KeyError, TypeError, AttributeError) as exc:
        raise ValueError("Expected a GeoJSON Polygon or MultiPolygon geometry") from exc
    return geom


def _reference(action: str, params: Optional[dict] = None) -> dict:
    """A ``gundi:reference`` ui_schema annotation: the portal fills the field from a
    reference action's options. It deliberately sets no ``ui:widget``, so portals
    without reference support keep showing a plain text field."""
    return {"action": action, "target": "self", "params": params or {}, "allow_free_text": True}


class PullEventsConfig(PullActionConfiguration):
    project_id: UUID = FieldWithUIOptions(
        ...,
        title="OlmoEarth project",
        description="The project's UUID, as in its Studio URL (/projects/<uuid>).",
    )
    model_id: UUID = FieldWithUIOptions(
        ...,
        title="LCC model",
        description="The model's UUID, as in its Studio URL. Use a model configured to vectorize post_change and emit ISO date properties.",
    )
    # The portal renders an "object" field as nested inputs with nowhere to paste,
    # so the schema advertises a string and parse_area turns the text into a dict.
    area_of_interest: Dict[str, Any] = FieldWithUIOptions(
        ...,
        title="Area (GeoJSON geometry)",
        description='Paste a Polygon or MultiPolygon geometry, e.g. {"type": "Polygon", "coordinates": [...]}.',
        ui_options=UIOptions(widget="textarea"),
    )
    cadence: Cadence = FieldWithUIOptions(
        Cadence.MONTHLY,
        title="Prediction frequency",
        ui_options=UIOptions(widget="select"),
    )
    schedule_start: date = FieldWithUIOptions(
        ...,
        title="First prediction date (UTC)",
        description="Missed schedule slots are processed oldest first.",
    )
    select_all_types: bool = FieldWithUIOptions(True, title="Select all change types")
    post_change_types: List[PostChange] = FieldWithUIOptions(
        default_factory=list,
        title="Change types",
        unique_items=True,
        ui_options=UIOptions(widget="select"),
    )
    changes_since: Optional[date] = pydantic.Field(
        None,
        description="Optional fixed event-date cutoff. Never advanced as an ingestion watermark.",
    )
    event_type: str = pydantic.Field("olmoearth_detection", min_length=1)

    class Config:
        @staticmethod
        def schema_extra(schema, model):
            schema["properties"]["area_of_interest"]["type"] = "string"

    @pydantic.validator("area_of_interest", pre=True)
    def parse_area(cls, value):
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except ValueError as exc:
                raise ValueError("Area must be GeoJSON geometry text") from exc
        return value

    _valid_area = pydantic.validator("area_of_interest", allow_reuse=True)(
        polygon_geometry
    )

    @pydantic.root_validator
    def require_selected_types(cls, values):
        if not values.get("select_all_types") and not values.get("post_change_types"):
            raise ValueError("Select at least one change type or enable Select all")
        return values

    @classmethod
    def ui_schema(cls, *args, **kwargs):
        ui = super().ui_schema(*args, **kwargs)
        ui.setdefault("project_id", {})["gundi:reference"] = _reference("list_projects")
        # $data paths start at the field's containing object, so a sibling is its key.
        ui.setdefault("model_id", {})["gundi:reference"] = _reference(
            "list_models", {"project_id": {"$data": "project_id"}}
        )
        return ui

    @property
    def selected_types(self) -> List[str]:
        types = list(PostChange) if self.select_all_types else self.post_change_types
        return [value.value for value in types]


class SubmitPredictionsConfig(PullActionConfiguration):
    run_on_schedule: bool = pydantic.Field(
        False,
        title="Enable scheduled prediction submission",
        description="Uses the saved Collect LCC events configuration. Running manually also submits a due prediction.",
    )


class ListProjectsConfig(ReferenceActionConfiguration):
    """Options for the project field: projects the API token can access."""


class ListModelsConfig(ReferenceActionConfiguration):
    """Options for the model field: finished fine-tuned models in one project."""

    project_id: UUID
