"""Validate polygon changes, key them to a site, and build Gundi event payloads.

Every detection becomes an event. Each one carries a ``site_id`` (the geohash
cell containing the polygon) and is marked as a new site, a new change type at a
known site, or a site seen before, so repeats stay visible but recognizable.
"""

from datetime import date, datetime, time, timezone
from typing import Optional
from uuid import NAMESPACE_URL, uuid5

from app.services.errors import IntegrationBadResponseError

from .client import Feature
from .configurations import PullEventsConfig, polygon_shape
from .monitor_state import Job, SiteRecord

# site_id is "gh7:" plus the 7-character geohash (a cell about 150 m across) of a
# point inside the polygon. A site that drifts or grows across a cell edge gets a
# second ID and is reported as new. A stand-in until OlmoEarth assigns its own site
# or track IDs; the scheme prefix keeps the two kinds of ID from colliding.
SITE_PRECISION = 7

# How precise the LCC model's change dates are. A property of the model, not a
# partner setting; revisit when the model's date output is settled.
CHANGE_DATE_PRECISION = "month"
# The polygon property holding the change date: the LCC model writes the start and
# end of the change window as ts_start and ts_end. Both reach event_details.
CHANGE_DATE_PROPERTY = "ts_start"

NEW_SITE = "new_site"
NEW_CHANGE_TYPE = "new_change_type"
SEEN_BEFORE = "seen_before"

_GEOHASH_ALPHABET = "0123456789bcdefghjkmnpqrstuvwxyz"


class InvalidFeatureError(IntegrationBadResponseError):
    """A detection in a result is unusable; retrying the same page cannot fix it."""


def parse_change_date(value) -> date:
    if not isinstance(value, str):
        raise InvalidFeatureError(
            "LCC change date must be an ISO date string; encoded months and timestep indices are unsupported."
        )
    try:
        if len(value) == 10:
            return date.fromisoformat(value)
        stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if stamp.tzinfo is None:
            raise ValueError("Missing timezone")
        return stamp.astimezone(timezone.utc).date()
    except ValueError as exc:
        raise InvalidFeatureError(
            "LCC change date is missing or is not an ISO date/time."
        ) from exc


def geohash(lat: float, lon: float, precision: int) -> str:
    lat_range, lon_range = [-90.0, 90.0], [-180.0, 180.0]
    bits, cell, even, out = 0, 0, True, []
    while len(out) < precision:
        span = lon_range if even else lat_range
        value = lon if even else lat
        middle = (span[0] + span[1]) / 2
        cell <<= 1
        if value >= middle:
            cell |= 1
            span[0] = middle
        else:
            span[1] = middle
        even = not even
        bits += 1
        if bits == 5:
            out.append(_GEOHASH_ALPHABET[cell])
            bits, cell = 0, 0
    return "".join(out)


def event_id(integration_id: str, job: Job, feature: Feature) -> str:
    """The stable Gundi external ID for one detection in one prediction result."""
    return str(uuid5(NAMESPACE_URL, f"{integration_id}:{job.result_id}:{feature.id}"))


def classify(
    feature: Feature, job: Job, config: PullEventsConfig
) -> Optional[tuple[str, date]]:
    """(change type, change date) for a detection the job monitors, else None.

    Raises InvalidFeatureError if a monitored detection has no usable date.
    """
    props = feature.properties.model_properties()
    change_type = props.get("post_change")
    if change_type not in job.selected_types:
        return None
    changed = parse_change_date(props.get(CHANGE_DATE_PROPERTY))
    if job.changes_since and changed < job.changes_since:
        return None
    return change_type, changed


def observe(
    sites: dict[str, SiteRecord], site: str, change_type: str, job: Job, now: datetime
) -> tuple[str, SiteRecord]:
    """Record a detection at ``site``; return how it relates to earlier predictions."""
    record = sites.get(site)
    if record is None:
        record = sites[site] = SiteRecord(
            first_seen_at=now, last_prediction_id=job.prediction_id
        )
    # Count predictions, not features: one prediction may split a site in several.
    elif record.last_prediction_id != job.prediction_id:
        record.last_prediction_id = job.prediction_id
        record.predictions += 1
    record.change_types.setdefault(change_type, job.prediction_id)
    if record.predictions == 1:
        return NEW_SITE, record
    if record.change_types[change_type] == job.prediction_id:
        return NEW_CHANGE_TYPE, record
    return SEEN_BEFORE, record


def prepare_event(
    feature: Feature,
    found: tuple[str, date],
    job: Job,
    config: PullEventsConfig,
    sites: dict[str, SiteRecord],
    *,
    integration_id: str,
    now: datetime,
) -> dict:
    """Build the Gundi event for one monitored detection and record its site.

    ``found`` is ``classify(feature, job, config)``. Mutates only ``sites``, the
    caller's working copy of the site records; performs no persistence or network
    operations.
    """
    if feature.id is None or feature.geometry is None:
        raise InvalidFeatureError(
            f"Detection {feature.id} has no stable feature ID or polygon geometry."
        )
    change_type, changed = found
    geometry = feature.geometry.dict(exclude_none=True)
    try:
        geom = polygon_shape(geometry)
    except ValueError as exc:
        raise InvalidFeatureError(
            f"Detection {feature.id} geometry is not a valid regional polygon."
        ) from exc
    point = geom.representative_point()
    site = f"gh{SITE_PRECISION}:{geohash(point.y, point.x, SITE_PRECISION)}"
    kind, record = observe(sites, site, change_type, job, now)
    label = change_type.replace("_", " ")
    title = {
        NEW_SITE: f"OlmoEarth: {label} (new site)",
        NEW_CHANGE_TYPE: f"OlmoEarth: {label} (new change at known site)",
        SEEN_BEFORE: f"OlmoEarth: {label} (seen before, {record.predictions} predictions)",
    }[kind]
    props = feature.properties.model_properties()
    props.update(
        {
            "prediction_id": job.prediction_id,
            "prediction_result_id": job.result_id,
            "oerun_prediction_result_id": feature.properties.oe_prediction_result_id,
            "external_event_id": event_id(integration_id, job, feature),
            "feature_id": feature.id,
            "model_id": str(config.model_id),
            "first_discovered_at": now.isoformat(),
            "estimated_change_date": changed.isoformat(),
            "change_date_precision": CHANGE_DATE_PRECISION,
            "prediction_reference_time": job.reference_time,
            "site_id": site,
            "detection_kind": kind,
        }
    )
    return {
        "title": title,
        "event_type": config.event_type,
        "source": f"olmoearth-monitor:{integration_id}",
        "recorded_at": datetime.combine(changed, time(), timezone.utc).isoformat(),
        "location": {"lat": point.y, "lon": point.x},
        "geometry": geometry,
        "event_details": props,
    }
