"""The monitor's persisted ledger and the identity that keeps configurations isolated."""

import json
from datetime import date, datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field

from .configurations import PullEventsConfig

STATE_ACTION = "lcc_monitor_v1"


class JobStatus(str, Enum):
    PENDING = "pending"
    COLLECTING = "collecting"
    COMPLETE = "complete"
    FAILED = "failed"
    NEEDS_EXPORT = "needs_export"


# Jobs that still have work to do; a new submission waits for these.
ACTIVE_JOB_STATUSES = frozenset({JobStatus.PENDING, JobStatus.COLLECTING})


class Job(BaseModel):
    """One submitted prediction and how far its result has been read.

    Only the settings that may change between jobs are kept; the rest are pinned
    by the monitor identity and read from the live configuration.
    """

    name: str
    submitted_at: datetime
    prediction_id: str
    reference_time: str
    selected_types: list[str]
    changes_since: Optional[date] = None
    result_id: Optional[str] = None
    status: JobStatus = JobStatus.PENDING
    offset: int = 0
    error: Optional[str] = None


class SiteRecord(BaseModel):
    """What has been alerted at one site (a ``site_id`` cell) across predictions."""

    first_seen_at: datetime
    last_prediction_id: str
    # Each change type seen at the site, with the prediction that first reported it.
    change_types: dict[str, str] = Field(default_factory=dict)
    predictions: int = 1


class Outbox(BaseModel):
    """One page's delivery; the job's offset advances only once Gundi accepts it."""

    job_name: str
    events: list[dict]
    next_offset: int
    complete: bool


class MonitorState(BaseModel):
    identity: dict
    next_slot: int = 0
    jobs: list[Job] = Field(default_factory=list)
    sites: dict[str, SiteRecord] = Field(default_factory=dict)
    outbox: Optional[Outbox] = None


IDENTITY_FIELDS = {
    "project_id",
    "model_id",
    "area_of_interest",
    "schedule_start",
    "cadence",
}


def monitor_identity(config: PullEventsConfig, base_url: str) -> dict:
    """The settings a monitor's jobs and event IDs depend on; changing any needs a new monitor."""
    return {
        "base_url": base_url,
        **json.loads(config.json(include=IDENTITY_FIELDS)),
    }
