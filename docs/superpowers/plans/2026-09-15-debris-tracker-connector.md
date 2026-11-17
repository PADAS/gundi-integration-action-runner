# Marine Debris Tracker Connector Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A `gundi-integration-debristracker` connector that pulls Marine Debris Tracker tracking sessions into Gundi as session-level events, with portal dropdowns for organization/list selection.

**Architecture:** Template-fork connector (fork of `gundi-integration-action-runner` `main`). All MDT API knowledge lives in `app/services/client.py` behind typed methods; `app/services/transformers.py` holds pure session→event functions; handlers wire them with watermark-based incremental sync.

**Tech Stack:** Python 3.10+, Pydantic v1, httpx, pytest + pytest-asyncio, template framework services (`state_manager`, `send_events_to_gundi`, `activity_logger`, reference actions).

**Spec:** `docs/superpowers/specs/2026-09-15-debris-tracker-connector-design.md`

## Global Constraints

- **Assumed API contract:** the MDT API is UNCONFIRMED. No code may call a live MDT endpoint; all client tests use `httpx.MockTransport` with fixtures under `tests/fixtures/mdt/`. Do not register the connector anywhere but dev.
- **Watermark naming:** the incremental-sync position is called `watermark` everywhere (`STATE_WATERMARK_KEY = "watermark"`). Never `last_run`, `cursor`, or `last_poll`.
- **Privacy:** volunteer usernames must never appear in any event payload. Only `mdt_user_id` (opaque id) is allowed. A test enforces this.
- **Naming:** integration type slug `debris_tracker`, display name "Marine Debris Tracker", default event type `mdt_cleanup_session`.
- Handlers are `async`, action functions are named `action_<id>`, configs subclass the bases in `app/actions/core.py`.
- Run tests with `pytest` from the repo root; every task ends with the full suite green.

---

### Task 1: Scaffold the repo from the template

**Files:**
- Create: repo `PADAS/gundi-integration-debristracker` (from template)
- Modify: `README.md` (title block only), `.env.example`

**Interfaces:**
- Produces: a working fork where `pytest` passes and `app/main.py` boots.

- [ ] **Step 1: Create the repo** (requires PADAS org permission — if the executor lacks it, stop and hand this step to a maintainer)

```bash
gh repo create PADAS/gundi-integration-debristracker \
  --template PADAS/gundi-integration-action-runner --private \
  --description "Gundi connector for Marine Debris Tracker (debristracker.org)"
git clone git@github.com:PADAS/gundi-integration-debristracker.git
cd gundi-integration-debristracker
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

- [ ] **Step 2: Verify the template suite passes untouched**

Run: `pytest`
Expected: PASS (template's own tests)

- [ ] **Step 3: Set the README title block**

Replace the first heading and paragraph of `README.md`:

```markdown
# gundi-integration-debristracker
Gundi connector for Marine Debris Tracker (debristracker.org) — pulls
citizen-science debris tracking sessions into Gundi as session-level events.
Design: docs in gundi-integration-action-runner
`docs/superpowers/specs/2026-09-15-debris-tracker-connector-design.md`.
⚠ The MDT API contract is pending confirmation; do not register outside dev.
```

- [ ] **Step 4: Commit**

```bash
git add README.md
git commit -m "chore: scaffold Marine Debris Tracker connector from template"
```

---

### Task 2: MDT API models and contract fixtures

**Files:**
- Create: `app/services/mdt_models.py`
- Create: `tests/fixtures/mdt/organizations.json`, `tests/fixtures/mdt/lists.json`, `tests/fixtures/mdt/sessions_page1.json`, `tests/fixtures/mdt/sessions_page2.json`
- Test: `tests/test_mdt_models.py`

**Interfaces:**
- Produces: `Organization(id, name)`, `DebrisList(id, name, organization_id)`, `DebrisItem(category, material, quantity, latitude, longitude, logged_at)`, `TrackingSession(id, list_id, list_name, organization_id, organization_name, started_at, ended_at, distance_m, duration_s, mdt_user_id, items)` — all Pydantic v1 models with `parse_obj`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_mdt_models.py
import json
from pathlib import Path
from app.services.mdt_models import TrackingSession, Organization

FIXTURES = Path(__file__).parent / "fixtures" / "mdt"


def test_tracking_session_parses_fixture():
    payload = json.loads((FIXTURES / "sessions_page1.json").read_text())
    session = TrackingSession.parse_obj(payload["results"][0])
    assert session.id == "sess-001"
    assert session.items[0].category == "Plastic bottle"
    assert session.items[0].quantity == 3
    assert session.mdt_user_id == "u-9f2"


def test_session_tolerates_missing_optionals():
    minimal = {
        "id": "sess-min", "list_id": "l1", "organization_id": "o1",
        "started_at": "2026-09-01T10:00:00Z", "ended_at": "2026-09-01T11:00:00Z",
        "items": [],
    }
    session = TrackingSession.parse_obj(minimal)
    assert session.distance_m is None and session.list_name is None
```

- [ ] **Step 2: Create the fixtures**

`tests/fixtures/mdt/organizations.json`:

```json
{"results": [
  {"id": "o1", "name": "Coastal Cleanup Alliance"},
  {"id": "o2", "name": "Island Watch"}
]}
```

`tests/fixtures/mdt/lists.json`:

```json
{"results": [
  {"id": "l1", "name": "North Beach Transect", "organization_id": "o1"},
  {"id": "l2", "name": "Harbor Sweep", "organization_id": "o2"}
]}
```

`tests/fixtures/mdt/sessions_page1.json` (page 2 mirrors it with `"id": "sess-002"`, `"next": null`):

```json
{"results": [{
  "id": "sess-001", "list_id": "l1", "list_name": "North Beach Transect",
  "organization_id": "o1", "organization_name": "Coastal Cleanup Alliance",
  "started_at": "2026-09-01T09:00:00Z", "ended_at": "2026-09-01T10:30:00Z",
  "distance_m": 1200, "duration_s": 5400, "mdt_user_id": "u-9f2",
  "username": "sea_cleaner_42",
  "items": [
    {"category": "Plastic bottle", "material": "Plastic", "quantity": 3,
     "latitude": 31.001, "longitude": -81.402, "logged_at": "2026-09-01T09:12:00Z"},
    {"category": "Rope", "material": "Cloth/Fiber", "quantity": 1,
     "latitude": 31.003, "longitude": -81.404, "logged_at": "2026-09-01T09:40:00Z"},
    {"category": "Plastic bag", "material": "Plastic", "quantity": 2,
     "latitude": null, "longitude": null, "logged_at": "2026-09-01T09:55:00Z"}
  ]
}], "next": "https://api.debristracker.org/sessions?page=2"}
```

(The fixture deliberately includes a `username` field the models must *not*
expose, and an item without coordinates.)

- [ ] **Step 3: Run test to verify it fails**

Run: `pytest tests/test_mdt_models.py -v`
Expected: FAIL with `ModuleNotFoundError: app.services.mdt_models`

- [ ] **Step 4: Write the models**

```python
# app/services/mdt_models.py
"""Typed models for the ASSUMED Marine Debris Tracker API contract.

See the spec's 'Assumed API contract (pending confirmation)' section.
When MDT confirms the real contract, these models and tests/fixtures/mdt/
are the only things (besides client.py) that change.
"""
from datetime import datetime
from typing import List, Optional

import pydantic


class Organization(pydantic.BaseModel):
    id: str
    name: str


class DebrisList(pydantic.BaseModel):
    id: str
    name: str
    organization_id: str


class DebrisItem(pydantic.BaseModel):
    category: str
    material: Optional[str] = None
    quantity: int = 1
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    logged_at: Optional[datetime] = None


class TrackingSession(pydantic.BaseModel):
    id: str
    list_id: str
    list_name: Optional[str] = None
    organization_id: str
    organization_name: Optional[str] = None
    started_at: datetime
    ended_at: datetime
    distance_m: Optional[float] = None
    duration_s: Optional[float] = None
    mdt_user_id: Optional[str] = None
    items: List[DebrisItem] = pydantic.Field(default_factory=list)

    class Config:
        # Privacy: the API may send `username`; never model or expose it.
        extra = "ignore"
```

- [ ] **Step 5: Run tests, verify pass, commit**

Run: `pytest tests/test_mdt_models.py -v` — Expected: PASS

```bash
git add app/services/mdt_models.py tests/fixtures/mdt tests/test_mdt_models.py
git commit -m "feat: MDT API models and contract fixtures (assumed contract)"
```

---

### Task 3: MDT client

**Files:**
- Create: `app/services/client.py`
- Test: `tests/test_client.py`

**Interfaces:**
- Consumes: Task 2's models.
- Produces:
  - `MDTClient(base_url: str, api_key: str)` (async context manager)
  - `async get_organizations() -> List[Organization]`
  - `async get_lists(organization_ids: List[str]) -> List[DebrisList]`
  - `async get_sessions(organization_ids, list_ids=None, updated_since=None, bbox=None) -> List[TrackingSession]` (follows `next` pagination)
  - `MDT_DEFAULT_BASE_URL = "https://api.debristracker.org"`
  - Raises the framework's `IntegrationAuthError` (401/403), `IntegrationRateLimitError` (429), `IntegrationBadResponseError` (5xx), `IntegrationConnectionError` (transport errors) from `app.services.errors`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_client.py
import json
from pathlib import Path

import httpx
import pytest

from app.services.client import MDTClient, MDT_DEFAULT_BASE_URL
from app.services.errors import IntegrationAuthError

FIXTURES = Path(__file__).parent / "fixtures" / "mdt"


def fixture_transport():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["Authorization"] == "Bearer test-key"
        path = request.url.path
        if path == "/organizations":
            return httpx.Response(200, text=(FIXTURES / "organizations.json").read_text())
        if path.endswith("/lists"):
            return httpx.Response(200, text=(FIXTURES / "lists.json").read_text())
        if path == "/sessions":
            page = request.url.params.get("page", "1")
            f = "sessions_page2.json" if page == "2" else "sessions_page1.json"
            return httpx.Response(200, text=(FIXTURES / f).read_text())
        return httpx.Response(404)
    return httpx.MockTransport(handler)


@pytest.mark.asyncio
async def test_get_sessions_follows_pagination():
    async with MDTClient(MDT_DEFAULT_BASE_URL, "test-key", transport=fixture_transport()) as client:
        sessions = await client.get_sessions(organization_ids=["o1"])
    assert [s.id for s in sessions] == ["sess-001", "sess-002"]


@pytest.mark.asyncio
async def test_401_raises_auth_error():
    transport = httpx.MockTransport(lambda req: httpx.Response(401))
    async with MDTClient(MDT_DEFAULT_BASE_URL, "bad-key", transport=transport) as client:
        with pytest.raises(IntegrationAuthError):
            await client.get_organizations()
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest tests/test_client.py -v`
Expected: FAIL with `ModuleNotFoundError` / import error

- [ ] **Step 3: Implement the client**

```python
# app/services/client.py
"""HTTP client for the ASSUMED Marine Debris Tracker API (pending confirmation)."""
from datetime import datetime
from typing import List, Optional

import httpx

from app.services.errors import (
    IntegrationAuthError,
    IntegrationBadResponseError,
    IntegrationConnectionError,
    IntegrationRateLimitError,
)
from app.services.mdt_models import DebrisList, Organization, TrackingSession

MDT_DEFAULT_BASE_URL = "https://api.debristracker.org"


class MDTClient:
    def __init__(self, base_url: str, api_key: str, transport: Optional[httpx.AsyncBaseTransport] = None):
        self._client = httpx.AsyncClient(
            base_url=base_url or MDT_DEFAULT_BASE_URL,
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=30.0,
            transport=transport,
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        await self._client.aclose()

    async def _get(self, url: str, params: Optional[dict] = None) -> dict:
        try:
            response = await self._client.get(url, params=params)
        except httpx.TransportError as e:
            raise IntegrationConnectionError(f"Could not reach Marine Debris Tracker: {e}") from e
        if response.status_code in (401, 403):
            raise IntegrationAuthError("Marine Debris Tracker rejected the API key", status_code=response.status_code)
        if response.status_code == 429:
            raise IntegrationRateLimitError("Rate limited by Marine Debris Tracker", status_code=429)
        if response.status_code >= 500:
            raise IntegrationBadResponseError(
                f"Marine Debris Tracker error (HTTP {response.status_code})", status_code=response.status_code
            )
        response.raise_for_status()
        return response.json()

    async def get_organizations(self) -> List[Organization]:
        data = await self._get("/organizations")
        return [Organization.parse_obj(o) for o in data.get("results", [])]

    async def get_lists(self, organization_ids: List[str]) -> List[DebrisList]:
        lists: List[DebrisList] = []
        for org_id in organization_ids:
            data = await self._get(f"/organizations/{org_id}/lists")
            lists.extend(DebrisList.parse_obj(l) for l in data.get("results", []))
        return lists

    async def get_sessions(
        self,
        organization_ids: List[str],
        list_ids: Optional[List[str]] = None,
        updated_since: Optional[datetime] = None,
        bbox: Optional[str] = None,
    ) -> List[TrackingSession]:
        sessions: List[TrackingSession] = []
        for org_id in organization_ids:
            params = {"organization_id": org_id}
            if list_ids:
                params["list_id"] = ",".join(list_ids)
            if updated_since:
                params["updated_since"] = updated_since.isoformat()
            if bbox:
                params["bbox"] = bbox
            url: Optional[str] = "/sessions"
            while url:
                data = await self._get(url, params=params)
                sessions.extend(TrackingSession.parse_obj(s) for s in data.get("results", []))
                url, params = data.get("next"), None  # `next` is absolute; params baked in
        return sessions
```

(If the framework's `IntegrationError` subclasses take different kwargs on
this template version, match their actual signatures in
`app/services/errors.py` rather than these.)

- [ ] **Step 4: Run tests, verify pass, commit**

Run: `pytest tests/test_client.py -v` — Expected: PASS

```bash
git add app/services/client.py tests/test_client.py
git commit -m "feat: MDT client with pagination and classified errors"
```

---

### Task 4: Transformer (session → event)

**Files:**
- Create: `app/services/transformers.py`
- Test: `tests/test_transformers.py`

**Interfaces:**
- Consumes: `TrackingSession` from Task 2.
- Produces: `transform_session(session: TrackingSession, event_type: str, event_prefix: str) -> Optional[dict]` — a Gundi event dict, or `None` when the session has no located items.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_transformers.py
import json
from pathlib import Path

from app.services.mdt_models import TrackingSession
from app.services.transformers import transform_session

FIXTURES = Path(__file__).parent / "fixtures" / "mdt"


def load_session() -> TrackingSession:
    payload = json.loads((FIXTURES / "sessions_page1.json").read_text())
    return TrackingSession.parse_obj(payload["results"][0])


def test_session_becomes_one_event_with_centroid_and_totals():
    event = transform_session(load_session(), "mdt_cleanup_session", "Debris: ")
    assert event["event_type"] == "mdt_cleanup_session"
    assert event["title"] == "Debris: North Beach Transect — 6 items"
    # Centroid of the two located items only (31.001,-81.402) and (31.003,-81.404)
    assert event["location"] == {"lat": 31.002, "lon": -81.403}
    assert event["recorded_at"] == "2026-09-01T10:30:00+00:00"
    details = event["event_details"]
    assert details["total_items"] == 6
    assert details["items_by_material"] == {"Plastic": 5, "Cloth/Fiber": 1}
    assert details["top_item"] == {"category": "Plastic bottle", "count": 3}
    assert details["session_id"] == "sess-001"


def test_no_username_anywhere_in_event():
    event = transform_session(load_session(), "mdt_cleanup_session", "Debris: ")
    assert "sea_cleaner_42" not in json.dumps(event)
    assert event["event_details"]["mdt_user_id"] == "u-9f2"


def test_session_without_located_items_returns_none():
    session = load_session().copy(deep=True)
    for item in session.items:
        item.latitude = item.longitude = None
    assert transform_session(session, "mdt_cleanup_session", "Debris: ") is None
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest tests/test_transformers.py -v`
Expected: FAIL with import error

- [ ] **Step 3: Implement**

```python
# app/services/transformers.py
"""Pure mapping: one MDT tracking session → one Gundi event (or None).

Privacy invariant (spec): volunteer usernames never enter the payload;
only the opaque mdt_user_id may appear.
"""
from collections import Counter
from typing import Optional

from app.services.mdt_models import TrackingSession


def transform_session(session: TrackingSession, event_type: str, event_prefix: str) -> Optional[dict]:
    located = [i for i in session.items if i.latitude is not None and i.longitude is not None]
    if not located:
        return None

    lat = round(sum(i.latitude for i in located) / len(located), 6)
    lon = round(sum(i.longitude for i in located) / len(located), 6)

    by_category = Counter()
    by_material = Counter()
    for item in session.items:
        by_category[item.category] += item.quantity
        if item.material:
            by_material[item.material] += item.quantity
    total = sum(by_category.values())
    top_category, top_count = by_category.most_common(1)[0]

    place = session.list_name or session.organization_name or "session"
    details = {
        "total_items": total,
        "items_by_category": dict(by_category),
        "items_by_material": dict(by_material),
        "top_item": {"category": top_category, "count": top_count},
        "session_id": session.id,
        "list_name": session.list_name,
        "organization_name": session.organization_name,
        "mdt_user_id": session.mdt_user_id,
    }
    if session.distance_m is not None:
        details["distance_m"] = session.distance_m
    if session.duration_s is not None:
        details["duration_s"] = session.duration_s

    return {
        "title": f"{event_prefix}{place} — {total} items",
        "event_type": event_type,
        "recorded_at": session.ended_at.isoformat(),
        "location": {"lat": lat, "lon": lon},
        "event_details": details,
    }
```

- [ ] **Step 4: Run tests, verify pass, commit**

Run: `pytest tests/test_transformers.py -v` — Expected: PASS

```bash
git add app/services/transformers.py tests/test_transformers.py
git commit -m "feat: session-to-event transformer with centroid and privacy guard"
```

---

### Task 5: Config models

**Files:**
- Create: `app/actions/configurations.py` (replace template placeholder)
- Test: `tests/test_configurations.py`

**Interfaces:**
- Consumes: bases from `app.actions.core`, UI helpers from `app.services.utils`.
- Produces: `AuthenticateConfig(api_key)`, `PullEventsConfig(organization_ids, list_ids, days_to_load, bounding_box, event_type, event_prefix)`, `ListOrganizationsQuery()`, `ListListsQuery(organization_ids)`. `PullEventsConfig.ui_schema()` must annotate `organization_ids`/`list_ids` items with `gundi:reference`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_configurations.py
from app.actions.configurations import (
    AuthenticateConfig, ListListsQuery, PullEventsConfig,
)


def test_pull_config_defaults():
    config = PullEventsConfig(organization_ids=["o1"])
    assert config.days_to_load == 7
    assert config.event_type == "mdt_cleanup_session"
    assert config.event_prefix == "Debris: "


def test_reference_annotations_in_ui_schema():
    schema = PullEventsConfig.ui_schema()
    assert schema["organization_ids"]["items"]["gundi:reference"]["action"] == "list_organizations"
    lists_ref = schema["list_ids"]["items"]["gundi:reference"]
    assert lists_ref["action"] == "list_lists"
    assert lists_ref["params"]["organization_ids"] == {"$data": "../../organization_ids"}


def test_auth_config_masks_key():
    config = AuthenticateConfig(api_key="secret")
    assert "secret" not in config.json()
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest tests/test_configurations.py -v`
Expected: FAIL (imports missing)

- [ ] **Step 3: Implement**

```python
# app/actions/configurations.py
from typing import List, Optional

import pydantic

from app.actions.core import (
    AuthActionConfiguration,
    ExecutableActionMixin,
    PullActionConfiguration,
    ReferenceActionConfiguration,
)
from app.services.utils import FieldWithUIOptions, GlobalUISchemaOptions, UIOptions


def _reference(action: str, params: Optional[dict] = None) -> dict:
    annotation = {"action": action, "target": "self", "allow_free_text": True}
    if params:
        annotation["params"] = params
    return annotation


class AuthenticateConfig(AuthActionConfiguration, ExecutableActionMixin):
    api_key: pydantic.SecretStr = FieldWithUIOptions(
        ...,
        title="API Key",
        description="Marine Debris Tracker API key (contact the Debris Tracker team).",
        format="password",
        ui_options=UIOptions(widget="password"),
    )


class PullEventsConfig(PullActionConfiguration):
    organization_ids: List[str] = pydantic.Field(
        ...,
        title="Organizations",
        description="Marine Debris Tracker organizations to pull sessions from.",
    )
    list_ids: Optional[List[str]] = pydantic.Field(
        None,
        title="Tracking lists",
        description="Specific tracking lists. Leave empty to pull all lists in the selected organizations.",
    )
    days_to_load: int = FieldWithUIOptions(
        7,
        title="Days to load",
        ge=1,
        le=30,
        description="Fallback window when no sync watermark exists yet.",
        ui_options=UIOptions(widget="range"),
    )
    bounding_box: Optional[str] = pydantic.Field(
        None,
        title="Bounding box",
        description="Optional search area: [ne_latitude, ne_longitude, sw_latitude, sw_longitude]",
    )
    event_type: str = pydantic.Field(
        "mdt_cleanup_session", title="Event type",
        description="EarthRanger event type for created events.",
    )
    event_prefix: str = pydantic.Field(
        "Debris: ", title="Event prefix",
        description="Prefix for event titles.",
    )
    ui_global_options: GlobalUISchemaOptions = GlobalUISchemaOptions(
        order=[
            "organization_ids", "list_ids", "days_to_load", "bounding_box",
            "event_type", "event_prefix", "run_on_schedule",
        ],
    )

    @classmethod
    def ui_schema(cls, *args, **kwargs):
        base = super().ui_schema(*args, **kwargs)
        base.setdefault("organization_ids", {})["items"] = {
            "gundi:reference": _reference("list_organizations")
        }
        base.setdefault("list_ids", {})["items"] = {
            "gundi:reference": _reference(
                "list_lists",
                params={"organization_ids": {"$data": "../../organization_ids"}},
            )
        }
        return base


class ListOrganizationsQuery(ReferenceActionConfiguration):
    """The query IS the config: list organizations visible to the API key."""


class ListListsQuery(ReferenceActionConfiguration):
    organization_ids: List[str] = pydantic.Field(
        default_factory=list,
        description="Organizations whose tracking lists to return (grouped per organization).",
    )
```

- [ ] **Step 4: Run tests, verify pass, commit**

Run: `pytest tests/test_configurations.py -v` — Expected: PASS

```bash
git add app/actions/configurations.py tests/test_configurations.py
git commit -m "feat: action config models with gundi:reference dropdown annotations"
```

---

### Task 6: Auth and reference handlers

**Files:**
- Create: `app/actions/handlers.py` (replace template placeholder; pull handler arrives in Task 7)
- Test: `tests/test_handlers_auth_reference.py`

**Interfaces:**
- Consumes: `MDTClient` (Task 3), configs (Task 5), `find_config_for_action` from `app.services.utils`, `ReferenceDataResponse`/`ReferenceOption` from `app.actions.core`.
- Produces:
  - `async action_auth(integration, action_config) -> dict` (`{"valid_credentials": bool, ...}`)
  - `async action_list_organizations(integration, action_config) -> dict` (ReferenceDataResponse shape)
  - `async action_list_lists(integration, action_config) -> dict` (options grouped by organization name)
  - Helper `_mdt_client(integration) -> MDTClient` reading `integration.base_url` (default `MDT_DEFAULT_BASE_URL`) and the auth action's `api_key`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_handlers_auth_reference.py
from unittest.mock import AsyncMock, patch

import pytest

from app.actions.configurations import ListListsQuery, ListOrganizationsQuery
from app.services.mdt_models import DebrisList, Organization


@pytest.mark.asyncio
async def test_action_auth_valid(integration_v2):
    from app.actions import handlers
    mock_client = AsyncMock()
    mock_client.__aenter__.return_value = mock_client
    mock_client.get_organizations.return_value = [Organization(id="o1", name="CCA")]
    with patch.object(handlers, "_mdt_client", return_value=mock_client):
        result = await handlers.action_auth(integration_v2, handlers.AuthenticateConfig(api_key="k"))
    assert result["valid_credentials"] is True


@pytest.mark.asyncio
async def test_list_lists_groups_by_organization(integration_v2):
    from app.actions import handlers
    mock_client = AsyncMock()
    mock_client.__aenter__.return_value = mock_client
    mock_client.get_organizations.return_value = [
        Organization(id="o1", name="CCA"), Organization(id="o2", name="Island Watch"),
    ]
    mock_client.get_lists.return_value = [
        DebrisList(id="l1", name="North Beach", organization_id="o1"),
        DebrisList(id="l2", name="Harbor Sweep", organization_id="o2"),
    ]
    with patch.object(handlers, "_mdt_client", return_value=mock_client):
        result = await handlers.action_list_lists(
            integration_v2, ListListsQuery(organization_ids=["o1", "o2"])
        )
    groups = {o["group"] for o in result["options"]}
    assert groups == {"CCA", "Island Watch"}
    assert result["options"][0]["value"] == "l1"
```

(If the template's `integration_v2` fixture lacks an `auth` action config
with an `api_key`, extend the fixture locally in this test file via
`integration_v2.configurations` — copy the fixture's existing shape.)

- [ ] **Step 2: Run to verify failure**

Run: `pytest tests/test_handlers_auth_reference.py -v`
Expected: FAIL (handlers module missing / attribute errors)

- [ ] **Step 3: Implement `_mdt_client`, `action_auth`, both reference handlers**

```python
# app/actions/handlers.py
import logging

from app.actions.configurations import (
    AuthenticateConfig,
    ListListsQuery,
    ListOrganizationsQuery,
    PullEventsConfig,
)
from app.actions.core import ReferenceDataResponse, ReferenceOption
from app.services.client import MDT_DEFAULT_BASE_URL, MDTClient
from app.services.errors import IntegrationAuthError
from app.services.utils import find_config_for_action

logger = logging.getLogger(__name__)
REFERENCE_CACHE_TTL_SECONDS = 3600  # org/list churn is low


def _mdt_client(integration) -> MDTClient:
    auth_config = find_config_for_action(integration.configurations, "auth")
    parsed = AuthenticateConfig.parse_obj(auth_config.data)
    return MDTClient(
        base_url=integration.base_url or MDT_DEFAULT_BASE_URL,
        api_key=parsed.api_key.get_secret_value(),
    )


async def action_auth(integration, action_config: AuthenticateConfig):
    try:
        async with _mdt_client(integration) as client:
            organizations = await client.get_organizations()
    except IntegrationAuthError:
        return {"valid_credentials": False}
    return {"valid_credentials": True, "organizations_visible": len(organizations)}


async def action_list_organizations(integration, action_config: ListOrganizationsQuery):
    async with _mdt_client(integration) as client:
        organizations = await client.get_organizations()
    return ReferenceDataResponse(
        options=[ReferenceOption(value=o.id, label=o.name) for o in organizations],
        cache_ttl_seconds=REFERENCE_CACHE_TTL_SECONDS,
    ).dict()


async def action_list_lists(integration, action_config: ListListsQuery):
    async with _mdt_client(integration) as client:
        organizations = await client.get_organizations()
        lists = await client.get_lists(action_config.organization_ids)
    org_names = {o.id: o.name for o in organizations}
    return ReferenceDataResponse(
        options=[
            ReferenceOption(value=l.id, label=l.name, group=org_names.get(l.organization_id))
            for l in lists
        ],
        cache_ttl_seconds=REFERENCE_CACHE_TTL_SECONDS,
    ).dict()
```

- [ ] **Step 4: Run tests, verify pass, commit**

Run: `pytest tests/test_handlers_auth_reference.py -v` — Expected: PASS

```bash
git add app/actions/handlers.py tests/test_handlers_auth_reference.py
git commit -m "feat: auth handler and grouped organization/list reference actions"
```

---

### Task 7: Pull handler with watermark sync

**Files:**
- Modify: `app/actions/handlers.py` (append)
- Test: `tests/test_pull_events.py`

**Interfaces:**
- Consumes: everything above, plus `IntegrationStateManager` (`app.services.state`), `send_events_to_gundi` (`app.services.gundi`), `activity_logger` (`app.services.activity_logger`), `crontab_schedule` (`app.services.action_scheduler`).
- Produces: `async action_pull_events(integration, action_config: PullEventsConfig) -> dict` returning `{"events_extracted": int, "events_sent": int, "sessions_skipped_no_location": int, "sessions_skipped_duplicate": int}`. State layout: watermark at `(integration_id, "pull_events")` key `"watermark"`; per-session dedupe at `(integration_id, "pull_events", session_id)`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_pull_events.py
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from app.actions.configurations import PullEventsConfig
from app.services.mdt_models import TrackingSession

FIXTURES = Path(__file__).parent / "fixtures" / "mdt"


def sessions_fixture():
    payload = json.loads((FIXTURES / "sessions_page1.json").read_text())
    return [TrackingSession.parse_obj(s) for s in payload["results"]]


def make_state_manager(initial=None):
    # Mirrors IntegrationStateManager's signatures:
    #   get_state(integration_id, action_id, source_id="no-source")
    #   set_state(integration_id, action_id, state, source_id="no-source")
    store = dict(initial or {})

    def _key(integration_id, action_id, source_id=None):
        return (str(integration_id), str(action_id)) + ((str(source_id),) if source_id else ())

    manager = AsyncMock()
    manager.get_state.side_effect = lambda iid, aid, sid=None: store.get(_key(iid, aid, sid), {})
    async def _set(iid, aid, state, sid=None):
        store[_key(iid, aid, sid)] = state
    manager.set_state.side_effect = _set
    manager.store = store
    return manager


@pytest.mark.asyncio
async def test_pull_sends_events_and_advances_watermark(integration_v2):
    from app.actions import handlers
    mock_client = AsyncMock()
    mock_client.__aenter__.return_value = mock_client
    mock_client.get_sessions.return_value = sessions_fixture()
    state = make_state_manager()
    with patch.object(handlers, "_mdt_client", return_value=mock_client), \
         patch.object(handlers, "state_manager", state), \
         patch.object(handlers, "send_events_to_gundi", AsyncMock(return_value=[{}])) as send:
        result = await handlers.action_pull_events(
            integration_v2, PullEventsConfig(organization_ids=["o1"])
        )
    assert result["events_sent"] == 1
    send.assert_awaited_once()
    watermark_state = state.store[(str(integration_v2.id), "pull_events")]
    assert "watermark" in watermark_state


@pytest.mark.asyncio
async def test_already_sent_session_is_skipped(integration_v2):
    from app.actions import handlers
    mock_client = AsyncMock()
    mock_client.__aenter__.return_value = mock_client
    mock_client.get_sessions.return_value = sessions_fixture()
    state = make_state_manager({
        (str(integration_v2.id), "pull_events", "sess-001"): {"sent_at": "2026-09-10T00:00:00+00:00"},
    })
    with patch.object(handlers, "_mdt_client", return_value=mock_client), \
         patch.object(handlers, "state_manager", state), \
         patch.object(handlers, "send_events_to_gundi", AsyncMock(return_value=[])) as send:
        result = await handlers.action_pull_events(
            integration_v2, PullEventsConfig(organization_ids=["o1"])
        )
    assert result["sessions_skipped_duplicate"] == 1
    send.assert_not_awaited()


@pytest.mark.asyncio
async def test_send_failure_leaves_watermark_unmoved(integration_v2):
    from app.actions import handlers
    mock_client = AsyncMock()
    mock_client.__aenter__.return_value = mock_client
    mock_client.get_sessions.return_value = sessions_fixture()
    state = make_state_manager()
    with patch.object(handlers, "_mdt_client", return_value=mock_client), \
         patch.object(handlers, "state_manager", state), \
         patch.object(handlers, "send_events_to_gundi", AsyncMock(side_effect=RuntimeError("boom"))):
        with pytest.raises(RuntimeError):
            await handlers.action_pull_events(
                integration_v2, PullEventsConfig(organization_ids=["o1"])
            )
    assert (str(integration_v2.id), "pull_events") not in state.store
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest tests/test_pull_events.py -v`
Expected: FAIL (`action_pull_events` not defined)

- [ ] **Step 3: Implement**

Append to `app/actions/handlers.py`:

```python
from datetime import datetime, timedelta, timezone

from app.services.action_scheduler import crontab_schedule
from app.services.activity_logger import activity_logger
from app.services.gundi import send_events_to_gundi
from app.services.state import IntegrationStateManager
from app.services.transformers import transform_session

state_manager = IntegrationStateManager()

STATE_WATERMARK_KEY = "watermark"
WATERMARK_OVERLAP = timedelta(hours=1)
EVENTS_BATCH_SIZE = 100


async def _get_watermark(integration_id: str, fallback_days: int) -> datetime:
    state = await state_manager.get_state(integration_id, "pull_events")
    if raw := state.get(STATE_WATERMARK_KEY):
        return datetime.fromisoformat(raw)
    return datetime.now(timezone.utc) - timedelta(days=fallback_days)


@crontab_schedule("0 * * * *")  # hourly — outermost, per template convention
@activity_logger()
async def action_pull_events(integration, action_config: PullEventsConfig):
    integration_id = str(integration.id)
    run_started_at = datetime.now(timezone.utc)
    watermark = await _get_watermark(integration_id, action_config.days_to_load)

    async with _mdt_client(integration) as client:
        sessions = await client.get_sessions(
            organization_ids=action_config.organization_ids,
            list_ids=action_config.list_ids,
            updated_since=watermark - WATERMARK_OVERLAP,
            bbox=action_config.bounding_box,
        )

    events, sent_session_ids = [], []
    skipped_no_location = skipped_duplicate = 0
    for session in sessions:
        already_sent = await state_manager.get_state(integration_id, "pull_events", session.id)
        if already_sent:
            skipped_duplicate += 1
            continue  # v1 does not re-send edited sessions (spec limitation)
        event = transform_session(session, action_config.event_type, action_config.event_prefix)
        if event is None:
            skipped_no_location += 1
            continue
        events.append(event)
        sent_session_ids.append(session.id)

    events_sent = 0
    for start in range(0, len(events), EVENTS_BATCH_SIZE):
        chunk = events[start:start + EVENTS_BATCH_SIZE]
        await send_events_to_gundi(events=chunk, integration_id=integration_id)
        events_sent += len(chunk)
        for session_id in sent_session_ids[start:start + EVENTS_BATCH_SIZE]:
            await state_manager.set_state(
                integration_id, "pull_events",
                {"sent_at": datetime.now(timezone.utc).isoformat()}, session_id,
            )

    # Advance the watermark only after every chunk sent successfully.
    await state_manager.set_state(
        integration_id, "pull_events", {STATE_WATERMARK_KEY: run_started_at.isoformat()},
    )
    return {
        "events_extracted": len(events),
        "events_sent": events_sent,
        "sessions_skipped_no_location": skipped_no_location,
        "sessions_skipped_duplicate": skipped_duplicate,
    }
```

(Check `IntegrationStateManager.set_state`'s parameter order in
`app/services/state.py` — `source_id` is the trailing parameter; the dedupe
writes above pass `session_id` as `source_id`.)

- [ ] **Step 4: Run the full suite, verify pass, commit**

Run: `pytest -v` — Expected: PASS (all tasks' tests)

```bash
git add app/actions/handlers.py tests/test_pull_events.py
git commit -m "feat: watermark-based pull_events with dedupe and chunked sends"
```

---

### Task 8: Registration wiring, ER event-type doc, catalog README

**Files:**
- Modify: `README.md`
- Create: `docs/er-event-type.md`
- Test: `tests/test_registration_surface.py`

**Interfaces:**
- Consumes: everything above.
- Produces: a registerable action surface (verified by test, not by registering); operator docs.

- [ ] **Step 1: Write the failing test** (locks the registration surface)

```python
# tests/test_registration_surface.py
from app.actions.core import discover_actions


def test_all_actions_discoverable():
    handlers = discover_actions(module_name="app.actions.handlers", prefix="action_")
    assert set(handlers.keys()) == {"auth", "pull_events", "list_organizations", "list_lists"}


def test_pull_events_is_periodic_with_crontab():
    from app.actions.handlers import action_pull_events
    assert action_pull_events.crontab_schedule is not None
```

- [ ] **Step 2: Run to verify it fails or passes honestly**

Run: `pytest tests/test_registration_surface.py -v`
Expected: PASS if Tasks 5–7 are correct (this test exists to catch future drift; if it fails, fix the discovery issue before continuing)

- [ ] **Step 3: Write `docs/er-event-type.md`** (site-admin setup doc)

```markdown
# EarthRanger event type for Marine Debris Tracker

Create this event type on the ER site (Admin → Event Types) before routing
the connector's data. The connector emits one event per tracking session.

- **Value:** `mdt_cleanup_session`
- **Display:** Marine Debris Cleanup Session
- **Details schema (suggested):** total_items (number), items_by_category
  (object), items_by_material (object), top_item (object), distance_m
  (number), duration_s (number), session_id (string), list_name (string),
  organization_name (string), mdt_user_id (string)

Volunteer usernames are never included, by design (privacy).
```

- [ ] **Step 4: Extend README** — add a "Configuration" section listing the four actions, the pull filters, the watermark behavior, and the ⚠ assumed-contract warning; add "Registering in dev" with:

```bash
export INTEGRATION_TYPE_SLUG=debris_tracker INTEGRATION_TYPE_NAME="Marine Debris Tracker"
python -m app.register  # dev only — do NOT register in prod until the MDT contract is confirmed
```

- [ ] **Step 5: Full suite, commit**

Run: `pytest -v` — Expected: PASS

```bash
git add README.md docs/er-event-type.md tests/test_registration_surface.py
git commit -m "docs: registration surface test, ER event-type doc, catalog README"
```

---

## Blocked-on-external checklist (not tasks)

- MDT team reply (debristracker101@gmail.com) → reconcile `mdt_models.py`, `client.py`, and `tests/fixtures/mdt/` with the real contract before any live call.
- PADAS org permission for Task 1's `gh repo create`.
- Dev registration + portal E2E (org dropdown → list dropdown cascade → pull → ER event) happens only after both of the above.
