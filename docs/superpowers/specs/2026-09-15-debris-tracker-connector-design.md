# Marine Debris Tracker Connector — Design

**Date:** 2026-09-15
**Status:** Approved design (spec review pending)
**Repo (to be created):** `gundi-integration-debristracker`
**Pattern reference:** `gundi-integration-inaturalist` (citizen-science pull connector)

## Problem

Marine Debris Tracker (debristracker.org — University of Georgia / NOAA /
Morgan Stanley) is the largest open citizen-science dataset of logged
marine-debris items: volunteers record individual items (category, material,
quantity, location, time) grouped into tracking sessions, typically during
cleanups or transects. Conservation operators using EarthRanger have no way
to see that activity in their operational picture. This connector pulls MDT
tracking sessions into Gundi as events, so debris hotspots and cleanup
activity appear alongside everything else an ER site monitors.

This is a **general catalog integration**: any Gundi operator should be able
to connect it from the portal, with organization/list selection via live
dropdowns and sensible filters — not a single-customer pilot.

## Naming

| Thing | Name |
|---|---|
| Repo | `gundi-integration-debristracker` |
| Integration type slug | `debris_tracker` |
| Display name | Marine Debris Tracker |
| Default ER event type | `mdt_cleanup_session` |

Deliberately distinct from `gundi-integration-marinemonitor` (ProtectedSeas
Marine Monitor / M2 radar) — an unrelated system with a confusable name.

## ⚠ Assumed API contract (pending confirmation)

**Marine Debris Tracker has no publicly documented API** (verified
2026-09-15: no developer docs; the data platform is a logged-in web app with
CSV export; JSON endpoint probes 404). This section is the contract the
design assumes, and doubles as the requirements list for outreach to the
Debris Tracker team (debristracker101@gmail.com):

1. Auth: static API key, `Authorization: Bearer <key>`.
2. `GET /organizations` — organizations visible to the key.
3. `GET /organizations/{id}/lists` — tracking lists in an organization.
4. `GET /sessions?organization_id=&list_id=&updated_since=&bbox=&page=` —
   tracking sessions, each with: id, start/end timestamps, item records
   (category, material, quantity, lat/lon, timestamp), and, if available,
   distance/duration and an opaque user id.
5. Pagination, and an `updated_since` (or equivalent) incremental filter —
   **the make-or-break requirement**. Without incremental filtering the
   connector must re-pull full windows and diff client-side, which changes
   the sync design and the load we place on their platform.
6. Rate limits and terms of use for automated access.

Every assumption is isolated in `app/services/client.py` and the test
fixtures; when the real contract arrives, those are the only things that
change. **Implementation of live client calls waits for their answer;**
models, transformer, handlers, and tests proceed against fixtures.

## Architecture

Standard template-fork connector (fork of `gundi-integration-action-runner`
`main`, which includes reference-action support):

```
app/
  actions/configurations.py   # config models (portal forms)
  actions/handlers.py         # action_auth, action_pull_events, reference actions
  services/client.py          # ALL MDT API knowledge (typed methods, httpx)
  services/transformers.py    # session → Gundi event mapping (pure functions)
```

### Actions

| Action | Type | Notes |
|---|---|---|
| `auth` | auth, `ExecutableActionMixin` | Validates key via `GET /organizations`; portal Test button |
| `pull_events` | pull, periodic (default crontab: hourly) | The main sync |
| `list_organizations` | reference | Options for the org dropdown |
| `list_lists` | reference, param `organization_ids` | Cascades from the org field; options grouped per organization |

### Config models

`AuthenticateConfig`: `api_key: SecretStr` (password widget).

`PullEventsConfig` (ordered via `ui_global_options`):

| Field | Type / default | Notes |
|---|---|---|
| `organization_ids` | `List[str]` (required) | `gundi:reference` → `list_organizations` |
| `list_ids` | `Optional[List[str]]` | `gundi:reference` → `list_lists`, `$data` cascade from `organization_ids`; empty = all lists |
| `days_to_load` | `int = 7`, range 1–30 | Fallback window when no watermark exists |
| `bounding_box` | `Optional[str]` | `[ne_lat, ne_lon, sw_lat, sw_lon]`, iNaturalist format |
| `event_type` | `str = "mdt_cleanup_session"` | ER event type value |
| `event_prefix` | `str = "Debris: "` | Event title prefix |
| `run_on_schedule` | inherited | Pause toggle |

Reference queries: `ListOrganizationsQuery(ReferenceActionConfiguration)`
(no params) and `ListListsQuery(ReferenceActionConfiguration)`
(`organization_ids: List[str]`, `$data`-cascaded from the sibling
`organization_ids` field). Because the org field is multi-select, the
handler queries lists across *all* selected organizations and sets each
option's `group` to the organization name — the portal renders a grouped
dropdown, and no per-org cascade is needed.

## Data mapping (session → one Gundi event)

One event per tracking session — item-level events were considered and
rejected (a single cleanup would flood the ER feed with hundreds of events;
session-level matches how operators act on the data).

- **Location:** centroid of the session's item coordinates. Items missing
  coordinates are excluded from the centroid; a session with *no* located
  items is skipped (counted in the run summary, logged at DEBUG).
- **`recorded_at`:** session end time (fallback: last item timestamp).
- **Title:** `"{event_prefix}{list name or org name} — {total_items} items"`.
- **`event_details`:**
  - `total_items` (sum of quantities)
  - `items_by_category`, `items_by_material` (dicts of name → count)
  - `top_item` (name + count)
  - `distance_m`, `duration_s` (when the API provides them)
  - `session_id`, `list_name`, `organization_name`, `mdt_user_id`
- **Privacy:** volunteer usernames are **never** sent to ER. Citizen
  scientists did not sign up to appear in a third-party operations platform.
  Only MDT's opaque user id is included, for traceability.

An ER-side event type definition for `mdt_cleanup_session` (display name,
details schema) ships as documentation for site admins; creating it on ER
sites is customer setup, not connector code.

## Sync algorithm and state

The iNaturalist pattern, with the sync position named **watermark**
(`STATE_WATERMARK_KEY = "watermark"` — house terminology; `last_run`-style
names are not used):

1. Read the watermark from `IntegrationStateManager`
   (`integration_id, "pull_events"`); if absent, use `now - days_to_load`.
2. Query sessions `updated_since = watermark - 1h` (overlap window absorbs
   clock skew and late writes).
3. Per-session dedupe: a state key per session id records that it was sent,
   so the overlap window and MDT-side edits don't create duplicates. An
   edited session updates its state entry but is **not re-sent in v1** — a
   documented limitation (ER events are not updated retroactively).
4. Transform and send in chunks via `send_events_to_gundi`.
5. Capture `run_started_at` before querying; advance the watermark to it
   **only after all chunks send successfully**. A partial failure leaves
   the watermark unmoved so the next run retries (dedupe keys make the
   retry idempotent).

## Error handling

- Auth handler returns the standard status dict; 401/403 from MDT raises
  `IntegrationAuthError`, connection/timeout raises
  `IntegrationConnectionError`, 429 raises `IntegrationRateLimitError` —
  the activity feed gets classified, human-first messages.
- Reference actions return `ReferenceDataResponse` (`options`,
  `cache_ttl_seconds=3600` — org/list churn is low, `truncated`).
- Scheduled runs honor the framework's quiet-skip semantics for
  missing/invalid config; manual runs stay strict.

## Testing

- **Transformer unit tests** on fixture sessions: quantity aggregation,
  centroid math, items without coordinates, empty session skipped, details
  shape, no username anywhere in output.
- **Handler tests** with a mocked client and the framework's pytest
  fixtures (`integration_v2`, `mock_gundi_client_v2`, `mock_publish_event`):
  watermark lifecycle (absent → fallback window; advanced only on success),
  overlap dedupe, chunking, error classification.
- **Reference-action tests**: option shape, cascade param.
- **Contract fixtures** (`tests/fixtures/mdt/*.json`) encode every
  assumption from the assumed-contract section; when MDT confirms the real
  API, updating these fixtures *is* the contract verification.

## Out of scope (v1)

- **Photo attachments.** MDT items can carry photos and the framework
  supports `send_event_attachments_to_gundi`, but a session-level event
  needs a photo-selection policy (a cleanup can log hundreds). Deferred to
  v1.1 behind an `include_photos` flag.
- Item-level events (rejected above).
- Webhooks/push — MDT offers none.
- Re-sending edited sessions (v1 limitation, documented).
- Any ER-side automation (event type creation, dashboards).
- CSV-export scraping as a data path — brittle and likely against terms;
  if MDT declines API access, this design pauses rather than falling back
  to scraping.

## Sequencing

1. This spec: review, then outreach email to the Debris Tracker team
   carrying the assumed-contract section (parallel track).
2. Create `gundi-integration-debristracker` from the template; scaffold
   actions, models, transformer, tests against fixtures.
3. On MDT's answer: reconcile `client.py` + fixtures with the real
   contract; wire live calls; register in dev
   (`REGISTER_REFERENCE_ACTIONS` considerations per the platform's
   reference-type rollout status).
4. Dev E2E: portal connection flow → org/list dropdowns → pull → ER event;
   then catalog docs.
