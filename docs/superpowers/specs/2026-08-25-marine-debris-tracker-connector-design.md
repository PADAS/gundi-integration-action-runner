# Marine Debris Tracker Connector — Design

**Date:** 2026-08-25
**Status:** Approved
**Target:** new repo `gundi-integration-marine-debris-tracker`, generated from this template

## Background

[Marine Debris Tracker](https://debristracker.org) (MDT) is a citizen-science
platform run by the University of Georgia (originally with the NOAA Marine Debris
Program, currently powered by Morgan Stanley with National Geographic Society).
Volunteers log litter/debris items via mobile apps; the open dataset holds
~2.9M records (2010–present) and remains active (~6.4k records in the first
25 days of August 2026).

This connector pulls MDT debris records into Gundi as **events** so EarthRanger
deployments can see debris/cleanup activity (e.g. coastal cleanup campaigns in a
protected area's operational footprint).

## Verified upstream API

MDT publishes no official API. Their public data page (a Gatsby SPA) calls an
AWS API Gateway REST service, discovered via the site's published source maps
and verified by direct request on 2026-08-25:

```
GET https://sb77ote20l.execute-api.us-east-1.amazonaws.com/production/items
    ?detail=4&limit=<n>&page=<n>&start=YYYY-MM-DD&end=YYYY-MM-DD[&list=<id>]
```

- **No authentication** for public reads.
- Response envelope: `{totalRecords, pageCount, count, items: [...]}`.
- `detail=4` record fields: `id`, `dt` (ISO-8601 UTC), `latitude`, `longitude`,
  `altitude`, `radius` (accuracy), `material`, `master_name` (item type),
  `description`, `quantity`, `degraded`, `list_id`/`list_name` (organization or
  campaign), `location` (reverse-geocoded place name), plus ETAP taxonomy fields.
- `start`/`end` filtering is **date-granular** (verified working); `list`
  filters server-side by organization/campaign list. **No server-side geo
  filter.**
- Related endpoints seen in their client code: `/organizations/{id}`,
  `/organizations/{id}/admins`, `/mdtEtaps/itemInstances`; a legacy PHP API at
  `https://marinedebris.engr.uga.edu/mdtapp` (avoid — passes credentials in
  query strings).

Because the API is unofficial, the base URL is a config field and the client
fails loudly on schema drift (see Error handling).

## Actions

### `auth`

No credentials exist. The handler is a connectivity check: `GET /items?limit=1`,
returns `{"valid": True}` on HTTP 200 with a parseable envelope. Config exposes
only `base_url` (default: the AWS gateway URL above).

### `pull_events`

`@crontab_schedule("0 */6 * * *")` — every 6 hours; upstream filters are
date-granular, so faster polling buys little.

Config (`PullActionConfiguration`, all fields via `FieldWithUIOptions`):

| Field | Type / default | Purpose |
|---|---|---|
| `list_id` | optional int | Server-side filter to one MDT org/campaign list |
| `bounding_box` | optional `(min_lon, min_lat, max_lon, max_lat)` | Client-side geo filter |
| `start_date` | date; default = 30 days before first run | Backfill floor; guards against pulling 2.9M historical records |
| `mapping_mode` | enum `individual` \| `aggregate`; default `individual` | Per-item events vs clustered summaries |
| `aggregation_grid_km` | float, default 1.0 | Grid cell size (aggregate mode only) |
| `aggregation_window` | enum `day` \| `pull`; default `day` | Time bucket (aggregate mode only) |
| `lookback_days` | int, default 2 | Overlap window for late-arriving records |

`mapping_mode` is deliberately a per-deployment choice: consumers of feeds like
this have differing goals — some want every item as an ER event, others want
digestible summaries.

Running with *neither* `list_id` nor `bounding_box` (global pull) is allowed
but the handler logs a WARNING activity each run so a misconfigured global
deployment is visible in the Portal.

## Components

### `services/client.py` — `MDTClient`

Thin async wrapper: `httpx.AsyncClient` (30s timeout), bounded retries with
exponential backoff on 5xx/timeouts. Public surface:

- `get_items(start, end, list_id=None) -> AsyncIterator[dict]` — handles
  pagination internally (page size ~10k; the site itself requests 200k, so this
  is conservative).
- `get_total(start, end, list_id=None) -> int` — `limit=1` probe reading
  `totalRecords` (cheap volume checks; also used by the explorer utility).

Typed errors: `MDTClientError` (HTTP/parse failures) and `MDTSchemaError`
raised when required fields (`id`, `dt`, `latitude`, `longitude`) are missing —
an early, loud signal of upstream API drift.

### `services/transformers.py`

Pure functions; defensive per template convention (skip and count bad records,
never fail the batch).

- `to_individual_events(records)` — one Gundi event per record:
  `event_type="mdt_debris"`, `recorded_at=dt`, location from lat/lon,
  `event_details` = material, item type (`master_name`), description, quantity,
  degraded flag, list name, place name, `mdt_id`.
- `to_aggregate_events(records, grid_km, window)` — bucket by
  (grid cell, time window); one event per bucket:
  `event_type="mdt_debris_summary"`, centroid location, details = record count,
  total quantity, quantity by material, top item types, bucket bounds.

### `actions/handlers.py`

Standard template orchestration: read state → fetch → filter (bounding box,
dedup) → transform per `mapping_mode` → `send_events_to_gundi` → update state →
log activity summary.

## Incremental extraction

Upstream `start`/`end` are date-granular, so a pure timestamp high-water mark
would refetch or miss same-day records. Scheme:

1. Fetch from `date(high_water_dt) − lookback_days` to today.
2. Keep records where `dt > high_water_dt` **or** `id` not in the
   recently-seen set.
3. State via `IntegrationStateManager`:
   `{"high_water_dt": ..., "seen_ids": [...]}` — `seen_ids` holds only ids
   within the lookback window, pruned each run, so it stays bounded.
4. Aggregate mode: buckets for the current (incomplete) window are held back
   until the window closes, so each summary event is emitted exactly once.

## Error handling & observability

- API failures: `log_action_activity(LogLevel.ERROR, ...)` with response
  details, then re-raise so the run is marked failed (framework retry).
- Record-level problems: skip, count, one WARNING log with the skip count.
- Every run returns
  `{"events_sent", "records_fetched", "records_skipped", "mode"}`.

## Explorer utility (`tools/mdt_explore.py`)

An ad-hoc exploration tool for learning MDT's data shape and volume and for
sharing findings with stakeholders while the integration's value is assessed.
One utility, two interfaces:

- **CLI** (argparse + the same `MDTClient`):
  - `query --start --end [--list] [--bbox] [--limit] [--json|--csv out.csv]` —
    fetch and print records as a table, or export.
  - `stats --start --end [--list] [--by month|list|material]` — volume
    exploration built on cheap `totalRecords` probes per bucket (no full
    pagination); prints counts per month, per list, or material breakdowns.
  - `lists` — discover organization/list ids via the `/organizations` endpoint.
  - `serve [--port 8001]` — launch the web form below.
- **Web form** (`serve` subcommand): a single-page local FastAPI app — form
  fields for date range, list id, bounding box; renders a results table,
  summary counts (records, total quantity, by material), and a CSV download.
  The browser talks only to the local server, which **proxies** MDT requests
  server-side — no CORS dependency on the upstream API.

The utility reuses `MDTClient` for all upstream access, so exploration
exercises the exact code path the connector ships with. It lives in the connector repo under `tools/`, excluded
from the service container.

## Testing

- **Transformers:** pure-function tests with real captured sample records —
  both modes, bad-record tolerance, aggregation bucketing edges (cell
  boundaries, window rollover).
- **Client:** `respx`-mocked httpx — pagination, retry/backoff, `MDTSchemaError`
  on drift.
- **Handler:** mocked client / `send_events_to_gundi` / state manager —
  high-water-mark advance, dedup across overlapping runs, bounding-box
  filtering, held-back incomplete aggregate buckets, global-pull WARNING.
- **Explorer:** `stats` bucketing math unit-tested; `serve` smoke test via
  FastAPI `TestClient` with a mocked `MDTClient`.

## Risks & prerequisites

1. **Unofficial API.** Before deployment (not before building), email the MDT
   team (debristracker101@gmail.com) for blessing and stability expectations.
   Configurable `base_url` mitigates a move; `MDTSchemaError` surfaces drift.
2. **Date-granular filters.** Fully handled by lookback + dedup, but a very
   large `lookback_days` against a busy list grows fetch size linearly.
3. **Destination event types.** `mdt_debris` / `mdt_debris_summary` must exist
   in the destination EarthRanger site — documented in the connector README,
   not enforced by the connector.
