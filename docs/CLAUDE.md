# CLAUDE.md – Gundi Kineis integration

Project-specific context for AI assistants working in this repo.

## What this repo is

- **Gundi v2 pull integration** for **Kineis/CLS** telemetry (Jira CONNECTORS-836).
- Pulls telemetry from the CLS API (bulk and realtime) and sends observations to Gundi/Earth Ranger.
- Python 3.10+, async where appropriate. Uses **httpx** (async), **pydantic** (~1.10), **FastAPI**-style app.

## Layout

- **`app/actions/`** – Kineis actions and config: `handlers.py` (auth, pull_telemetry), `configurations.py` (AuthenticateKineisConfig, PullTelemetryConfiguration, get_auth_config), `transformers.py` (telemetry → Gundi observations).
- **`app/services/kineis_client.py`** – Async Kineis/CLS API client used by the integration: token cache, `retrieve_bulk_telemetry`, `retrieve_realtime_telemetry`, `fetch_telemetry`, `fetch_telemetry_realtime`. Single device list (deviceRefs **or** deviceUids per API).
- **`app/services/state.py`** – `IntegrationStateManager` (Redis): get/set state per integration+action; used for realtime checkpoint (`kineis_realtime_checkpoint`).
- **`app/datasource/kineis_client.py`** – **Reference/example** client: async, httpx, `KineisClient` with `get_devices()`, `fetch_bulk()`, `fetch_realtime()`, `poll_realtime_locations()`. Not used by the action runner; illustrates patterns.
- **`app/settings/integration.py`** – Kineis env: `KINEIS_AUTH_BASE_URL`, `KINEIS_API_BASE_URL`, `KINEIS_AUTH_PATH`.
- **`app/services/tests/test_kineis_*.py`** – Tests for client, transformers, and actions (mocked auth, fetch, state).

## Conventions

- **Credentials:** From the Auth action config via `get_auth_config(integration)`; not from env in production.
- **Device filter:** Only one of `device_refs` or `device_uids` (validated in config; client sends one).
- **Realtime:** Checkpoint stored in Redis state; if state unavailable or `use_realtime=False`, fall back to bulk with lookback.
- **API:** Aligned with API-Telemetry User Manual v1.2 (CLS) and OpenAPI spec: cursor pagination (first/after), retrieve-bulk and retrieve-realtime.

## Where to change things

- Add/change pull or auth behavior → `app/actions/handlers.py` and `app/actions/configurations.py`.
- Change how we call CLS (bulk/realtime, params) → `app/services/kineis_client.py`.
- Change telemetry → observation mapping → `app/actions/transformers.py` (handles `msgTs`, `gpsLocLat`/`gpsLocLon`, `dopplerLocLat`/`dopplerLocLon`, etc.).
- Reference implementation / standalone scripts → `app/datasource/kineis_client.py`.

## Testing

- Kineis tests: `pytest app/services/tests/test_kineis_client.py app/services/tests/test_kineis_transformers.py app/services/tests/test_kineis_action.py -v`
- Pull tests use `use_realtime=False` and mock `fetch_telemetry`; realtime path is tested with mocked state and `fetch_telemetry_realtime`.

## Docs

- **README.md** – User-facing setup and Kineis flow (realtime vs bulk, datasource example).
- **`docs/kineis-api-reference.md`** – Kineis API reference: endpoints, auth roles, message types (`msgType`), message structure (core and optional fields for GPS/Doppler, datetime formats). Use when interpreting response data or mapping to observations.
- **`docs/kineis-api-samples/`** – Sample request/response payloads for Kineis endpoints: `retrieve-bulk-request.json`, `retrieve-bulk-response.json`, `retrieve-realtime-request.json`, `retrieve-realtime-response.json`. Use these when matching client code or transformers to the API shape.
- **`docs/`** – Project and AI context (this file). API PDF/OpenAPI spec are external (e.g. ~/Downloads).
