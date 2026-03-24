# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

This is a **Gundi v2 integration** — a FastAPI service that receives and processes webhooks from external data sources and forwards transformed data to the [Gundi](https://gundiservice.org) platform. It supports both pull-based actions (triggered via GCP PubSub) and push-based webhooks.

This specific integration implements a **generic webhook handler** using JQ filters for JSON-to-JSON transformations, supporting both observations (`obv`) and events (`ev`) as output types.

## Commands

### Install dependencies
```bash
pip install -r requirements.txt
```

### Run tests
```bash
pytest
```

### Run a single test
```bash
pytest app/services/tests/test_webhooks.py::test_process_webhook_request_with_fixed_schema -v
```

### Run with coverage
```bash
pytest --tb=short -v
```

### Run the server locally
```bash
uvicorn app.main:app --reload --port 8080
```

### Compile dependencies (after editing `requirements.in`)
```bash
pip-compile --output-file=requirements.txt requirements-base.in requirements-dev.in requirements.in
```

### Local development with Docker
```bash
cd local && docker compose up --build
```
API docs available at http://localhost:8080/docs

## Architecture

### Request flow

1. **Webhook ingress**: `POST /webhooks` → `app/routers/webhooks.py` → `app/services/webhooks.py::process_webhook()`
2. **Integration lookup**: The service resolves the integration from the request header `x-consumer-username` (Kong gateway) or `x-gundi-integration-id` or query param `integration_id`
3. **Handler introspection**: `app/webhooks/core.py::get_webhook_handler()` dynamically loads `app/webhooks/handlers.py::webhook_handler` and inspects type annotations to determine payload and config models
4. **Payload parsing**: If `GenericJsonPayload` + `DynamicSchemaConfig`, a Pydantic model is built at runtime from the JSON schema stored in Gundi. Otherwise the annotated model is used directly.
5. **JQ transformation**: The handler applies a `jq_filter` from the webhook config to transform the payload
6. **Gundi forwarding**: Transformed data is sent via `app/services/gundi.py` as observations or events

### Action flow (PubSub)

- `POST /` → decodes base64 GCP PubSub message → `app/services/action_runner.py::execute_action()`
- Action handlers live in `app/actions/handlers.py`, configs in `app/actions/configurations.py`
- Actions can be scheduled via `@crontab_schedule()` decorator or `app/register.py`

### Key modules

| Path | Purpose |
|------|---------|
| `app/webhooks/handlers.py` | **Entry point for customization** — implement `webhook_handler()` here |
| `app/webhooks/configurations.py` | Payload and config Pydantic models for this integration |
| `app/webhooks/core.py` | Base classes: `WebhookPayload`, `WebhookConfiguration`, `GenericJsonTransformConfig`, `DynamicSchemaConfig`, `HexStringConfig` |
| `app/actions/handlers.py` | Pull/push action handlers |
| `app/actions/configurations.py` | Action configuration models |
| `app/services/webhooks.py` | Orchestrates webhook processing, payload parsing, error publishing |
| `app/services/action_runner.py` | Orchestrates action execution |
| `app/services/gundi.py` | Sends observations/events to Gundi API |
| `app/services/activity_logger.py` | `@activity_logger()` / `@webhook_activity_logger()` decorators + `log_activity()` |
| `app/services/config_manager.py` | Fetches and caches integration config from Gundi (Redis-backed, 60s TTL) |
| `app/services/utils.py` | `FieldWithUIOptions`, `UIOptions`, `GlobalUISchemaOptions`, `StructHexString`, `DyntamicFactory` |
| `app/settings/base.py` | All env-var-driven settings |
| `app/conftest.py` | Shared pytest fixtures for the entire test suite |

### Webhook configuration modes

- **Fixed schema**: Annotate `payload` with a `WebhookPayload` subclass; strict Pydantic validation
- **Dynamic schema**: Annotate `payload` with `GenericJsonPayload` + `webhook_config` with `DynamicSchemaConfig`; Pydantic model built at runtime from `json_schema` stored in Gundi portal
- **JQ transform**: Annotate `webhook_config` with `GenericJsonTransformConfig`; applies `jq_filter` and routes to `obv` (observations) or `ev` (events) output
- **Hex string**: Use `HexStringPayload` + `HexStringConfig` for binary data encoded as hex strings; parsed using Python `struct` format strings

### UI schema customization

Use `FieldWithUIOptions(...)` with `UIOptions(widget=...)` and `GlobalUISchemaOptions(order=[...])` in config models to control how fields render in the Gundi portal (uses react-jsonschema-form ui schema).

## Testing

Tests use `pytest-asyncio` and `pytest-mock`. All external services (Gundi API, PubSub, Redis) are mocked. The `app/conftest.py` contains shared fixtures including mock integrations, mock webhook handlers, and mock request headers/payloads.

Test files mirror the service structure under `app/services/tests/`.

## Key env vars

| Variable | Purpose |
|----------|---------|
| `GUNDI_API_BASE_URL` | Gundi platform API endpoint |
| `KEYCLOAK_CLIENT_SECRET` | Auth secret (required for local dev against stage) |
| `INTEGRATION_TYPE_SLUG` | Unique identifier for this integration type |
| `INTEGRATION_SERVICE_URL` | Public URL of this service (for self-registration) |
| `REGISTER_ON_START` | Set `true` to auto-register with Gundi on startup |
| `REDIS_HOST` / `REDIS_PORT` | Config cache and state store |
| `INTEGRATION_EVENTS_TOPIC` | GCP PubSub topic for activity/error events |
| `PROCESS_WEBHOOKS_IN_BACKGROUND` | Default `true`; processes webhooks async |
