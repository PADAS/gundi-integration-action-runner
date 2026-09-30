# Extension API reference

## Registering actions

```python
from gundi_action_runner import action

@action.auth(config=AuthConfig)
async def auth(integration, action_config): ...

@action.pull(config=PullConfig, title="Pull Observations")
async def pull_observations(integration, action_config): ...

@action.push(config=PushConfig)
async def push_positions(integration, action_config, data: PositionData, metadata): ...

@action.generic(config=MaintenanceConfig, id="run_maintenance")
async def maintenance(integration, action_config): ...
```

- Handlers must be `async`. The action id defaults to the function name;
  override with `id=`.
- `config=` must subclass the matching base
  (`AuthActionConfiguration`, `PullActionConfiguration`,
  `PushActionConfiguration`, `GenericActionConfiguration` from
  `gundi_action_runner.actions.core`).
- `title=` sets the display name registered in the Gundi portal.
- Push actions must accept a `data` parameter annotated with a pydantic
  model, plus a `metadata` parameter.
- Invalid registrations (duplicate id, wrong config base, missing
  parameters, sync function) raise `RegistryError` at import time with a
  message naming the offending function.

### Decorator ordering

`@action.*` / `@webhook` must be the **outermost (topmost)** decorator — it
registers the exact function object it receives. Wrapping decorators go
below it:

```python
@action.pull(config=PullConfig, title="Pull Observations")
@crontab_schedule("*/15 * * * *")
@activity_logger()
async def pull_observations(integration, action_config): ...
```

## Registering a webhook handler

```python
from gundi_action_runner import webhook

@webhook
async def webhook_handler(payload: MyPayload, integration, webhook_config: MyConfig): ...
```

One webhook handler per connector. Payload/config models are introspected
from the annotations (or passed explicitly:
`@webhook(payload=MyPayload, config=MyConfig)`).

## Building the app

```python
from gundi_action_runner import create_app

app = create_app(handlers_modules=["myconnector.handlers"])
```

Without arguments, `create_app()` reads `GUNDI_HANDLERS_MODULES`
(comma-separated import paths) and falls back to scanning the legacy
template convention (`GUNDI_LEGACY_ACTIONS_MODULE` /
`GUNDI_LEGACY_WEBHOOKS_MODULE`, defaulting to `app.actions.handlers` /
`app.webhooks.handlers`) when the registry is empty.

## Framework services

| Import | Purpose |
|---|---|
| `gundi_action_runner.services.gundi` | `send_observations_to_gundi()`, `send_events_to_gundi()` |
| `gundi_action_runner.services.state.IntegrationStateManager` | Cursors / high-water marks between runs |
| `gundi_action_runner.services.activity_logger` | `@activity_logger()`, `log_action_activity()` |
| `gundi_action_runner.services.action_scheduler` | `@crontab_schedule("*/15 * * * *")` |
| `gundi_action_runner.services.utils` | `FieldWithUIOptions`, `UIOptions`, `GlobalUISchemaOptions` |

## Error reporting

When a handler raises, the framework logs the failure to the portal activity
log. Two levels of polish are available:

**Automatic classification.** Failures during handler execution are
classified heuristically: HTTP 401/403 responses render as authentication
failures, 429 as rate limiting, 5xx as a bad provider response, and
connection/timeout exceptions as connectivity problems. Operators see a
short, human-first message ("Authentication failed — ... (HTTP 401)")
instead of a raw traceback; full details are still captured alongside.

**Explicit classification.** For precise control, raise one of the
`IntegrationError` subclasses from your handler:

```python
from gundi_action_runner.services.errors import (
    IntegrationAuthError,          # "Authentication failed"
    IntegrationConnectionError,    # "Could not reach the provider"
    IntegrationRateLimitError,     # "Rate limited by the provider"
    IntegrationBadResponseError,   # "Unexpected response from the provider"
)


@action.pull(config=PullConfig, title="Pull Observations")
async def pull_observations(integration, action_config):
    response = await client.fetch(...)
    if response.status_code == 401:
        raise IntegrationAuthError("API key rejected", status_code=401)
    ...
```

Explicitly raised `IntegrationError`s always win over the heuristics and
classify in every context (the heuristics only apply to handler execution,
so e.g. a 401 from Gundi's own portal API is never misreported as a
provider auth failure). The same classification also flows through
`@activity_logger()`-decorated handlers.

## Testing your connector

Installing `gundi-action-runner` registers a pytest plugin exposing the
framework's fixtures (`integration_v2`, `mock_gundi_client_v2`,
`mock_publish_event`, ...) with no conftest wiring. Test deps ship as an
extra:

```bash
pip install "gundi-action-runner[testing]"
```
