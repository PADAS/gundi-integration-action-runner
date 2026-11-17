# Migrating an existing fork to the gundi-action-runner library

Forks of this template keep working without changes: merging upstream gives
you compatibility shims (`app/services/*` etc. re-export the library, with
`DeprecationWarning`s) and the framework rides in-tree under `src/` until you
migrate. Migration is optional and incremental.

## Step 0 — merge upstream (nothing else changes)

After merging, your CI (inherited via `.github/workflows/_tests.yml`) runs
`pip install -e . --no-deps` automatically. Your Dockerfile is fork-owned: add
the equivalent lines (`COPY pyproject.toml .` / `COPY ./src src/` /
`RUN pip install -e . --no-deps`) before deploying, or the container will fail
at startup since the `app/*` shims import `gundi_action_runner`. Your
handlers, configurations, tests, and `uvicorn app.main:app` all keep working.

**What to expect during the merge:**

- **`app/conftest.py` will conflict** if you appended custom fixtures (most
  forks did). Resolution is mechanical: keep the upstream star-import line
  AND your custom fixtures below it.
- **Your `pytest` run now also collects upstream suites** (`tests/`,
  `examples/` via the inherited `pyproject.toml` testpaths). They pass in a
  fork context and pin the compatibility contract — treat failures there as
  signals, not noise. Trim `testpaths` if you must.
- **DeprecationWarnings** from `app.*` imports are expected — they mark the
  shim layer, which is removed after an announced window.

## Step 1 — adopt decorators in place (optional, incremental)

Decorate handlers inside your existing `app/actions/handlers.py`; the legacy
import fires the decorators, so decorated and `action_`-prefixed handlers can
coexist in that file:

```python
from gundi_action_runner import action

@action.pull(config=PullObservationsConfig, title="Pull Observations")
async def pull_observations(integration, action_config): ...   # was action_pull_observations
```

**Caution — moving handlers to a NEW module:** discovery via
`GUNDI_HANDLERS_MODULES` skips the legacy scan once ANY action is registered.
Don't split handlers across a new decorator module and a legacy module — move
them all at once, or keep decorating in place.

## Step 2 — cut over to the library layout

1. Point discovery at your module: set `GUNDI_HANDLERS_MODULES=myconnector.handlers`
   (or `app = create_app(handlers_modules=["myconnector.handlers"])` in `main.py`).
2. Change `app.*` imports to `gundi_action_runner.*` (mechanical
   find/replace; the shims made both names the same module objects).
3. Add `gundi-action-runner~=X.Y` to your requirements, delete the inherited
   `src/` tree and `app/` shims, keep only your connector code.

## Behavior changes to know about

- **Handler discovery is lazy.** The template scanned `app.actions.handlers`
  at import; the library populates on `create_app()` /
  `register_integration_in_gundi()` / first `execute_action()`. A broken
  import inside your handlers module now fails at first use instead of at
  process import — still loudly, just later.
- **Decorator ordering:** `@action.*`/`@webhook` must be the outermost
  decorator (see the [extension API](extension-api.md)).
- **Legacy discovery is stricter.** `action_`-prefixed functions without an
  `action_config` parameter are skipped with a warning instead of being
  registered (and the `action_title` decorator itself is never mistaken for
  a handler when imported alongside them). If an action disappears after
  merging, check its signature.
- **Gundi OAuth tokens are cached and shared.** With `gundi-client-v2 >= 3.7`
  every Gundi client the runner builds — including a bare `GundiClient()` in
  your own code — shares one token per set of credentials, in process memory
  and in Redis database `REDIS_TOKEN_CACHE_DB` (default `2`). Nothing to
  configure unless that database is taken: set `REDIS_TOKEN_CACHE_DB`, or
  `GUNDI_TOKEN_CACHE_URL` for a different backend (`file:///dir`, or `""` for
  in-process only). Tests that count token requests get the process cache
  cleared between tests by the framework's pytest plugin.
- **Preferred OAuth variable names are `GUNDI_OAUTH_*`.** `OAUTH_*` and
  `KEYCLOAK_*` still work; when both spellings are set the `GUNDI_`-prefixed
  one wins. Scaffolded `.env` examples emit the new names.
- **Batched fan-outs.** `trigger_actions(integration_id, action_id, configs)`
  and `publish_events(events, topic)` publish one request per batch instead of
  one session, token and round trip per message; batches split on Pub/Sub's
  count and byte limits. Use them where you loop over `trigger_action`.
- **A scaffolded project carries the fork's CI.** `gundi-runner new` emits
  `.github/workflows/{pr.yaml,main.yaml,_tests.yml}` mirroring the template's
  tests → build → `update_hcl` pipeline; a fork that cuts over to the library
  layout keeps its own workflows and only changes how tests install (`pip
  install -e ".[dev]"` instead of `pip-compile`).
- **Action activity-log events are redacted.** The configuration, request
  and response attached to action events are masked before publishing: values
  under secret-looking keys, and fields your config model declares as
  `SecretStr`, `Field(format="password")`, `UIOptions(widget="password")` or
  `Field(writeOnly=True)` (`gundi_action_runner.services.redaction`). Webhook
  failure events still carry the saved webhook configuration unredacted, as on
  the template; keep webhook secrets out of that configuration until that
  lands upstream.
- **A classified bad response with a permanent status is not redelivered.**
  An `IntegrationBadResponseError` carrying a 4xx other than 429 (on itself or
  on the exception it was raised from) is acked as final, like a raw HTTP 4xx.
  Map a status that is transient for your provider to
  `IntegrationRateLimitError` to keep it retried.
- **The integration type description is configurable.** Set
  `INTEGRATION_TYPE_DESCRIPTION`, or pass `gundi-runner register --description`,
  instead of editing the default "Default type for integrations with <name>"
  in the Gundi admin after each registration.
- **Transient config-event failures are redelivered.** A configuration event
  that fails because the portal or Redis is briefly unavailable now answers
  Pub/Sub with a 500 instead of being acked and dropped; a final failure (a
  4xx, a bug) is still acked. Late redelivery is only safe on an ordered
  subscription, which the standard deployment has: the portal publishes config
  events under one ordering key and the config-events subscription enables
  message ordering.
- **Outbound URL allowlist entries are trimmed.** `a.com, b.com` now matches
  both hosts; the whitespace `env.list()` keeps after a comma used to make
  every entry but the first unmatchable.
- **`async_return()` no longer returns an `asyncio.Future`.** It returns a
  loop-independent awaitable (`ResolvedAwaitable`), so sync fixtures keep
  working on pytest-asyncio 1.x. Awaiting it is unchanged; a test that called
  `Future` methods on the result (`.result()`, `.done()`) needs adjusting.
- **FastAPI moves to the 0.125 line (starlette 0.50.x), marshmallow to 3.26.**
  0.125 is the last FastAPI line that accepts pydantic 1.x. A fork that merges
  upstream also gets the two-lock layout: `requirements.txt` (production image)
  is compiled from `requirements-base.in` and `requirements.in` alone, and test
  dependencies compile into `requirements-dev.txt` from `requirements-dev.in`
  and the fork-owned `requirements-test.in`. Move test-only packages out of
  `requirements.in`, and install `requirements-dev.txt` in your own CI.

## If you customized framework files

Forks that edited `app/services/*` or other framework internals will see
merge conflicts against the shim layer (upstream replaced those files with
one-line re-exports). Resolve them in two steps: keep the shim, then re-apply
your customization to the corresponding `gundi_action_runner` module — or
drop it if upstream has since absorbed the fix. Framework changes now land in
the library, so this porting step happens once; afterwards, framework updates
arrive via `pip install -U gundi-action-runner` with no merges at all.
- `python -m app.register` still works; `gundi-runner register` is its
  library-native replacement.
