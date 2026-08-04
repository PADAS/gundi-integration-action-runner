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
