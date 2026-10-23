# Gundi Action Runner Library Extraction — Implementation Plan (Plan 1 of 3)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract this template's framework code into an installable `gundi-action-runner` package (src layout) with a decorator registry and `create_app()` factory, while `app/` becomes fork-compat shims so existing forks keep working after merging upstream.

**Architecture:** Framework modules move from `app/` to `src/gundi_action_runner/` with imports rewritten. A new `registry.py` + `decorators.py` replace name-prefix scanning (which survives as a "legacy loader" for fork migration). Every moved `app/*` leaf module becomes a `sys.modules`-aliasing shim so `app.services.X` *is* the library module — underscore names and `mocker.patch("app.services...")` targets keep working in forks. `app/main.py` becomes `app = create_app()`.

**Tech Stack:** Python 3.10, FastAPI 0.115, pydantic 1.10 (v1 — required by gundi-client-v2 3.x), httpx 0.28, gundi-client-v2 3.5, setuptools src layout, pytest 7.4 + pytest-asyncio 0.21 + pytest-mock.

**Spec:** `docs/superpowers/specs/2026-07-07-action-runner-library-design.md` (this plan covers spec phases 1–3; the pytest-plugin entry point, PyPI publishing, and CLI/copier are Plans 2–3).

## Global Constraints

- Work on branch `design/action-runner-library`. Do NOT merge to `main` in this plan — the merge gate is Plan 2 (published package). Forks merging would still work (the library rides in-tree under `src/` and CI/Dockerfile gain `pip install -e .`), but hold the merge until publishing exists.
- **Never edit these fork-owned files** (forks have replaced their contents; upstream edits cause merge conflicts): `app/actions/handlers.py`, `app/actions/configurations.py`, `app/webhooks/handlers.py`, `app/webhooks/configurations.py`, `app/settings/integration.py`, `app/register.py`.
- **pydantic stays `~=1.10.15` (v1).** `gundi-client-v2 3.5.0` metadata: `Requires-Dist: pydantic<2,>=1.10` — verified 2026-07-07. Do not migrate to pydantic v2.
- Dependency floors: `fastapi~=0.115.0`, `uvicorn~=0.30.0`, `gundi-client-v2~=3.5`, `httpx>=0.28` (transitive), `requires-python = ">=3.10"`.
- Package name `gundi-action-runner`, import name `gundi_action_runner`, version `0.1.0.dev0`.
- Test command is `pytest` from the repo root inside `.venv` (`source .venv/bin/activate`). The full suite must be green at the end of every task.
- Rewrite scripts are Python (written to the session scratchpad), not `sed` — this is macOS; avoid `sed -i` portability traps.
- Commit messages end with:
  `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`

---

### Task 1: Dependency modernization

Bump fastapi/uvicorn/gundi-client-v2 so the library never inherits the httpx-0.28 TestClient breakage (absorbs `FIX_TESTCLIENT_PLAN.md`).

**Files:**
- Modify: `requirements-base.in`
- Regenerate: `requirements.txt`

**Interfaces:**
- Consumes: nothing (first task).
- Produces: an environment where `starlette.testclient.TestClient(app)` works with `httpx>=0.28`, and `gundi_client_v2` is 3.5.x. All later tasks assume these pins.

- [ ] **Step 1: Edit `requirements-base.in`**

Change exactly three lines:

```diff
-pydantic~=1.10.15
-fastapi~=0.103.2
-uvicorn~=0.23.2
+pydantic~=1.10.15
+fastapi~=0.115.0
+uvicorn~=0.30.0
```
```diff
-gundi-client-v2~=2.4.0
+gundi-client-v2~=3.5
```

(`pydantic` line shown for anchoring only — it does not change. `gundi-core~=1.11.1` also stays: gundi-client-v2 3.5 requires `gundi-core<3`.)

- [ ] **Step 2: Recompile the lockfile**

```bash
source .venv/bin/activate
pip install pip-tools
pip-compile --output-file=requirements.txt requirements-base.in requirements-dev.in requirements.in
```

Expected in the `requirements.txt` diff: `fastapi==0.115.x`, `starlette==0.41.x` (or later), `uvicorn==0.30.x`, `httpx==0.28.x`, `gundi-client-v2==3.5.x`. No other major-version jumps. If something else moves a major version, stop and report before proceeding.

- [ ] **Step 3: Install and smoke-import**

```bash
pip install -r requirements.txt
python -c "from gundi_client_v2 import GundiClient; import gundi_core.events; import httpx; print(httpx.__version__)"
```

Expected: prints `0.28.x` (or later), no ImportError. If `GundiClient` fails to import, gundi-client-v2 3.x changed its public surface — STOP and report; do not improvise renames.

- [ ] **Step 4: Run the full suite**

```bash
pytest
```

Expected: all tests pass (same count as on `main` today). The three TestClient-using files (`test_action_runner.py`, `test_config_events_consumer.py`, `test_self_registration.py`) collect and pass.

- [ ] **Step 5: Commit**

```bash
git add requirements-base.in requirements.txt
git commit -m "Modernize deps: fastapi 0.115, uvicorn 0.30, gundi-client-v2 3.5 (httpx 0.28-compatible)"
```

---

### Task 2: Package scaffolding

Create the installable package skeleton and teach CI + Docker to install it, before any code moves.

**Files:**
- Create: `pyproject.toml`, `src/gundi_action_runner/__init__.py`
- Modify: `.github/workflows/_tests.yml`, each Dockerfile under `docker/`

**Interfaces:**
- Consumes: pins from Task 1.
- Produces: `import gundi_action_runner` works after `pip install -e . --no-deps`; `gundi_action_runner.__version__ == "0.1.0.dev0"`. CI and Docker images install the in-repo package, which every later task relies on.

- [ ] **Step 1: Write `pyproject.toml`**

```toml
[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.build_meta"

[project]
name = "gundi-action-runner"
version = "0.1.0.dev0"
description = "Framework for building Gundi integration action runners"
readme = "README.md"
license = { file = "LICENSE" }
requires-python = ">=3.10"
dependencies = [
    "environs~=9.5",
    "pydantic~=1.10.15",        # v1 — gundi-client-v2 3.x requires pydantic<2
    "fastapi~=0.115.0",
    "uvicorn~=0.30.0",
    "gundi-core~=1.11.1",
    "gundi-client-v2~=3.5",
    "stamina~=23.2.0",
    "redis~=5.0.1",
    "gcloud-aio-pubsub~=6.0.0",
    "click~=8.1.7",
    "pyjq~=2.6.0",
    "python-json-logger~=2.0.7",
    "marshmallow~=3.22.0",
]

[project.optional-dependencies]
dev = [
    "pytest~=7.4.3",
    "pytest-asyncio~=0.21.1",
    "pytest-mock~=3.12.0",
]

[tool.setuptools.packages.find]
where = ["src"]

[tool.pytest.ini_options]
testpaths = ["app"]
```

`testpaths = ["app"]` matches today's collection (tests live under `app/services/tests/`). Task 3 changes it to `["tests", "app"]`; Task 6 adds `"examples"`. Keeping `"app"` permanently means forks' tests (which live under `app/`) still collect after merging this pyproject.

- [ ] **Step 2: Create the package skeleton**

```bash
mkdir -p src/gundi_action_runner
```

`src/gundi_action_runner/__init__.py`:

```python
__version__ = "0.1.0.dev0"
```

- [ ] **Step 3: Install editable and verify**

```bash
pip install -e . --no-deps
python -c "import gundi_action_runner; print(gundi_action_runner.__version__)"
```

Expected: `0.1.0.dev0`.

- [ ] **Step 4: Add the install step to CI**

In `.github/workflows/_tests.yml`, after the `Install dependencies` step and before `Run unit tests`, add:

```yaml
      - name: Install the gundi-action-runner library (in-repo)
        run: pip install -e . --no-deps
```

- [ ] **Step 5: Add the install to every Dockerfile**

```bash
ls docker/
```

For each Dockerfile found (expected: a main one and possibly a debug variant), add immediately after the existing `COPY ./app app/` line:

```dockerfile
COPY pyproject.toml .
COPY ./src src/
RUN pip install -e . --no-deps
```

This keeps `uvicorn app.main:app` working once `app/` becomes shims (Task 3+), both here and in forks that merge upstream before the package is on PyPI.

- [ ] **Step 6: Run the suite (must be unaffected)**

```bash
pytest
```

Expected: identical pass count to Task 1.

- [ ] **Step 7: Commit**

```bash
git add pyproject.toml src/ .github/workflows/_tests.yml docker/
git commit -m "Scaffold gundi-action-runner package (src layout); install it in CI and Docker"
```

---

### Task 3: Move framework code into the library; shim `app/`

The big mechanical move. Modules migrate with imports rewritten; every vacated `app/*` path becomes a compat shim; the test suite and conftest fixtures move too. Suite must be green at the end (single test cycle for the whole move — intermediate states cannot pass).

**Files:**
- Move (git mv): all of `app/services/*.py` except `__init__.py` and `tests/` → `src/gundi_action_runner/services/`; `app/routers/{actions,webhooks,config_events}.py` → `src/gundi_action_runner/routers/`; `app/actions/core.py` → `src/gundi_action_runner/actions/core.py`; `app/actions/__init__.py` → `src/gundi_action_runner/actions/__init__.py`; `app/webhooks/core.py` → `src/gundi_action_runner/webhooks/core.py`; `app/webhooks/__init__.py` → `src/gundi_action_runner/webhooks/__init__.py`; `app/api_schemas.py` → `src/gundi_action_runner/api_schemas.py`; `app/settings/base.py` → `src/gundi_action_runner/settings.py`; `app/conftest.py` → `src/gundi_action_runner/testing/fixtures.py`; `app/services/tests/test_*.py` → `tests/`
- Create: shims at every vacated `app/` path; `src/gundi_action_runner/{services,routers,testing}/__init__.py` (empty); `tests/conftest.py`; `app/conftest.py` (re-export); `tests/test_shims.py`
- Modify (in place): `app/main.py` (imports only — it keeps building the app until Task 5)
- Do not touch: `app/actions/handlers.py`, `app/actions/configurations.py`, `app/webhooks/handlers.py`, `app/webhooks/configurations.py`, `app/settings/integration.py`, `app/register.py`, `app/services/__init__.py`

**Interfaces:**
- Consumes: package skeleton from Task 2.
- Produces: `gundi_action_runner.services.*`, `.routers.*`, `.actions.core`, `.actions` (still using today's eager `discover_actions("app.actions.handlers")` — Task 5 rewires it), `.webhooks.core`, `.api_schemas`, `.settings`, `.testing.fixtures`. Legacy `app.*` imports resolve to these same module objects. Tests live in `tests/`.

- [ ] **Step 1: Move the files**

```bash
mkdir -p src/gundi_action_runner/services src/gundi_action_runner/routers \
         src/gundi_action_runner/actions src/gundi_action_runner/webhooks \
         src/gundi_action_runner/testing tests
touch src/gundi_action_runner/services/__init__.py \
      src/gundi_action_runner/routers/__init__.py \
      src/gundi_action_runner/testing/__init__.py

for m in action_runner action_scheduler activity_logger config_events_consumer \
         config_manager core errors gundi self_registration state utils webhooks; do
  git mv app/services/$m.py src/gundi_action_runner/services/$m.py
done
for r in actions webhooks config_events; do
  git mv app/routers/$r.py src/gundi_action_runner/routers/$r.py
done
git mv app/actions/core.py src/gundi_action_runner/actions/core.py
git mv app/actions/__init__.py src/gundi_action_runner/actions/__init__.py
git mv app/webhooks/core.py src/gundi_action_runner/webhooks/core.py
git mv app/webhooks/__init__.py src/gundi_action_runner/webhooks/__init__.py
git mv app/api_schemas.py src/gundi_action_runner/api_schemas.py
git mv app/settings/base.py src/gundi_action_runner/settings.py
git mv app/conftest.py src/gundi_action_runner/testing/fixtures.py
for t in test_action_runner test_activity_logger test_config_events_consumer \
         test_config_manager test_gundi_api test_self_registration test_state_manager; do
  git mv app/services/tests/$t.py tests/$t.py
done
git rm app/services/tests/__init__.py
```

- [ ] **Step 2: Rewrite imports with a script**

Write to the scratchpad as `rewrite_imports.py` and run from the repo root:

```python
"""One-shot import rewrite for the library extraction: app.* -> gundi_action_runner.*"""
import pathlib
import re

TARGETS = [
    *pathlib.Path("src/gundi_action_runner").rglob("*.py"),
    *pathlib.Path("tests").rglob("*.py"),
    pathlib.Path("app/main.py"),
]
RULES = [
    (re.compile(r"\bfrom app import settings\b"), "from gundi_action_runner import settings"),
    (re.compile(r"\bimport app\.settings as settings\b"), "from gundi_action_runner import settings"),
    (re.compile(r"\bfrom app\."), "from gundi_action_runner."),
    # String literals used as mocker.patch()/import targets in tests
    (re.compile(r"([\"'])app\.(services|actions|webhooks|routers|settings|api_schemas)"),
     r"\1gundi_action_runner.\2"),
]
for path in TARGETS:
    text = original = path.read_text()
    for pattern, replacement in RULES:
        text = pattern.sub(replacement, text)
    # app.main stays app.main — the deployable entry point remains in app/
    text = text.replace("from gundi_action_runner.main import", "from app.main import")
    # The legacy discovery targets are FORK module paths, not library modules —
    # revert them (rule 4 would otherwise rewrite these string literals and
    # silently break fork-convention scanning):
    text = text.replace("gundi_action_runner.actions.handlers", "app.actions.handlers")
    text = text.replace("gundi_action_runner.webhooks.handlers", "app.webhooks.handlers")
    if text != original:
        path.write_text(text)
        print(f"rewrote {path}")
```

```bash
python /path/to/scratchpad/rewrite_imports.py
```

- [ ] **Step 3: Manual fixes after the script**

1. `src/gundi_action_runner/routers/actions.py`: delete the now-dangling line `import app.settings` (it was already unused — verified: no `app.settings.` references in the file).
2. Verify nothing in the library still references `app.` (except intentionally):

```bash
grep -rn "\bapp\." src/gundi_action_runner/ | grep -vE "app\.actions\.handlers|app\.webhooks\.handlers|\.app\b|app = |FastAPI|# "
```

Expected: no output. The two allowed exceptions are the hardcoded legacy discovery targets `"app.actions.handlers"` (in `actions/__init__.py`) and `"app.webhooks.handlers"` (in `webhooks/core.py`) — they keep working in-repo and in forks until Task 5 makes them configurable.

3. Same check for tests — expected output is ONLY `from app.main import app` lines (three files):

```bash
grep -rn "\bapp\." tests/ | grep -v "gundi_action_runner" | grep -v "app = " | grep "app\."
```

- [ ] **Step 4: Write the conftest re-exports**

`tests/conftest.py` and `app/conftest.py` get identical content (the `app/` copy keeps forks' tests working — their tests live under `app/` and use these fixtures):

```python
from gundi_action_runner.testing.fixtures import *  # noqa: F401,F403
```

(Verified: the old conftest defines only fixtures — `grep -c "def pytest_" == 0` — so star re-export is sufficient.)

- [ ] **Step 5: Generate the leaf-alias shims**

Write to the scratchpad as `make_shims.py` and run from the repo root. The `sys.modules[__name__] = <target>` pattern makes each legacy path literally *be* the library module, so underscore names (e.g. `_portal`, imported by fork-owned `app/register.py`) and `mocker.patch("app.services...")` targets in fork tests keep working:

```python
"""Generate compat shims at every vacated app/ leaf-module path."""
import pathlib

SHIMS = {
    **{f"app/services/{m}.py": f"gundi_action_runner.services.{m}"
       for m in ["action_runner", "action_scheduler", "activity_logger",
                 "config_events_consumer", "config_manager", "core", "errors",
                 "gundi", "self_registration", "state", "utils", "webhooks"]},
    "app/routers/actions.py": "gundi_action_runner.routers.actions",
    "app/routers/webhooks.py": "gundi_action_runner.routers.webhooks",
    "app/routers/config_events.py": "gundi_action_runner.routers.config_events",
    "app/actions/core.py": "gundi_action_runner.actions.core",
    "app/webhooks/core.py": "gundi_action_runner.webhooks.core",
    "app/api_schemas.py": "gundi_action_runner.api_schemas",
    "app/settings/base.py": "gundi_action_runner.settings",
}
TEMPLATE = '''"""Deprecated compatibility shim — this module moved to {target}."""
import importlib
import sys
import warnings

warnings.warn(
    "'{legacy}' is deprecated; import '{target}' instead.",
    DeprecationWarning,
    stacklevel=2,
)
sys.modules[__name__] = importlib.import_module("{target}")
'''
for legacy_path, target in SHIMS.items():
    legacy = legacy_path[:-3].replace("/", ".")
    pathlib.Path(legacy_path).write_text(TEMPLATE.format(target=target, legacy=legacy))
    print(f"shimmed {legacy_path}")
```

- [ ] **Step 6: Write the package-level shims by hand**

These three CANNOT be `sys.modules`-aliased: they are real packages containing fork-owned files (`handlers.py`, `configurations.py`, `integration.py`) that must stay importable as `app.actions.handlers` etc.

`app/actions/__init__.py` — uses PEP 562 `__getattr__` to avoid an import cycle. (The cycle: the library's `actions/__init__.py` eagerly scans `app.actions.handlers` at import; if this shim eagerly imported names back out of the partially-initialized library package, `from gundi_action_runner.actions import action_handlers` would fail whenever the library side imports first. Lazy attribute access sidesteps it — do not "simplify" this to a star import of the package.)

```python
"""Deprecated compatibility package — base classes and the handler registry moved
to gundi_action_runner.actions. This package remains so forks keep their
app/actions/handlers.py and app/actions/configurations.py files."""
import warnings

from gundi_action_runner.actions.core import *  # noqa: F401,F403

warnings.warn(
    "'app.actions' is deprecated; import 'gundi_action_runner.actions' instead.",
    DeprecationWarning,
    stacklevel=2,
)


def __getattr__(name):
    # PEP 562: resolve registry-level names (action_handlers, get_actions,
    # setup_action_handlers, get_action_handler_by_data_type) lazily to avoid
    # an import cycle with the library's eager legacy discovery.
    import gundi_action_runner.actions as _lib
    return getattr(_lib, name)
```

`app/webhooks/__init__.py` (no cycle here — webhook discovery is lazy, at call time):

```python
"""Deprecated compatibility package — webhook base classes moved to
gundi_action_runner.webhooks. This package remains so forks keep their
app/webhooks/handlers.py and app/webhooks/configurations.py files."""
import warnings

from gundi_action_runner.webhooks import *  # noqa: F401,F403

warnings.warn(
    "'app.webhooks' is deprecated; import 'gundi_action_runner.webhooks' instead.",
    DeprecationWarning,
    stacklevel=2,
)
```

`app/settings/__init__.py` (keeps the fork extension point `integration.py` merged in, as today):

```python
"""Deprecated compatibility package — framework settings moved to
gundi_action_runner.settings. app/settings/integration.py remains the
fork-specific settings extension point."""
import warnings

from gundi_action_runner.settings import *  # noqa: F401,F403
from .integration import *  # noqa: F401,F403

warnings.warn(
    "'app.settings' is deprecated; import 'gundi_action_runner.settings' instead.",
    DeprecationWarning,
    stacklevel=2,
)
```

- [ ] **Step 7: Update pytest testpaths**

In `pyproject.toml`:

```toml
[tool.pytest.ini_options]
testpaths = ["tests", "app"]
```

- [ ] **Step 8: Write the shim contract test**

`tests/test_shims.py` — the fork-compatibility contract, and the CI check the spec asked for:

```python
"""Fork-compatibility contract: every legacy app.* path must keep importing
and resolve to its gundi_action_runner counterpart."""
import importlib
import sys

import pytest

ALIASED = {
    **{f"app.services.{m}": f"gundi_action_runner.services.{m}"
       for m in ["action_runner", "action_scheduler", "activity_logger",
                 "config_events_consumer", "config_manager", "core", "errors",
                 "gundi", "self_registration", "state", "utils", "webhooks"]},
    "app.routers.actions": "gundi_action_runner.routers.actions",
    "app.routers.webhooks": "gundi_action_runner.routers.webhooks",
    "app.routers.config_events": "gundi_action_runner.routers.config_events",
    "app.actions.core": "gundi_action_runner.actions.core",
    "app.webhooks.core": "gundi_action_runner.webhooks.core",
    "app.api_schemas": "gundi_action_runner.api_schemas",
    "app.settings.base": "gundi_action_runner.settings",
}


@pytest.mark.parametrize("legacy, target", sorted(ALIASED.items()))
def test_leaf_shim_is_the_library_module(legacy, target):
    sys.modules.pop(legacy, None)
    with pytest.warns(DeprecationWarning):
        module = importlib.import_module(legacy)
    assert module is importlib.import_module(target)


def test_app_actions_package_reexports():
    import app.actions
    import gundi_action_runner.actions as lib
    from gundi_action_runner.actions.core import PullActionConfiguration, action_title
    assert app.actions.PullActionConfiguration is PullActionConfiguration
    assert app.actions.action_title is action_title
    # __getattr__-resolved registry names
    assert app.actions.action_handlers is lib.action_handlers


def test_app_settings_package_reexports():
    import app.settings
    import gundi_action_runner.settings as lib
    assert app.settings.INTEGRATION_TYPE_SLUG == lib.INTEGRATION_TYPE_SLUG
    assert hasattr(app.settings, "GUNDI_API_BASE_URL")
    assert hasattr(app.settings, "INTEGRATION_TYPE_NAME")


def test_app_webhooks_package_reexports():
    import app.webhooks
    from gundi_action_runner.webhooks.core import GenericJsonTransformConfig
    assert app.webhooks.GenericJsonTransformConfig is GenericJsonTransformConfig


def test_app_main_exposes_fastapi_app():
    from app.main import app as fastapi_app
    assert fastapi_app.title == "Gundi Integration Actions Execution Service"
```

- [ ] **Step 9: Run the full suite**

```bash
pytest
```

Expected: previous suite passes (now collected from `tests/`) plus the new shim tests. DeprecationWarnings in the output are expected and intentional. If collection errors appear, fix import paths before touching any test logic — the moved tests should not need behavioral changes.

- [ ] **Step 10: Commit**

```bash
git add -A
git commit -m "Move framework code into gundi_action_runner; shim app/* for fork compatibility"
```

---

### Task 4: Decorator registry (TDD)

New code: the `ActionRegistry` and the `@action.*` / `@webhook` decorators that replace name-prefix scanning as the primary registration mechanism.

**Files:**
- Create: `src/gundi_action_runner/registry.py`, `src/gundi_action_runner/decorators.py`, `tests/test_registry.py`
- Modify: `src/gundi_action_runner/__init__.py`

**Interfaces:**
- Consumes: `gundi_action_runner.actions.core` base classes and `discover_actions` (moved in Task 3).
- Produces:
  - `registry: ActionRegistry` singleton with `action_handlers: dict[str, tuple[func, config_model, data_model]]` (same tuple shape as today's `discover_actions`), `webhook_handler: tuple[func, payload_model, config_model] | None`, and methods `register_action`, `register_webhook`, `load_modules(module_names)`, `load_legacy_actions(module_name)`, `load_legacy_webhook(module_name)`, `reset()`.
  - `action.auth/pull/push/generic(config=..., id=None, title=None)` decorators and `webhook` decorator (bare or with `payload=`/`config=` overrides).
  - `RegistryError` exception.
  - Task 5 adds `ensure_loaded()` to the registry (needs new settings).

- [ ] **Step 1: Write the failing tests**

`tests/test_registry.py`:

```python
import sys
import types

import pytest

from gundi_action_runner.actions.core import (
    AuthActionConfiguration,
    PullActionConfiguration,
    PushActionConfiguration,
)
from gundi_action_runner.decorators import action, webhook
from gundi_action_runner.registry import RegistryError, registry


class DummyAuthConfig(AuthActionConfiguration):
    pass


class DummyPullConfig(PullActionConfiguration):
    pass


class DummyPushConfig(PushActionConfiguration):
    pass


class DummyData:
    pass


@pytest.fixture(autouse=True)
def clean_registry():
    saved_actions = dict(registry.action_handlers)
    saved_webhook = registry.webhook_handler
    registry.reset()
    yield registry
    registry.reset()
    registry.action_handlers.update(saved_actions)
    registry.webhook_handler = saved_webhook


def test_pull_decorator_registers_handler_under_function_name():
    @action.pull(config=DummyPullConfig)
    async def pull_observations(integration, action_config):
        return {}

    func, config_model, data_model = registry.action_handlers["pull_observations"]
    assert func is pull_observations
    assert config_model is DummyPullConfig
    assert data_model is None


def test_decorator_returns_the_function_unwrapped():
    @action.auth(config=DummyAuthConfig)
    async def auth(integration, action_config):
        return {}

    assert auth.__name__ == "auth"


def test_explicit_id_overrides_function_name():
    @action.pull(config=DummyPullConfig, id="pull_things")
    async def some_function(integration, action_config):
        return {}

    assert "pull_things" in registry.action_handlers
    assert "some_function" not in registry.action_handlers


def test_title_sets_action_title_attribute():
    @action.pull(config=DummyPullConfig, title="Fetch Collar Positions")
    async def pull_observations(integration, action_config):
        return {}

    assert pull_observations.action_title == "Fetch Collar Positions"


def test_duplicate_action_id_raises():
    @action.pull(config=DummyPullConfig)
    async def pull_observations(integration, action_config):
        return {}

    with pytest.raises(RegistryError, match="pull_observations"):
        @action.pull(config=DummyPullConfig, id="pull_observations")
        async def another(integration, action_config):
            return {}


def test_config_must_subclass_expected_base():
    with pytest.raises(RegistryError, match="AuthActionConfiguration"):
        @action.auth(config=DummyPullConfig)
        async def auth(integration, action_config):
            return {}


def test_handler_must_be_async():
    with pytest.raises(RegistryError, match="async"):
        @action.pull(config=DummyPullConfig)
        def not_async(integration, action_config):
            return {}


def test_handler_must_accept_integration_and_action_config():
    with pytest.raises(RegistryError, match="integration"):
        @action.pull(config=DummyPullConfig)
        async def bad(action_config):
            return {}


def test_push_requires_annotated_data_param():
    with pytest.raises(RegistryError, match="data"):
        @action.push(config=DummyPushConfig)
        async def push(integration, action_config, data, metadata):
            return {}


def test_push_requires_metadata_param():
    with pytest.raises(RegistryError, match="metadata"):
        @action.push(config=DummyPushConfig)
        async def push(integration, action_config, data: DummyData):
            return {}


def test_push_captures_data_model():
    @action.push(config=DummyPushConfig)
    async def push(integration, action_config, data: DummyData, metadata):
        return {}

    _, _, data_model = registry.action_handlers["push"]
    assert data_model is DummyData


def test_webhook_bare_decorator_introspects_annotations():
    from gundi_action_runner.webhooks.core import GenericJsonPayload, GenericJsonTransformConfig

    @webhook
    async def webhook_handler(payload: GenericJsonPayload, integration,
                              webhook_config: GenericJsonTransformConfig):
        return {}

    func, payload_model, config_model = registry.webhook_handler
    assert func is webhook_handler
    assert payload_model is GenericJsonPayload
    assert config_model is GenericJsonTransformConfig


def test_second_webhook_raises():
    @webhook
    async def webhook_handler(payload, webhook_config):
        return {}

    with pytest.raises(RegistryError, match="already registered"):
        @webhook
        async def another(payload, webhook_config):
            return {}


def test_webhook_requires_payload_and_config_params():
    with pytest.raises(RegistryError, match="webhook_config"):
        @webhook
        async def bad(payload):
            return {}


@pytest.fixture
def legacy_module():
    module = types.ModuleType("fake_legacy_handlers")

    async def action_pull_things(integration, action_config: DummyPullConfig):
        return {}

    module.action_pull_things = action_pull_things
    sys.modules["fake_legacy_handlers"] = module
    yield "fake_legacy_handlers"
    del sys.modules["fake_legacy_handlers"]


def test_load_legacy_actions_registers_prefixed_functions(legacy_module):
    registry.load_legacy_actions(legacy_module)
    func, config_model, data_model = registry.action_handlers["pull_things"]
    assert config_model is DummyPullConfig


def test_load_legacy_actions_never_overrides_decorator_registrations(legacy_module):
    @action.pull(config=DummyPullConfig, id="pull_things")
    async def decorated(integration, action_config):
        return {}

    registry.load_legacy_actions(legacy_module)
    assert registry.action_handlers["pull_things"][0] is decorated


def test_load_legacy_webhook_adopts_module_handler(legacy_module):
    async def webhook_handler(payload, webhook_config):
        return {}

    sys.modules[legacy_module].webhook_handler = webhook_handler
    registry.load_legacy_webhook(legacy_module)
    assert registry.webhook_handler[0] is webhook_handler
```

- [ ] **Step 2: Run to verify failure**

```bash
pytest tests/test_registry.py -v
```

Expected: collection error — `ModuleNotFoundError: No module named 'gundi_action_runner.decorators'`.

- [ ] **Step 3: Implement `registry.py`**

`src/gundi_action_runner/registry.py`:

```python
import importlib
import inspect
import logging

from gundi_action_runner.actions.core import PushActionConfiguration, discover_actions

logger = logging.getLogger(__name__)


class RegistryError(Exception):
    """Raised for invalid or conflicting handler registrations."""


class ActionRegistry:
    """Holds the connector's action and webhook handlers.

    Populated by the @action.* / @webhook decorators at import time, or by the
    legacy loaders that scan `action_`-prefixed functions (the template fork
    convention). `action_handlers` keeps the same tuple shape the template's
    discover_actions() produced — (func, config_model, data_model) — so the
    services bind this dict directly.
    """

    def __init__(self):
        self.action_handlers = {}
        self.webhook_handler = None  # (func, payload_model, config_model) or None

    def reset(self):
        self.action_handlers.clear()
        self.webhook_handler = None

    def register_action(self, func, *, config_model, expected_config_base,
                        action_id=None, title=None):
        if not inspect.iscoroutinefunction(func):
            raise RegistryError(
                f"Action handler '{func.__module__}.{func.__qualname__}' must be an async function."
            )
        action_id = action_id or func.__name__
        if action_id in self.action_handlers:
            existing = self.action_handlers[action_id][0]
            raise RegistryError(
                f"Duplicate action id '{action_id}': already registered by "
                f"'{existing.__module__}.{existing.__qualname__}'."
            )
        if not (inspect.isclass(config_model) and issubclass(config_model, expected_config_base)):
            raise RegistryError(
                f"Action '{action_id}' ('{func.__module__}.{func.__qualname__}'): config must "
                f"subclass {expected_config_base.__name__}, got {config_model!r}."
            )
        params = inspect.signature(func).parameters
        for required in ("integration", "action_config"):
            if required not in params:
                raise RegistryError(
                    f"Action '{action_id}' ('{func.__module__}.{func.__qualname__}') must accept "
                    f"an '{required}' parameter."
                )
        data_model = None
        if issubclass(config_model, PushActionConfiguration):
            data_param = params.get("data")
            if data_param is None or data_param.annotation is inspect.Parameter.empty:
                raise RegistryError(
                    f"Push action '{action_id}' must accept a 'data' parameter annotated "
                    f"with a data model."
                )
            if "metadata" not in params:
                raise RegistryError(f"Push action '{action_id}' must accept a 'metadata' parameter.")
            data_model = data_param.annotation
        if title:
            func.action_title = title  # read by self-registration (PR #77 convention)
        self.action_handlers[action_id] = (func, config_model, data_model)
        return func

    def register_webhook(self, func, *, payload_model=None, config_model=None):
        if self.webhook_handler is not None:
            existing = self.webhook_handler[0]
            raise RegistryError(
                f"A webhook handler is already registered "
                f"('{existing.__module__}.{existing.__qualname__}'); only one is allowed."
            )
        params = inspect.signature(func).parameters
        for required in ("payload", "webhook_config"):
            if required not in params:
                raise RegistryError(
                    f"Webhook handler '{func.__module__}.{func.__qualname__}' must accept "
                    f"a '{required}' parameter."
                )
        if payload_model is None and params["payload"].annotation is not inspect.Parameter.empty:
            payload_model = params["payload"].annotation
        if config_model is None and params["webhook_config"].annotation is not inspect.Parameter.empty:
            config_model = params["webhook_config"].annotation
        self.webhook_handler = (func, payload_model, config_model)
        return func

    def load_modules(self, module_names):
        """Import handler modules so their decorators register with this registry."""
        for name in module_names:
            importlib.import_module(name)

    def load_legacy_actions(self, module_name):
        """Scan a fork-convention module for `action_`-prefixed handler functions.

        Decorator registrations always win over legacy scans of the same id.
        """
        for action_id, entry in discover_actions(module_name=module_name, prefix="action_").items():
            if action_id not in self.action_handlers:
                self.action_handlers[action_id] = entry

    def load_legacy_webhook(self, module_name):
        """Adopt a fork-convention `webhook_handler` function if none is registered."""
        if self.webhook_handler is not None:
            return
        module = importlib.import_module(module_name)
        # AttributeError when the module defines no webhook_handler — the same
        # failure mode the template had; callers decide whether that is fatal.
        self.register_webhook(module.webhook_handler)


registry = ActionRegistry()
```

- [ ] **Step 4: Implement `decorators.py`**

`src/gundi_action_runner/decorators.py`:

```python
from gundi_action_runner.actions.core import (
    AuthActionConfiguration,
    GenericActionConfiguration,
    PullActionConfiguration,
    PushActionConfiguration,
)
from gundi_action_runner.registry import registry


def _action_decorator(expected_config_base):
    def outer(*, config, id=None, title=None):
        def register(func):
            return registry.register_action(
                func,
                config_model=config,
                expected_config_base=expected_config_base,
                action_id=id,
                title=title,
            )
        return register
    return outer


class _ActionDecorators:
    """Namespace for the @action.* handler decorators."""
    auth = staticmethod(_action_decorator(AuthActionConfiguration))
    pull = staticmethod(_action_decorator(PullActionConfiguration))
    push = staticmethod(_action_decorator(PushActionConfiguration))
    generic = staticmethod(_action_decorator(GenericActionConfiguration))


action = _ActionDecorators()


def webhook(func=None, *, payload=None, config=None):
    """Register the connector's webhook handler.

    Usable bare (`@webhook`, models introspected from annotations) or with
    explicit overrides (`@webhook(payload=..., config=...)`).
    """
    def register(f):
        return registry.register_webhook(f, payload_model=payload, config_model=config)

    if func is not None:
        return register(func)
    return register
```

- [ ] **Step 5: Export from the package root**

`src/gundi_action_runner/__init__.py` becomes:

```python
__version__ = "0.1.0.dev0"

from gundi_action_runner.decorators import action, webhook  # noqa: F401
from gundi_action_runner.registry import RegistryError, registry  # noqa: F401
```

- [ ] **Step 6: Run the registry tests, then the full suite**

```bash
pytest tests/test_registry.py -v
pytest
```

Expected: all registry tests pass; full suite still green.

- [ ] **Step 7: Commit**

```bash
git add src/gundi_action_runner/registry.py src/gundi_action_runner/decorators.py \
        src/gundi_action_runner/__init__.py tests/test_registry.py
git commit -m "Add decorator registry: @action.auth/pull/push/generic and @webhook with legacy loaders"
```

---

### Task 5: `create_app()` factory and registry rewiring

Wire the framework to the registry: `create_app()` builds the FastAPI app after loading handler modules; the library's `actions/__init__.py` and `get_webhook_handler()` become registry-backed; `app/main.py` becomes a one-line shim.

**Files:**
- Create: `src/gundi_action_runner/app_factory.py`, `tests/test_app_factory.py`
- Modify: `src/gundi_action_runner/settings.py`, `src/gundi_action_runner/registry.py` (add `ensure_loaded`), `src/gundi_action_runner/actions/__init__.py`, `src/gundi_action_runner/webhooks/core.py` (only `get_webhook_handler`), `src/gundi_action_runner/services/self_registration.py` (one line), `src/gundi_action_runner/__init__.py`, `app/main.py`

**Interfaces:**
- Consumes: `registry`, decorators (Task 4); routers/services/settings (Task 3).
- Produces: `create_app(handlers_modules: list[str] | None = None) -> FastAPI` exported from `gundi_action_runner`; settings `GUNDI_HANDLERS_MODULES`, `GUNDI_LEGACY_ACTIONS_MODULE`, `GUNDI_LEGACY_WEBHOOKS_MODULE`; `registry.ensure_loaded()`. Task 6's example connector calls `create_app(handlers_modules=[...])`.

- [ ] **Step 1: Write the failing tests**

`tests/test_app_factory.py`:

```python
import sys
import types

import pytest
from fastapi.testclient import TestClient

from gundi_action_runner import create_app
from gundi_action_runner.registry import registry


@pytest.fixture(autouse=True)
def clean_registry():
    saved_actions = dict(registry.action_handlers)
    saved_webhook = registry.webhook_handler
    registry.reset()
    yield registry
    registry.reset()
    registry.action_handlers.update(saved_actions)
    registry.webhook_handler = saved_webhook


@pytest.fixture
def decorated_module():
    from gundi_action_runner.actions.core import PullActionConfiguration
    from gundi_action_runner.decorators import action

    module = types.ModuleType("fake_decorated_handlers")

    def _register():
        @action.pull(config=PullActionConfiguration, id="pull_stuff")
        async def pull_stuff(integration, action_config):
            return {}

    module.register = _register
    sys.modules["fake_decorated_handlers"] = module
    # Registration happens when create_app imports the module; emulate
    # decorator-at-import by registering on first import via module-level call.
    _register()
    yield "fake_decorated_handlers"
    del sys.modules["fake_decorated_handlers"]


def test_create_app_builds_routes():
    app = create_app(handlers_modules=[])
    paths = {route.path for route in app.routes}
    assert "/" in paths
    assert "/push-data" in paths
    assert any(path.startswith("/v1/actions") for path in paths)
    assert any(path.startswith("/webhooks") for path in paths)


def test_health_endpoint():
    app = create_app(handlers_modules=[])
    client = TestClient(app)
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}


def test_actions_endpoint_lists_registered_actions(decorated_module):
    app = create_app(handlers_modules=[])  # registry already populated by fixture
    client = TestClient(app)
    response = client.get("/v1/actions/")
    assert response.status_code == 200
    assert "pull_stuff" in response.json()


def test_create_app_scans_legacy_module_when_registry_empty(monkeypatch):
    from gundi_action_runner.actions.core import PullActionConfiguration

    module = types.ModuleType("fake_legacy_for_factory")

    async def action_pull_legacy(integration, action_config: PullActionConfiguration):
        return {}

    module.action_pull_legacy = action_pull_legacy
    sys.modules["fake_legacy_for_factory"] = module
    monkeypatch.setattr(
        "gundi_action_runner.settings.GUNDI_LEGACY_ACTIONS_MODULE", "fake_legacy_for_factory"
    )
    try:
        create_app()
        assert "pull_legacy" in registry.action_handlers
    finally:
        del sys.modules["fake_legacy_for_factory"]
```

- [ ] **Step 2: Run to verify failure**

```bash
pytest tests/test_app_factory.py -v
```

Expected: `ImportError: cannot import name 'create_app' from 'gundi_action_runner'`.

- [ ] **Step 3: Add the discovery settings**

Append to `src/gundi_action_runner/settings.py` (next to `INTEGRATION_TYPE_SLUG`):

```python
# How the connector's handlers are discovered by create_app():
# comma-separated import paths of modules using the @action/@webhook decorators...
GUNDI_HANDLERS_MODULES = env.str("GUNDI_HANDLERS_MODULES", None)
# ...and/or the legacy template-fork convention (action_-prefixed functions,
# module-level webhook_handler), scanned as a fallback.
GUNDI_LEGACY_ACTIONS_MODULE = env.str("GUNDI_LEGACY_ACTIONS_MODULE", "app.actions.handlers")
GUNDI_LEGACY_WEBHOOKS_MODULE = env.str("GUNDI_LEGACY_WEBHOOKS_MODULE", "app.webhooks.handlers")
```

- [ ] **Step 4: Add `ensure_loaded` to the registry**

Append this method to `ActionRegistry` in `src/gundi_action_runner/registry.py`:

```python
    def ensure_loaded(self):
        """Populate the registry from env-configured modules if it is empty.

        Called by create_app() and register_integration_in_gundi() so both the
        server path and the CLI registration path see the connector's handlers.
        """
        from gundi_action_runner import settings  # deferred: avoid import-time env coupling

        if not self.action_handlers and settings.GUNDI_HANDLERS_MODULES:
            self.load_modules(
                [m.strip() for m in settings.GUNDI_HANDLERS_MODULES.split(",") if m.strip()]
            )
        if not self.action_handlers:
            try:
                self.load_legacy_actions(settings.GUNDI_LEGACY_ACTIONS_MODULE)
            except ModuleNotFoundError:
                logger.warning(
                    f"No legacy actions module '{settings.GUNDI_LEGACY_ACTIONS_MODULE}' found; "
                    f"no actions registered."
                )
        if self.webhook_handler is None:
            try:
                self.load_legacy_webhook(settings.GUNDI_LEGACY_WEBHOOKS_MODULE)
            except (ModuleNotFoundError, AttributeError):
                pass  # connectors without a webhook handler are fine
```

- [ ] **Step 5: Write `app_factory.py`**

`src/gundi_action_runner/app_factory.py` — a faithful port of the pre-shim `app/main.py` (compare against `git show 113d3c4:app/main.py` while porting; behavior must be identical):

```python
import base64
import json
import logging
import os
from contextlib import asynccontextmanager

from fastapi import BackgroundTasks, FastAPI, HTTPException, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from gundi_action_runner import settings
from gundi_action_runner.registry import registry
from gundi_action_runner.routers import actions, config_events, webhooks
from gundi_action_runner.services.action_runner import _portal, execute_action
from gundi_action_runner.services.self_registration import register_integration_in_gundi
from gundi_action_runner.services.webhooks import close_diagnostic_client

logger = logging.getLogger(__name__)


def create_app(handlers_modules=None):
    """Build the connector's FastAPI app.

    handlers_modules: import paths of modules that register handlers via the
    @action/@webhook decorators. Defaults to the GUNDI_HANDLERS_MODULES env
    setting; the legacy template convention (app.actions.handlers /
    app.webhooks.handlers) is scanned as a fallback when the registry is empty.
    """
    if handlers_modules is not None:
        registry.load_modules(handlers_modules)
    registry.ensure_loaded()

    # For running behind a proxy, configure the root path for the OpenAPI browser.
    root_path = os.environ.get("ROOT_PATH", "")  # noqa: F841 — parity with the template

    @asynccontextmanager
    async def lifespan(app):
        if settings.REGISTER_ON_START:
            await register_integration_in_gundi(gundi_client=_portal)
        yield
        await _portal.close()
        await close_diagnostic_client()

    app = FastAPI(
        title="Gundi Integration Actions Execution Service",
        description="API to trigger actions against third-party systems",
        version="1",
        lifespan=lifespan,
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.get(
        "/",
        tags=["health-check"],
        summary="Check that the service is healthy",
    )
    def read_root(request: Request):
        return {"status": "healthy"}

    @app.post("/", summary="Execute an action from GCP PubSub")
    async def execute(request: Request, background_tasks: BackgroundTasks):
        json_data = await request.json()
        logger.debug(f"JSON: {json_data}")
        payload = base64.b64decode(json_data["message"]["data"]).decode("utf-8").strip()
        json_payload = json.loads(payload)
        logger.debug(f"JSON Payload: {json_payload}")
        if settings.PROCESS_PUBSUB_MESSAGES_IN_BACKGROUND:
            background_tasks.add_task(
                execute_action,
                integration_id=json_payload.get("integration_id"),
                action_id=json_payload.get("action_id"),
                config_overrides=json_payload.get("config_overrides"),
                triggered_by=json_payload.get("triggered_by"),
            )
        else:
            await execute_action(
                integration_id=json_payload.get("integration_id"),
                action_id=json_payload.get("action_id"),
                config_overrides=json_payload.get("config_overrides"),
                triggered_by=json_payload.get("triggered_by"),
            )
        return {}

    @app.post("/push-data", summary="Process messages from PubSub and run push actions")
    async def push_data(request: Request):
        json_body = await request.json()
        payload = base64.b64decode(json_body["message"]["data"]).decode("utf-8").strip()
        json_payload = json.loads(payload)
        attributes = json_body["message"].get("attributes", {})
        destination_id = attributes.get("destination_id")
        if not destination_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Missing required attribute: 'destination_id'",
            )
        return await execute_action(
            integration_id=destination_id,
            data=json_payload,
            metadata=attributes,
        )

    app.include_router(actions.router, prefix="/v1/actions", tags=["actions"])
    app.include_router(webhooks.router, prefix="/webhooks", tags=["webhooks"])
    app.include_router(config_events.router, prefix="/config-events", tags=["configurations"])

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(request: Request, exc: RequestValidationError):
        logger.debug(
            "Failed handling body: %s",
            jsonable_encoder({"detail": exc.errors(), "body": exc.body}),
        )
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content=jsonable_encoder({"detail": exc.errors(), "body": exc.body}),
        )

    return app
```

Preserve the original's comments where they carry rationale (the `triggered_by` comment block above the PubSub endpoint) — copy them from `git show 113d3c4:app/main.py`.

- [ ] **Step 6: Rewire the library's `actions/__init__.py` to the registry**

Replace the full contents of `src/gundi_action_runner/actions/__init__.py`:

```python
from .core import *  # noqa: F401,F403
from gundi_action_runner.registry import registry

# The live handler mapping. Binding the registry's dict here (same mutable
# object) keeps the template convention working: services and forks do
# `from ...actions import action_handlers` and see registrations as they land.
action_handlers = registry.action_handlers


def setup_action_handlers():
    """Legacy template convention: scan app.actions.handlers into the registry."""
    from gundi_action_runner import settings
    registry.load_legacy_actions(module_name=settings.GUNDI_LEGACY_ACTIONS_MODULE)
    return registry.action_handlers


def get_actions():
    return list(registry.action_handlers.keys())


def get_action_handler_by_data_type(type_name: str):
    for action_id, value in registry.action_handlers.items():
        func, config_model, data_model = value
        if data_model and data_model.__name__ == type_name.strip():
            return action_id, func, config_model, data_model
    else:
        raise ValueError(f"No action handler found for data type '{type_name}'.")
```

Notes: the eager module-import-time scan is gone — population now happens in `create_app()` / `ensure_loaded()`. `get_actions` moves here from `actions/core.py`; delete the old `get_actions` function at the bottom of `src/gundi_action_runner/actions/core.py` (it re-scanned on every call). `discover_actions` stays in `core.py` — the registry's legacy loader uses it.

- [ ] **Step 7: Make `get_webhook_handler` registry-backed**

In `src/gundi_action_runner/webhooks/core.py`, replace the body of `get_webhook_handler` (keep the function name and return shape — `(handler, payload_model, config_model)`):

```python
def get_webhook_handler():
    from gundi_action_runner.registry import registry  # deferred: avoids a module cycle

    if registry.webhook_handler is None:
        from gundi_action_runner import settings
        registry.load_legacy_webhook(settings.GUNDI_LEGACY_WEBHOOKS_MODULE)
    return registry.webhook_handler
```

(Failure modes preserved: `ModuleNotFoundError` / `AttributeError` propagate exactly as the template's hardcoded `importlib.import_module("app.webhooks.handlers")` did.)

- [ ] **Step 8: Ensure the CLI registration path loads handlers**

In `src/gundi_action_runner/services/self_registration.py`, add as the FIRST line inside `register_integration_in_gundi` (fork `app/register.py` invokes this directly, without `create_app()`):

```python
    from gundi_action_runner.registry import registry
    registry.ensure_loaded()
```

- [ ] **Step 9: Shim `app/main.py` and export `create_app`**

`app/main.py` becomes:

```python
"""Deprecated compatibility entry point — the app is now built by
gundi_action_runner.create_app(). `uvicorn app.main:app` keeps working."""
import warnings

from gundi_action_runner import create_app

warnings.warn(
    "'app.main' is deprecated; build the app with 'gundi_action_runner.create_app()' instead.",
    DeprecationWarning,
    stacklevel=2,
)

app = create_app()
```

`src/gundi_action_runner/__init__.py` becomes:

```python
__version__ = "0.1.0.dev0"

from gundi_action_runner.decorators import action, webhook  # noqa: F401
from gundi_action_runner.registry import RegistryError, registry  # noqa: F401
from gundi_action_runner.app_factory import create_app  # noqa: F401
```

(`create_app` import must come after the others — `app_factory` imports back into the package; keep the order shown.)

- [ ] **Step 10: Run the new tests, then the full suite**

```bash
pytest tests/test_app_factory.py -v
pytest
```

Expected: all pass. The three moved test files that do `from app.main import app` at module level now exercise the shim → `create_app()` path — that's intentional coverage of the fork entry point. If `test_self_registration.py` tests fail on the `ensure_loaded` addition, check that they patch `...self_registration.action_handlers` (they do — the patched name is what the function body reads; `ensure_loaded` only touches the real registry).

- [ ] **Step 11: Commit**

```bash
git add -A
git commit -m "Add create_app() factory; rewire discovery through the decorator registry"
```

---

### Task 6: Reference connector example

A small connector consuming the public API — living proof of the decorator surface and an integration test of the whole stack. Lives in `examples/` (NOT in `app/`, whose handler files are fork-owned).

**Files:**
- Create: `examples/reference_connector/conftest.py`, `examples/reference_connector/reference_connector/__init__.py`, `.../configurations.py`, `.../handlers.py`, `.../main.py`, `examples/reference_connector/tests/test_reference_connector.py`
- Modify: `pyproject.toml` (testpaths)

**Interfaces:**
- Consumes: `action`, `webhook`, `create_app` from `gundi_action_runner`; base config classes; `FieldWithUIOptions`/`UIOptions`/`GlobalUISchemaOptions` from `gundi_action_runner.services.utils`.
- Produces: the canonical usage example Plans 2–3 will point docs and the copier template at.

- [ ] **Step 1: Write the example package**

`examples/reference_connector/conftest.py`:

```python
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent))
```

`examples/reference_connector/reference_connector/__init__.py`: empty file.

`examples/reference_connector/reference_connector/configurations.py`:

```python
from gundi_action_runner.actions.core import AuthActionConfiguration, PullActionConfiguration
from gundi_action_runner.services.utils import (
    FieldWithUIOptions,
    GlobalUISchemaOptions,
    UIOptions,
)
from gundi_action_runner.webhooks.core import WebhookConfiguration, WebhookPayload


class AuthConfig(AuthActionConfiguration):
    api_key: str = FieldWithUIOptions(
        ...,
        title="API Key",
        description="API key for the reference tracking API",
        format="password",
        ui_options=UIOptions(widget="password"),
    )
    ui_global_options = GlobalUISchemaOptions(order=["api_key"])


class PullObservationsConfig(PullActionConfiguration):
    lookback_days: int = FieldWithUIOptions(
        7,
        ge=1,
        le=30,
        title="Lookback Days",
        description="How many days back to fetch on the first run",
        ui_options=UIOptions(widget="range"),
    )
    ui_global_options = GlobalUISchemaOptions(order=["lookback_days"])


class ReferenceWebhookPayload(WebhookPayload):
    device_id: str
    lat: float
    lon: float
    recorded_at: str


class ReferenceWebhookConfig(WebhookConfiguration):
    default_subject_type: str = "unknown"
```

`examples/reference_connector/reference_connector/handlers.py`:

```python
from gundi_action_runner import action, webhook

from .configurations import (
    AuthConfig,
    PullObservationsConfig,
    ReferenceWebhookConfig,
    ReferenceWebhookPayload,
)


@action.auth(config=AuthConfig)
async def auth(integration, action_config):
    # A real connector validates credentials against the external API here
    # and returns {"valid_credentials": bool}.
    return {"valid_credentials": bool(action_config.api_key)}


@action.pull(config=PullObservationsConfig, title="Pull Reference Observations")
async def pull_observations(integration, action_config):
    # A real connector fetches from the external API, transforms the data, and
    # calls send_observations_to_gundi() here. Add @crontab_schedule(...) and
    # @activity_logger() from gundi_action_runner.services in a real connector.
    return {"observations_extracted": 0}


@webhook
async def webhook_handler(payload: ReferenceWebhookPayload, integration,
                          webhook_config: ReferenceWebhookConfig):
    # A real connector transforms the payload and forwards it to Gundi here.
    return {"data_points_qty": 1}
```

`examples/reference_connector/reference_connector/main.py`:

```python
from gundi_action_runner import create_app

app = create_app(handlers_modules=["reference_connector.handlers"])
```

- [ ] **Step 2: Write the failing tests**

`examples/reference_connector/tests/test_reference_connector.py`:

```python
import importlib

import pytest
from fastapi.testclient import TestClient

from gundi_action_runner import create_app
from gundi_action_runner.registry import registry


@pytest.fixture
def reference_registry():
    registry.reset()
    import reference_connector.handlers as handlers
    importlib.reload(handlers)  # re-fire decorators against the clean registry
    yield registry
    registry.reset()


def test_actions_are_registered(reference_registry):
    assert set(reference_registry.action_handlers) == {"auth", "pull_observations"}
    pull_func, pull_config, _ = reference_registry.action_handlers["pull_observations"]
    assert pull_func.action_title == "Pull Reference Observations"


def test_webhook_is_registered(reference_registry):
    from reference_connector.configurations import ReferenceWebhookPayload
    func, payload_model, config_model = reference_registry.webhook_handler
    assert payload_model is ReferenceWebhookPayload


def test_actions_endpoint_lists_reference_actions(reference_registry):
    app = create_app(handlers_modules=[])  # registry populated by the fixture
    client = TestClient(app)
    response = client.get("/v1/actions/")
    assert response.status_code == 200
    assert set(response.json()) == {"auth", "pull_observations"}
```

- [ ] **Step 3: Add examples to testpaths and verify failure→pass**

`pyproject.toml`:

```toml
[tool.pytest.ini_options]
testpaths = ["tests", "examples", "app"]
```

```bash
pytest examples/ -v
```

Expected: the three tests pass (Step 1 already created the package; if you wrote tests first they fail with `ModuleNotFoundError: reference_connector` until Step 1's files exist).

- [ ] **Step 4: Run the full suite**

```bash
pytest
```

Expected: everything green. Watch for cross-contamination: the example fixture resets the global registry — if any `tests/` test starts failing order-dependently, it is relying on registry state instead of its mocks; fix that test, not the fixture.

- [ ] **Step 5: Commit**

```bash
git add examples/ pyproject.toml
git commit -m "Add reference connector example exercising the decorator API end-to-end"
```

---

### Task 7: Final verification and cleanup

**Files:**
- Delete: `FIX_TESTCLIENT_PLAN.md` (absorbed by Task 1)
- Modify: `README.md` (add library preview section)

**Interfaces:**
- Consumes: everything.
- Produces: a branch ready for Plan 2 (publishing).

- [ ] **Step 1: Remove the superseded plan file**

```bash
git rm FIX_TESTCLIENT_PLAN.md
```

- [ ] **Step 2: Add a README section**

Insert after the README's opening section:

```markdown
## Using as a library (preview)

The framework in this repo is being extracted into an installable package,
`gundi-action-runner` (not yet on PyPI). Connectors will register handlers with
decorators instead of editing template files:

​```python
from gundi_action_runner import action, create_app

@action.pull(config=MyPullConfig, title="Pull Observations")
async def pull_observations(integration, action_config):
    ...

app = create_app(handlers_modules=["myconnector.handlers"])
​```

Existing forks are unaffected: `app/*` modules remain as compatibility shims
(emitting `DeprecationWarning`), and `uvicorn app.main:app` still works. See
`examples/reference_connector/` for a complete example and
`docs/superpowers/specs/2026-07-07-action-runner-library-design.md` for the design.
```

(Remove the zero-width characters around the inner code fence when writing the actual file — they exist only to nest the fence in this plan.)

- [ ] **Step 3: Full verification sweep**

```bash
# 1. Clean-room install check
pip uninstall -y gundi-action-runner && pip install -e .[dev] && pytest
# 2. No stray legacy imports inside the library
grep -rn "\bfrom app\.\|\bimport app\b" src/gundi_action_runner/ ; echo "expected: no output above"
# 3. Legacy entry point still boots (module import implies FastAPI construction)
python -c "from app.main import app; print(type(app).__name__, len(app.routes), 'routes')"
# 4. Register CLI path still imports
python -c "import app.register"
```

Expected: full suite passes; greps clean; `FastAPI N routes` prints; no ImportError. Optionally, if Docker is available: `cd local && docker compose build` succeeds.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "Finalize library extraction: README preview section, drop absorbed dep-fix plan"
```

---

## Deferred to Plans 2–3 (do not do here)

- `[project.entry-points.pytest11]` declaration for `gundi_action_runner.testing` (Plan 2) — the fixtures module already lives at its final home.
- PyPI publishing workflow, version tagging, adding `gundi-action-runner` to `requirements-base.in` (Plan 2).
- `gundi-runner` CLI, copier template, `add-action` codegen (Plan 3).
- Fork migration guide + CLAUDE.md/README full rewrite (Plan 3).
- Merging this branch to `main` (gated on Plan 2 publishing).
