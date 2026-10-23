# Gundi Action Runner CLI & Scaffolding — Implementation Plan (Plan 3 of 3)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship the guided developer experience: `gundi-runner new` (copier scaffold), `run`, `register`, `add-action`, plus the quickstart / extension-API / fork-migration docs.

**Architecture:** A `click` CLI at `src/gundi_action_runner/cli.py` (console script `gundi-runner`, deps via a `cli` extra — copier is the only new dependency). The project template lives in `template/` with a root `copier.yml` using `_subdirectory: template`, so generated projects record this GitHub repo as their template source and `copier update` tracks its tags. `add-action` is pure text codegen against a scaffolded project's `handlers.py`/`configurations.py`. Docs are three focused markdown files under `docs/`.

**Tech Stack:** click 8 (existing core dep), copier ~7.2 (cli extra; the last pydantic-v1-compatible line), Jinja templates (copier), uvicorn (existing core dep) for `run`.

**Spec:** `docs/superpowers/specs/2026-07-07-action-runner-library-design.md` (phases 6–8, Section 4). Plans 1–2 are complete on branch `design/action-runner-library` (PR #78); suite is 130 passing; single version source is `__version__` in `src/gundi_action_runner/__init__.py`.

## Global Constraints

- Work on branch `design/action-runner-library`. Never `git add -A` / `git add .` — stage explicit paths (tree contains unrelated untracked user files and an uncommitted README draft).
- **Never edit fork-owned files:** `app/actions/handlers.py`, `app/actions/configurations.py`, `app/webhooks/handlers.py`, `app/webhooks/configurations.py`, `app/settings/integration.py`, `app/register.py`.
- Always `source .venv/bin/activate` and `python -m pip` (bare `pip` is the system pip on this machine).
- Version stays `0.1.0.dev0`. Scaffolded projects depend on `gundi-action-runner~=0.1` — correct for the upcoming first release.
- CLI command name `gundi-runner`; extras: `cli = ["copier~=7.2", "pyyaml-include<2"]` (copier 8+ requires pydantic v2, which conflicts with the library's `pydantic<2` pin; pyyaml-include 2.x breaks copier 7), `testing = ["pytest~=7.4.3", "pytest-mock~=3.12.0", "pytest-asyncio~=0.21.1"]`.
- Default template source for `new` is `gh:PADAS/gundi-integration-action-runner` (overridable with `--template` — tests use a local staged copy).
- Decorator-ordering rule holds everywhere docs/templates show stacked decorators: `@action.*`/`@webhook` outermost.
- Suite green at end of every task (130 + new tests). Commit messages end with a blank line then:
  `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`

---

### Task 1: CLI skeleton — `gundi-runner run` and `register`

**Files:**
- Create: `src/gundi_action_runner/cli.py`
- Modify: `pyproject.toml` (console script + `cli`/`testing` extras)
- Test: `tests/test_cli.py`

**Interfaces:**
- Consumes: `create_app()` env-driven discovery (`GUNDI_HANDLERS_MODULES`), `register_integration_in_gundi(gundi_client, type_slug, type_name, service_url, action_schedules)`, `CrontabSchedule.parse_obj_from_crontab`, `_portal` from `gundi_action_runner.services.action_runner`.
- Produces: `cli` click group with `run` + `register` (Tasks 3–4 append `new` + `add-action` to the same group); `_apply_handlers_setting(handlers: tuple[str, ...]) -> None` helper reused by later commands.

- [ ] **Step 1: pyproject additions**

In `[project.optional-dependencies]` add after the `dev` list:

```toml
cli = [
    # copier 8+ requires pydantic v2 (conflicts with our pydantic<2 pin);
    # pyyaml-include 2.x removed the API copier 7 uses.
    "copier~=7.2",
    "pyyaml-include<2",
]
testing = [
    "pytest~=7.4.3",
    "pytest-mock~=3.12.0",
    "pytest-asyncio~=0.21.1",
]
```

After the `[project.entry-points.pytest11]` table add:

```toml
[project.scripts]
gundi-runner = "gundi_action_runner.cli:cli"
```

Then reinstall so the console script registers:

```bash
source .venv/bin/activate
python -m pip install -e ".[cli]" --no-deps
python -m pip install "copier~=7.2" "pyyaml-include<2"
gundi-runner --help || echo "EXPECTED FAILURE until cli.py exists"
```

- [ ] **Step 2: Write the failing tests**

`tests/test_cli.py`:

```python
"""gundi-runner CLI: run + register (new/add-action tested in their own tasks)."""
from unittest.mock import AsyncMock

import pytest
from click.testing import CliRunner

from gundi_action_runner.cli import cli


@pytest.fixture
def runner():
    return CliRunner()


def test_cli_group_lists_commands(runner):
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    for command in ("run", "register"):
        assert command in result.output


def test_run_invokes_uvicorn_with_factory(runner, mocker, monkeypatch):
    monkeypatch.delenv("GUNDI_HANDLERS_MODULES", raising=False)
    # The CLI mutates this module attribute; monkeypatch restores it at
    # teardown so later tests' ensure_loaded() doesn't import myconn.handlers.
    monkeypatch.setattr("gundi_action_runner.settings.GUNDI_HANDLERS_MODULES", None)
    uvicorn_run = mocker.patch("uvicorn.run")
    result = runner.invoke(
        cli, ["run", "--handlers", "myconn.handlers", "--port", "9000"]
    )
    assert result.exit_code == 0, result.output
    args, kwargs = uvicorn_run.call_args
    assert args[0] == "gundi_action_runner.app_factory:create_app"
    assert kwargs["factory"] is True
    assert kwargs["port"] == 9000
    # The reload subprocess reads env; the in-process factory reads settings
    import os

    from gundi_action_runner import settings

    assert os.environ["GUNDI_HANDLERS_MODULES"] == "myconn.handlers"
    assert settings.GUNDI_HANDLERS_MODULES == "myconn.handlers"


def test_register_forwards_options(runner, mocker):
    register = mocker.patch(
        "gundi_action_runner.cli.register_integration_in_gundi", new_callable=AsyncMock
    )
    result = runner.invoke(
        cli,
        [
            "register",
            "--slug", "x_tracker",
            "--name", "X Tracker",
            "--service-url", "https://x.example.com",
            "--schedule", "pull_observations:0 */4 * * *",
        ],
    )
    assert result.exit_code == 0, result.output
    kwargs = register.call_args.kwargs
    assert kwargs["type_slug"] == "x_tracker"
    assert kwargs["type_name"] == "X Tracker"
    assert kwargs["service_url"] == "https://x.example.com"
    assert "pull_observations" in kwargs["action_schedules"]


def test_register_rejects_bad_schedule(runner, mocker):
    mocker.patch(
        "gundi_action_runner.cli.register_integration_in_gundi", new_callable=AsyncMock
    )
    result = runner.invoke(cli, ["register", "--schedule", "not-a-schedule"])
    assert result.exit_code != 0
    assert "Invalid schedule format" in result.output
```

- [ ] **Step 3: Run to verify failure**

Run: `pytest tests/test_cli.py -v`
Expected: collection error — `ModuleNotFoundError: No module named 'gundi_action_runner.cli'`.

- [ ] **Step 4: Implement `cli.py`**

`src/gundi_action_runner/cli.py`:

```python
"""gundi-runner: developer CLI for building and operating Gundi connectors."""
import asyncio
import os

import click
import pydantic

from gundi_action_runner import settings
from gundi_action_runner.services.action_runner import _portal
from gundi_action_runner.services.action_scheduler import CrontabSchedule
from gundi_action_runner.services.self_registration import register_integration_in_gundi


def _apply_handlers_setting(handlers):
    """Point discovery at the given handler modules for this process AND
    any child process (uvicorn --reload workers read the env var)."""
    if not handlers:
        return
    value = ",".join(handlers)
    os.environ["GUNDI_HANDLERS_MODULES"] = value
    settings.GUNDI_HANDLERS_MODULES = value


@click.group()
def cli():
    """Build, run, and register Gundi action-runner connectors."""


@cli.command()
@click.option("--handlers", "-m", multiple=True,
              help="Import path(s) of modules registering handlers with @action/@webhook. "
                   "Defaults to GUNDI_HANDLERS_MODULES or the legacy app.* convention.")
@click.option("--host", default="127.0.0.1", show_default=True)
@click.option("--port", default=8080, show_default=True, type=int)
@click.option("--reload/--no-reload", default=True, show_default=True)
def run(handlers, host, port, reload):
    """Run the connector locally with uvicorn."""
    import uvicorn

    _apply_handlers_setting(handlers)
    uvicorn.run(
        "gundi_action_runner.app_factory:create_app",
        factory=True,
        host=host,
        port=port,
        reload=reload,
    )


@cli.command()
@click.option("--slug", default=None, help="Slug ID for the integration type")
@click.option("--name", default=None,
              help="Display name for the integration type (defaults to a name derived from the slug)")
@click.option("--service-url", default=None,
              help="Service URL used to trigger actions or receive webhooks")
@click.option("--handlers", "-m", multiple=True,
              help="Import path(s) of handler modules (defaults to env/legacy discovery)")
@click.option("--schedule", multiple=True,
              help='Schedules as "action_id:crontab" (e.g. "pull_events:0 */4 * * *")')
def register(slug, name, service_url, handlers, schedule):
    """Register this integration type (actions, schemas, schedules) in Gundi."""
    _apply_handlers_setting(handlers)
    schedules = {}
    for item in schedule:
        try:
            action_id, cron = item.split(":", 1)
            schedules[action_id.strip()] = CrontabSchedule.parse_obj_from_crontab(cron.strip())
        except (pydantic.ValidationError, ValueError) as e:
            raise click.BadParameter(
                f"Invalid schedule format: {item}.\n"
                f"Expected 'action_id:MIN HOUR DOM MON DOW [TZ]', "
                f"e.g. 'pull_events:0 */4 * * * -5'.\n{e}"
            )
    asyncio.run(
        register_integration_in_gundi(
            gundi_client=_portal,
            type_slug=slug,
            type_name=name,
            service_url=service_url,
            action_schedules=schedules,
        )
    )


if __name__ == "__main__":
    cli()
```

- [ ] **Step 5: Run tests, then full suite**

```bash
pytest tests/test_cli.py -v
pytest
gundi-runner --help
```

Expected: 4 CLI tests pass; full suite 134 passing; `--help` lists run/register.

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml src/gundi_action_runner/cli.py tests/test_cli.py
git commit -m "Add gundi-runner CLI with run and register commands"
```

---

### Task 2: Copier template

The scaffold source. Root `copier.yml` (with `_subdirectory: template`) + `template/` tree. Tested by staging the template to a plain tmp dir (avoids copier's dirty-git-repo handling), generating a project, and running the generated project's own test suite in a subprocess — the true end-to-end.

**Files:**
- Create: `copier.yml`, `template/` tree (11 files), `tests/test_template.py`
- Modify: `tests/conftest.py` (append two fixtures)

**Interfaces:**
- Consumes: the library's public API (`action`, `webhook`, `create_app`), the pytest11 plugin, the `testing` extra from Task 1.
- Produces: `stage_template(tmp_path_factory)` and `generate_project(...)` fixtures in `tests/conftest.py` that Task 3's `new`-command tests reuse; template answers: `project_name`, `package_name`, `integration_type_slug`, `display_name`, `include_pull`, `include_webhook`.

- [ ] **Step 1: Write `copier.yml` (repo root)**

```yaml
# Copier template for new Gundi action-runner connectors.
# Generated projects record this repo as their template source, so
# `copier update` can pull scaffold improvements from future tags.
_subdirectory: template
_min_copier_version: "7.2.0"

project_name:
  type: str
  help: Human-readable connector name (e.g. "Savannah Tracking")

package_name:
  type: str
  help: Python package name for your connector code
  default: "{{ project_name | lower | replace(' ', '_') | replace('-', '_') }}"
  validator: >-
    {% if not package_name.isidentifier() or package_name != package_name.lower() %}
    package_name must be a valid lowercase Python identifier
    {% endif %}

integration_type_slug:
  type: str
  help: Unique slug for this integration type in Gundi
  default: "{{ package_name }}"

display_name:
  type: str
  help: Display name shown in the Gundi portal
  default: "{{ project_name }}"

include_pull:
  type: bool
  help: Include a scheduled pull action (polling connector)?
  default: true

include_webhook:
  type: bool
  help: Include a webhook handler (push-based connector)?
  default: false
```

- [ ] **Step 2: Write the template tree**

`template/{{ package_name }}/__init__.py.jinja`:

```
```

(one empty file — the `.jinja` suffix keeps copier from treating it as raw copy; content is empty.)

`template/{{ package_name }}/configurations.py.jinja`:

```python
from gundi_action_runner.actions.core import AuthActionConfiguration{% if include_pull %}, PullActionConfiguration{% endif %}
from gundi_action_runner.services.utils import (
    FieldWithUIOptions,
    GlobalUISchemaOptions,
    UIOptions,
)
{% if include_webhook %}from gundi_action_runner.webhooks.core import WebhookConfiguration, WebhookPayload{% endif %}


class AuthConfig(AuthActionConfiguration):
    api_key: str = FieldWithUIOptions(
        ...,
        title="API Key",
        description="API key for the {{ project_name }} API",
        format="password",
        ui_options=UIOptions(widget="password"),
    )
    ui_global_options = GlobalUISchemaOptions(order=["api_key"])
{% if include_pull %}


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
{% endif %}
{% if include_webhook %}


class {{ package_name | replace('_', ' ') | title | replace(' ', '') }}WebhookPayload(WebhookPayload):
    device_id: str
    lat: float
    lon: float
    recorded_at: str


class {{ package_name | replace('_', ' ') | title | replace(' ', '') }}WebhookConfig(WebhookConfiguration):
    default_subject_type: str = "unknown"
{% endif %}
```

`template/{{ package_name }}/client.py.jinja`:

```python
"""HTTP client for the {{ project_name }} API.

Keep API interaction here, separate from transformation logic
(see transformers.py). Always use httpx.AsyncClient with a timeout.
"""
import httpx


class {{ package_name | replace('_', ' ') | title | replace(' ', '') }}Client:
    def __init__(self, api_key: str, base_url: str = "https://api.example.com"):
        self.api_key = api_key
        self.base_url = base_url

    async def fetch_observations(self, since=None):
        async with httpx.AsyncClient(timeout=30.0) as client:
            # Replace with the real endpoint and auth scheme:
            # response = await client.get(f"{self.base_url}/observations",
            #                             params={"since": since},
            #                             headers={"Authorization": f"Bearer {self.api_key}"})
            # response.raise_for_status()
            # return response.json()
            return []
```

`template/{{ package_name }}/transformers.py.jinja`:

```python
"""Raw {{ project_name }} data -> Gundi observation/event mapping.

Write defensive transformation code: external APIs return inconsistent
data. Use .get() with defaults and skip bad records instead of failing
the whole batch.
"""


def transform_to_observations(raw_records):
    observations = []
    for record in raw_records:
        try:
            observations.append(
                {
                    "source": record["device_id"],
                    "subject_type": record.get("subject_type", "unknown"),
                    "recorded_at": record["recorded_at"],
                    "location": {"lat": record["lat"], "lon": record["lon"]},
                    "additional": {},
                }
            )
        except KeyError:
            continue  # skip malformed records; consider logging a warning
    return observations
```

`template/{{ package_name }}/handlers.py.jinja`:

```python
from gundi_action_runner import action{% if include_webhook %}, webhook{% endif %}

from .configurations import AuthConfig{% if include_pull %}, PullObservationsConfig{% endif %}{% if include_webhook %}, {{ package_name | replace('_', ' ') | title | replace(' ', '') }}WebhookConfig, {{ package_name | replace('_', ' ') | title | replace(' ', '') }}WebhookPayload{% endif %}


@action.auth(config=AuthConfig)
async def auth(integration, action_config):
    # Validate credentials against the {{ project_name }} API and return
    # {"valid_credentials": bool}.
    return {"valid_credentials": bool(action_config.api_key)}
{% if include_pull %}


@action.pull(config=PullObservationsConfig, title="Pull Observations")
async def pull_observations(integration, action_config):
    # Fetch from the API (client.py), transform (transformers.py), then
    # send_observations_to_gundi(). Add @crontab_schedule(...) and
    # @activity_logger() BELOW @action.pull — the @action decorator must be
    # outermost (it registers the function object it receives).
    return {"observations_extracted": 0}
{% endif %}
{% if include_webhook %}


@webhook
async def webhook_handler(payload: {{ package_name | replace('_', ' ') | title | replace(' ', '') }}WebhookPayload, integration,
                          webhook_config: {{ package_name | replace('_', ' ') | title | replace(' ', '') }}WebhookConfig):
    # Transform the pushed payload and forward it to Gundi.
    return {"data_points_qty": 1}
{% endif %}
```

`template/main.py.jinja`:

```python
from gundi_action_runner import create_app

app = create_app(handlers_modules=["{{ package_name }}.handlers"])
```

`template/conftest.py.jinja`:

```python
# Root conftest: anchors pytest's sys.path insertion so `{{ package_name }}`
# imports in tests. Test fixtures come from the gundi-action-runner pytest
# plugin — no wiring needed here.
```

`template/tests/test_handlers.py.jinja`:

```python
import pytest

from gundi_action_runner.registry import registry

import {{ package_name }}.handlers  # noqa: F401 — registers via decorators on import


def test_actions_are_registered():
    assert "auth" in registry.action_handlers
{% if include_pull %}    assert "pull_observations" in registry.action_handlers
{% endif %}

@pytest.mark.asyncio
async def test_auth_accepts_api_key(integration_v2):
    from {{ package_name }}.configurations import AuthConfig
    from {{ package_name }}.handlers import auth

    result = await auth(integration_v2, AuthConfig(api_key="test-key"))
    assert result == {"valid_credentials": True}
```

`template/pyproject.toml.jinja`:

```toml
[build-system]
requires = ["setuptools>=77"]
build-backend = "setuptools.build_meta"

[project]
name = "{{ package_name | replace('_', '-') }}"
version = "0.1.0"
description = "Gundi connector for {{ project_name }}"
requires-python = ">=3.10"
dependencies = [
    "gundi-action-runner~=0.1",
]

[project.optional-dependencies]
dev = [
    "gundi-action-runner[testing]~=0.1",
]

[tool.setuptools.packages.find]
include = ["{{ package_name }}*"]

[tool.pytest.ini_options]
testpaths = ["tests"]
```

`template/Dockerfile.jinja`:

```dockerfile
FROM python:3.10-slim

# Build deps for pyjq (used by the framework's webhook JQ transforms)
RUN apt-get update && apt-get install -y autoconf automake libtool make python3-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /code
COPY pyproject.toml .
COPY {{ package_name }}/ {{ package_name }}/
COPY main.py .
RUN pip install --no-cache-dir .

EXPOSE 8080
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
```

`template/.env.example.jinja`:

```bash
# Gundi platform
GUNDI_API_BASE_URL=
KEYCLOAK_CLIENT_ID=
KEYCLOAK_CLIENT_SECRET=
# This connector
INTEGRATION_TYPE_SLUG={{ integration_type_slug }}
INTEGRATION_TYPE_NAME={{ display_name }}
INTEGRATION_SERVICE_URL=
# Redis (config cache + state store)
REDIS_HOST=localhost
REDIS_PORT=6379
```

`template/README.md.jinja`:

```markdown
# {{ project_name }} Gundi Connector

Generated with `gundi-runner new` from the
[gundi-action-runner](https://github.com/PADAS/gundi-integration-action-runner) template.

## Develop

```bash
pip install -e ".[dev]"
pytest
gundi-runner run --handlers {{ package_name }}.handlers
```

## Register in Gundi

```bash
gundi-runner register --slug {{ integration_type_slug }} --name "{{ display_name }}" \
  --handlers {{ package_name }}.handlers
```

## Update the scaffold

This project tracks the upstream template; pull scaffold improvements with
`copier update`.
```

`template/.gitignore.jinja`:

```
__pycache__/
*.py[cod]
.venv/
.env
dist/
.pytest_cache/
```

(Copier auto-generates `.copier-answers.yml` in the destination when invoked with `--answers-file`/defaults; no answers-file template needed since `gundi-runner new` passes `data=` directly — Task 3 handles recording.)

- [ ] **Step 3: Append the shared fixtures to `tests/conftest.py`**

Append below the existing star-import line:

```python
import pathlib
import shutil

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent


@pytest.fixture(scope="session")
def staged_template(tmp_path_factory):
    """Copy copier.yml + template/ into a plain (non-git) dir so copier
    generation is deterministic regardless of this repo's git state."""
    stage = tmp_path_factory.mktemp("template-stage")
    shutil.copy(REPO_ROOT / "copier.yml", stage / "copier.yml")
    shutil.copytree(REPO_ROOT / "template", stage / "template")
    return stage


@pytest.fixture
def generate_project(staged_template, tmp_path):
    """Generate a scaffolded connector project; returns its path."""
    import copier

    def _generate(**answers):
        defaults = {
            "project_name": "Acme Tracker",
            "package_name": "acme_tracker",
            "integration_type_slug": "acme_tracker",
            "display_name": "Acme Tracker",
            "include_pull": True,
            "include_webhook": False,
        }
        defaults.update(answers)
        dst = tmp_path / "generated"
        copier.run_copy(
            str(staged_template), str(dst), data=defaults,
            defaults=True, overwrite=True, quiet=True,
        )
        return dst

    return _generate
```

- [ ] **Step 4: Write the failing template tests**

`tests/test_template.py`:

```python
"""The copier scaffold: generation correctness + generated-project e2e."""
import subprocess
import sys


def test_generates_expected_tree(generate_project):
    dst = generate_project()
    for path in (
        "pyproject.toml", "main.py", "Dockerfile", ".env.example", "README.md",
        "conftest.py", "acme_tracker/__init__.py", "acme_tracker/handlers.py",
        "acme_tracker/configurations.py", "acme_tracker/client.py",
        "acme_tracker/transformers.py", "tests/test_handlers.py",
    ):
        assert (dst / path).exists(), f"missing {path}"
    handlers = (dst / "acme_tracker" / "handlers.py").read_text()
    assert "@action.auth" in handlers
    assert "@action.pull" in handlers
    assert "@webhook" not in handlers  # include_webhook=False default


def test_webhook_variant(generate_project):
    dst = generate_project(include_webhook=True, include_pull=False)
    handlers = (dst / "acme_tracker" / "handlers.py").read_text()
    assert "@webhook" in handlers
    assert "@action.pull" not in handlers
    configurations = (dst / "acme_tracker" / "configurations.py").read_text()
    assert "AcmeTrackerWebhookPayload" in configurations


def test_generated_files_are_valid_python(generate_project):
    dst = generate_project(include_webhook=True)
    for py in dst.rglob("*.py"):
        compile(py.read_text(), str(py), "exec")


def test_generated_project_test_suite_passes(generate_project):
    """The end-to-end contract: a fresh scaffold's own tests pass using the
    installed library + its pytest plugin (fixtures with no conftest wiring)."""
    dst = generate_project()
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "-q"],
        cwd=dst, capture_output=True, text=True, timeout=120,
    )
    assert result.returncode == 0, f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    assert "2 passed" in result.stdout
```

- [ ] **Step 5: Run to verify failure, then iterate to green**

```bash
pytest tests/test_template.py -v
```

Expected first run: failures (template files don't exist yet if you wrote tests first, or Jinja errors as you iterate). Iterate until all 4 pass. The e2e subprocess test runs the generated project's 2 tests in an isolated process — registry pollution is impossible by construction.

- [ ] **Step 6: Full suite + commit**

```bash
pytest
git add copier.yml template/ tests/test_template.py tests/conftest.py
git commit -m "Add copier template for scaffolding new connectors"
```

Expected: 138 passing (134 + 4).

---

### Task 3: `gundi-runner new`

**Files:**
- Modify: `src/gundi_action_runner/cli.py` (append command), `tests/test_cli.py` (append tests)

**Interfaces:**
- Consumes: `copier.run_copy` (cli extra), the `staged_template` fixture (Task 2), the `cli` group + naming conventions from Task 1.
- Produces: `gundi-runner new DESTINATION [--template SRC] [--vcs-ref REF] [--data KEY=VALUE]...` — documented in Task 5's quickstart.

- [ ] **Step 1: Write the failing tests (append to `tests/test_cli.py`)**

```python
def test_new_generates_project_from_local_template(runner, staged_template, tmp_path):
    dst = tmp_path / "my-connector"
    result = runner.invoke(
        cli,
        [
            "new", str(dst),
            "--template", str(staged_template),
            "--defaults",
            "--data", "project_name=Acme Tracker",
            "--data", "include_pull=true",
            "--data", "include_webhook=false",
        ],
    )
    assert result.exit_code == 0, result.output
    assert (dst / "acme_tracker" / "handlers.py").exists()
    assert "Next steps" in result.output


def test_new_requires_copier(runner, mocker, tmp_path):
    import builtins

    real_import = builtins.__import__

    def _no_copier(name, *args, **kwargs):
        if name == "copier":
            raise ImportError("No module named 'copier'")
        return real_import(name, *args, **kwargs)

    mocker.patch("builtins.__import__", side_effect=_no_copier)
    result = runner.invoke(cli, ["new", str(tmp_path / "x")])
    assert result.exit_code != 0
    assert "pip install 'gundi-action-runner[cli]'" in result.output
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest tests/test_cli.py -k new -v`
Expected: `Error: No such command 'new'`-style failures.

- [ ] **Step 3: Implement (append to `cli.py`)**

```python
DEFAULT_TEMPLATE = "gh:PADAS/gundi-integration-action-runner"


@cli.command()
@click.argument("destination", type=click.Path())
@click.option("--template", default=DEFAULT_TEMPLATE, show_default=True,
              help="Copier template source (git URL or local path)")
@click.option("--vcs-ref", default=None,
              help="Template git ref (tag/branch); defaults to the latest tag")
@click.option("--data", "data_pairs", multiple=True,
              help="Answer as KEY=VALUE (repeatable); unanswered questions prompt interactively")
@click.option("--defaults", is_flag=True, default=False,
              help="Fill unanswered questions with their defaults instead of prompting "
                   "(recommended for CI/scripted use; incomplete answers without this flag "
                   "in a non-TTY produce a broken scaffold — copier does not error)")
def new(destination, template, vcs_ref, data_pairs, defaults):
    """Scaffold a new connector project from the official template."""
    try:
        import copier
    except ImportError:
        raise click.UsageError(
            "copier is not installed. Install the CLI extras first:\n"
            "  pip install 'gundi-action-runner[cli]'"
        )
    data = {}
    for pair in data_pairs:
        key, _, value = pair.partition("=")
        if value.lower() in ("true", "false"):
            value = value.lower() == "true"
        data[key] = value
    copier.run_copy(
        template, destination, data=data, vcs_ref=vcs_ref, defaults=defaults,
    )
    click.echo(
        f"\nProject created at {destination}. Next steps:\n"
        f"  cd {destination}\n"
        f"  pip install -e '.[dev]'\n"
        f"  pytest\n"
        f"  gundi-runner run --handlers <package>.handlers"
    )
```

- [ ] **Step 4: Run tests, full suite, commit**

```bash
pytest tests/test_cli.py -v
pytest
git add src/gundi_action_runner/cli.py tests/test_cli.py
git commit -m "Add gundi-runner new: copier-backed project scaffolding"
```

Expected: 140 passing (138 + 2).

---

### Task 4: `gundi-runner add-action`

Interactive codegen: appends a stub handler + config class to a scaffolded project. Pure text generation; refuses politely when the expected files are missing, ambiguous, or the id already exists.

**Files:**
- Modify: `src/gundi_action_runner/cli.py` (append command + helpers), `tests/test_cli.py` (append tests)

**Interfaces:**
- Consumes: the scaffold layout from Task 2 (`{package}/handlers.py`, `{package}/configurations.py`).
- Produces: `gundi-runner add-action [--type ...] [--id ...] [--title ...] [--crontab ...] [--package DIR]`.

- [ ] **Step 1: Write the failing tests (append to `tests/test_cli.py`)**

```python
@pytest.fixture
def scaffolded_project(generate_project, monkeypatch):
    dst = generate_project()
    monkeypatch.chdir(dst)
    return dst


def test_add_action_appends_pull_stub(runner, scaffolded_project):
    result = runner.invoke(
        cli,
        ["add-action", "--type", "pull", "--id", "pull_events",
         "--title", "Pull Events", "--crontab", "0 */2 * * *"],
    )
    assert result.exit_code == 0, result.output
    handlers = (scaffolded_project / "acme_tracker" / "handlers.py").read_text()
    configurations = (scaffolded_project / "acme_tracker" / "configurations.py").read_text()
    assert '@action.pull(config=PullEventsConfig, title="Pull Events")' in handlers
    assert '@crontab_schedule("0 */2 * * *")' in handlers
    assert "async def pull_events(integration, action_config):" in handlers
    assert "class PullEventsConfig(PullActionConfiguration):" in configurations
    # Still valid python
    compile(handlers, "handlers.py", "exec")
    compile(configurations, "configurations.py", "exec")


def test_add_action_push_generates_data_model(runner, scaffolded_project):
    result = runner.invoke(
        cli, ["add-action", "--type", "push", "--id", "push_positions"]
    )
    assert result.exit_code == 0, result.output
    handlers = (scaffolded_project / "acme_tracker" / "handlers.py").read_text()
    configurations = (scaffolded_project / "acme_tracker" / "configurations.py").read_text()
    assert "data: PushPositionsData" in handlers
    assert "class PushPositionsData(pydantic.BaseModel):" in configurations


def test_add_action_refuses_duplicate_id(runner, scaffolded_project):
    result = runner.invoke(
        cli, ["add-action", "--type", "pull", "--id", "pull_observations"]
    )
    assert result.exit_code != 0
    assert "already defines" in result.output


def test_add_action_refuses_outside_project(runner, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(cli, ["add-action", "--type", "auth", "--id", "auth2"])
    assert result.exit_code != 0
    assert "Could not locate" in result.output
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest tests/test_cli.py -k add_action -v`
Expected: `No such command 'add-action'` failures.

- [ ] **Step 3: Implement (append to `cli.py`)**

```python
_CONFIG_BASES = {
    "auth": "AuthActionConfiguration",
    "pull": "PullActionConfiguration",
    "push": "PushActionConfiguration",
    "generic": "GenericActionConfiguration",
}


def _find_package_dir(explicit):
    import pathlib

    if explicit:
        pkg = pathlib.Path(explicit)
        if not (pkg / "handlers.py").exists() or not (pkg / "configurations.py").exists():
            raise click.UsageError(
                f"Could not locate handlers.py + configurations.py under {pkg}."
            )
        return pkg
    candidates = [
        d for d in pathlib.Path(".").iterdir()
        if d.is_dir() and not d.name.startswith((".", "_"))
        and (d / "handlers.py").exists() and (d / "configurations.py").exists()
    ]
    if len(candidates) != 1:
        raise click.UsageError(
            "Could not locate a connector package (a directory containing "
            "handlers.py and configurations.py). Run from your project root "
            "or pass --package."
        )
    return candidates[0]


def _class_name(action_id, suffix):
    return "".join(part.capitalize() for part in action_id.split("_")) + suffix


@cli.command("add-action")
@click.option("--type", "action_type", type=click.Choice(list(_CONFIG_BASES)),
              prompt="Action type")
@click.option("--id", "action_id", prompt="Action id (snake_case)",
              callback=lambda ctx, param, value: value.strip())
@click.option("--title", default="", help="Display name shown in the Gundi portal")
@click.option("--crontab", default="",
              help="Crontab schedule (pull actions only), e.g. '*/15 * * * *'")
@click.option("--package", "package_dir", default=None,
              help="Connector package directory (auto-detected by default)")
def add_action(action_type, action_id, title, crontab, package_dir):
    """Append a stub handler + config class to your connector."""
    if not action_id.isidentifier() or action_id.lower() != action_id:
        raise click.UsageError(f"'{action_id}' is not a valid snake_case identifier.")
    pkg = _find_package_dir(package_dir)
    handlers_path, config_path = pkg / "handlers.py", pkg / "configurations.py"
    handlers_src = handlers_path.read_text()
    if f"def {action_id}(" in handlers_src:
        raise click.UsageError(f"{handlers_path} already defines '{action_id}'.")

    config_cls = _class_name(action_id, "Config")
    base = _CONFIG_BASES[action_type]
    config_block = (
        f"\n\nclass {config_cls}({base}):\n"
        f"    # Add configuration fields (FieldWithUIOptions) here.\n"
        f"    pass\n"
    )
    imports = [f"from gundi_action_runner.actions.core import {base}"]
    if action_type == "push":
        data_cls = _class_name(action_id, "Data")
        config_block += (
            f"\n\nclass {data_cls}(pydantic.BaseModel):\n"
            f"    # Shape of the data this push action receives.\n"
            f"    pass\n"
        )
        imports.append("import pydantic")

    title_arg = f', title="{title}"' if title else ""
    handler_lines = [
        "",
        "",
        f"from .configurations import {config_cls}" + (
            f", {_class_name(action_id, 'Data')}" if action_type == "push" else ""
        ),
    ]
    if crontab:
        handler_lines.append("from gundi_action_runner.services.action_scheduler import crontab_schedule")
    handler_lines.append("")
    handler_lines.append(f"@action.{action_type}(config={config_cls}{title_arg})")
    if crontab:
        handler_lines.append(f'@crontab_schedule("{crontab}")')
    if action_type == "push":
        handler_lines.append(
            f"async def {action_id}(integration, action_config, "
            f"data: {_class_name(action_id, 'Data')}, metadata):"
        )
    else:
        handler_lines.append(f"async def {action_id}(integration, action_config):")
    handler_lines.append("    # Implement the action; return a summary dict.")
    handler_lines.append("    return {}")
    handler_lines.append("")

    config_src = config_path.read_text()
    import_lines = "\n".join(i for i in imports if i not in config_src)
    config_path.write_text(
        config_src.rstrip("\n") + "\n"
        + (("\n" + import_lines + "\n") if import_lines else "")
        + config_block
    )
    handlers_path.write_text(handlers_src.rstrip("\n") + "\n" + "\n".join(handler_lines))
    click.echo(f"Added '{action_id}' ({action_type}) to {handlers_path} and {config_path}.")
```

- [ ] **Step 4: Run tests, full suite, commit**

```bash
pytest tests/test_cli.py -v
pytest
git add src/gundi_action_runner/cli.py tests/test_cli.py
git commit -m "Add gundi-runner add-action interactive codegen"
```

Expected: 144 passing (140 + 4).

---

### Task 5: Documentation

**Files:**
- Create: `docs/quickstart.md`, `docs/extension-api.md`, `docs/fork-migration.md`
- Modify: `README.md` (link the three docs from the library-preview section)

**Interfaces:**
- Consumes: everything shipped in Plans 1–3.
- Produces: the developer-facing docs the copier README and PyPI page point at.

**README caution:** `README.md` may carry uncommitted user modifications at the top (a Tracpoint draft). Make your edit inside the "Using as a library (preview)" section only; after `git add README.md`, verify `git diff --cached README.md` contains ONLY your added lines — if unrelated content is staged, STOP and report BLOCKED.

- [ ] **Step 1: Write `docs/quickstart.md`**

```markdown
# Quickstart: build a Gundi connector with gundi-action-runner

## Install

```bash
pip install "gundi-action-runner[cli]"
```

## Scaffold a project

```bash
gundi-runner new my-connector
# answer the prompts (project name, slug, pull/webhook support)
cd my-connector
pip install -e ".[dev]"
pytest
```

The generated project contains:

| Path | Purpose |
|---|---|
| `<package>/handlers.py` | Your action/webhook handlers (`@action.*`, `@webhook`) |
| `<package>/configurations.py` | Pydantic config models rendered in the Gundi portal |
| `<package>/client.py` | HTTP client for the external API |
| `<package>/transformers.py` | Raw data → Gundi observations/events |
| `main.py` | `app = create_app(handlers_modules=[...])` |
| `tests/` | Example tests using the built-in pytest fixtures |

## Run locally

```bash
gundi-runner run --handlers <package>.handlers
# API docs at http://127.0.0.1:8080/docs
```

## Add another action

```bash
gundi-runner add-action   # prompts for type, id, title, schedule
```

## Register in Gundi

```bash
export GUNDI_API_BASE_URL=... KEYCLOAK_CLIENT_ID=... KEYCLOAK_CLIENT_SECRET=...
gundi-runner register --slug my_connector --name "My Connector" \
  --handlers <package>.handlers --schedule "pull_observations:0 */4 * * *"
```

## Keep the scaffold fresh

Generated projects record the template source; pull scaffold improvements with
`copier update` (framework updates come via `pip install -U gundi-action-runner`).

Next: [extension API reference](extension-api.md) ·
[migrating an existing fork](fork-migration.md)
```

- [ ] **Step 2: Write `docs/extension-api.md`**

```markdown
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

## Testing your connector

Installing `gundi-action-runner` registers a pytest plugin exposing the
framework's fixtures (`integration_v2`, `mock_gundi_client_v2`,
`mock_publish_event`, ...) with no conftest wiring. Test deps ship as an
extra:

```bash
pip install "gundi-action-runner[testing]"
```
```

- [ ] **Step 3: Write `docs/fork-migration.md`**

```markdown
# Migrating an existing fork to the gundi-action-runner library

Forks of this template keep working without changes: merging upstream gives
you compatibility shims (`app/services/*` etc. re-export the library, with
`DeprecationWarning`s) and the framework rides in-tree under `src/` until you
migrate. Migration is optional and incremental.

## Step 0 — merge upstream (nothing else changes)

After merging, your CI/Dockerfile (inherited) run `pip install -e . --no-deps`
so `app.*` imports resolve to the library. Your handlers, configurations,
tests, and `uvicorn app.main:app` all keep working.

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
- `python -m app.register` still works; `gundi-runner register` is its
  library-native replacement.
```

- [ ] **Step 4: Link the docs from the README**

In the "Using as a library (preview)" section, replace the sentence that begins `See \`examples/reference_connector/\`` (keeping the maintainer line that follows) with:

```markdown
See `examples/reference_connector/` for a complete example, the
[quickstart](docs/quickstart.md), [extension API reference](docs/extension-api.md),
and the [fork migration guide](docs/fork-migration.md).
```

- [ ] **Step 5: Verify and commit**

```bash
pytest
python -c "import pathlib; [print(p) for p in pathlib.Path('docs').glob('*.md')]"
git add docs/quickstart.md docs/extension-api.md docs/fork-migration.md README.md
git diff --cached README.md   # MUST show only the link-sentence change
git commit -m "Add quickstart, extension API reference, and fork migration guide"
```

Expected: 144 passing; README staged diff clean.

---

## Not in this plan

- Publishing `v0.1.0` (human-gated; see RELEASING.md) and un-drafting PR #78.
- `copier update` end-to-end verification against a git tag — possible only after the first tagged release; the template's answers-file mechanics are exercised by consumers.
- CLAUDE.md rewrite for the new repo layout (worth doing at merge time, alongside un-drafting).
