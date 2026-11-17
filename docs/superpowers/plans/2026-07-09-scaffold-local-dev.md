# Scaffold Local Dev Stack — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every `gundi-runner new` project gets a `local/` docker-compose dev stack (connector + redis + pubsub-emulator + topic initializer) equivalent to the fork-based runners' setup.

**Architecture:** Five new files under `template/local/` plus a multi-stage rewrite of `template/Dockerfile.jinja` (`base` → `dev` with debugpy/reload → `prod` last so plain `docker build .` stays production). The compose `connector` service builds the `dev` target, volume-mounts the package and `main.py` for hot reload, and the helper script wires the emulator's push subscription back to `http://connector:8080/` (the sub-action loopback). Generation-level tests only — no Docker in CI.

**Tech Stack:** docker compose (consumer-side), copier/Jinja templating (existing), yaml parsing in tests.

**Spec:** `docs/superpowers/specs/2026-07-09-scaffold-local-dev-design.md`. Reference implementation: `/Users/chrisdo/padas/gundi-integration-cmore/local/`.

## Global Constraints

- Branch `design/action-runner-library`. Never `git add -A` — explicit paths (tree contains unrelated untracked user files; README.md untouched).
- `source .venv/bin/activate`; `python -m pip` only.
- Names must agree everywhere: services `redis`, `pubsub_emulator`, `pubsub_topic_initializer`, `connector`; push endpoint `http://connector:8080/`; project `local-project`; topics `integration-events` + `local-actions-topic`.
- Only `template/**`, `tests/test_template.py`, and `docs/quickstart.md` change. Nothing fork-owned; this repo's own `local/` untouched.
- Copier is 7.2: files WITHOUT `.jinja` suffix copy raw; files WITH it render. Only files needing substitution get the suffix.
- Suite: 146 → 147 passing. Commits end with a blank line then:
  `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`

---

### Task 1: Template local/ stack + multi-stage Dockerfile + tests

**Files:**
- Create: `template/local/docker-compose.yml.jinja`, `template/local/helpers/create_subscriptions.sh`, `template/local/.env.local.example.jinja`, `template/local/.gitignore`, `template/local/LOCAL_DEVELOPMENT.md.jinja`
- Modify: `template/Dockerfile.jinja` (full rewrite below), `tests/test_template.py`

**Interfaces:**
- Consumes: copier answers `package_name`, `project_name`, `integration_type_slug`, `display_name`; the `generate_project` fixture (tests/conftest.py).
- Produces: generated `local/` tree that Task 2's docs reference; `dev`/`prod` Dockerfile targets.

- [ ] **Step 1: Write the failing test (append to `tests/test_template.py`)**

```python
def test_local_dev_stack(generate_project):
    import yaml

    dst = generate_project()
    for path in (
        "local/docker-compose.yml", "local/helpers/create_subscriptions.sh",
        "local/.env.local.example", "local/.gitignore", "local/LOCAL_DEVELOPMENT.md",
    ):
        assert (dst / path).exists(), f"missing {path}"

    compose = yaml.safe_load((dst / "local" / "docker-compose.yml").read_text())
    assert set(compose["services"]) == {
        "redis", "pubsub_emulator", "pubsub_topic_initializer", "connector"
    }
    connector = compose["services"]["connector"]
    assert connector["build"]["target"] == "dev"
    assert any("acme_tracker" in volume for volume in connector["volumes"])
    assert connector["depends_on"]["pubsub_topic_initializer"]["condition"] == (
        "service_completed_successfully"
    )

    helper = (dst / "local" / "helpers" / "create_subscriptions.sh").read_text()
    assert "http://connector:8080/" in helper
    assert "local-actions-topic" in helper

    env_example = (dst / "local" / ".env.local.example").read_text()
    assert "INTEGRATION_TYPE_SLUG=acme_tracker" in env_example
    assert "PUBSUB_EMULATOR_HOST=pubsub_emulator:8085" in env_example
    assert "INTEGRATION_COMMANDS_TOPIC=local-actions-topic" in env_example

    dockerfile = (dst / "Dockerfile").read_text()
    assert "AS dev" in dockerfile and "AS prod" in dockerfile
    # prod must be the LAST stage so a bare `docker build .` builds production
    assert dockerfile.rindex("AS prod") > dockerfile.rindex("AS dev")
    assert "debugpy" in dockerfile
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest tests/test_template.py::test_local_dev_stack -v` (from repo root, venv active)
Expected: FAIL — `missing local/docker-compose.yml`.

- [ ] **Step 3: Rewrite `template/Dockerfile.jinja` (full content)**

```dockerfile
FROM python:3.10-slim AS base

# Build deps for pyjq (used by the framework's webhook JQ transforms)
RUN apt-get update && apt-get install -y autoconf automake libtool make python3-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /code
COPY pyproject.toml .
COPY {{ package_name }}/ {{ package_name }}/
COPY main.py .
RUN pip install --no-cache-dir .

EXPOSE 8080

FROM base AS dev
RUN pip install --no-cache-dir debugpy
EXPOSE 5678
CMD ["python", "-m", "debugpy", "--listen", "0.0.0.0:5678", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080", "--reload"]

FROM base AS prod
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
```

(`prod` is intentionally last — the default build target for deploys.)

- [ ] **Step 4: Write `template/local/docker-compose.yml.jinja`**

```yaml
services:

  redis:
    image: redis:latest
    ports:
      - "6379:6379"
    healthcheck:
      test: ["CMD-SHELL", "redis-cli ping"]
      interval: 15s
      timeout: 15s
      retries: 10

  pubsub_emulator:
    image: google/cloud-sdk:latest
    platform: linux/amd64
    entrypoint: ["gcloud", "beta", "emulators", "pubsub", "start", "--project=local-project", "--host-port=0.0.0.0:8085"]
    environment:
      PUBSUB_PROJECT_ID: local-project
      PUBSUB_EMULATOR_HOST: pubsub_emulator:8085
    ports:
      - "8085:8085"
    healthcheck:
      test: ["CMD-SHELL", "curl -f http://0.0.0.0:8085/"]
      interval: 15s
      timeout: 15s
      retries: 10

  pubsub_topic_initializer:
    image: curlimages/curl:latest
    depends_on:
      pubsub_emulator:
        condition: service_healthy
    entrypoint: ["sh", "-c"]
    command: ["source /helpers/create_subscriptions.sh"]
    volumes:
      - ./helpers:/helpers

  connector:
    build:
      context: ..
      dockerfile: Dockerfile
      target: dev
    ports:
      - "8080:8080"
      - "5678:5678"
    volumes:
      - ../{{ package_name }}:/code/{{ package_name }}
      - ../main.py:/code/main.py
    env_file:
      - path: ".env.local"
        required: true
    depends_on:
      redis:
        condition: service_healthy
      pubsub_emulator:
        condition: service_healthy
      pubsub_topic_initializer:
        condition: service_completed_successfully
    healthcheck:
      test: ["CMD-SHELL", "python -c \"import urllib.request; urllib.request.urlopen('http://0.0.0.0:8080/')\""]
      interval: 5s
      timeout: 5s
      retries: 5
```

(Deliberate deviation from cmore: no `container_name` fields — fixed names collide when a developer runs two connectors; compose project scoping names them fine.)

- [ ] **Step 5: Write `template/local/helpers/create_subscriptions.sh`** (raw file, no `.jinja` — nothing to substitute)

```bash
#!/bin/sh

# Bootstraps the local Pub/Sub emulator for action-runner development.

# Wait until the emulator responds, creating the integration-events topic.
until curl -X PUT http://pubsub_emulator:8085/v1/projects/local-project/topics/integration-events; do sleep 2; done

# The topic the action runner publishes sub-actions to.
curl -X PUT http://pubsub_emulator:8085/v1/projects/local-project/topics/local-actions-topic

# Push subscription looping sub-action messages back into the runner service.
curl http://pubsub_emulator:8085/v1/projects/local-project/subscriptions/local-actions-subscription \
 --data '{"topic": "projects/local-project/topics/local-actions-topic", "pushConfig": {"pushEndpoint": "http://connector:8080/"}}' \
  -X PUT -H 'content-type: application/json'
```

- [ ] **Step 6: Write `template/local/.env.local.example.jinja`**

```bash
# Copy this file to .env.local and fill in the secret below.

# --- Gundi stage environment ---
# Ask the Gundi team for a stage client secret.
KEYCLOAK_CLIENT_SECRET="a-secret-from-gundi-stage"

LOG_LEVEL="DEBUG"
CDIP_ADMIN_ENDPOINT="https://api.stage.gundiservice.org"
GUNDI_API_BASE_URL="https://api.stage.gundiservice.org"
KEYCLOAK_AUDIENCE="cdip-admin-portal"
KEYCLOAK_CLIENT_ID="cdip-integrations"
KEYCLOAK_ISSUER="https://cdip-auth.pamdas.org/auth/realms/cdip-dev"
SENSORS_API_BASE_URL="https://sensors.api.stage.gundiservice.org"
REDIS_HOST="redis"
REDIS_PORT="6379"

# --- This connector ---
INTEGRATION_TYPE_SLUG={{ integration_type_slug }}
INTEGRATION_TYPE_NAME={{ display_name }}

# --- Local Pub/Sub emulator (leave as-is; helper scripts depend on these) ---
INTEGRATION_EVENTS_TOPIC=integration-events
GCP_PROJECT_ID=local-project
PUBSUB_EMULATOR_HOST=pubsub_emulator:8085
PUBSUB_PROJECT_ID=local-project
INTEGRATION_COMMANDS_TOPIC=local-actions-topic
```

- [ ] **Step 7: Write `template/local/.gitignore`** (raw)

```
.env.local
```

- [ ] **Step 8: Write `template/local/LOCAL_DEVELOPMENT.md.jinja`**

```markdown
# Running {{ project_name }} locally

Run this connector with Docker Compose alongside its supporting services —
redis (state/config cache) and a Google Pub/Sub emulator wired so sub-actions
loop back into the runner.

## Requirements

- Docker + Docker Compose

## Steps

From this `local/` directory:

1. Copy `.env.local.example` to `.env.local`.
2. Set `KEYCLOAK_CLIENT_SECRET` to a stage secret (ask the Gundi team).
3. `docker compose up --build`

Then open http://localhost:8080/docs for the connector's browsable API.

## Notes

- Code changes under `{{ package_name }}/` and `main.py` hot-reload (the
  container runs uvicorn `--reload` with your source mounted).
- A debugpy server listens on port 5678 — attach from VS Code with a standard
  "Python: Remote Attach" configuration (host `localhost`, port `5678`).
- The image build installs `gundi-action-runner` from PyPI, so this stack works
  once the library's first release is published.
- This setup talks to https://stage.gundiservice.org for Gundi core services.
```

- [ ] **Step 9: Run the new test, then the full suite**

```bash
pytest tests/test_template.py -v
pytest
```

Expected: `test_local_dev_stack` passes; full suite 147 (146 + 1). If the compose YAML fails to parse after Jinja rendering, fix the template (most likely a quoting issue in the rendered volumes lines).

- [ ] **Step 10: Commit**

```bash
git add template/ tests/test_template.py
git commit -m "Scaffold a local docker-compose dev stack in generated connectors"
```

---

### Task 2: Quickstart pointer

**Files:**
- Modify: `docs/quickstart.md`

**Interfaces:**
- Consumes: the generated `local/LOCAL_DEVELOPMENT.md` from Task 1.
- Produces: docs-site section; must keep `mkdocs build --strict` green (the docs workflow gates PRs on it).

- [ ] **Step 1: Add the section**

In `docs/quickstart.md`, immediately after the "## Run locally" section (before "## Add another action"), insert:

````markdown
## Run locally with Docker

Scaffolded projects include a `local/` docker-compose stack: the connector plus
redis and a Pub/Sub emulator wired so sub-actions loop back into the runner.
See the generated `local/LOCAL_DEVELOPMENT.md` for setup; in short:

```bash
cd local
cp .env.local.example .env.local   # then set KEYCLOAK_CLIENT_SECRET
docker compose up --build
```
````

(When writing the file, the inner fence is a normal three-backtick block — the nesting above is a plan artifact.)

- [ ] **Step 2: Verify the docs build and suite**

```bash
mkdocs build --strict
pytest -q
```

Expected: strict build zero warnings; 147 passed.

- [ ] **Step 3: Commit**

```bash
git add docs/quickstart.md
git commit -m "Point the quickstart at the scaffolded Docker dev stack"
```

---

## Not in this plan

- Live `docker compose up` verification (manual, post-v0.1.0 — the image build needs the package on PyPI; documented in the generated LOCAL_DEVELOPMENT.md).
- web-ui service, `gundi-runner local` command, reference-connector local/ (spec's out-of-scope list).
