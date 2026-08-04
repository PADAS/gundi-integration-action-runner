# Design: local dev environment (docker compose) for scaffolded connectors

**Date:** 2026-07-09
**Status:** Approved (brainstorming session)
**Repo:** PADAS/gundi-integration-action-runner (branch design/action-runner-library, PR #78)
**Reference implementation:** /Users/chrisdo/padas/gundi-integration-cmore/local/

## Problem

Fork-based action runners ship a `local/` docker-compose stack (runner + redis +
pubsub-emulator + topic initializer) that developers rely on for local development against
stage Gundi. Projects scaffolded by `gundi-runner new` have no equivalent — a scaffolded
connector can only be run bare (`gundi-runner run`) without the redis state store or the
pubsub sub-action loop.

**Goal:** every `gundi-runner new` project gets an equivalent `local/` stack, delivered
through the copier template so `copier update` propagates improvements.

## Decisions made

| Question | Decision |
|---|---|
| Delivery | Copier template files only — no `gundi-runner local` CLI command (YAGNI). |
| Stack scope | Core trio (redis, pubsub-emulator, topic initializer) + hot reload + debugpy. No web-ui (cmore-specific experiment). |
| Topology | Adapted port of the cmore stack — same services, images, healthchecks, and helper-script pattern the team already knows. |

## Components (all under `template/`, rendered into scaffolded projects)

### 1. `local/docker-compose.yml.jinja`

Four services, mirroring cmore with renames/adaptations:

- **redis** — `redis:latest`, port 6379, `redis-cli ping` healthcheck.
- **pubsub_emulator** — `google/cloud-sdk:latest` (`platform: linux/amd64`), entrypoint
  `gcloud beta emulators pubsub start --project=local-project --host-port=0.0.0.0:8085`,
  port 8085, curl healthcheck.
- **pubsub_topic_initializer** — `curlimages/curl:latest`, one-shot, mounts `./helpers`,
  runs `create_subscriptions.sh`, `depends_on: pubsub_emulator: service_healthy`.
- **connector** (cmore calls this `fastapi`) — `build: context: .., dockerfile: Dockerfile,
  target: dev`; ports `8080:8080` and `5678:5678` (debugpy); volumes mounting
  `../{{ package_name }}` and `../main.py` into `/code` for hot reload; `env_file:
  .env.local` (required); `depends_on` redis (healthy) + emulator (healthy) + initializer
  (completed); HTTP healthcheck against `http://0.0.0.0:8080/`.

### 2. `local/helpers/create_subscriptions.sh`

cmore's script with one change — the push endpoint hostname is the compose service name:

- wait-loop `PUT .../topics/integration-events` until the emulator responds
- `PUT .../topics/local-actions-topic`
- `PUT .../subscriptions/local-actions-subscription` with
  `pushConfig.pushEndpoint = http://connector:8080/`

This loopback is what makes sub-actions/triggered actions work locally: the runner
publishes to `local-actions-topic`, and the emulator pushes back into `POST /`.

### 3. `local/.env.local.example.jinja`

Two blocks:

- **Stage Gundi** — `KEYCLOAK_CLIENT_SECRET` placeholder ("ask the Gundi team"),
  `CDIP_ADMIN_ENDPOINT` / `GUNDI_API_BASE_URL` = `https://api.stage.gundiservice.org`,
  `KEYCLOAK_AUDIENCE/CLIENT_ID/ISSUER` (cmore's stage values),
  `SENSORS_API_BASE_URL = https://sensors.api.stage.gundiservice.org`,
  `LOG_LEVEL=DEBUG`, `REDIS_HOST=redis`, `REDIS_PORT=6379`, and this connector's
  `INTEGRATION_TYPE_SLUG={{ integration_type_slug }}` /
  `INTEGRATION_TYPE_NAME={{ display_name }}` pre-filled from copier answers.
- **Emulator (leave as-is)** — `GCP_PROJECT_ID=local-project`,
  `PUBSUB_EMULATOR_HOST=pubsub_emulator:8085`, `PUBSUB_PROJECT_ID=local-project`,
  `INTEGRATION_EVENTS_TOPIC=integration-events`,
  `INTEGRATION_COMMANDS_TOPIC=local-actions-topic` — names verified against
  `gundi_action_runner.settings` (GCP_PROJECT_ID, INTEGRATION_EVENTS_TOPIC,
  INTEGRATION_COMMANDS_TOPIC) and the Google client's `PUBSUB_EMULATOR_HOST` contract.

The scaffold's existing root `.env.example` (deploy-oriented) is unchanged.

### 4. `local/.gitignore` and `local/LOCAL_DEVELOPMENT.md`

- `.gitignore`: `.env.local`
- `LOCAL_DEVELOPMENT.md`, adapted from cmore: requirements (Docker + Compose), steps
  (copy `.env.local.example` → `.env.local`, set the secret, `docker compose up --build`),
  browsable API at `http://localhost:8080/docs`, attach-a-debugger note (debugpy on 5678),
  and a note that the image build installs `gundi-action-runner` from PyPI — so the stack
  works once v0.1.0 is published (same gate as the rest of the scaffold).

### 5. `template/Dockerfile.jinja` becomes multi-stage

- Current single-stage content becomes stage `base`; a final `prod` stage (default build
  target) keeps today's CMD — **production builds are unchanged** (`docker build .` with
  no `--target` produces the prod image).
- New `dev` stage (from `base`): `pip install debugpy`; CMD
  `python -m debugpy --listen 0.0.0.0:5678 -m uvicorn main:app --host 0.0.0.0 --port 8080
  --reload`. Compose targets `dev`.

## Constraints

- Service/volume/host names in compose, helper script, and env example must agree
  (`connector`, `redis`, `pubsub_emulator`) — the loopback endpoint depends on it.
- All new files live under `template/local/` + the Dockerfile edit; nothing outside the
  template changes except tests and one docs pointer.
- Fork-merge safety: template-only changes; nothing fork-owned touched.

## Testing

Extend `tests/test_template.py` (generation-level; no Docker in CI):

- Generated tree includes the five `local/` files.
- `local/docker-compose.yml` parses with `yaml.safe_load`; has the four expected service
  keys; the connector service's volumes reference the rendered package name.
- `create_subscriptions.sh` contains the `http://connector:8080/` push endpoint.
- `.env.local.example` contains the rendered slug/display-name and the emulator block.
- Generated `Dockerfile` contains both `AS prod` and `AS dev` stage markers and the debugpy
  CMD in the dev stage.
- Existing e2e (generated project's pytest) unaffected.

Live `docker compose up` is validated manually post-release (documented in
LOCAL_DEVELOPMENT.md), not in CI.

## Docs

`docs/quickstart.md` gains a short "Run locally with Docker" subsection pointing at the
generated `local/LOCAL_DEVELOPMENT.md` (two sentences + the compose command).

## Out of scope

- web-ui service; `gundi-runner local` CLI command.
- Adding a `local/` to `examples/reference_connector` (template tests cover the feature).
- Changes to this repo's own `local/` directory (user-owned, untracked files live there).
- CI-level docker compose smoke test (requires Docker + published package).
