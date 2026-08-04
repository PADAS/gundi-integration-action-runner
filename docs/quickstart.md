# Quickstart: build a Gundi connector with gundi-action-runner

## Install

```bash
pip install "gundi-action-runner[cli]"
```

> **Note:** until the first release (v0.1.0) is published, install from a
> checkout of this repo (`pip install -e ".[cli]"`) and pass
> `--template`/`--vcs-ref` to `gundi-runner new` explicitly.

## Scaffold a project

```bash
gundi-runner new my-connector
# answer the prompts (project name, slug, pull/webhook support)
cd my-connector
pip install -e ".[dev]"
pytest
```

For CI or scripted use, pass `--defaults` (plus `--data KEY=VALUE` overrides) — without it, incomplete answers in a non-interactive shell produce a broken scaffold because copier fills nothing and does not error.

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

## Run locally with Docker

Scaffolded projects include a `local/` docker-compose stack: the connector plus
redis and a Pub/Sub emulator wired so sub-actions loop back into the runner.
See the generated `local/LOCAL_DEVELOPMENT.md` for setup; in short:

```bash
cd local
cp .env.local.example .env.local   # then set GUNDI_USERNAME / GUNDI_PASSWORD
docker compose up --build
```

## Add another action

```bash
gundi-runner add-action   # --type and --id prompt interactively; pass --title/--crontab to set them (they default to empty)
```

## Authenticating with Gundi

All runner↔Gundi calls (including `gundi-runner register`) authenticate
through the same client, in one of two modes:

- **Personal login (easiest for local dev):** set `GUNDI_USERNAME` and
  `GUNDI_PASSWORD` to your stage Gundi login, with
  `OAUTH_CLIENT_ID="cdip-oauth2"` (a public client — no secret needed).
  Operations run with **your** account's permissions; a 403 (for example on
  registration) means your account lacks that permission, not that something
  is broken.
- **Service client:** set `OAUTH_CLIENT_ID` and `OAUTH_CLIENT_SECRET` to a
  credential issued by the Gundi team.

When user credentials are present the client selects the OAuth2 password
grant automatically; otherwise it uses the client-credentials grant.

## Register in Gundi

```bash
export GUNDI_API_BASE_URL=... GUNDI_USERNAME=... GUNDI_PASSWORD=... OAUTH_CLIENT_ID=cdip-oauth2
gundi-runner register --slug my_connector --name "My Connector" \
  --handlers <package>.handlers --schedule "pull_observations:0 */4 * * *"
```

## Keep the scaffold fresh

Generated projects record the template source; pull scaffold improvements with
`copier update` (framework updates come via `pip install -U gundi-action-runner`).

Next: [extension API reference](extension-api.md) ·
[migrating an existing fork](fork-migration.md)
