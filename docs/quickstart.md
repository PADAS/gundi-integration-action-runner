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
| `local/` | docker-compose dev stack: connector + redis + Pub/Sub emulator |
| `.github/workflows/` | The fork pipeline: tests on PR; tests → image → deploy on push (see [Deploy](#deploy)) |

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
  `GUNDI_OAUTH_CLIENT_ID="cdip-oauth2"` (a public client — no secret needed).
  Operations run with **your** account's permissions; a 403 (for example on
  registration) means your account lacks that permission, not that something
  is broken.
- **Service client:** set `GUNDI_OAUTH_CLIENT_ID` and `GUNDI_OAUTH_CLIENT_SECRET`
  to a credential issued by the Gundi team.

When user credentials are present the client selects the OAuth2 password
grant automatically; otherwise it uses the client-credentials grant. The
un-prefixed `OAUTH_*` names and the older `KEYCLOAK_*` names still work as
fallbacks; when both spellings are set, the `GUNDI_`-prefixed one wins.

The runner mints one OAuth token per set of credentials and shares it across
every Gundi client it builds — and across replicas — through a token cache in
its Redis, database `REDIS_TOKEN_CACHE_DB` (default `2`, beside the state store
at `0` and the config cache at `1`). Nothing to configure unless that database
is taken. `GUNDI_TOKEN_CACHE_URL` overrides the derived `redis://` URL: point it
at `file:///some/dir` on a host without Redis, or set it to an empty string to
share tokens within the process only. An unusable URL is logged and the runner
falls back to in-process sharing rather than refusing to start. Treat that
database like the config cache: it holds bearer credentials.

## Register in Gundi

```bash
export GUNDI_API_BASE_URL=... GUNDI_USERNAME=... GUNDI_PASSWORD=... GUNDI_OAUTH_CLIENT_ID=cdip-oauth2
gundi-runner register --slug my_connector --name "My Connector" \
  --handlers <package>.handlers --schedule "pull_observations:0 */4 * * *"
```

## Deploy

A scaffolded project carries the same GitHub Actions pipeline a fork of the
template gets: `pr.yaml` runs the tests on every pull request, and `main.yaml`
runs them on a push, builds the image, and deploys by updating the connector's
`terragrunt.hcl` in `PADAS/gundi-integrations-v2-infra` — `main` to dev,
`release-**` branches to stage and then prod. The pipeline is keyed on the
GitHub repository name; the generated `README.md` ("Deploy") lists the
variables, the secret, and the infra files it expects to exist.

## Keep the scaffold fresh

Generated projects record the template source; pull scaffold improvements with
`copier update` (framework updates come via `pip install -U gundi-action-runner`).

Next: [extension API reference](extension-api.md) ·
[migrating an existing fork](fork-migration.md)
