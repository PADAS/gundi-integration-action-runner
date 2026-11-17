# Design: password-grant authentication DX for action runners

**Date:** 2026-07-09
**Status:** Approved (brainstorming session)
**Repo:** PADAS/gundi-integration-action-runner (branch design/action-runner-library, PR #78)

## Problem

Running a connector locally requires a Keycloak client secret (`KEYCLOAK_CLIENT_SECRET`)
that developers must request from the Gundi team. gundi-client-v2 3.5 already supports the
OAuth2 password grant — set `GUNDI_USERNAME`/`GUNDI_PASSWORD` and the client picks it
automatically ("password grant wins when user credentials are present"), with refresh-token
handling built in. The action runner passes through cleanly: every Gundi call path uses a
bare `GundiClient()` (portal `_portal`, config manager, sensors api-key fetch), so the
capability exists end-to-end today — it just isn't verified, surfaced, or documented.

## Decisions made

| Question | Decision |
|---|---|
| Scope | Verify (contract tests) + surface in DX (env examples, docs). No new auth code. |
| CLI `gundi-runner login` | Out of scope (YAGNI). |
| Public client id for password grant on stage | `cdip-oauth2` (no secret needed). |
| Env var naming | **Adopt `OAUTH_*` everywhere from now on** (`OAUTH_CLIENT_ID`, `OAUTH_CLIENT_SECRET`, `OAUTH_ISSUER`, `OAUTH_AUDIENCE`) — the client's preferred names; `KEYCLOAK_*` aliases are legacy. All template/docs surfaces (shipped this week, no fork legacy) sweep to `OAUTH_*`. |
| Runner's inert `KEYCLOAK_ALGORITHMS/AUDIENCE/AUTH_SERVICE/REALM/ISSUER` settings | Leave untouched — zero consumers (verified), but removal would break fork star-imports; a deprecation-window question, not this feature. |

## Components

### 1. Contract tests — `tests/test_password_grant.py`

Two tests, no network:

- **Grant selection**: mock `gundi_client_v2.auth.get_access_token_password_grant` and
  `gundi_client_v2.auth.get_access_token_client_credentials`; construct a `GundiClient`
  configured with username/password (constructor kwargs, as the env vars would set);
  call `get_access_token()`; assert the password-grant path was called and
  client-credentials was not. This pins the client behavior the feature relies on against
  future gundi-client-v2 upgrades.
- **Runner pass-through**: assert `gundi_action_runner.services.action_runner._portal`
  is a `GundiClient` constructed with no explicit credential overrides (the bareness IS
  the contract — env-driven auth selection must reach it). Implementation detail for the
  plan: assert the relevant client attributes equal the client-library settings values
  rather than inspecting call args, since `_portal` is module-level.

### 2. Scaffold `template/local/.env.local.example.jinja` — dual-mode auth block

Replaces the current single-secret block. `OAUTH_*` names throughout:

- **Option A — personal login (active by default):**
  `GUNDI_USERNAME=`, `GUNDI_PASSWORD=`, `OAUTH_CLIENT_ID="cdip-oauth2"` — comment: log in
  as yourself; no client secret needed.
- **Option B — service client (commented out):**
  `# OAUTH_CLIENT_ID="cdip-integrations"`, `# OAUTH_CLIENT_SECRET="ask-the-gundi-team"`.
- Shared lines (both modes): `OAUTH_AUDIENCE="cdip-admin-portal"`,
  `OAUTH_ISSUER="https://cdip-auth.pamdas.org/auth/realms/cdip-dev"`.

### 3. Scaffold `template/.env.example.jinja` (deploy-oriented)

`KEYCLOAK_CLIENT_ID=`/`KEYCLOAK_CLIENT_SECRET=` → `OAUTH_CLIENT_ID=`/`OAUTH_CLIENT_SECRET=`.
(Deployed runners keep using the service-client credentials; only the names change.)

### 4. Scaffold `template/local/LOCAL_DEVELOPMENT.md.jinja`

Step 2 becomes: set `GUNDI_USERNAME`/`GUNDI_PASSWORD` to your stage Gundi login (default),
or comment that block and use the service-client option if you have a client secret.

### 5. Docs — `docs/quickstart.md`

- New short subsection "Authenticating with Gundi" (near the register section): the two
  modes, `OAUTH_*` names, `cdip-oauth2` for personal login, and the note that
  password-grant operations run with YOUR account's permissions — a 403 on registration
  is a permissions matter, not a bug. All runner↔Gundi calls (including `gundi-runner
  register`) honor either mode; same client underneath.
- The two existing `KEYCLOAK_*` mentions in quickstart.md sweep to the new pattern
  (the `cp .env.local.example` comment and the register export line, which becomes the
  username/password form).

### 6. Template test update

Extend the template test (`test_local_dev_stack` or sibling) to assert the rendered
`.env.local.example` contains `GUNDI_USERNAME=`, `OAUTH_CLIENT_ID="cdip-oauth2"`, and no
`KEYCLOAK_` occurrences; assert `.env.example` likewise uses `OAUTH_*`.

## Constraints

- No changes to `src/gundi_action_runner/` runtime code (settings included) — this feature
  is tests + template + docs only.
- Nothing fork-owned; `mkdocs build --strict` stays green; suite 147 → 149 (two new
  contract tests; template assertions extend existing tests).
- `OAUTH_*` names must match gundi-client-v2's settings exactly (`OAUTH_CLIENT_ID`,
  `OAUTH_CLIENT_SECRET`, `OAUTH_ISSUER`, `OAUTH_AUDIENCE`, `GUNDI_USERNAME`,
  `GUNDI_PASSWORD`).

## Testing

- The two contract tests above (mocked, no network).
- Extended template generation assertions.
- Manual acceptance (post-merge): `docker compose up` a scaffold with only
  username/password set; the runner authenticates against stage.

## Out of scope

- `gundi-runner login` CLI; mirroring the credentials into runner settings; removing the
  runner's inert `KEYCLOAK_*` settings; any gundi-client-v2 changes; updating the
  fork-oriented root `local/` of this repo.
