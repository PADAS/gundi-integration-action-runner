# Password-Grant Auth DX — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let developers run a connector locally with their own Gundi username/password (no client secret): verify the existing gundi-client-v2 pass-through with contract tests and surface the dual-mode auth in the scaffold env files and docs, sweeping to `OAUTH_*` names.

**Architecture:** Zero runtime code changes. Two mocked contract tests pin the behavior we rely on (password-grant selection in gundi-client-v2, and the runner's bare env-driven `GundiClient`). The scaffold's env examples get a dual-mode auth block defaulting to personal login (`cdip-oauth2` public client); template + docs sweep `KEYCLOAK_*` → `OAUTH_*`.

**Tech Stack:** pytest + pytest-mock (existing), gundi-client-v2 3.5's `auth` module and `OAuthToken` model.

**Spec:** `docs/superpowers/specs/2026-07-09-password-grant-dx-design.md`.

## Global Constraints

- Branch `design/action-runner-library`. Never `git add -A`; explicit paths (tree has unrelated untracked user files; README.md untouched).
- `source .venv/bin/activate`; `python -m pip` only.
- **No changes under `src/gundi_action_runner/`** — tests + template + docs only.
- Exact env var names (must match gundi-client-v2 settings): `OAUTH_CLIENT_ID`, `OAUTH_CLIENT_SECRET`, `OAUTH_ISSUER`, `OAUTH_AUDIENCE`, `GUNDI_USERNAME`, `GUNDI_PASSWORD`. Public client id for personal login: `cdip-oauth2`. Service client id: `cdip-integrations`.
- Rendered scaffold env files must contain NO `KEYCLOAK_` occurrences after this plan.
- Verified constructor facts (do not rediscover): `GundiClient(**kwargs)` takes `oauth_client_id`, `username`, `password`, `oauth_token_url` (NOT `client_id`); `OAuthToken` requires all of `access_token, refresh_token, token_type, expires_in, refresh_expires_in`.
- Suite 147 → 149. `mkdocs build --strict` stays green. Commits end with a blank line then:
  `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`

---

### Task 1: Contract tests

These are characterization tests of EXISTING behavior — no RED/GREEN loop. Write them, run them; expected: PASS immediately. If either FAILS, STOP and report (that would mean the feature's premise is wrong — a finding, not something to fix by editing the library).

**Files:**
- Create: `tests/test_password_grant.py`

**Interfaces:**
- Consumes: `gundi_client_v2.GundiClient`, `gundi_client_v2.auth` (mock targets), `gundi_client_v2.settings`, `gundi_action_runner.services.action_runner._portal`.
- Produces: the pinned contract Task 2's docs describe.

- [ ] **Step 1: Write the tests**

`tests/test_password_grant.py`:

```python
"""Contract tests for password-grant authentication.

The action runner does not implement authentication itself — it relies on
gundi-client-v2 selecting the OAuth2 password grant when user credentials are
present, and on every runner code path using a bare env-driven GundiClient.
These tests pin both halves of that contract against future upgrades.
"""
from unittest.mock import AsyncMock

import pytest

from gundi_client_v2 import GundiClient
from gundi_client_v2.auth import OAuthToken


def _token():
    return OAuthToken(
        access_token="test-access",
        refresh_token="test-refresh",
        token_type="Bearer",
        expires_in=300,
        refresh_expires_in=1800,
    )


@pytest.mark.asyncio
async def test_password_grant_selected_when_user_credentials_present(mocker):
    password_grant = mocker.patch(
        "gundi_client_v2.auth.get_access_token_password_grant",
        new_callable=AsyncMock,
        return_value=_token(),
    )
    client_credentials = mocker.patch(
        "gundi_client_v2.auth.get_access_token_client_credentials",
        new_callable=AsyncMock,
    )
    client = GundiClient(
        username="dev@example.com",
        password="not-a-real-password",
        oauth_client_id="cdip-oauth2",
        oauth_token_url="https://auth.example.com/token",
    )
    token = await client.get_access_token()
    assert token.access_token == "test-access"
    password_grant.assert_awaited_once()
    client_credentials.assert_not_called()


def test_runner_portal_is_bare_env_driven_client():
    """The runner must not override credentials on its portal client —
    bareness is what lets GUNDI_USERNAME/GUNDI_PASSWORD select the grant."""
    from gundi_client_v2 import settings as client_settings

    from gundi_action_runner.services.action_runner import _portal

    assert isinstance(_portal, GundiClient)
    assert _portal.username == client_settings.GUNDI_USERNAME
    assert _portal.password == client_settings.GUNDI_PASSWORD
    assert _portal.client_id == client_settings.OAUTH_CLIENT_ID
    assert _portal.client_secret == client_settings.OAUTH_CLIENT_SECRET
```

- [ ] **Step 2: Run them**

Run: `pytest tests/test_password_grant.py -v`
Expected: **2 passed** on first run. (If the first test fails on an unexpected auth call path — e.g. token-endpoint discovery firing despite `oauth_token_url` — capture the traceback and report; do not modify library code.)

- [ ] **Step 3: Full suite + commit**

```bash
pytest
git add tests/test_password_grant.py
git commit -m "Pin the password-grant pass-through contract with tests"
```

Expected: 149 passed (147 + 2).

---

### Task 2: OAUTH_* sweep + dual-mode auth in template and docs

**Files:**
- Modify: `template/local/.env.local.example.jinja`, `template/.env.example.jinja`, `template/local/LOCAL_DEVELOPMENT.md.jinja`, `docs/quickstart.md`, `tests/test_template.py`

**Interfaces:**
- Consumes: the contract from Task 1; existing template test `test_local_dev_stack` and `generate_project` fixture.
- Produces: scaffolds that authenticate with personal login by default.

- [ ] **Step 1: Extend the template test first (append inside `test_local_dev_stack` in `tests/test_template.py`, after the existing env_example assertions)**

```python
    # Dual-mode auth: personal login default, OAUTH_* names only
    assert "GUNDI_USERNAME=" in env_example
    assert "GUNDI_PASSWORD=" in env_example
    assert 'OAUTH_CLIENT_ID="cdip-oauth2"' in env_example
    assert "KEYCLOAK_" not in env_example
    root_env = (dst / ".env.example").read_text()
    assert "OAUTH_CLIENT_ID=" in root_env
    assert "KEYCLOAK_" not in root_env
```

Run: `pytest tests/test_template.py::test_local_dev_stack -v`
Expected: FAIL on `GUNDI_USERNAME=` (templates not yet updated).

- [ ] **Step 2: Rewrite the Gundi sections of `template/local/.env.local.example.jinja`**

Replace everything from the top of the file through the `REDIS_PORT` line (keep the `# --- This connector ---` and emulator sections unchanged) with:

```bash
# Copy this file to .env.local and fill in your credentials below.

# --- Gundi stage environment ---
LOG_LEVEL="DEBUG"
CDIP_ADMIN_ENDPOINT="https://api.stage.gundiservice.org"
GUNDI_API_BASE_URL="https://api.stage.gundiservice.org"
SENSORS_API_BASE_URL="https://sensors.api.stage.gundiservice.org"
OAUTH_ISSUER="https://cdip-auth.pamdas.org/auth/realms/cdip-dev"
OAUTH_AUDIENCE="cdip-admin-portal"
REDIS_HOST="redis"
REDIS_PORT="6379"

# --- Gundi authentication (choose ONE option) ---
# Option A (default): log in as yourself — no client secret needed.
# Operations run with your account's permissions.
GUNDI_USERNAME=""
GUNDI_PASSWORD=""
OAUTH_CLIENT_ID="cdip-oauth2"

# Option B: service client (ask the Gundi team for a stage secret).
# Comment out Option A above and uncomment these two lines:
# OAUTH_CLIENT_ID="cdip-integrations"
# OAUTH_CLIENT_SECRET="a-secret-from-gundi-stage"
```

- [ ] **Step 3: Sweep `template/.env.example.jinja`**

Change the two lines:

```
KEYCLOAK_CLIENT_ID=
KEYCLOAK_CLIENT_SECRET=
```

to:

```
OAUTH_CLIENT_ID=
OAUTH_CLIENT_SECRET=
```

- [ ] **Step 4: Update `template/local/LOCAL_DEVELOPMENT.md.jinja` step 2**

Replace the line `2. Set \`KEYCLOAK_CLIENT_SECRET\` to a stage secret (ask the Gundi team).` with:

```markdown
2. Set `GUNDI_USERNAME` / `GUNDI_PASSWORD` to your own stage Gundi login
   (default — no client secret needed), or switch to the service-client
   option in the file if the Gundi team issued you a client secret.
```

- [ ] **Step 5: Update `docs/quickstart.md`**

5a. Insert a new section immediately BEFORE "## Register in Gundi":

```markdown
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
```

5b. In "## Register in Gundi", change the export line to:

```bash
export GUNDI_API_BASE_URL=... GUNDI_USERNAME=... GUNDI_PASSWORD=... OAUTH_CLIENT_ID=cdip-oauth2
```

5c. In "## Run locally with Docker", change the `cp` comment line to:

```bash
cp .env.local.example .env.local   # then set GUNDI_USERNAME / GUNDI_PASSWORD
```

- [ ] **Step 6: Verify everything**

```bash
pytest tests/test_template.py -v     # all pass, incl. the new assertions
mkdocs build --strict                # zero warnings
pytest                               # 149 passed
grep -rn "KEYCLOAK" template/ docs/quickstart.md   # expected: no output
```

- [ ] **Step 7: Commit**

```bash
git add template/local/.env.local.example.jinja template/.env.example.jinja \
        template/local/LOCAL_DEVELOPMENT.md.jinja docs/quickstart.md tests/test_template.py
git commit -m "Default scaffolds to password-grant login; adopt OAUTH_* names"
```

---

## Not in this plan

- `gundi-runner login`; runner-settings mirroring; removing the runner's inert `KEYCLOAK_*` settings (fork star-import compatibility); gundi-client-v2 changes; this repo's root `local/` files.
- Live verification against stage (manual, post-merge: scaffold + compose up with only username/password set).
