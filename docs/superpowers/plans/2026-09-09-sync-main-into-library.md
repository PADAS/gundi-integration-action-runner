# Sync origin/main into the Action Runner Library Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bring `design/action-runner-library` up to date with the 70 commits merged to `origin/main` since merge base `58ad279` (PR #81), porting each change from main's `app/*` layout into the branch's `src/gundi_action_runner/*` package.

**Architecture:** The branch's `src/gundi_action_runner/**` modules are near-verbatim copies of main's `app/**` modules — measured divergence is 0–25 lines per file, almost entirely the `app.*` → `gundi_action_runner.*` import rewrite. The branch's `tests/*` are byte-identical to main's `app/services/tests/*` under that same rewrite, with **zero branch-only tests**. So this port is not a hunk-by-hunk merge: for all but two files it is "take main's version wholesale, apply the mechanical import rewrite, re-apply the short list of known branch divergences." The two exceptions (`app_factory.py`, `settings.py`) are hand-ported. A final parity task proves nothing was dropped by re-running the rewrite over every file and asserting a zero diff.

**Tech Stack:** Python 3.10, pydantic v1, FastAPI 0.115, pytest 7.4 + pytest-asyncio, redis-py 5, `gundi-client-v2[redis]` 3.7.1, stamina 24.3.

**Spec:** None — `origin/main` *is* the specification. The change set is PRs #82, #84, #89, #98, #99, #101, #102, #103, #104, #105, #106, #107, #108, #109 (`git log --oneline --no-merges origin/main ^HEAD`, 70 commits, +6,216/−325 across 40 files).

---

## Status and remaining work (2026-09-22)

Tasks 1–6 are committed (`2356488`..`2a84585`); section A finished 2026-09-22 (`c616586`..`c0e62db`), suite **443 passed**, every parity residual is a documented divergence. Main has since gained PR #114 (batched fan-outs) and PR #116 (template-repo guard, not applicable). A full parity sweep against current `origin/main` gives this list. Ordered by dependency; each item ends green.

**A. Finish the code port**
- [x] A1. Fix the helper: the three settings rules must not end in `$` — `from app import settings  # noqa: F401` appears in `errors`, `retry_policies`, `config_manager`, `gundi` and would be skipped. Use `s|^(\s*)from app import settings\b|...|`.
- [x] A2. Task 7 — `services/webhooks.py` (+135) + `tests/test_diagnostic_url_validation.py` (4→14). **Keep the branch's `routers/webhooks.py`**: main still `print()`s the raw body and headers.
- [x] A3. Task 8 — `services/config_events_consumer.py` (+164) + `tests/test_config_events_consumer.py` (6→26).
- [x] A4. Task 9 remainder — `self_registration.py` (REFERENCE type, `is_transient_gundi_error` retry) + `tests/test_self_registration.py` (13→15); `tests/test_gundi_api.py` (3→10); `tests/test_actions_core.py` (new, 4); land the missing `tests/test_retry_policies.py` (6).
- [x] A5. PR #114 — `action_scheduler.trigger_actions`, `activity_logger.publish_events` + `_publish_batches`; `tests/test_action_scheduler.py` (new); `tests/test_activity_logger.py` (13→19+, **has one branch-only test — merge, do not overwrite**).
- [x] A6. Task 10 remainder — `tests/test_token_cache.py` (16) with the entry-point assertion path changed to `gundi_action_runner/settings.py`.
- [x] A7. Task 12 — parity script over every file; `grep` for leaked `app.*`; full suite; wheel builds. Target ≈ **380 passed**.

**B. Fork parity for a generated connector**
- [x] B1. Emit CI in the template: `.github/workflows/pr.yaml.jinja` (tests on PR) and `main.yaml.jinja` (tests → `PADAS/gundi-workflows/build_docker.yml@v2` → `update_hcl.yml@v2.1` for dev/stage/prod, parametrised on the connector slug), mirroring main's. Add a `tests/test_template.py` case that renders them.
- [x] B2. Template env: `OAUTH_*` → `GUNDI_OAUTH_*`, add `REDIS_TOKEN_CACHE_DB` / `GUNDI_TOKEN_CACHE_URL` (Task 11).
- [x] B3. Docs: quickstart, fork-migration (token cache, `GUNDI_OAUTH_*`, batched `trigger_actions`, what CI the scaffold now emits) (Task 11).
- [x] B4. Confirm `local/docker-compose.yml.jinja` + `Dockerfile.jinja` still match main's dev stack after the Redis db-2 addition.

**C. Ship and prove it**
> Done 2026-09-22: `v0.1.0rc4` published (run 35799312075, all jobs green; the first push of that tag was refused by the suite and re-pointed before anything reached PyPI). Proof ran entirely from the published package and the pushed tag: fresh venv → `pip install gundi-action-runner[cli,testing]==0.1.0rc4` → `gundi-runner new` (default template, tag `v0.1.0rc4`) → 2 generated tests pass → `pip install -e .[dev]` resolves → `docker build --target prod` pulls rc4 and imports → `gundi-runner register` against stage returned 201 as **`acme_tracker_e2e`** (service URL `https://acme-tracker-e2e.invalid`) — **delete that integration type from stage when done**. Upstream: issue #117 (webhook `print`). `stash@{0}` dropped.

- [x] C1. Bump to `0.1.0rc4`; `RELEASING.md` flow; publish.
- [x] C2. End-to-end proof: `gundi-runner new` a throwaway connector against the *published* rc4, run its tests, build its image, `gundi-runner register` against stage. That is the parity claim, demonstrated rather than asserted.
- [x] C3. Drop `stash@{0}`; upstream note to main about the webhook `print()`.

---

## Global Constraints

These apply to every task.

**Verification baseline.** Current suite: `.venv/bin/python -m pytest tests/ -q` → **186 passed** in ~2.3s. Every task ends with a green suite; the count only ever goes up. Final expected count ≈ **347** (186 + 161 ported tests).

**Interpreter.** `.venv/bin/python`. There is no `python` or `timeout` on PATH.

**The import rewrite.** Main's module paths map to the package as:

| main | branch |
|---|---|
| `app.settings` | `gundi_action_runner.settings` |
| `app.services.<mod>` | `gundi_action_runner.services.<mod>` |
| `app.actions`, `app.actions.core` | `gundi_action_runner.actions[.core]` |
| `app.webhooks`, `app.webhooks.core` | `gundi_action_runner.webhooks[.core]` |
| `app.api_schemas` | `gundi_action_runner.api_schemas` |
| `app.routers` | `gundi_action_runner.routers` |
| `from app import settings` | `from gundi_action_runner import settings` |
| `import app.settings as settings` | `from gundi_action_runner import settings` |
| `app.conftest` | `gundi_action_runner.testing.fixtures` |

**Strings that must NOT be rewritten** — a blind `s/app\./gundi_action_runner./g` corrupts all of these:

- `app.include_router`, `app.post`, `app.get`, `app.add_middleware`, `app.exception_handler`, `app.register` — method calls on the FastAPI `app` object, not module paths.
- `app.actions.handlers`, `app.webhooks.handlers` — the legacy fork convention. These are the *default values* of `GUNDI_LEGACY_ACTIONS_MODULE` / `GUNDI_LEGACY_WEBHOOKS_MODULE` in `settings.py` and must stay literally `app.*`.
- `app.example.com` — a hostname in the url_policy tests.
- `app.main` — left literal by the helper and resolved per occurrence, because it has three different branch equivalents:
  - `from app.main import app` → **keep as is.** The branch's own `tests/test_config_events_consumer.py` and `test_self_registration.py` import the FastAPI instance through the `app/main.py` shim; this also exercises the shim.
  - `mocker.patch("app.main.execute_action")` → `mocker.patch("gundi_action_runner.app_factory.execute_action")` (the branch already does this at `tests/test_action_runner.py:602`).
  - `caplog.set_level(..., logger="app.main")` → `logger="gundi_action_runner.app_factory"` (`app_factory.py:20` is `logging.getLogger(__name__)`).
  - In `test_token_cache.py`'s entry-point list, `"app.main"` and `"app.register"` stay literal — both exist on the branch and reach `settings` through the shims — but the assertion `endswith('app/settings/base.py')` becomes `endswith('gundi_action_runner/settings.py')`.

**Why the helper is perl, not sed.** BSD sed (macOS) does not support `\b`; a `sed -E` version of these rules silently no-ops every word-bounded pattern while the un-bounded ones work, producing a file that *looks* ported and still imports `app.*`. This was observed, not theorised: validated against the merge base, the sed version left 192 residual lines in `test_action_runner.py`; the perl version leaves 0 on every file whose only divergence is the import rewrite.

**The three settings rules must allow leading whitespace.** Main's tests import settings *inside* test functions (`    from app import settings`, four times in `test_action_runner.py` alone) so they can `mocker.patch.object` it. An `^`-anchored rule skips those, and the failure is silent and confusing rather than an ImportError: `app/settings/__init__.py` on this branch is a **star-import re-export package**, not a `sys.modules` alias like the other shims, so `from app import settings` resolves to a *different module object* whose values are copies taken at import time. `patch.object` then mutates the copy, the module under test reads the original, and the test fails asserting the un-patched behaviour. This cost two failures in Task 6; the rules below carry `^(\s*)` for exactly this reason.

**A side effect worth knowing.** Six tests landed by the previous port (4ac919c) still patch through the shims — `mocker.patch("app.services.action_runner._portal", ...)` and friends at `tests/test_action_runner.py:626-717` and `test_activity_logger.py:227,256`. They work (the shim aliases `sys.modules`) but are the source of the five `DeprecationWarning`s in the baseline run. Taking main's files normalises them; the warnings disappear.

**Branch divergences to preserve** — after taking main's version of a file, re-apply these. They are the entire hand-written delta between the two layouts:

1. `services/action_runner.py` — imports `from gundi_action_runner.registry import registry`, and inside `execute_action` (main line ~200) carries the lazy-discovery fallback:
   ```python
   if not action_handlers:
       # Forks call execute_action directly (without create_app), relying on
       # the template's old import-time handler discovery — populate lazily.
       registry.ensure_loaded()
   ```
2. `services/self_registration.py` — inside the registration function, after the docstring:
   ```python
   from gundi_action_runner.registry import registry
   registry.ensure_loaded()
   ```
3. `services/gundi.py` — five `assert <cond>, "<msg>"` statements are `if not <cond>: raise ValueError("<msg>")` on the branch (a library must not rely on asserts, which `-O` strips). Lines with `gundi_api_key` and four `integration_id` guards.
4. `settings.py` — `LOGGING_LEVEL = env.str("LOGGING_LEVEL", env.str("LOG_LEVEL", "INFO"))` (alias), plus the three `GUNDI_HANDLERS_MODULES` / `GUNDI_LEGACY_ACTIONS_MODULE` / `GUNDI_LEGACY_WEBHOOKS_MODULE` settings.
5. `actions/__init__.py`, `webhooks/core.py`, `actions/core.py`, `routers/actions.py` — carry decorator/registry wiring absent from main. Diff before overwriting.

**Dependency pins to adopt from main** (`requirements-base.in` and `pyproject.toml` `[project].dependencies` both, kept in sync):

- `gundi-client-v2[redis]~=3.7.1` — the `[redis]` extra is the token cache backend. **3.7.1, not 3.7.0**: 3.7.0 served a token the API rejected with a plain 401 to every client sharing the cache until it expired. Do not add a 401 workaround; 3.7.1 handles it (PADAS/gundi-client#61).
- `stamina~=24.3.0` — 24.3 is the first release whose `on=` accepts a predicate, required by `services/retry_policies.py`.
- `httpx~=0.28.1` — imported directly by `services/*`; must stay on the 0.28 line gundi-client-v2 3.x requires.
- `aiohttp` — already declared on the branch (`aiohttp~=3.9`); keep the branch's pin.
- `uvicorn` — branch is on `~=0.30.0`, main on `~=0.23.2`. **Keep the branch's**, it is ahead.
- `fastapi~=0.115.0` — already matching.

**Do not adopt from main:** `.github/workflows/main.yaml` (the `update_hcl` v2.1 bump, PR #84) targets the template's deploy pipeline, which the library branch does not have. Skip it and say so in the merge commit.

**Superseded local work.** `stash@{0}` holds an earlier local re-implementation of the token cache (per-call-site `token_cache_url=` kwargs). Main's design supersedes it: main installs the URL into `gundi_client_settings.GUNDI_TOKEN_CACHE_URL` at import so even bare `GundiClient()` calls in connector code pick it up. Take main's. Only the branch-only `docs/` and `template/` edits are recovered from the stash (Task 11), rewritten against main's final design. A superseded local `tests/test_token_cache.py` (88 lines) is parked at `<scratchpad>/local-test_token_cache.py`; main's 16-test version replaces it.

---

## File Structure

| main source | branch destination | branch divergence | main added | task |
|---|---|---|---|---|
| `app/settings/base.py` | `src/gundi_action_runner/settings.py` | 11 | +132 | 2 |
| `app/services/errors.py` | `services/errors.py` | 0 | +97 | 3 |
| `app/services/url_policy.py` | `services/url_policy.py` | **new file** | +117 | 3 |
| `app/services/retry_policies.py` | `services/retry_policies.py` | **new file** | +53 | 3 |
| `app/services/gundi.py` | `services/gundi.py` | 15 | +88 | 4 |
| `app/services/state.py` | `services/state.py` | 2 | +38 | 4 |
| `app/services/config_manager.py` | `services/config_manager.py` | 2 | +491 | 5 |
| `app/services/action_runner.py` | `services/action_runner.py` | 11 | +435 | 6 |
| `app/services/webhooks.py` | `services/webhooks.py` | 10 | +135 | 7 |
| `app/services/config_events_consumer.py` | `services/config_events_consumer.py` | 0 | +164 | 8 |
| `app/services/self_registration.py` | `services/self_registration.py` | 8 | +18 | 9 |
| `app/services/action_scheduler.py` | `services/action_scheduler.py` | 2 | +5 | 9 |
| `app/services/activity_logger.py` | `services/activity_logger.py` | 4 | +11 | 9 |
| `app/services/core.py` | `services/core.py` | 0 | +1 | 9 |
| `app/actions/core.py` | `actions/core.py` | 5 | +39/−1 | 9 |
| `app/actions/__init__.py` | `actions/__init__.py` | 20 | +6 | 9 |
| `app/api_schemas.py` | `api_schemas.py` | 0 | +26/−4 | 9 |
| `app/routers/actions.py` | `routers/actions.py` | 7 | +10/−1 | 9 |
| `app/main.py` | `app_factory.py` | **250 (hand-port)** | +25/−10 | 10 |
| `app/conftest.py` | `testing/fixtures.py` | 10 | +40 | 10 |

Test files map `app/services/tests/test_X.py` → `tests/test_X.py` and `app/actions/tests/test_core.py` → `tests/test_actions_core.py`. Per-file test deltas (branch → main): action_runner 22→75, config_manager 17→49, config_events_consumer 6→26, diagnostic_url_validation 4→14, errors 12→18, gundi_api 3→10, state_manager 7→12, self_registration 13→15, activity_logger 12→12; new: retry_policies 6, token_cache 16, actions/test_core 4.

**Branch-only files no task touches:** `registry.py`, `cli.py`, `decorators.py`, `app_factory.py` (except Task 10), `tests/test_registry.py`, `test_cli.py`, `test_app_factory.py`, `test_shims.py`, `test_template.py`, `test_pytest_plugin.py`, `test_password_grant.py`, all of `template/`, `docs/`, `copier.yml`.

---

## The port helper

Tasks 3–9 all use this. Create it once in Task 1 at `<scratchpad>/port.sh` and `source` it; it is a developer tool, not a repo file — **do not commit it**.

```bash
# rewrite — stdin to stdout, main's import paths to the library's. perl, not
# sed: BSD sed has no \b (see Global Constraints).
rewrite() {
  perl -pe '
    s/\bapp\.actions\.handlers\b/\@\@LEGACY_A\@\@/g;
    s/\bapp\.webhooks\.handlers\b/\@\@LEGACY_W\@\@/g;
    s/\bapp\.example\.com\b/\@\@HOST\@\@/g;
    s/\bapp\.main\b/\@\@APPMAIN\@\@/g;
    s/\bapp\.settings\.base\b/gundi_action_runner.settings/g;
    s/\bapp\.conftest\b/gundi_action_runner.testing.fixtures/g;
    s|^(\s*)import app\.settings as settings\b|${1}from gundi_action_runner import settings|;
    s|\bfrom app import settings\b|from gundi_action_runner import settings|g;
    s|^(\s*)import app\.settings\b|${1}from gundi_action_runner import settings|;
    s/\bapp\.(settings|services|actions|api_schemas|routers|webhooks)\b/gundi_action_runner.$1/g;
    s/\@\@LEGACY_A\@\@/app.actions.handlers/g;
    s/\@\@LEGACY_W\@\@/app.webhooks.handlers/g;
    s/\@\@HOST\@\@/app.example.com/g;
    s/\@\@APPMAIN\@\@/app.main/g;
  '
}
# port <main-path> <branch-path> — main's file, rewritten, written to the branch path.
port() { git show "origin/main:$1" | rewrite > "$2"; }
# audit <branch-path> — every remaining app.* reference needing a human decision.
audit() { grep -nE '\bapp\.[a-z_]+' "$1" | grep -vE 'app\.(actions|webhooks)\.handlers|app\.example\.com'; }
```

After every `port`, run `audit` on the result. The only hits should be `app.main` (resolve per the table in Global Constraints) and, in `settings.py`, the two legacy defaults. Anything else means main introduced a module path the allowlist does not know — add it to the `(settings|services|...)` group, not by hand-editing the output.

**Validation the helper has already passed.** Rewriting the merge-base (`58ad279`) version of every shared file and diffing against the branch's current file gives residual 0 for every file whose divergence is import-only (11 source files, 6 test files) and exactly the documented hand-written lines for the rest: `gundi.py` 15, `settings.py` 11, `action_runner.py` 5, `self_registration.py` 2. Those four numbers are the parity allowances in Task 12.

---

### Task 1: Merge origin/main, keeping the shims

Records the merge and lands everything that is not a port: dependencies, root conftest, the two new modules' *absence* is deferred to Task 3. `src/` and `tests/` stay at branch versions here so the suite stays green; Tasks 2–10 fill them in and Task 12 proves nothing was missed.

**Files:**
- Modify: `requirements-base.in`, `pyproject.toml:13-29`, `conftest.py`, `local/.gitignore`, `.gitignore`
- Resolve-to-ours (keep the deprecation shims): all 18 conflicted paths under `app/`

**Interfaces:**
- Produces: a merge commit whose second parent is `origin/main`; `settings.GUNDI_TOKEN_CACHE_URL` does not exist yet (Task 2).

- [ ] **Step 1: Confirm a clean tree and the stash**

```bash
git status --short          # only untracked: .claude/ CLAUDE.md connector.code-workspace local/.env.* local/test-web-ui.sh uv.lock
git stash list | head -1    # stash@{0}: ... superseded by main PR #106
git fetch origin
```

- [ ] **Step 2: Start the merge**

```bash
git merge origin/main --no-commit --no-ff
```
Expected: `Automatic merge failed; fix conflicts`, 27 files in `git diff --name-only --diff-filter=U`.

- [ ] **Step 3: Resolve every `app/` conflict to the branch's shim**

Each of these is an 11-line `sys.modules[__name__] = importlib.import_module(...)` shim on the branch and a real module on main. The branch's shim is correct — the real content goes to `src/` in later tasks.

```bash
for f in app/actions/__init__.py app/actions/core.py app/api_schemas.py app/conftest.py \
         app/main.py app/routers/actions.py app/settings/base.py \
         app/services/action_runner.py app/services/action_scheduler.py \
         app/services/activity_logger.py app/services/config_events_consumer.py \
         app/services/config_manager.py app/services/core.py app/services/errors.py \
         app/services/gundi.py app/services/self_registration.py \
         app/services/state.py app/services/webhooks.py; do
  git checkout --ours "$f" && git add "$f"
done
```

- [ ] **Step 4: Verify the shims survived**

```bash
wc -l app/services/*.py app/actions/core.py app/api_schemas.py app/routers/actions.py app/settings/base.py
```
Expected: every file 11 lines (`app/main.py` 13, `app/conftest.py` 1, `app/actions/__init__.py` 20).

- [ ] **Step 5: Keep branch versions of the conflicted test files for now**

```bash
for f in tests/test_action_runner.py tests/test_config_manager.py \
         tests/test_diagnostic_url_validation.py tests/test_gundi_api.py; do
  git checkout --ours "$f" && git add "$f"
done
git rm -q --cached tests/test_retry_policies.py tests/test_token_cache.py 2>/dev/null
rm -f tests/test_retry_policies.py tests/test_token_cache.py
```
The two removed files arrive in Task 3 alongside the modules they test.

- [ ] **Step 6: Resolve `conftest.py` — take both sides**

```python
# Root conftest: enables the `pytester` fixture for plugin self-tests and
# anchors pytest's rootdir-based sys.path insertion, so isolated runs like
# `pytest tests/test_registry.py` can import the in-repo `app` package. It also
# runs before pytest imports gundi_action_runner.settings, so an environment
# default set here is in place before that module loads.
import os

pytest_plugins = ["pytester"]

# Tests share Gundi OAuth tokens within the process only. The runner's default
# is redis://<REDIS_HOST>:<REDIS_PORT>/2, and a developer with a local Redis up
# would otherwise have a token minted by one pytest run persisted and served to
# the next, while CI (no Redis) sees none of it. An explicit value in the
# environment is respected.
os.environ.setdefault("GUNDI_TOKEN_CACHE_URL", "")
```

```bash
git add conftest.py
```

- [ ] **Step 7: Resolve `requirements-base.in`**

Take main's file, then restore the branch's `uvicorn` pin:

```bash
git show origin/main:requirements-base.in > requirements-base.in
sed -i '' 's/^uvicorn~=0\.23\.2$/uvicorn~=0.30.0/' requirements-base.in
grep -E 'uvicorn|gundi-client|stamina|httpx|aiohttp' requirements-base.in
```
Expected: `uvicorn~=0.30.0`, `gundi-client-v2[redis]~=3.7.1`, `stamina~=24.3.0`, `httpx~=0.28.1`, `aiohttp`.

- [ ] **Step 8: Mirror the pins into `pyproject.toml`**

In `[project].dependencies` (`pyproject.toml:13-29`):

```python
    "gundi-client-v2[redis]~=3.7.1",  # [redis]: shared OAuth token cache backend.
                                      # >=3.7.1: 3.7.0 served a token the API rejected
                                      # with a plain 401 to every client sharing the
                                      # cache (PADAS/gundi-client#61).
    "stamina~=24.3.0",   # 24.3 is the first release whose `on=` accepts a predicate
    "httpx~=0.28.1",     # imported directly by services/*; the line gundi-client-v2 3.x needs
```
replacing `"gundi-client-v2~=3.5"` and `"stamina~=23.2.0"`, and adding `httpx`.

- [ ] **Step 9: Regenerate `requirements.txt`**

```bash
uvx --from pip-tools pip-compile --output-file=requirements.txt \
    requirements-base.in requirements-dev.in requirements.in
```
If the header comment comes out naming `uvx` rather than the documented command, hand-edit that one line to match the existing header — a known quirk.

- [ ] **Step 10: Take main's `local/.gitignore`, and stop tracking real secrets**

`local/.env.production` and `local/.env.stage` are untracked, unignored, and each contains a live `KEYCLOAK_CLIENT_SECRET`. A `git add -A` anywhere in this port would commit stage and production credentials.

```bash
git checkout --theirs local/.gitignore 2>/dev/null || git show origin/main:local/.gitignore > local/.gitignore
printf '.env.production\n.env.stage\n' >> local/.gitignore
git add local/.gitignore
git status --short | grep -c 'local/.env' # expected: 0
```

- [ ] **Step 11: Install and run the suite**

```bash
.venv/bin/python -m pip install -e '.[dev,cli,docs]' -q
.venv/bin/python -m pytest tests/ -q 2>&1 | tail -3
```
Expected: **186 passed**. A failure here is a dependency-pin problem (stamina 24 or httpx 0.28), not a port problem — fix it before committing.

- [ ] **Step 12: Commit the merge**

```bash
git add -u && git status --short   # confirm no local/.env.* staged
git commit
```
Message:
```
Merge origin/main: dependency and test-harness groundwork

Merges 70 commits (PRs #82-#109). This commit lands only what is not a
port: gundi-client-v2 3.7.1 with the [redis] token-cache extra, stamina
24.3 (predicate `on=`), a direct httpx pin, the root conftest's
GUNDI_TOKEN_CACHE_URL default, and local/.gitignore. Every app/* module
stays a deprecation shim; main's changes to them are ported into
src/gundi_action_runner over the following commits, tracked by the
parity check in docs/superpowers/plans/2026-09-09-sync-main-into-library.md.

Skipped: .github/workflows/main.yaml (update_hcl v2.1) — it drives the
template's deploy pipeline, which this branch does not carry.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
```

---

### Task 2: Port settings — the shared token cache

**Files:**
- Modify: `src/gundi_action_runner/settings.py`
- Test: `tests/test_token_cache.py` (lands in Task 10, once every module it imports is ported; the settings half is exercised there). Until then, Step 6's smoke test is the verification.

**Interfaces:**
- Produces: `settings.GUNDI_TOKEN_CACHE_URL: str`, `settings.REDIS_TOKEN_CACHE_DB: int`, `settings.default_token_cache_url(host, port, db) -> str`, `settings.validated_token_cache_url(url) -> str`. Assigns `gundi_client_settings.GUNDI_TOKEN_CACHE_URL` as an import side effect — every later task depends on that being in place.

- [ ] **Step 1: Diff to see exactly what main added**

```bash
git diff 58ad279 origin/main -- app/settings/base.py
```

- [ ] **Step 2: Port the file**

```bash
source <scratchpad>/port.sh
port app/settings/base.py src/gundi_action_runner/settings.py
audit src/gundi_action_runner/settings.py
```

- [ ] **Step 3: Re-apply the four branch divergences**

The `port` helper will have rewritten `app.actions.handlers` correctly (protected), but these are branch-only lines main does not have — re-add them:

```python
# LOGGING_LEVEL preferred; LOG_LEVEL accepted as an alias (used by existing env examples)
LOGGING_LEVEL = env.str("LOGGING_LEVEL", env.str("LOG_LEVEL", "INFO"))
```
replacing main's plain `LOGGING_LEVEL = env.str("LOGGING_LEVEL", "INFO")`, and after the Redis block:
```python
# How the connector's handlers are discovered by create_app():
# comma-separated import paths of modules using the @action/@webhook decorators...
GUNDI_HANDLERS_MODULES = env.str("GUNDI_HANDLERS_MODULES", None)
# ...and/or the legacy template-fork convention (action_-prefixed functions,
# module-level webhook_handler), scanned as a fallback.
GUNDI_LEGACY_ACTIONS_MODULE = env.str("GUNDI_LEGACY_ACTIONS_MODULE", "app.actions.handlers")
GUNDI_LEGACY_WEBHOOKS_MODULE = env.str("GUNDI_LEGACY_WEBHOOKS_MODULE", "app.webhooks.handlers")
```

- [ ] **Step 4: Fix the docstring references to main's paths**

Main's loader-ordering comment names `app.main`, `app.register`, `app.services.action_runner`. On the branch the entry points are `gundi_action_runner.create_app`, the `gundi-runner` CLI, and `gundi_action_runner.services.action_runner`. Reword the comment; the *behaviour* (runner's `read_env()` before `gundi_client_v2`'s loader, first loader wins per key) is unchanged and is pinned by a test in Task 3.

- [ ] **Step 5: Confirm the legacy defaults were not rewritten**

```bash
grep -n 'app\.actions\.handlers\|app\.webhooks\.handlers' src/gundi_action_runner/settings.py
```
Expected: exactly two hits, both still `app.*`. If they read `gundi_action_runner.*`, the rewrite ate them and every legacy fork's discovery breaks.

- [ ] **Step 6: Smoke-test the module**

```bash
.venv/bin/python -c "
from gundi_action_runner import settings
from gundi_client_v2 import settings as gcs
print('runner :', repr(settings.GUNDI_TOKEN_CACHE_URL))
print('client :', repr(gcs.GUNDI_TOKEN_CACHE_URL))
print('db     :', settings.REDIS_TOKEN_CACHE_DB)
print('ipv6   :', settings.default_token_cache_url('::1', 6379, 2))
"
```
Expected: runner and client agree; db `2`; the IPv6 form bracketed as `redis://[::1]:6379/2`.

- [ ] **Step 7: Run the suite**

```bash
.venv/bin/python -m pytest tests/ -q 2>&1 | tail -3
```
Expected: **186 passed**.

- [ ] **Step 8: Commit**

```bash
git add src/gundi_action_runner/settings.py
git commit -m "$(cat <<'EOF'
Port the shared OAuth token cache into the library's settings

Every GundiClient in the process now shares one token per credential set,
in memory and in Redis db 2, instead of minting one per portal call. The
URL is validated at import and degrades to process-only sharing with a
warning rather than taking the service down, and is installed into
gundi-client-v2's own settings so bare GundiClient() calls in connector
code pick it up without passing a kwarg.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 3: Port errors, and add url_policy + retry_policies

Three low-level modules with no dependency on the not-yet-ported ones, plus their tests. This is the first task that raises the test count. (`tests/test_token_cache.py` waits until Task 10: its loader-ordering test imports every service module and asserts each reaches `settings` before `gundi_client_v2`, which is only true once they are all ported.)

**Files:**
- Modify: `src/gundi_action_runner/services/errors.py`
- Create: `src/gundi_action_runner/services/url_policy.py`, `src/gundi_action_runner/services/retry_policies.py`
- Test: `tests/test_errors.py` (12→18), `tests/test_retry_policies.py` (new, 6)

**Interfaces:**
- Consumes: `settings.GUNDI_TOKEN_CACHE_URL` from Task 2.
- Produces: `errors.source_status_code(exc) -> int | None`; `retry_policies.REDIS_RETRY`, `retry_policies.GUNDI_API_RETRY`-style stamina policies; `url_policy`'s host/IP allowlist helpers. Tasks 4–8 import all three.

- [ ] **Step 1: Port the three modules**

```bash
source <scratchpad>/port.sh
port app/services/errors.py         src/gundi_action_runner/services/errors.py
port app/services/url_policy.py     src/gundi_action_runner/services/url_policy.py
port app/services/retry_policies.py src/gundi_action_runner/services/retry_policies.py
for f in errors url_policy retry_policies; do audit src/gundi_action_runner/services/$f.py; done
```
`url_policy.py` imports only stdlib, so its rewrite is a no-op. `retry_policies.py` has `from app import settings  # noqa: F401` → `from gundi_action_runner import settings  # noqa: F401`; the import is load-bearing for loader ordering, keep the noqa.

- [ ] **Step 2: Port the two test files**

```bash
port app/services/tests/test_errors.py         tests/test_errors.py
port app/services/tests/test_retry_policies.py tests/test_retry_policies.py
for f in test_errors test_retry_policies; do audit tests/$f.py; done
```
Expected: `audit` prints nothing for either file.

- [ ] **Step 3: Run to verify they fail for the right reason first**

```bash
.venv/bin/python -m pytest tests/test_retry_policies.py -q 2>&1 | tail -5
```
Run this *before* the modules are importable if you ported tests first; otherwise confirm the new modules exist and skip. Expected on a correct port: PASS. A `ModuleNotFoundError` means Step 1 was skipped; an `AttributeError` on `stamina` means the 24.3 pin from Task 1 did not install.

- [ ] **Step 4: Run the two files, then the suite**

```bash
.venv/bin/python -m pytest tests/test_errors.py tests/test_retry_policies.py -q 2>&1 | tail -3
.venv/bin/python -m pytest tests/ -q 2>&1 | tail -3
```
Expected: 24 passed for the two files (18 + 6); **198 passed** overall (186 − 12 old errors tests + 24).

- [ ] **Step 5: Commit**

```bash
git add src/gundi_action_runner/services/{errors,url_policy,retry_policies}.py \
        tests/{test_errors,test_retry_policies}.py
git commit -m "$(cat <<'EOF'
Port error classification, URL policy, and the shared retry policies

url_policy and retry_policies are new modules on main; errors gains
aiohttp status classification and source_status_code, which the retry
predicates and the ephemeral error path both read.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 4: Port gundi.py and state.py

**Files:**
- Modify: `src/gundi_action_runner/services/gundi.py`, `src/gundi_action_runner/services/state.py`
- Test: `tests/test_gundi_api.py` (3→10), `tests/test_state_manager.py` (7→12)

**Interfaces:**
- Consumes: `retry_policies`, `errors` from Task 3; `settings` from Task 2.
- Produces: `gundi.GUNDI_API_RETRY`, `gundi._block_if_ephemeral(...)` — both imported by `config_manager` (Task 5) and `action_runner` (Task 6); `state`'s ephemeral write guard.

- [ ] **Step 1: Port both modules and both test files**

```bash
source <scratchpad>/port.sh
port app/services/gundi.py src/gundi_action_runner/services/gundi.py
port app/services/state.py src/gundi_action_runner/services/state.py
port app/services/tests/test_gundi_api.py     tests/test_gundi_api.py
port app/services/tests/test_state_manager.py tests/test_state_manager.py
for f in src/gundi_action_runner/services/gundi.py src/gundi_action_runner/services/state.py \
         tests/test_gundi_api.py tests/test_state_manager.py; do audit "$f"; done
```

- [ ] **Step 2: Re-apply the assert→ValueError divergence in `gundi.py`**

Main uses bare asserts; a library must not, because `python -O` strips them and the guard silently vanishes. Convert all five:

```python
    if not gundi_api_key:
        raise ValueError(f"Cannot get a valid API Key for integration {integration_id}")
```
and, in four places:
```python
    if not integration_id:
        raise ValueError("integration_id is required")
```

- [ ] **Step 3: Verify no asserts came back**

```bash
grep -n '^\s*assert ' src/gundi_action_runner/services/gundi.py
```
Expected: no output.

- [ ] **Step 4: Run**

```bash
.venv/bin/python -m pytest tests/test_gundi_api.py tests/test_state_manager.py -q 2>&1 | tail -3
.venv/bin/python -m pytest tests/ -q 2>&1 | tail -3
```
Expected: 22 passed for the two files; **210 passed** overall.

- [ ] **Step 5: Commit**

```bash
git add src/gundi_action_runner/services/{gundi,state}.py tests/{test_gundi_api,test_state_manager}.py
git commit -m "$(cat <<'EOF'
Port the Gundi API retry policy and ephemeral state guards

gundi.py gains the shared GUNDI_API_RETRY policy and _block_if_ephemeral,
which config_manager and action_runner both use; state.py guards every
write on the ephemeral path and logs when it skips one. The branch keeps
its ValueError guards where main asserts: -O strips asserts.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 5: Port config_manager — absence sentinels and generations

Main's largest single change (+491). PRs #103, #104, #105: action-absence sentinels written with `NX`, Redis-issued sentinel generations so a recovery cannot undo a concurrent delete, compare-and-set on every consumer write, portal reload on `ActionConfigUpdated` hitting an absence, and expiry of legacy permanent webhook entries on read.

**Files:**
- Modify: `src/gundi_action_runner/services/config_manager.py`
- Test: `tests/test_config_manager.py` (17→49)

**Interfaces:**
- Consumes: `retry_policies.REDIS_RETRY` (Task 3), `gundi.GUNDI_API_RETRY` and `gundi._block_if_ephemeral` (Task 4).
- Produces: the sentinel/generation API that `config_events_consumer` (Task 8) and `action_runner` (Task 6) call.

- [ ] **Step 1: Read main's change before porting**

```bash
git diff 58ad279 origin/main -- app/services/config_manager.py | head -200
```
This one is worth reading rather than trusting blind — it is the most intricate change in the sync.

- [ ] **Step 2: Port module and tests**

```bash
source <scratchpad>/port.sh
port app/services/config_manager.py src/gundi_action_runner/services/config_manager.py
port app/services/tests/test_config_manager.py tests/test_config_manager.py
audit src/gundi_action_runner/services/config_manager.py
audit tests/test_config_manager.py
```
The branch's only divergence here was the `from app import settings` line, which `port` handles. Keep main's import-order comment (`# before gundi_client_v2: .env loader precedence`) with the path updated to `gundi_action_runner/settings.py`.

- [ ] **Step 3: Run**

```bash
.venv/bin/python -m pytest tests/test_config_manager.py -q 2>&1 | tail -3
.venv/bin/python -m pytest tests/ -q 2>&1 | tail -3
```
Expected: 49 passed for the file; **242 passed** overall.

- [ ] **Step 4: Commit**

```bash
git add src/gundi_action_runner/services/config_manager.py tests/test_config_manager.py
git commit -m "$(cat <<'EOF'
Port action absence sentinels and Redis-issued generations

Sentinels are written with NX so a stale reload cannot bury a new config,
every consumer write is a compare-and-set against a Redis-issued
generation so a recovery cannot undo a concurrent delete, and an
ActionConfigUpdated that hits an absence reloads from the portal. Legacy
permanent webhook entries now expire on read.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 6: Port action_runner — ephemeral reference and auth execution

Main's second-largest change (+435). PRs #98, #99, #101: GUNDI-5562 ephemeral execution, reference actions run without a stored config row on every path, `trigger_action` blocked on the ephemeral path, redacted error text with source status propagation, `_request_error_response`.

**Files:**
- Modify: `src/gundi_action_runner/services/action_runner.py`
- Test: `tests/test_action_runner.py` (22→75)

**Interfaces:**
- Consumes: everything from Tasks 2–5.
- Produces: `action_runner._request_error_response(action_id, message)` — imported by `routers/actions.py` in Task 9; the `integration_state` parameter on `execute_action`.

> **Sequencing correction (found while executing this task).** `test_action_runner.py` is not a `services/` test file: it posts through `TestClient` and spans three tasks' worth of source. Ported on its own it fails 43 times, because the router never forwards `integration_state` (Task 9) and the app factory still echoes the request body and ignores the PubSub `triggered_by` attribute (Task 10). **Do Task 9's `routers/actions.py`, Task 9's `action_scheduler.py` guard, and Task 10's two `app_factory.py` changes as part of this task**, or this task cannot go green. The "295 passed" figure below assumes that; the remaining Task 9 and 10 steps stay where they are.

- [ ] **Step 1: Port module and tests**

```bash
source <scratchpad>/port.sh
port app/services/action_runner.py src/gundi_action_runner/services/action_runner.py
port app/services/tests/test_action_runner.py tests/test_action_runner.py
audit src/gundi_action_runner/services/action_runner.py
audit tests/test_action_runner.py
```

- [ ] **Step 2: Re-apply the lazy-discovery divergence**

`port` produces main's file, which has no registry. Add to the imports:

```python
from gundi_action_runner.registry import registry
```
and inside `execute_action`, at the point where main reads `action_handlers` (main line ~200), before the lookup:

```python
            if not action_handlers:
                # Forks call execute_action directly (without create_app), relying on
                # the template's old import-time handler discovery — populate lazily.
                registry.ensure_loaded()
```

- [ ] **Step 3: Verify the fallback is present and reachable**

```bash
grep -n 'registry.ensure_loaded' src/gundi_action_runner/services/action_runner.py
```
Expected: one hit, inside `execute_action`. `tests/test_registry.py` covers this path — if it goes red the insertion point is wrong.

- [ ] **Step 4: Run**

```bash
.venv/bin/python -m pytest tests/test_action_runner.py tests/test_registry.py -q 2>&1 | tail -3
.venv/bin/python -m pytest tests/ -q 2>&1 | tail -3
```
Expected: 94 passed for the two files (75 + 19); **295 passed** overall.

- [ ] **Step 5: Commit**

```bash
git add src/gundi_action_runner/services/action_runner.py tests/test_action_runner.py
git commit -m "$(cat <<'EOF'
Port ephemeral reference and auth action execution

Reference and auth actions run from a supplied integration_state with no
stored config row, on every path. State writes, trigger_action, and the
portal are all blocked there; error text is allowlisted so neither
connector validator messages nor raw exception text reach the caller,
while the source's HTTP status still propagates. The branch's lazy
registry fallback is preserved for forks calling execute_action directly.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 7: Port webhooks and the URL validation tests

**Files:**
- Modify: `src/gundi_action_runner/services/webhooks.py`
- Test: `tests/test_diagnostic_url_validation.py` (4→14)

**Interfaces:**
- Consumes: `url_policy` (Task 3), `gundi.GUNDI_API_RETRY` (Task 4), `config_manager` (Task 5).

- [ ] **Step 1: Check whether `webhooks/core.py` needs anything**

```bash
git diff 58ad279 origin/main -- app/webhooks/core.py
```
Expected: no output — main did not touch it, and the branch's 25-line divergence stays as is. If there *is* output, port it by hand; the branch version is not a copy.

- [ ] **Step 2: Port the module and test file**

```bash
source <scratchpad>/port.sh
port app/services/webhooks.py src/gundi_action_runner/services/webhooks.py
port app/services/tests/test_diagnostic_url_validation.py tests/test_diagnostic_url_validation.py
audit src/gundi_action_runner/services/webhooks.py
audit tests/test_diagnostic_url_validation.py
```

- [ ] **Step 3: Confirm the test hostname survived**

```bash
grep -c 'app\.example\.com' tests/test_diagnostic_url_validation.py
```
Expected: a non-zero count, still `app.example.com`. If it reads `gundi_action_runner.example.com` the protection in `port` failed and the URL-policy assertions are meaningless.

- [ ] **Step 4: Run**

```bash
.venv/bin/python -m pytest tests/test_diagnostic_url_validation.py -q 2>&1 | tail -3
.venv/bin/python -m pytest tests/ -q 2>&1 | tail -3
```
Expected: 14 passed for the file; **305 passed** overall.

- [ ] **Step 5: Commit**

```bash
git add src/gundi_action_runner/services/webhooks.py tests/test_diagnostic_url_validation.py
git commit -m "$(cat <<'EOF'
Port the webhook lookup's retry policy and URL redaction

The webhook integration lookup uses the shared Gundi retry policy,
unnested and no longer blocking, and URL parser errors are redacted so a
malformed URL cannot echo credentials into a log. Two newer IPv6 prefixes
join the diagnostic URL policy.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 8: Port config_events_consumer

**Files:**
- Modify: `src/gundi_action_runner/services/config_events_consumer.py`
- Test: `tests/test_config_events_consumer.py` (6→26)

**Interfaces:**
- Consumes: `config_manager`'s sentinel API (Task 5).

- [ ] **Step 1: Port module and tests**

```bash
source <scratchpad>/port.sh
port app/services/config_events_consumer.py src/gundi_action_runner/services/config_events_consumer.py
port app/services/tests/test_config_events_consumer.py tests/test_config_events_consumer.py
audit src/gundi_action_runner/services/config_events_consumer.py
audit tests/test_config_events_consumer.py
```
The branch had zero divergence in this module, so the rewrite is the whole port.

- [ ] **Step 2: Run**

```bash
.venv/bin/python -m pytest tests/test_config_events_consumer.py -q 2>&1 | tail -3
.venv/bin/python -m pytest tests/ -q 2>&1 | tail -3
```
Expected: 26 passed for the file; **325 passed** overall.

- [ ] **Step 3: Commit**

```bash
git add src/gundi_action_runner/services/config_events_consumer.py tests/test_config_events_consumer.py
git commit -m "$(cat <<'EOF'
Port config-event handling for absence sentinels

An ActionConfigUpdated arriving against an absence sentinel now reloads
from the portal instead of being dropped, and cache reloads preserve
in-flight tombstones.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 9: Port the small modules

Seven files with small main-side changes, grouped because none is worth its own review gate and they share one test run.

**Files:**
- Modify: `services/self_registration.py`, `services/action_scheduler.py`, `services/activity_logger.py`, `services/core.py`, `actions/core.py`, `actions/__init__.py`, `api_schemas.py`, `routers/actions.py`
- Test: `tests/test_self_registration.py` (13→15), `tests/test_activity_logger.py` (12, unchanged — port anyway to stay byte-comparable), `tests/test_actions_core.py` (new, 4)

**Interfaces:**
- Consumes: `action_runner._request_error_response` (Task 6), `ActionTypeEnum.REFERENCE` (this task, `services/core.py`).

- [ ] **Step 1: Port the four zero- and low-divergence files**

```bash
source <scratchpad>/port.sh
port app/services/core.py           src/gundi_action_runner/services/core.py
port app/api_schemas.py             src/gundi_action_runner/api_schemas.py
port app/services/action_scheduler.py src/gundi_action_runner/services/action_scheduler.py
port app/services/activity_logger.py  src/gundi_action_runner/services/activity_logger.py
```
`services/core.py` gains exactly one line: `REFERENCE = "reference"` on `ActionTypeEnum`. `api_schemas.py` gains `integration_state` on `ActionRequest`.

- [ ] **Step 2: Port self_registration and re-apply its divergence**

```bash
port app/services/self_registration.py src/gundi_action_runner/services/self_registration.py
```
Then re-add, inside the registration function after the docstring:
```python
    from gundi_action_runner.registry import registry
    registry.ensure_loaded()
```

- [ ] **Step 3: Hand-port `actions/__init__.py` — do NOT overwrite**

The branch version (27 lines) carries decorator/registry wiring main has no equivalent for. Main added only a settings-first import. Add to the top of the branch file:

```python
# Settings first: discover_actions() below executes the connector's handlers
# module at import, and gundi_action_runner.settings installs the shared token
# cache URL into gundi-client-v2's settings, so a GundiClient() built at module
# scope there only picks it up if this runs first. Done here rather than in the
# package __init__ so lightweight imports stay light.
from gundi_action_runner import settings  # noqa: F401
```

- [ ] **Step 4: Hand-port `actions/core.py` and `routers/actions.py`**

Both diverge on the branch (5 and 7 lines). Apply main's additions by hand. For `routers/actions.py`, add the import and the three request-shape guards:

```python
from gundi_action_runner.services.action_runner import execute_action, ActionTrigger, _request_error_response
```
and, after `triggered_by` is resolved:
```python
    # Request-shape rejections share the runner's {"detail": {"action_id",
    # "error"}} response and, like the runner's, publish no activity event.
    if request.integration_id is None and request.integration_state is None:
        return _request_error_response(request.action_id, "Provide either integration_id or integration_state.")
    if request.integration_id is not None and request.integration_state is not None:
        return _request_error_response(request.action_id, "Provide either integration_id or integration_state, not both.")
    if request.integration_state is not None and request.run_in_background:
        return _request_error_response(request.action_id, "Ephemeral executions cannot run in background.")
```
and pass `integration_state=request.integration_state` to the foreground `execute_action` call.

For `actions/core.py`, take main's +39 — the `ReferenceOption` and `ReferenceDataResponse` pydantic models (PR #109), which belong beside the `ReferenceActionConfiguration` marker the branch already has, plus `List` on the `typing` import. Diff first, since the branch file diverges by 5 lines:
```bash
git diff 58ad279 origin/main -- app/actions/core.py
diff <(git show 58ad279:app/actions/core.py) src/gundi_action_runner/actions/core.py
```

- [ ] **Step 5: Port the test files**

```bash
port app/services/tests/test_self_registration.py tests/test_self_registration.py
port app/services/tests/test_activity_logger.py   tests/test_activity_logger.py
port app/actions/tests/test_core.py               tests/test_actions_core.py
for f in tests/test_self_registration.py tests/test_activity_logger.py tests/test_actions_core.py; do audit "$f"; done
```
Note the rename: main's `app/actions/tests/test_core.py` becomes `tests/test_actions_core.py`, because `tests/` is flat and `test_core` would collide with a services-level name.

- [ ] **Step 6: Run**

```bash
.venv/bin/python -m pytest tests/ -q 2>&1 | tail -3
```
Expected: **331 passed**.

- [ ] **Step 7: Commit**

```bash
git add -u src/gundi_action_runner tests
git add tests/test_actions_core.py
git commit -m "$(cat <<'EOF'
Port reference action registration and request-shape rejections

Reference actions always register with the "reference" type;
/execute rejects a malformed request (both or neither of
integration_id/integration_state, or an ephemeral background run) with
the runner's own error shape and no activity event. Settings load before
handler discovery so a GundiClient built at connector module scope sees
the shared token cache.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 10: Hand-port app_factory and the test fixtures

`app_factory.py` is the one file that is a genuine rewrite rather than a copy (250 lines diverged: `create_app()` factory vs main's module-level `app`). Main changed it in three specific ways; apply those three, not the file.

**Files:**
- Modify: `src/gundi_action_runner/app_factory.py:77-94,130-137`, `src/gundi_action_runner/testing/fixtures.py`
- Test: `tests/test_app_factory.py` (existing, 6), `tests/test_token_cache.py` (new, 16 — deferred from Task 3 because its loader-ordering test needs every service module ported)

**Interfaces:**
- Consumes: `settings` (Task 2), `routers/actions.py` (Task 9).

- [ ] **Step 1: Read main's three changes**

```bash
git diff 58ad279 origin/main -- app/main.py
```

- [ ] **Step 2: Apply change 1 — settings before routers**

Already true on the branch: `app_factory.py:13` imports `settings` and the routers come at line 15. Only main's comment is missing; add it above line 13 so the ordering survives an import sort:

```python
# settings first: the routers pull in gundi_client_v2, which loads a .env of
# its own, and the first loader wins per key (see settings.py).
from gundi_action_runner import settings
```

- [ ] **Step 3: Apply change 2 — read `triggered_by` from PubSub attributes**

At `app_factory.py:77`, before the background/foreground branch, replacing both `triggered_by=json_payload.get("triggered_by")` call arguments (lines 87 and 94) with `triggered_by=triggered_by`:

```python
    # It is read from the PubSub message attributes as well as the body:
    # gundi_core's RunIntegrationAction command has no `triggered_by` field, so
    # a portal that serializes that model cannot put the marker in the payload
    # and the MANUAL branch would never be reachable over PubSub.
    triggered_by = json_payload.get("triggered_by") or (
        json_data["message"].get("attributes") or {}
    ).get("triggered_by")
```

- [ ] **Step 4: Apply change 3 — keep the request body out of 422s and logs**

Replace the body of `validation_exception_handler` at `app_factory.py:130-137`:

```python
    # The request body can carry draft credentials on the ephemeral path, so
    # neither the response nor the log gets it: log access and retention are
    # usually broader than access to the originating request. Keep only
    # loc/msg/type per error. On the pinned pydantic 1.x, `ctx` can carry
    # values from the offending input; `input` is dropped too so a pydantic 2
    # upgrade, which mirrors the value there, does not reopen the leak.
    safe_errors = [
        {k: v for k, v in err.items() if k not in ("input", "ctx")}
        for err in exc.errors()
    ]
    logger.debug("Failed handling body: %s", jsonable_encoder({"detail": safe_errors}))
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content=jsonable_encoder({"detail": safe_errors}),
    )
```

- [ ] **Step 5: Verify the body cannot leak**

```bash
grep -n 'exc.body' src/gundi_action_runner/app_factory.py
```
Expected: no output.

- [ ] **Step 6: Port the test fixtures**

```bash
source <scratchpad>/port.sh
port app/conftest.py src/gundi_action_runner/testing/fixtures.py
audit src/gundi_action_runner/testing/fixtures.py
```
Main added 40 lines of fixtures. The branch's 10-line divergence is imports, which `port` handles. Confirm the module still exports what `[project.entry-points.pytest11]` advertises — it is the installed pytest plugin, so a missing fixture breaks every downstream connector's suite, not just this repo's.

- [ ] **Step 7: Port `test_token_cache.py` and adapt its loader-ordering test**

```bash
port app/services/tests/test_token_cache.py tests/test_token_cache.py
audit tests/test_token_cache.py
```
`audit` will report the entry-point list in `test_runner_settings_load_before_the_client_library` — `"app.main"` and `"app.register"` stay literal (both exist on the branch and reach `settings` through the shims). Then, in that test's probe string, change the assertion path:

```python
        "print(paths[0].endswith('gundi_action_runner/settings.py'), "
```
(was `'app/settings/base.py'`). The subprocess runs from the repo root, where the in-repo `app` package and the editable-installed `gundi_action_runner` are both importable. The docstring's `app/settings/base.py` and `app/settings/integration.py` references become `gundi_action_runner/settings.py` and "the connector's own settings module".

- [ ] **Step 8: Run**

```bash
.venv/bin/python -m pytest tests/test_token_cache.py -q 2>&1 | tail -3
.venv/bin/python -m pytest tests/ -q 2>&1 | tail -3
```
Expected: 16 passed for the file; **347 passed** overall. If a parametrised `test_runner_settings_load_before_the_client_library[...]` case fails naming a service module, that module's ported copy imports `gundi_client_v2` before `settings` — the fix is in the module (restore main's import order), not the test.

- [ ] **Step 9: Commit**

```bash
git add src/gundi_action_runner/app_factory.py src/gundi_action_runner/testing/fixtures.py tests/test_token_cache.py
git commit -m "$(cat <<'EOF'
Keep request bodies out of 422s; read triggered_by from PubSub attributes

A validation error no longer echoes the request body, which on the
ephemeral path can carry draft credentials, into either the response or
the debug log. triggered_by is read from the PubSub message attributes as
well as the body, so the MANUAL branch is reachable for a portal
serializing gundi_core's RunIntegrationAction, which has no such field.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 11: Update the branch-only docs and template

Main has no `docs/` or `template/`, so nothing merges here — but the token cache and the `GUNDI_OAUTH_*` rename are user-facing and the scaffolds still emit the old names. Recover the shape of this work from `stash@{0}`, but write it against main's *final* design (validated URL, client-settings install, 3.7.1), not the stash's superseded one.

**Files:**
- Modify: `docs/quickstart.md`, `docs/fork-migration.md`, `local/.env.local.example`, `local/LOCAL_DEVELOPMENT.md`, `template/.env.example.jinja`, `template/local/.env.local.example.jinja`
- Test: `tests/test_template.py` (5)

- [ ] **Step 1: Recover the stashed doc edits as a reference**

```bash
git stash show -p 'stash@{0}' -- docs template local > <scratchpad>/stashed-docs.patch
wc -l <scratchpad>/stashed-docs.patch
```
Read it; do not apply it blind. Its factual claims about per-call-site `token_cache_url=` kwargs are wrong for main's design.

- [ ] **Step 2: Rename the OAuth vars in the two scaffold templates**

`template/.env.example.jinja` and `template/local/.env.local.example.jinja`: `OAUTH_*` / `KEYCLOAK_*` → `GUNDI_OAUTH_*`, matching main's `.env.example`. Add the Redis database note:
```
# Redis databases: state 0, config cache 1, shared Gundi OAuth token cache 2.
REDIS_TOKEN_CACHE_DB=2
# GUNDI_TOKEN_CACHE_URL overrides the derived redis:// URL (e.g. file:///dir); empty = in-process only.
```

- [ ] **Step 3: Same rename in `local/.env.local.example` and `local/LOCAL_DEVELOPMENT.md`**

Take main's `local/.env.local.example` verbatim (`git show origin/main:local/.env.local.example`) — it is not a template file and main's version is authoritative. In `LOCAL_DEVELOPMENT.md`, `KEYCLOAK_CLIENT_SECRET` → `GUNDI_OAUTH_CLIENT_SECRET`.

- [ ] **Step 4: Document the cache in `docs/quickstart.md`**

Under the authentication section, after the grant-selection paragraph. State: the un-prefixed `OAUTH_*` and older `KEYCLOAK_*` names still work as fallbacks and the `GUNDI_`-prefixed one wins when both are set; one token per credential set is shared across every client and across replicas via Redis db `REDIS_TOKEN_CACHE_DB` (default 2); `GUNDI_TOKEN_CACHE_URL` overrides with a `redis://`, `rediss://` or `file:///dir` URL, or `""` for process-only; an unusable URL degrades to process-only with a warning rather than failing startup; treat that database like the config cache — it holds bearer credentials. Update the `export` example to `GUNDI_OAUTH_CLIENT_ID`.

- [ ] **Step 5: Add the fork-migration note in `docs/fork-migration.md`**

A bullet in the existing list: tokens are cached and shared from `gundi-client-v2 >= 3.7`; nothing to configure unless Redis db 2 is taken; tests that count token requests get the process cache cleared by the framework's pytest plugin; the preferred names are `GUNDI_OAUTH_*` with the old spellings still accepted.

- [ ] **Step 6: Run the template tests**

```bash
.venv/bin/python -m pytest tests/test_template.py -q 2>&1 | tail -3
.venv/bin/python -m pytest tests/ -q 2>&1 | tail -3
```
Expected: 5 passed; **347 passed** overall. `test_template.py` renders the scaffold, so a Jinja typo surfaces here.

- [ ] **Step 7: Commit**

```bash
git add docs template local/.env.local.example local/LOCAL_DEVELOPMENT.md
git commit -m "$(cat <<'EOF'
Document the shared token cache; scaffold GUNDI_OAUTH_* names

Generated connectors emit the GUNDI_OAUTH_* spellings and the token-cache
database note. The quickstart and fork-migration guides cover what the
cache does, how to move or disable it, and that it holds bearer
credentials.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 12: Parity check — prove nothing was dropped

The merge commit in Task 1 tells git that all of main is integrated. This task is what makes that claim true. It re-derives every ported file from `origin/main` and asserts the working tree matches, so a file silently skipped in Tasks 2–10 fails loudly here rather than resurfacing as a bug in a downstream connector.

**Files:**
- Create: nothing committed — a throwaway script at `<scratchpad>/parity.sh`

- [ ] **Step 1: Write the parity script**

```bash
#!/usr/bin/env bash
source <scratchpad>/port.sh
tmp=$(mktemp -d); status=0
check() {  # check <main-path> <branch-path> <allowed-diverged-lines>
  port "$1" "$tmp/f"
  n=$(diff "$tmp/f" "$2" | grep -c '^[<>]')
  if [ "$n" -gt "$3" ]; then
    printf 'FAIL %-44s %s diverged lines (allowed %s)\n' "$2" "$n" "$3"; status=1
  else
    printf 'ok   %-44s %s\n' "$2" "$n"
  fi
}
S=src/gundi_action_runner
check app/services/errors.py                 $S/services/errors.py                 0
check app/services/url_policy.py             $S/services/url_policy.py             0
check app/services/retry_policies.py         $S/services/retry_policies.py         0
check app/services/config_events_consumer.py $S/services/config_events_consumer.py 0
check app/services/utils.py                  $S/services/utils.py                  0
check app/services/core.py                   $S/services/core.py                   0
check app/api_schemas.py                     $S/api_schemas.py                     0
check app/services/state.py                  $S/services/state.py                  0
check app/services/config_manager.py         $S/services/config_manager.py         2
check app/services/action_scheduler.py       $S/services/action_scheduler.py       0
check app/services/activity_logger.py        $S/services/activity_logger.py        0
check app/services/webhooks.py               $S/services/webhooks.py               0
check app/services/self_registration.py      $S/services/self_registration.py      2
check app/services/action_runner.py          $S/services/action_runner.py          5
check app/services/gundi.py                  $S/services/gundi.py                  15
check app/conftest.py                        $S/testing/fixtures.py                0
check app/settings/base.py                   $S/settings.py                        11
for t in errors retry_policies gundi_api state_manager config_manager \
         action_runner diagnostic_url_validation config_events_consumer \
         self_registration activity_logger; do
  check "app/services/tests/test_$t.py" "tests/test_$t.py" 0
done
check app/services/tests/test_token_cache.py tests/test_token_cache.py 8
check app/actions/tests/test_core.py         tests/test_actions_core.py  0
rm -rf "$tmp"; exit $status
```

- [ ] **Step 2: Run it**

```bash
bash <scratchpad>/parity.sh
```
Expected: every line `ok`, exit 0. Every allowance above was **measured** by rewriting the merge-base file and diffing against the branch (see "Validation the helper has already passed" under the port helper), so it is the exact size of the documented divergence — `gundi.py` 15 (assert→raise ×5), `settings.py` 11 (LOG_LEVEL alias + 3 handler settings), `action_runner.py` 5 (registry import + 4-line fallback), `self_registration.py` 2 (registry import + call), `config_manager.py` 2 (comment path), `test_token_cache.py` 8 (assertion path + docstring, Task 10 Step 7). `test_action_runner.py` is 0, not 2: the one `app.main.execute_action` patch target is resolved to `gundi_action_runner.app_factory.execute_action` by hand, and `rewrite` leaves `app.main` literal, so the check will show that single line — resolve it in the check by running `rewrite` and then the same one-line substitution, or accept an allowance of 2 for that file only. **Anything above these numbers is a dropped port, not a bad threshold — go fix the file, do not raise the number.**

`actions/core.py`, `actions/__init__.py`, `routers/actions.py`, `webhooks/core.py` and `app_factory.py` are deliberately absent: they are hand-ported and diverge structurally. Verify those by re-reading `git diff 58ad279 origin/main -- <main path>` and confirming each hunk has an equivalent in the branch file.

- [ ] **Step 3: Confirm no `app.*` import leaked into the package**

```bash
grep -rnE '\bfrom app\b|\bimport app\b' src/ tests/ | grep -v 'app.actions.handlers\|app.webhooks.handlers'
```
Expected: no output.

- [ ] **Step 4: Confirm the shims still work**

```bash
.venv/bin/python -m pytest tests/test_shims.py -q 2>&1 | tail -3
.venv/bin/python -c "import warnings; warnings.simplefilter('ignore'); from app.services.action_runner import execute_action; print('shim ok')"
```

- [ ] **Step 5: Full suite, plus a clean-install check**

```bash
.venv/bin/python -m pytest tests/ -q 2>&1 | tail -3
.venv/bin/python -m build --wheel -o <scratchpad>/dist -q && ls <scratchpad>/dist
```
Expected: **347 passed**; a wheel builds.

- [ ] **Step 6: Compare against main's own suite as a cross-check**

```bash
git -C .worktrees/main pull --ff-only && (cd .worktrees/main && .venv/bin/python -m pytest app -q 2>&1 | tail -3)
```
Main's count should be within a handful of the ported files' subtotal. A large gap means a test file was missed. (This worktree already exists; if its venv is not set up, skip this step and rely on Step 2.)

- [ ] **Step 7: Drop the superseded stash and commit any parity fixes**

Only once Step 2 is green:
```bash
git stash drop 'stash@{0}'
rm -f <scratchpad>/local-test_token_cache.py
```

---

## Self-Review

**Spec coverage.** Walked the 14 merged PRs against the tasks: #82 (lockfile) → Task 1 Step 9; #84 (CI update_hcl) → explicitly skipped, Global Constraints; #89/#102 (template bugs, retry nesting, log redaction, cache reloads) → Tasks 3, 5, 7; #98/#99/#101 (ephemeral reference/auth execution, reference type) → Tasks 6, 9; #103/#104 (host policy, sentinel bounds, portal reloads, redaction) → Tasks 5, 7; #105 (sentinel generations) → Task 5; #106/#107/#108 (token cache, 3.7.1, loader ordering) → Tasks 1, 2, 3, 10; #109 (reference response models) → Task 9, Steps 4 and 5: it is the whole of `actions/core.py`'s +39 (the `ReferenceOption` and `ReferenceDataResponse` pydantic models, which two connectors had each grown independently) plus the new `app/actions/tests/test_core.py`. No gap.

**Placeholder scan.** No TBDs. Every code step carries the literal text to insert; every verification step carries the command and its expected output. The two "diff it first" steps (Task 9 Step 4, Task 12 Step 2) are deliberate — those files diverge structurally and a canned patch would be wrong.

**Type consistency.** `_request_error_response(action_id, message)` is produced in Task 6 and consumed in Task 9 with that signature. `source_status_code` is produced in Task 3 (`errors`) and imported by `retry_policies` in the same task. `GUNDI_API_RETRY` / `_block_if_ephemeral` are produced in Task 4 (`gundi`) and consumed in Task 5. `settings.GUNDI_TOKEN_CACHE_URL` is produced in Task 2 and consumed everywhere after. `registry.ensure_loaded()` is spelled identically in Tasks 6 and 9. Test counts chain consistently: 186 → 198 (T3) → 210 (T4) → 242 (T5) → 295 (T6) → 305 (T7) → 325 (T8) → 331 (T9) → 347 (T10).

**Known risk.** The `port` helper's allowlist is the load-bearing piece. If main introduced a new `app.<something>` module path after this plan was written, the rewrite silently leaves it as `app.*` and the import fails at test time — loudly, which is the desired failure mode. The `audit` call after every `port` exists to catch it before then.
