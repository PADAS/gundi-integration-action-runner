# Sync origin/main (PRs #120–#135) into the Action Runner Library Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bring `design/action-runner-library` level with `origin/main` at `5d618f7` (PRs #120–#124, #128–#130, #135), so the published `gundi-action-runner` package carries the activity-log redaction, the status-aware retry verdict and the integration type description.

**Architecture:** Same recipe as the 2026-09-30 re-sync (`5ff40b5`), documented in `docs/superpowers/plans/2026-09-09-sync-main-into-library.md`: one merge commit that records `origin/main` as integrated while keeping every `app/*` deprecation shim and the branch's `src/`/`tests/` untouched (suite stays green), then one commit per ported area, where each `src/gundi_action_runner/**` and `tests/*` file is re-derived from main's file through the `rewrite` helper and the documented divergences are re-applied. A parity script at the end proves nothing was dropped.

**Tech Stack:** Python 3.10, Pydantic `~=1.10.15` (v1 API; `redaction.py` uses `pydantic.fields.ModelField` and `BaseModel._iter`), FastAPI, click, pytest.

**Spec:** No standalone spec. The requirements are the nine merged PRs on `origin/main` since the last sync's merge base `7f1e141` (`git log --first-parent 7f1e141..5d618f7`). Measurements below were taken on 2026-10-23 against `origin/design/action-runner-library` at `c05176d`.

## Delta at a glance

| PR | What | Branch destination |
|---|---|---|
| #120 | Redact secret config values in activity-log events (`redaction.py` new) | `services/redaction.py`, `services/activity_logger.py`, `services/action_runner.py` |
| #121 | Redact request/response data on failure events | `services/action_runner.py`, `services/redaction.py` |
| #123 | Redact `config_data` on failure events | `services/action_runner.py` |
| #124 | `redact_text` linear on long runs | `services/redaction.py` |
| #122 | Mask URL userinfo | `services/redaction.py` |
| #129 | urlsplit test fixture that fails on every 3.10 patch | `tests/test_redaction.py` |
| #128 | `INTEGRATION_TYPE_DESCRIPTION`, `type_description=`, `--description` | `settings.py`, `services/self_registration.py`, `cli.py` (hand) |
| #130 | bad_response retried by HTTP status; `is_http_failure_status`; chain walk | `services/errors.py`, `services/retry_policies.py`, `services/action_runner.py` |
| #135 | Skip deploy jobs in the template repo | `.github/workflows/main.yaml` (see Task 1) |

Per-file measurements (`base` = rewritten merge-base file vs branch file, i.e. the existing divergence; `now` = rewritten current main file vs branch file, i.e. the work):

| main file | branch file | base | now |
|---|---|---|---|
| `app/services/action_runner.py` | `src/gundi_action_runner/services/action_runner.py` | 5 | 45 |
| `app/services/activity_logger.py` | `…/services/activity_logger.py` | 0 | 30 |
| `app/services/errors.py` | `…/services/errors.py` | 0 | 19 |
| `app/services/retry_policies.py` | `…/services/retry_policies.py` | 0 | 61 |
| `app/services/self_registration.py` | `…/services/self_registration.py` | 2 | 13 |
| `app/settings/base.py` | `…/settings.py` | 17 | 18 |
| `app/services/redaction.py` | `…/services/redaction.py` | new | new |
| `app/services/tests/test_action_runner.py` | `tests/test_action_runner.py` | 4 | 213 |
| `app/services/tests/test_activity_logger.py` | `tests/test_activity_logger.py` | 0 | 214 |
| `app/services/tests/test_errors.py` | `tests/test_errors.py` | 0 | 32 |
| `app/services/tests/test_pubsub_delivery.py` | `tests/test_pubsub_delivery.py` | 0 | 46 |
| `app/services/tests/test_self_registration.py` | `tests/test_self_registration.py` | 0 | 88 |
| `app/services/tests/test_redaction.py` | `tests/test_redaction.py` | new | new |

Every `base` number equals the allowance recorded in the September plan, so the branch has not drifted and the mechanical port still applies. `app/main.py` and `app/conftest.py` did not change, so `app_factory.py` and `testing/fixtures.py` need no hand-port this time.

Test counts collected on main, merge base → now: action_runner 88→93, activity_logger 26→34, errors 50→62, pubsub_delivery 46→61, self_registration 15→19, redaction 0→66. Total **+110**. Branch today: **499**. Target after this plan: **610** (499 + 110 + 1 new CLI test in Task 5).

## Global Constraints

- Work in a fresh worktree off `origin/design/action-runner-library`. The main checkout at `/Users/chrisdo/padas/gundi-integration-action-runner` has unrelated files **staged** (`.claude/commands/**`, `CLAUDE.md`, `connector.code-workspace`, `local/test-web-ui.sh`, `uv.lock`, the 2026-10-08 plan); a merge there would commit them. Do not touch that index.
- Every `app/**` path stays an 11-line `sys.modules` shim (`app/main.py` 13, `app/conftest.py` 1, `app/actions/__init__.py` 20). Real code lives only under `src/gundi_action_runner/`.
- Port with the helper from the September plan's "The port helper" section (`rewrite`, `port`, `audit`; perl, not BSD sed). After every `port`, run `audit`; the only allowed hits are `app.main`, `app.actions.handlers`, `app.webhooks.handlers`, `app.example.com`.
- Port overwrites. Rename detection auto-merges main's edits into `tests/test_errors.py`, `tests/test_pubsub_delivery.py`, `tests/test_self_registration.py` and `README.md` without conflict; do not trust those results, re-derive them with `port` (README by hand, Task 6).
- Documented divergences to re-apply after porting: `action_runner.py` 5 lines (registry import + `registry.ensure_loaded()` fallback), `self_registration.py` 2 lines (registry import + call), `settings.py` 17 lines (LOG_LEVEL alias, `GUNDI_HANDLERS_MODULES`, `GUNDI_LEGACY_*`), `tests/test_action_runner.py` 4 lines (`app.main` patch targets resolved to `gundi_action_runner.app_factory`).
- Commit subjects: one imperative sentence, no conventional-commit prefix. End each commit message with the attribution line in effect for the session.
- Tests run as `.venv/bin/python -m pytest tests -q -p no:cacheprovider` from the worktree, reusing the main checkout's `.venv` (Python 3.10.14, pydantic 1.10.x).
- Out of scope: the 2026-10-08 redaction-gap plan (survey123 PR #5 port). It targets main and has not shipped; if it merges later it is a separate sync.

## Review Focus

1. **A fork imports `app.services.redaction`.** Main forks that adopted PR #120 may import it directly. Expected: the import works through a shim and resolves to the same module object as `gundi_action_runner.services.redaction`. Pinned in Task 2 (shim + `test_shims` picks it up from the `app/services` listing).
2. **The registry fallback is lost when `action_runner.py` is overwritten.** Expected: a connector using `@action` decorators is still discovered. Pinned by the Task 4 parity allowance of exactly 5 and by `tests/test_app_factory.py` staying green.
3. **`gundi-runner register --description` silently drops the value.** The same defect PR #128's review caught on main's CLI. Expected: `register_integration_in_gundi` is awaited with `type_description` set. Pinned in Task 5.
4. **The branch's stale `main.yaml` reverts PRs #84 and #135 when the library lands on main.** Expected: after this sync the branch's `main.yaml` is byte-identical to main's. Pinned in Task 1 Step 4.
5. **A merge resolution takes "theirs" for an `app/` path.** That replaces a shim with a real module and creates two module identities, so a test patching `app.services.x` stops affecting `gundi_action_runner.services.x`. Expected: every `app/**` file stays a shim. Pinned in Task 1 Step 3 and Task 7 Step 3.

---

### Task 0: Worktree, environment, baseline

**Files:** none modified.

**Interfaces:**
- Produces: worktree `.worktrees/library-sync-1023` on branch `cd/library-sync-1023` at `origin/design/action-runner-library`; `<scratchpad>/port.sh`; baseline 499.

- [x] **Step 1: Create the worktree** (use `superpowers:using-git-worktrees`; equivalent):

```bash
cd /Users/chrisdo/padas/gundi-integration-action-runner
git fetch origin
git worktree add -b cd/library-sync-1023 .worktrees/library-sync-1023 origin/design/action-runner-library
cd .worktrees/library-sync-1023 && ln -s ../../.venv .venv
git status --short   # expect no output
```

- [x] **Step 2: Write the helper** — copy the `rewrite`/`port`/`audit` block verbatim from the September plan into `<scratchpad>/port.sh` and `source` it. Do not commit it.

- [x] **Step 3: Baseline**

Run: `.venv/bin/python -m pytest tests -q -p no:cacheprovider 2>&1 | tail -1`
Expected: `499 passed`.

### Task 1: Merge origin/main, keeping the shims

**Files:**
- Resolve to ours: `app/services/action_runner.py`, `app/services/activity_logger.py`, `app/services/errors.py`, `app/services/retry_policies.py`, `app/services/self_registration.py`, `app/settings/base.py`, `tests/test_action_runner.py`, `tests/test_activity_logger.py`, `README.md`
- Resolve to theirs: `.github/workflows/main.yaml`
- Defer (remove from the merge, re-added in later tasks): `app/services/redaction.py`, `tests/test_redaction.py`
- Keep auto-merged for now: `app/register.py` (legacy entry point, real file on the branch; main's `--description` merges in cleanly)

**Interfaces:**
- Produces: a merge commit whose second parent is `origin/main` (`5d618f7` or later); `src/` and `tests/` unchanged apart from the three auto-merged test files, which are reset to ours here and re-ported later.

- [x] **Step 1: Start the merge**

Run: `git merge origin/main --no-commit --no-ff; git diff --name-only --diff-filter=U`
Expected: 10 unmerged paths: `.github/workflows/main.yaml`, the six `app/` paths and two `tests/` paths in the "ours" list (README auto-merges), and `tests/test_redaction.py` (file-location conflict; git may place it under `app/services/tests/` instead). If `origin/main` has moved past `5d618f7`, stop and re-measure: new PRs need their own rows in this plan.

- [x] **Step 2: Resolve**

```bash
for f in app/services/action_runner.py app/services/activity_logger.py app/services/errors.py \
         app/services/retry_policies.py app/services/self_registration.py app/settings/base.py \
         tests/test_action_runner.py tests/test_activity_logger.py README.md \
         tests/test_errors.py tests/test_pubsub_delivery.py tests/test_self_registration.py; do
  git checkout --ours -- "$f" && git add "$f"
done
git checkout --theirs -- .github/workflows/main.yaml && git add .github/workflows/main.yaml
git rm -q --cached -f app/services/redaction.py tests/test_redaction.py app/services/tests/test_redaction.py 2>/dev/null
rm -f app/services/redaction.py tests/test_redaction.py app/services/tests/test_redaction.py
```

`main.yaml` is taken from main this time, reversing the September decision: with PR #135's repository gate the deploy jobs are inert in the template repo, the workflow does not trigger on this branch at all (`main`/`release-**` only), and keeping the branch's stale copy would revert PRs #84 (`update_hcl.yml@v2.1`) and #135 on main when the library lands.

- [x] **Step 3: Verify the shims survived**

Run: `wc -l app/services/*.py app/settings/base.py | sort -n | head -20; git diff --name-only --diff-filter=U`
Expected: every `app/services/*.py` except `__init__.py` is 11 lines, `app/settings/base.py` 11; no unmerged paths.

- [x] **Step 4: Verify the workflow matches main**

Run: `git diff --quiet origin/main -- .github/workflows/main.yaml && echo identical`
Expected: `identical`.

- [x] **Step 5: Suite still at baseline**

Run: `.venv/bin/python -m pytest tests -q -p no:cacheprovider 2>&1 | tail -1`
Expected: `499 passed`.

- [x] **Step 6: Commit the merge**

```bash
git commit -F - <<'EOF'
Merge origin/main through PR #135 into the library branch, keeping the shims

Records PRs #120-#124, #128-#130 and #135 as integrated. Ports follow
in separate commits; main.yaml is taken from main (see the 2026-10-23
sync plan).
EOF
```

### Task 2: Port the redaction module

**Files:**
- Create: `src/gundi_action_runner/services/redaction.py`, `app/services/redaction.py` (shim), `tests/test_redaction.py`

**Interfaces:**
- Produces: `gundi_action_runner.services.redaction` exporting `REDACTED`, `redact_secrets`, `redact_config_data`, `redact_body`, `redact_text`, `redact_url` (names as on main; Tasks 4 consumes them).

- [x] **Step 1: Port the test and the module**

```bash
port app/services/tests/test_redaction.py tests/test_redaction.py && audit tests/test_redaction.py
port app/services/redaction.py src/gundi_action_runner/services/redaction.py && audit src/gundi_action_runner/services/redaction.py
```
Expected: `audit` prints nothing for either (the module imports no `app.*`; the test's `app.services.utils` import is rewritten).

- [x] **Step 2: Add the shim** — `app/services/redaction.py`, the same 11 lines as `app/services/errors.py` with `errors` replaced by `redaction` in the docstring, the warning text and the import target.

- [x] **Step 3: Run**

Run: `.venv/bin/python -m pytest tests/test_redaction.py tests/test_shims.py -q -p no:cacheprovider 2>&1 | tail -1`
Expected: all pass; `test_redaction.py` collects 66; `test_shims.py` gains one case for `app.services.redaction`.

- [x] **Step 4: Commit** — `git add` the three files; subject: `Port the activity-log redaction module from main`.

### Task 3: Port the status-aware retry verdict

**Files:**
- Modify: `src/gundi_action_runner/services/errors.py`, `src/gundi_action_runner/services/retry_policies.py`, `tests/test_errors.py`, `tests/test_pubsub_delivery.py`

**Interfaces:**
- Produces: `errors.is_http_failure_status(status_code: int) -> bool`; `retry_policies._failure_status(exc) -> Optional[int]`; `"bad_response"` removed from `RETRYABLE_INTEGRATION_ERROR_TYPES`. Task 4's `action_runner` imports `is_http_failure_status`.

- [x] **Step 1: Port the tests**

```bash
for t in errors pubsub_delivery; do port app/services/tests/test_$t.py tests/test_$t.py && audit tests/test_$t.py; done
```

- [x] **Step 2: Run them to see the gap**

Run: `.venv/bin/python -m pytest tests/test_errors.py tests/test_pubsub_delivery.py -q -p no:cacheprovider 2>&1 | tail -1`
Expected: collection error on `is_http_failure_status` import (test_errors) and failing bad_response cases.

- [x] **Step 3: Port the modules**

```bash
for m in errors retry_policies; do port app/services/$m.py src/gundi_action_runner/services/$m.py && audit src/gundi_action_runner/services/$m.py; done
```

- [x] **Step 4: Run**

Run: `.venv/bin/python -m pytest tests/test_errors.py tests/test_pubsub_delivery.py tests/test_retry_policies.py -q -p no:cacheprovider 2>&1 | tail -1`
Expected: all pass; test_errors collects 62, test_pubsub_delivery 61. `test_pubsub_delivery` imports the app through the `app.main` shim, which is why it ports with no hand edits.

- [x] **Step 5: Commit** — subject: `Port the status-aware bad_response retry verdict from main`.

### Task 4: Port activity logging and the action runner

**Files:**
- Modify: `src/gundi_action_runner/services/activity_logger.py`, `src/gundi_action_runner/services/action_runner.py`, `tests/test_activity_logger.py`, `tests/test_action_runner.py`

**Interfaces:**
- Consumes: Task 2's redaction functions; Task 3's `is_http_failure_status`.
- Produces: failure events whose `config_data`, `request_url`, `request_data`, `server_response_body` and error text are redacted.

- [x] **Step 1: Save the branch-only lines before overwriting**

Run: `git show HEAD:tests/test_activity_logger.py > <scratchpad>/branch-test_activity_logger.py; git show HEAD:src/gundi_action_runner/services/action_runner.py > <scratchpad>/branch-action_runner.py`
The September plan notes `tests/test_activity_logger.py` once carried a branch-only test; at `base` = 0 it no longer does, so a plain port is correct. Keep the copy only to diff against.

- [x] **Step 2: Port the tests, resolve the `app.main` patch targets**

```bash
for t in activity_logger action_runner; do port app/services/tests/test_$t.py tests/test_$t.py && audit tests/test_$t.py; done
```
`audit` on `test_action_runner.py` lists the `app.main` patch targets. Replace each `app.main.` with `gundi_action_runner.app_factory.`, exactly as in the branch's previous version (`git diff HEAD -- tests/test_action_runner.py` shows the 4 lines).

- [x] **Step 3: Port the modules and re-apply the registry fallback**

```bash
port app/services/activity_logger.py src/gundi_action_runner/services/activity_logger.py
port app/services/action_runner.py   src/gundi_action_runner/services/action_runner.py
diff <scratchpad>/branch-action_runner.py src/gundi_action_runner/services/action_runner.py | grep -n registry
```
Re-insert the registry import and the 4-line `registry.ensure_loaded()` fallback from the saved copy at the same place in the new file.

- [x] **Step 4: Run**

Run: `.venv/bin/python -m pytest tests -q -p no:cacheprovider 2>&1 | tail -1`
Expected: `605 passed` (499 + 66 + 12 + 15 + 8 + 5). `tests/test_app_factory.py` and `tests/test_registry.py` must be among the passes.

- [x] **Step 5: Commit** — subject: `Port failure-event redaction in the action runner and activity logger from main`.

### Task 5: Port the integration type description

**Files:**
- Modify: `src/gundi_action_runner/settings.py` (after the `INTEGRATION_TYPE_NAME` line, currently :159), `src/gundi_action_runner/services/self_registration.py`, `src/gundi_action_runner/cli.py:51-80`, `tests/test_self_registration.py`, `tests/test_cli.py`

**Interfaces:**
- Produces: `settings.INTEGRATION_TYPE_DESCRIPTION: Optional[str]`; `register_integration_in_gundi(gundi_client, type_slug=None, type_name=None, service_url=None, action_schedules=None, type_description=None)` (new parameter last, as on main after review); `gundi-runner register --description TEXT`.

- [x] **Step 1: Write the failing CLI test** in `tests/test_cli.py`, next to `test_register_forwards_options`, using the same `runner` fixture and patch target that test uses:

```python
def test_register_forwards_the_description(runner, mocker):
    registration = mocker.patch(<same target as test_register_forwards_options>, new_callable=AsyncMock)
    result = runner.invoke(cli, ["register", "--slug", "x_tracker", "--description", "X Tracker collars"])
    assert result.exit_code == 0, result.output
    assert registration.await_args.kwargs["type_description"] == "X Tracker collars"
```

- [x] **Step 2: Port the registration test**

Run: `port app/services/tests/test_self_registration.py tests/test_self_registration.py && audit tests/test_self_registration.py`
Main's file also contains `test_register_cli_passes_the_description_through`, which drives main's `app.register`. On the branch `app/register.py` is the legacy real module (auto-merged in Task 1 with `--description`), so the rewritten test still targets it via the unrewritten `app.register` path; keep it, it covers the legacy entry point.

- [x] **Step 3: Run both to see them fail**

Run: `.venv/bin/python -m pytest tests/test_self_registration.py tests/test_cli.py -q -p no:cacheprovider 2>&1 | tail -3`
Expected: failures on `type_description` / `INTEGRATION_TYPE_DESCRIPTION` and on the new CLI test.

- [x] **Step 4: Implement**
  - `settings.py`: add main's line verbatim after `INTEGRATION_TYPE_NAME`: `INTEGRATION_TYPE_DESCRIPTION = env.str("INTEGRATION_TYPE_DESCRIPTION", None)` with main's comment.
  - `self_registration.py`: `port` from main, then re-apply the 2 registry lines from `git show HEAD:<path>`.
  - `cli.py`: add `@click.option("--description", default=None, help=<main's help text>)` after `--name`, add `description` to `register(...)`, pass `type_description=description` to `register_integration_in_gundi`.

- [x] **Step 5: Run**

Run: `.venv/bin/python -m pytest tests -q -p no:cacheprovider 2>&1 | tail -1`
Expected: `610 passed`.

- [x] **Step 6: Commit** — subject: `Port the integration type description setting and add --description to gundi-runner register`.

### Task 6: Docs

**Files:**
- Modify: `README.md` (activity-log bullet list, after the `log_action_activity()` line, currently :18), `docs/fork-migration.md`, `template/.env.example.jinja:7`

- [x] **Step 1: README** — insert main's redaction paragraph (`git diff 7f1e141 origin/main -- README.md`), with `app/services/redaction.py` changed to `gundi_action_runner.services.redaction`.

- [x] **Step 2: fork-migration** — add a short "Since 0.1.0rc4" section, one bullet each: activity-log events are redacted (declare secrets as `SecretStr` / `format="password"` / `writeOnly=True`); a classified `bad_response` with a 4xx other than 429 is no longer redelivered, map provider-specific transient statuses to `IntegrationRateLimitError`; `INTEGRATION_TYPE_DESCRIPTION` / `gundi-runner register --description` set the portal's type description.

- [x] **Step 3: Template env** — add `# INTEGRATION_TYPE_DESCRIPTION=""` after `INTEGRATION_TYPE_NAME` in `template/.env.example.jinja`.

Run: `.venv/bin/python -m pytest tests/test_template.py -q -p no:cacheprovider 2>&1 | tail -1`
Expected: all pass.

- [x] **Step 4: Commit** — subject: `Document redaction, the retry change and the type description for library users`.

### Task 7: Parity check and finish

**Files:** the throwaway `<scratchpad>/parity.sh`; this plan, committed.

- [x] **Step 1: Parity script** — the September plan's Task 12 script, run unchanged, with these lines added or updated:

```bash
check app/services/redaction.py             $S/services/redaction.py         0
check app/services/errors.py                $S/services/errors.py            0
check app/services/retry_policies.py        $S/services/retry_policies.py    0
check app/services/activity_logger.py       $S/services/activity_logger.py   0
check app/services/action_runner.py         $S/services/action_runner.py     5
check app/services/self_registration.py     $S/services/self_registration.py 2
check app/settings/base.py                  $S/settings.py                   17
for t in redaction errors pubsub_delivery activity_logger self_registration; do
  check "app/services/tests/test_$t.py" "tests/test_$t.py" 0
done
check app/services/tests/test_action_runner.py tests/test_action_runner.py 4
```
Run: `bash <scratchpad>/parity.sh`
Expected: every line `ok`, exit 0. A number above its allowance is a dropped port: fix the file, do not raise the number.

- [x] **Step 2: No leaked imports**

Run: `grep -rnE '\bfrom app\b|\bimport app\b' src/ tests/ | grep -v 'app.actions.handlers\|app.webhooks.handlers\|app.register\|app.main'`
Expected: no output.

- [x] **Step 3: Shims intact**

Run: `wc -l app/services/*.py | awk '$1 != 11 && $2 !~ /__init__|total/'`
Expected: no output.

- [x] **Step 4: Full suite and wheel**

Run: `.venv/bin/python -m pytest tests -q -p no:cacheprovider 2>&1 | tail -1 && .venv/bin/python -m build --wheel -o <scratchpad>/dist -q && ls <scratchpad>/dist`
Expected: `610 passed`; one wheel.

- [x] **Step 5: Commit the plan with its execution log** — copy this file into the worktree's `docs/superpowers/plans/`, tick the boxes, add a one-paragraph log (merge SHA, final count), commit.

- [x] **Step 6: Land on the library branch**

```bash
git push origin cd/library-sync-1023:design/action-runner-library
```
A fast-forward: the worktree branch started at the library tip. If it is refused, someone pushed to the library branch meanwhile; fetch and merge, do not force.

### Task 8 (optional, your call): Publish 0.1.0rc5

The last published version is `0.1.0rc4` (2026-09-22). PRs #115–#119 (synced 2026-09-30) and everything in this plan are unpublished. Follow `RELEASING.md`: bump `__version__` in `src/gundi_action_runner/__init__.py` to `0.1.0rc5`, merge, tag `v0.1.0rc5`, push the tag; then repeat the September end-to-end proof (`gundi-runner new` against the published rc5, generated tests, `docker build --target prod`).


## Execution log (2026-10-23)

Executed inline. Tasks 0-7 done; Task 8 (publish rc5) not run, left to the maintainer. Merge `e789ab9` (second parent `5d618f7`), then `ab16241` (retry verdict), `99961d3` (redaction module), `9938f56` (action runner + activity logger), `aa66641` (type description + `--description`), `747b3bd` and `faf9dee` (docs). Every commit from the merge on passes the suite on its own; final count **611 passed** (the plan's 610 plus a `test_shims` case for `redaction`, which the plan wrongly assumed was picked up automatically). Parity 33/33 ok; `tests/test_gundi_api.py` (5) and `tests/test_token_cache.py` (15) carry pre-existing path adaptations not listed in the September allowances.

Deviations: the shared `.venv` is an editable install of the main checkout, so every run needs `PYTHONPATH=src`. The pre-commit TruffleHog hook blocks `99961d3` on example-credential URL fixtures that are byte-identical to main; the maintainer committed it with the hook skipped. Whole-branch review (Opus) found the committed history unimportable without that commit, which is why the branch was rebuilt with it in place, and that webhook failure events still publish the webhook config unredacted (inherited from main; `docs/fork-migration.md` says so). Deferred minors: the local `.env` template lacks `INTEGRATION_TYPE_DESCRIPTION`; a stale `app/services/errors.py` comment in `retry_policies.py`; one import in `action_runner.py` sits apart from main's order; `test_shims.py` omits `retry_policies` and `url_policy`.

---

## Self-Review

**Coverage.** Every PR in `git log --first-parent 7f1e141..5d618f7` maps to a task: #120/#121/#122/#123/#124 → Tasks 2 and 4; #129 → Task 2 (its change lives only in `test_redaction.py`); #128 → Task 5 (plus `app/register.py` via the Task 1 auto-merge); #130 → Tasks 3 and 4 (`action_runner` imports `is_http_failure_status`); #135 → Task 1 Step 2. Every file in `git diff --name-only 7f1e141 origin/main` is either ported, hand-ported (`cli.py`), taken (`main.yaml`) or documented (`README.md`).

**Counts chain.** 499 → 499 (T1) → 565 (T2, +66) → 592 (T3, +12 +15) → 605 (T4, +8 +5) → 610 (T5, +4 +1) → 610 (T6, T7).

**Known risk.** If `origin/main` moves before execution, Task 1 Step 1 stops the run. The `rewrite` allowlist covers every `app.*` path the new files use (`app.services.*`, `app.settings`); `audit` after each `port` catches anything new.
