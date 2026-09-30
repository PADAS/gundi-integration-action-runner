# Gundi Action Runner Publishing — Implementation Plan (Plan 2 of 3)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `gundi_action_runner.testing` a declared pytest plugin and give the repo tag-driven PyPI release automation (trusted publishing), so `pip install gundi-action-runner` becomes real.

**Architecture:** A `pytest11` entry point exposes the existing `gundi_action_runner.testing.fixtures` module to any project that installs the package — no conftest wiring needed. A tag-triggered GitHub Actions workflow builds sdist+wheel, verifies the tag matches the package version, and publishes via PyPI trusted publishing (OIDC, no long-lived tokens). A RELEASING.md documents the human steps (one-time PyPI setup, per-release version bump + tag).

**Tech Stack:** setuptools src layout (existing), pytest entry points, `build` + `twine check`, `pypa/gh-action-pypi-publish` with OIDC trusted publishing.

**Spec:** `docs/superpowers/specs/2026-07-07-action-runner-library-design.md` (phases 4–5). Plan 1 landed the library on branch `design/action-runner-library` (PR #78); this plan continues on the same branch.

## Global Constraints

- Work on branch `design/action-runner-library` (PR #78 stays draft until this plan lands and the first publish succeeds).
- **Never edit fork-owned files:** `app/actions/handlers.py`, `app/actions/configurations.py`, `app/webhooks/handlers.py`, `app/webhooks/configurations.py`, `app/settings/integration.py`, `app/register.py`.
- Never `git add -A` / `git add .` — the working tree contains unrelated untracked user files. Stage explicit paths.
- Package name `gundi-action-runner`, import name `gundi_action_runner`. Version stays `0.1.0.dev0` in this plan — the bump to `0.1.0` is a release-time human step documented in RELEASING.md, not performed here.
- Publishing uses **trusted publishing only** — no PyPI API tokens in repo secrets, ever.
- Test command: `pytest` from repo root in `.venv`. Suite is 128 passing at plan start; must be green (plus new tests) at the end of every task.
- **Spec deviation (decided):** the spec's deferred list said "add `gundi-action-runner` to `requirements-base.in` (Plan 2)". We deliberately DON'T: while `src/` rides in-tree and CI/Docker `pip install -e .`, a PyPI pin would install the package twice at potentially different versions. The pin belongs in the copier template (Plan 3) and in forks that delete `src/` after migrating. Documented in RELEASING.md (Task 4).
- Commit messages end with a blank line then:
  `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`

---

### Task 1: pytest plugin entry point

Declare `gundi_action_runner.testing.fixtures` as a `pytest11` plugin so installed consumers get the fixtures (`integration_v2`, `mock_gundi_client_v2`, `mock_publish_event`, ...) with zero conftest wiring. Verified with pytest's own `pytester` harness running an isolated mini-project that uses a fixture without importing anything.

**Files:**
- Modify: `pyproject.toml` (add `[project.entry-points.pytest11]`)
- Create: `conftest.py` (repo root — enables `pytester`; also puts the repo root on sys.path for isolated test-file runs, fixing the quirk where `pytest tests/test_registry.py` couldn't import `app`)
- Create: `tests/test_pytest_plugin.py`

**Interfaces:**
- Consumes: `gundi_action_runner.testing.fixtures` (exists since Plan 1; pure fixture definitions, no `pytest_*` hooks).
- Produces: entry point `gundi-action-runner = "gundi_action_runner.testing.fixtures"` under `pytest11` — Task 2 verifies it lands in the wheel metadata; Plan 3's scaffolded projects rely on it.

- [ ] **Step 1: Add the entry point to `pyproject.toml`**

Insert after the `[project.optional-dependencies]` block:

```toml
[project.entry-points.pytest11]
gundi-action-runner = "gundi_action_runner.testing.fixtures"
```

- [ ] **Step 2: Reinstall so the entry point registers**

Entry points are read from installed metadata — the editable install must be refreshed:

```bash
source .venv/bin/activate
pip install -e . --no-deps
pip show -f gundi-action-runner | head -3
```

Expected: reinstall succeeds, version `0.1.0.dev0`.

- [ ] **Step 3: Create the root conftest**

`conftest.py` (repo root):

```python
# Root conftest: enables the `pytester` fixture for plugin self-tests and
# anchors pytest's rootdir-based sys.path insertion, so isolated runs like
# `pytest tests/test_registry.py` can import the in-repo `app` package.
pytest_plugins = ["pytester"]
```

(`pytest_plugins` is only legal in a ROOT conftest since pytest 7 — do not move this into `tests/conftest.py`.)

- [ ] **Step 4: Write the failing plugin test**

`tests/test_pytest_plugin.py`:

```python
"""The pytest11 entry point contract: a project that installs
gundi-action-runner gets the testing fixtures with no conftest wiring."""


def test_fixtures_available_without_conftest(pytester):
    pytester.makepyfile(
        """
        def test_uses_plugin_fixture(integration_v2):
            # integration_v2 comes from gundi_action_runner.testing.fixtures,
            # loaded via the pytest11 entry point — this file has no imports
            # and the temp project has no conftest.
            assert str(integration_v2.id)
        """
    )
    result = pytester.runpytest()
    result.assert_outcomes(passed=1)


def test_plugin_is_registered_by_entry_point(pytester):
    result = pytester.runpytest("--trace-config")
    result.stdout.fnmatch_lines(["*gundi_action_runner.testing.fixtures*"])
```

- [ ] **Step 5: Run to verify state**

```bash
pytest tests/test_pytest_plugin.py -v
```

Expected BEFORE Steps 1–2 are effective: `fixture 'integration_v2' not found` inside the pytester run (i.e. `assert_outcomes(passed=1)` fails with errors=1). If you already completed Steps 1–2, temporarily verify the RED state with `pip uninstall -y gundi-action-runner && pytest tests/test_pytest_plugin.py -v`, then `pip install -e . --no-deps` again. Expected AFTER: both tests PASS.

Note: `pytester` runs pytest in a temp dir where the plugin loads via the installed entry point — inherited plugins from the parent process are disabled by default in subprocess mode but entry-point plugins load normally in in-process mode (the default). If the first test passes even with the package uninstalled, something is leaking the fixtures — investigate before proceeding (the likely culprit is an unexpected conftest in the pytester tmp path; there should be none).

- [ ] **Step 6: Run the full suite**

```bash
pytest
```

Expected: 130 passing (128 + 2 new). Watch for: the plugin now ALSO loads in the repo's own runs, defining the same fixture names as `tests/conftest.py`'s star-import — conftest definitions shadow plugin ones by design; there must be no duplicate-fixture errors.

- [ ] **Step 7: Commit**

```bash
git add pyproject.toml conftest.py tests/test_pytest_plugin.py
git commit -m "Declare gundi_action_runner.testing as a pytest11 plugin"
```

---

### Task 2: Packaging metadata and build verification

Prove the artifact is right before automating its publication: sdist+wheel build cleanly, contain the library (including `testing/fixtures.py` and the entry point), and exclude the repo-only trees (`app/`, `tests/`, `examples/`, `docs/`, `local/`).

**Files:**
- Modify: `pyproject.toml` (add `[project.urls]`, extend dev extra)

**Interfaces:**
- Consumes: entry point from Task 1.
- Produces: a verified `python -m build` invocation and dist-content expectations that Task 3's workflow re-runs in CI.

- [ ] **Step 1: Add project URLs and build tooling**

In `pyproject.toml`, after the `license` line add:

```toml
[project.urls]
Repository = "https://github.com/PADAS/gundi-integration-action-runner"
Documentation = "https://github.com/PADAS/gundi-integration-action-runner#using-as-a-library-preview"
```

(Place the `[project.urls]` table after the `[project]` table's simple keys — TOML requires tables to follow scalar keys; putting it between `dependencies` and `[project.optional-dependencies]` is fine.)

Extend the dev extra:

```toml
dev = [
    "pytest~=7.4.3",
    "pytest-asyncio~=0.21.1",
    "pytest-mock~=3.12.0",
    "build~=1.2",
    "twine~=5.1",
]
```

- [ ] **Step 2: Build and inspect the artifacts**

```bash
source .venv/bin/activate
pip install "build~=1.2" "twine~=5.1"
rm -rf dist/ && python -m build
twine check dist/*
```

Expected: one `.tar.gz` + one `.whl` in `dist/`; `twine check` reports PASSED for both.

- [ ] **Step 3: Verify wheel contents**

```bash
unzip -l dist/*.whl | grep -E "gundi_action_runner/(testing/fixtures|registry|app_factory|settings)\.py" | wc -l   # expected: 4
unzip -l dist/*.whl | grep -cE "^.*(app/|tests/|examples/|docs/|local/)" || echo "CLEAN"                             # expected: CLEAN (0 matches)
unzip -p dist/*.whl "*/entry_points.txt" ; # expected output contains: [pytest11] and gundi-action-runner = gundi_action_runner.testing.fixtures
```

If `app/` files appear in the wheel, the packages.find config regressed — stop and investigate; do not hand-exclude paths.

- [ ] **Step 4: Verify sdist contents**

```bash
tar -tzf dist/*.tar.gz | grep -cE "/(app|tests|examples|docs|local)/" || echo "CLEAN"   # expected: CLEAN
tar -tzf dist/*.tar.gz | grep -c "src/gundi_action_runner/testing/fixtures.py"          # expected: 1
```

Note: setuptools sdists include `pyproject.toml`, `README.md`, `LICENSE`, and `src/` by default. If repo-only trees leak into the sdist, add a `MANIFEST.in` with `prune app`, `prune tests`, `prune examples`, `prune docs`, `prune local` — but only if the greps above actually fail.

- [ ] **Step 5: Smoke-install the wheel in a scratch venv**

```bash
SCRATCH=$(mktemp -d) && python -m venv "$SCRATCH/venv"
"$SCRATCH/venv/bin/pip" install dist/*.whl --quiet
"$SCRATCH/venv/bin/python" -c "from gundi_action_runner import action, webhook, create_app, registry; import gundi_action_runner; print(gundi_action_runner.__version__)"
rm -rf "$SCRATCH"
```

Expected: prints `0.1.0.dev0`, no ImportError. (This is the true `pip install gundi-action-runner` simulation — it pulls real deps from PyPI, so it needs network and takes a minute.)

- [ ] **Step 6: Run the suite, commit**

```bash
pytest
git add pyproject.toml
git commit -m "Add project URLs and build tooling; verify dist contents"
```

Expected: 130 passing. (`dist/` is transient — confirm it is git-ignored with `git status --short dist/`; if it shows up, add `dist/` to `.gitignore` in this commit.)

---

### Task 3: Tag-driven publish workflow (trusted publishing)

**Files:**
- Create: `.github/workflows/publish.yaml`

**Interfaces:**
- Consumes: `_tests.yml` reusable workflow (existing); the `python -m build` + `twine check` invocation verified in Task 2.
- Produces: on pushing a tag `v*`, the package publishes to PyPI. Requires one-time human setup on PyPI + GitHub (documented in Task 4) before the first tag.

- [ ] **Step 1: Write the workflow**

`.github/workflows/publish.yaml`:

```yaml
name: Publish to PyPI

on:
  push:
    tags:
      - "v*"

jobs:
  run_unit_tests:
    uses: ./.github/workflows/_tests.yml

  build_and_publish:
    needs: run_unit_tests
    runs-on: ubuntu-latest
    environment: pypi
    permissions:
      id-token: write   # OIDC for PyPI trusted publishing
      contents: read
    steps:
      - name: Checkout tag
        uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"   # tomllib is stdlib from 3.11; independent of the package's requires-python
      - name: Verify tag matches package version
        run: |
          python - <<'EOF'
          import os, sys, tomllib
          with open("pyproject.toml", "rb") as f:
              version = tomllib.load(f)["project"]["version"]
          tag = os.environ["GITHUB_REF_NAME"].removeprefix("v")
          if tag != version:
              sys.exit(f"Tag v{tag} does not match pyproject version {version}. "
                       f"Bump [project].version before tagging (see RELEASING.md).")
          if "dev" in version:
              sys.exit(f"Refusing to publish a dev version ({version}).")
          EOF
      - name: Build sdist and wheel
        run: |
          python -m pip install "build~=1.2" "twine~=5.1"
          python -m build
          python -m twine check dist/*
      - name: Publish to PyPI
        uses: pypa/gh-action-pypi-publish@release/v1
```

- [ ] **Step 2: Validate the workflow file**

```bash
python -c "import yaml, pathlib; yaml.safe_load(pathlib.Path('.github/workflows/publish.yaml').read_text()); print('YAML OK')"
```

Expected: `YAML OK`. (PyYAML is available transitively; if not, `pip install pyyaml`.)

- [ ] **Step 3: Rehearse the version guard locally**

```bash
GITHUB_REF_NAME=v0.1.0 python - <<'EOF'
import os, sys
try:
    import tomllib
except ModuleNotFoundError:
    sys.exit("SKIP: python < 3.11 locally, guard runs on 3.11 in CI")
with open("pyproject.toml", "rb") as f:
    version = tomllib.load(f)["project"]["version"]
tag = os.environ["GITHUB_REF_NAME"].removeprefix("v")
print("MISMATCH (expected — version is still 0.1.0.dev0)" if tag != version else "MATCH")
EOF
```

Expected: `MISMATCH (expected — version is still 0.1.0.dev0)` — proving the guard blocks tagging without a version bump — or the SKIP line on a 3.10-only machine.

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/publish.yaml
git commit -m "Add tag-driven PyPI publish workflow (trusted publishing)"
```

---

### Task 4: RELEASING.md and the one-time setup runbook

**Files:**
- Create: `RELEASING.md`
- Modify: `README.md` (one line: point to RELEASING.md from the library-preview section)

**Interfaces:**
- Consumes: the workflow contract from Task 3 (tag `vX.Y.Z` == `[project].version`, environment `pypi`).
- Produces: the runbook the maintainer follows for the first and every subsequent release.

- [ ] **Step 1: Write RELEASING.md**

```markdown
# Releasing gundi-action-runner

## One-time setup (before the first release)

1. **PyPI pending publisher** (no API tokens): on pypi.org → your account →
   Publishing → "Add a pending publisher" with:
   - PyPI project name: `gundi-action-runner`
   - Owner: `PADAS` — Repository: `gundi-integration-action-runner`
   - Workflow name: `publish.yaml`
   - Environment name: `pypi`
2. **GitHub environment**: repo Settings → Environments → create `pypi`.
   Optionally add required reviewers to gate publishes behind an approval.

## Every release

1. Bump `[project].version` in `pyproject.toml` (e.g. `0.1.0.dev0` → `0.1.0`).
   The publish workflow refuses `dev` versions and mismatched tags.
2. If this is the first non-dev release, update the README "Using as a
   library (preview)" section: replace "(not yet on PyPI)" with the install
   command `pip install gundi-action-runner`.
3. Commit, merge to `main`, then tag and push:

   ```bash
   git tag v0.1.0
   git push origin v0.1.0
   ```

4. The `Publish to PyPI` workflow runs tests, builds sdist+wheel, verifies
   the tag matches the version, and publishes via trusted publishing.

## Versioning

Semver; stay on `0.x` while the extension API settles. Between releases the
version in `pyproject.toml` carries a `.devN` suffix so an accidental tag
cannot publish.

## Why `requirements-base.in` does NOT pin gundi-action-runner

While the library source rides in-tree under `src/` (the fork-transition
window), CI and Docker install it with `pip install -e . --no-deps`. Adding a
PyPI pin to `requirements-base.in` would install the package twice at
potentially different versions. The pin belongs in projects generated by the
`gundi-runner new` scaffold, and in forks that delete `src/` + shims after
fully migrating to the library.
```

- [ ] **Step 2: Link it from the README**

In `README.md`'s "Using as a library (preview)" section, append after the final paragraph:

```markdown
Maintainers: see `RELEASING.md` for how releases are cut and the one-time
PyPI trusted-publishing setup.
```

- [ ] **Step 3: Full suite + commit**

```bash
pytest
git add RELEASING.md README.md
git commit -m "Add release runbook for PyPI trusted publishing"
```

Expected: 130 passing.

---

## Not in this plan

- The actual `v0.1.0` release (human-gated: PyPI pending-publisher setup, version bump, merge, tag).
- Docs site, fixture reference docs, migration guide, CLI/copier scaffold — Plan 3.
- Un-drafting PR #78 — do after the one-time PyPI setup is done, so the merge and first release can happen together.
