# Design + Plan: dedicated PyPI readme for gundi-action-runner

**Date:** 2026-07-14
**Status:** Approved (brainstorming session; combined spec+plan by agreement — single-task scope)
**Repo:** PADAS/gundi-integration-action-runner (branch design/action-runner-library, PR #78)

## Problem

The PyPI page for `gundi-action-runner` renders the repo's 487-line fork-template README:
it leads with "Fork this repo", relegates the library to a "preview" section that says
"not yet on PyPI" (false since 0.1.0rc1), and its relative links 404 on PyPI. PyPI pages
are frozen per release — this fix ships with the next release.

## Decision

Dedicated package readme (`README-pypi.md`) targeted at pip users; repo README stays
fork-oriented with its stale preview wording fixed.

## Changes (one task)

### 1. Create `README-pypi.md`

Exact content:

````markdown
# gundi-action-runner

A framework for building [Gundi](https://gundiservice.org) integration
connectors in Python. Register handlers with decorators, and the framework
provides the FastAPI service, scheduling, state, activity logging, portal
schemas, and delivery to Gundi.

```python
from gundi_action_runner import action, create_app

@action.pull(config=PullObservationsConfig, title="Pull Observations")
async def pull_observations(integration, action_config):
    ...

app = create_app(handlers_modules=["myconnector.handlers"])
```

## Install

```bash
pip install "gundi-action-runner[cli]"
```

> The current release is a release candidate — until the first stable release,
> pin it exactly: `pip install "gundi-action-runner[cli]==0.1.0rc1"`
> (avoid `--pre`, which cascades pre-releases to all dependencies).

## What you get

- **`gundi-runner new`** — scaffold a complete connector project interactively
  (copier-backed; generated projects can pull template updates later)
- **`gundi-runner run` / `register` / `add-action`** — local dev server,
  Gundi registration, and handler codegen
- **Pytest fixtures** — installing the package registers a pytest plugin with
  ready-made Gundi mocks (`integration_v2`, `mock_gundi_client_v2`, ...)
- **Local dev stack** — scaffolds include a docker-compose environment
  (connector + redis + Pub/Sub emulator with sub-action loopback)
- **Two auth modes** — service client credentials, or your own Gundi login
  via the OAuth2 password grant for local development

## Documentation

- Source, quickstart, and guides:
  https://github.com/PADAS/gundi-integration-action-runner
- Docs site (live with the first stable release):
  https://padas.github.io/gundi-integration-action-runner/

Apache-2.0. Maintained by the [Gundi](https://gundiservice.org) team at
EarthRanger.
````

### 2. `pyproject.toml`

`readme = "README.md"` → `readme = "README-pypi.md"`. (setuptools auto-includes the
referenced readme in sdists; no MANIFEST.in change.)

### 3. Repo `README.md` — refresh the preview section

In "## Using as a library (preview)": replace the sentence containing "(not yet on PyPI)"
with published phrasing and the exact-pin install line:

- Old: "The framework in this repo is being extracted into an installable package,
  `gundi-action-runner` (not yet on PyPI)."
- New: "The framework in this repo now ships as an installable package,
  [`gundi-action-runner` on PyPI](https://pypi.org/project/gundi-action-runner/)
  (currently a release candidate — install with
  `pip install \"gundi-action-runner[cli]==0.1.0rc1\"`)."
- Also: "Connectors will register handlers" → "Connectors register handlers".

**Working-tree caution:** README.md carries an uncommitted user draft (Tracpoint) —
the controller stashes it around this edit; the implementer verifies
`git diff --cached README.md` shows only the intended lines before committing.

### 4. `RELEASING.md` — retarget the first-stable step

Step 2 under "Every release" currently says to update the README preview wording on the
first non-dev release. Replace it with:

- New step 2: "If this is the first stable release: in `README-pypi.md`, remove the
  release-candidate pin note under Install; in `README.md`'s library section, drop the
  release-candidate phrasing (plain `pip install \"gundi-action-runner[cli]\"`); and in
  `README-pypi.md`, remove '(live with the first stable release)' from the docs-site link."

## Verification

- `python -m build && python -m twine check dist/*` — twine validates the new long
  description renders (PASSED ×2 expected).
- `unzip -p dist/*.whl "*/METADATA" | grep -c "Fork this repo"` → 0;
  same grep for "gundi-runner new" → ≥1 (the right readme shipped).
- `pytest -q` → 149 passed (nothing functional changed).
- `mkdocs build --strict` unaffected (README not part of the site) — skip.

## Out of scope

- Re-publishing (the new page text ships with rc2 or the final 0.1.0).
- Restructuring the fork-template README beyond the preview-section refresh.
