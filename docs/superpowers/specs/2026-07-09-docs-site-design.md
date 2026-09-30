# Design: gundi-action-runner documentation site on GitHub Pages

**Date:** 2026-07-09
**Status:** Approved (brainstorming session)
**Repo:** PADAS/gundi-integration-action-runner (branch design/action-runner-library, PR #78)

## Problem

Plan 3 shipped three markdown guides (`docs/quickstart.md`, `docs/extension-api.md`,
`docs/fork-migration.md`), but they are only readable in the repo. Third-party connector
developers — the library's target audience — need a browsable, searchable docs site.

## Decisions made

| Question | Decision |
|---|---|
| Content scope | Existing three pages + a new landing page. No generated API reference, no new guides (YAGNI). |
| Deploy trigger | On push to `main` touching docs; PRs get a build-only link check. |
| Stack | MkDocs Material — matches the PADAS org precedent (gundi-client-v2's `docs` extra pins mkdocs-material + mkdocs-exclude). |
| Pages mechanism | Official GitHub Actions Pages flow (`actions/upload-pages-artifact` + `actions/deploy-pages`); no `gh-pages` branch. |
| Versioning | Single version. No `mike` until a second released version makes pinning worthwhile. |

## Components

### 1. `mkdocs.yml` (repo root)

- `site_name: Gundi Action Runner`, `repo_url` + `edit_uri` pointing at this repo.
- `theme: material` with light/dark palette toggle and navigation/search defaults.
- Nav: Home (`index.md`), Quickstart (`quickstart.md`), Extension API (`extension-api.md`),
  Migrating a Fork (`fork-migration.md`).
- `mkdocs-exclude` plugin excluding `superpowers/` — the internal specs/plans directory
  must never appear on the public site.
- `strict: true` behavior comes from the build flag (below), not the config, so local
  `mkdocs serve` stays forgiving.

### 2. `docs/index.md` (landing page)

What the library is (one paragraph), the 30-second decorator example (same one the README
uses), install commands, and plain links to the three guides plus RELEASING.md and the
GitHub repo. No new conceptual content — it routes, it doesn't teach.

### 3. `pyproject.toml` — `docs` extra

```toml
docs = [
    "mkdocs-material~=9.5",
    "mkdocs-exclude~=1.0",
]
```

Dev-only tooling, same pattern as the `cli` and `testing` extras.

### 4. `.github/workflows/docs.yaml`

Two jobs:

- **build** — on `pull_request` and `push` to `main` when `docs/**`, `mkdocs.yml`, or the
  workflow itself change: install the `docs` extra, run `mkdocs build --strict`
  (fails on broken internal links / missing nav pages), upload the site as a Pages
  artifact.
- **deploy** — `push` to `main` only, `needs: build`, `environment: github-pages`,
  permissions `pages: write` + `id-token: write` scoped to the job, runs
  `actions/deploy-pages`.

The existing `pr.yaml`/`main.yaml` are untouched; docs builds are an independent workflow
with path filters so connector CI stays fast.

## Constraints

- `docs/superpowers/` (specs, plans) is excluded from the published site — verify in the
  built output, not just config.
- The three existing guide files are NOT moved or renamed — in-repo links keep working
  (README links to `docs/quickstart.md` etc.).
- Fork-merge safety: everything added is new files plus a pyproject extra; nothing
  fork-owned changes.
- The site URL will be `https://padas.github.io/gundi-integration-action-runner/`. Once
  live, add the URL to `[project.urls]` as `Documentation` (replacing the README-anchor
  link) — this happens in this project, not deferred.

## One-time human step

Repo Settings → Pages → Build and deployment → Source: **GitHub Actions**. Documented in
RELEASING.md's one-time-setup section alongside the PyPI steps.

## Testing

- `mkdocs build --strict` in CI is the regression gate (broken links, orphaned nav).
- A repo test is unnecessary — the workflow gate covers it; the pytest suite is unaffected.
- Manual acceptance: after the first deploy, the site renders all four pages, search works,
  and `/superpowers/` returns 404.

## Out of scope

- Versioned docs (`mike`), mkdocstrings API reference, custom domain, new guide content.
- Deploying before PR #78 merges (Pages deploys from `main`; the PR carries the workflow).
