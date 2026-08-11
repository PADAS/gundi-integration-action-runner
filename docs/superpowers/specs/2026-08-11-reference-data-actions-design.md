# Reference Data Actions — Design

**Date:** 2026-08-11
**Status:** Approved
**Target:** `main` (legacy `app/` layout) first, then ported into `src/gundi_action_runner/` on `design/action-runner-library`

## Background

Gundi now supports **reference data** end to end: the API (cdip PR #461) accepts a
`"reference"` integration action type and proxies execution to the runner, and the
Portal renders config fields annotated with `gundi:reference` as live comboboxes
that fetch options by executing those actions. The pattern was pioneered in
`gundi-integration-cmore` (its PR #25), where the framework-level support lives in
that repo's vendored copy of the runner. This design ports the framework pieces
into the template/library so every integration can define reference actions.

A reference action is a stateless action whose Pydantic config model **is the
query** and whose handler returns a standardized options envelope. The Portal
invokes it through the existing execute proxy
(`POST /v2/integrations/{id}/actions/{value}/execute/` →
`POST /v1/actions/execute` on the runner) with query params passed as
`config_overrides`.

## Verified platform contract

These facts were verified against the implemented code (cdip `feature/reference-action-type`,
gundi-portal `ReferenceSelectWidget` et al.), not the original cmore RFC. Divergences
from the RFC are marked.

### API (cdip)

- Registration accepts `type: "reference"`; send `is_periodic_action: false`.
  `is_executable` is **not** a registration field and must not be sent (server-side
  enforcement was deliberately deferred). *(RFC divergence: the "auth keyed on action
  type" ask was resolved by not adding a gate at all.)*
- Execute proxy permissions: superuser or org admin of the owning org; org viewers
  get 403. No action-type gate.
- The API is a pure passthrough: it does not validate the options envelope or cache.
  The runner receives `{integration_id, action_id, run_in_background, triggered_by,
  config_overrides}` and its JSON response is returned verbatim.
- Reference actions are excluded from per-integration config materialization,
  backfill, and repair. They must never have an `IntegrationConfiguration`.

### Portal (gundi-portal)

- Detection key is the literal `"gundi:reference"` at the field's ui_schema node
  (positional rjsf path, e.g. `myfield` or `myarray.items.myfield`). *(RFC divergence:
  not under `ui:options`.)* The Portal sets `ui:widget` itself — the runner must never
  set it, so portals without support keep plain text fields.
- Annotation fields (exactly the RFC four; unknown keys ignored):
  `action` (reference action `value`), `target` (`"self"` | `"provider"`),
  `params` (literals or `{"$data": "<relative path>"}`), `allow_free_text`
  (defaults **true** when absent).
- **`target: "provider"` is live** *(RFC divergence: was reserved for Phase 2)*,
  resolved client-side: the Portal lists connections where the configured integration
  is the **destination**, keeps providers whose integration type registers the same
  action `value` with `type: "reference"`, executes against each provider, and unions
  options deduped by `value` (first occurrence wins).
- `$data` paths resolve from the object containing the annotated field; array indices
  are their own level; each `../` climbs one level; a bare name is a sibling. Params
  resolving to `undefined`/`null`/`""` block the fetch and render a
  "Select {param keys} first" hint — **param keys are user-visible**.
- Response parsed as `{options: [{value, label?, description?, group?}],
  cache_ttl_seconds?, truncated?}`. `label` defaults to `value`; `group` switches to
  grouped rendering; **`description` and `truncated` are accepted but not rendered
  today**. `cache_ttl_seconds` defaults to 300 (portal-side cache only).
- Any runner error (422 or 500 or timeout) renders identically: text input,
  attention icon, Retry. Handlers should therefore be tolerant rather than strict.

## Design

### 1. Contract types — `app/actions/core.py`

Ported verbatim from cmore:

```python
class ReferenceActionConfiguration(ActionConfiguration):
    """Marker base for reference-data actions: the config model IS the query.

    Reference actions are stateless — they read the integration's auth config
    but store no configuration of their own; callers (the Gundi portal)
    supply query params via config_overrides. They return a
    ReferenceDataResponse dict.
    """

class ReferenceOption(BaseModel):
    value: str
    label: Optional[str] = None        # portal defaults label to value
    description: Optional[str] = None  # accepted by the portal, not yet rendered
    group: Optional[str] = None        # optional grouping for long lists

class ReferenceDataResponse(BaseModel):
    options: List[ReferenceOption]
    cache_ttl_seconds: int = 300       # portal-side cache hint
    truncated: bool = False            # true if the list was capped
```

`discover_actions` needs no changes — reference actions ride the existing
`action_`-prefix discovery.

### 2. Annotation helper — `app/actions/core.py`

Promoted from cmore's private `_reference()`:

```python
def reference_annotation(
    action: str,
    *,
    params: Optional[dict] = None,
    target: str = "self",
    allow_free_text: bool = True,
) -> dict:
```

Returns the dict to place under the `"gundi:reference"` key at the field's
ui_schema node. Raises `ValueError` for `target` outside `{"self", "provider"}`.
Never sets `ui:widget`. The docstring encodes the verified gotchas: user-visible
param keys, array indices as `$data` levels, empty-string resolutions blocking
the fetch, and provider-target requirements.

### 3. Registration — `app/services/self_registration.py`

One insertion at the head of the action-type branch chain
(`ReferenceActionConfiguration` subclasses `ActionConfiguration`, so it must be
checked first):

```python
if issubclass(config_model, ReferenceActionConfiguration):
    action_type = ActionTypeEnum.REFERENCE.value
elif issubclass(config_model, AuthActionConfiguration):
    ...
```

Plus `ActionTypeEnum.REFERENCE = "reference"` in `app/services/core.py`.
Reference actions fall into the existing `is_periodic_action = False` branch and
do not set `is_executable`. **No feature flag** — cmore's default-off
`REGISTER_REFERENCE_ACTIONS` is dropped; the platform now accepts the type in all
environments.

### 4. Runner changes — `app/services/action_runner.py`

1. **Stateless execution.** The "no stored config and no overrides → 404" branch
   gains a third condition: reference actions proceed (zero-param queries must
   work). Required query params still fail via the existing Pydantic-parse → 422
   path.
2. **Secret redaction.** The timeout and generic-exception failure paths pass
   `config_data=None` for reference actions, so the integration's stored
   configurations (which include raw auth secrets) never enter the error response
   or a published event.
3. **Activity-log suppression.** `_handle_error` gains
   `publish_event: bool = True`; reference-action failures pass `False`. Failures
   at dropdown-open frequency log to stdout (`logger.exception`) but do not flood
   the integration's activity log. This resolves the cmore RFC's "throttle
   reference-failure events" follow-up by suppression instead of a rate limiter —
   the Portal already shows the operator a retryable error state.
4. **`ReferenceDataError` → 422.** New exception in `app/services/errors.py`.
   Reference handlers raise it for unknown-value cases (unknown tag/field/branch);
   the runner catches it ahead of the generic handler and returns 422 with
   `config_data=None` and no published event. Plain `ValueError` still surfaces
   as 500 — arbitrary exceptions are not reinterpreted. This resolves the RFC's
   "unknown values should be 422, not 500" follow-up.

### 5. Drift-guard test utility

`assert_reference_annotations_valid(ui_schema, handlers)` as a public test helper
(generalized from cmore's test): walks a ui_schema tree; for every
`gundi:reference` asserts the named action is a discovered reference action,
declared params ⊆ the query model's fields, the query model's required fields ⊆
declared params, and no `ui:widget` is set alongside the annotation. Integrations
call it from their own test suite in one line.

Home on main: a new `app/services/testing.py` module (importable from a fork's
test suite). Library home: `gundi_action_runner.testing`.

### 6. Tests

Ported from cmore (adapted to no-flag behavior):

- Contract-type shape and defaults.
- `test_execute_reference_action_with_no_stored_config_and_no_overrides` — 200.
- `test_execute_reference_action_handler_error_does_not_leak_config` — the raw
  auth token appears in neither the JSON error body nor any published event.
- `test_non_pull_non_reference_action_with_no_config_and_no_overrides_still_404s`
  — pins the three-way branch.
- Registration: reference actions register with `type: "reference"` and
  `is_periodic_action: false` (replaces cmore's flag-off/flag-on pair).

New:

- `ReferenceDataError` → 422, `config_data` redacted, no event published.
- Reference-action failure publishes no activity-log event; a non-reference
  failure still does (pins the suppression to reference actions only).
- `reference_annotation()` output shape, target validation, and the
  never-sets-`ui:widget` invariant.
- Drift-guard utility: passes on a valid schema; fails on unknown action,
  undeclared param, missing required param, and `ui:widget` present.

### 7. Documentation

- `CLAUDE.md`: a "Reference data actions" section — when to use one, the marker
  base + envelope pattern, `reference_annotation()` usage.
- Docs site: a full page using cmore's tag → field → field-options cascade as the
  worked example, including: `$data` relative-path semantics with the array-climb
  example; provider-target requirements (provider's integration type must register
  the same action `value` as `type: "reference"`; the connection must have the
  configured integration as destination; option `value`s should be globally
  meaningful because cross-provider duplicates collapse); tolerant-handler guidance
  (the Portal renders 422s and 500s identically, so prefer empty option lists over
  hard failures for recoverable cases, and `ReferenceDataError` for unknown
  values); and the note that `description`/`truncated` are accepted but not yet
  rendered.

### 8. Rollout

1. Feature branch off `main` → PR with everything above.
2. After merge: port into `src/gundi_action_runner/` on
   `design/action-runner-library` (same hunks against the library modules;
   `reference_annotation` and the contract types exported from
   `gundi_action_runner.actions.core`; drift guard in
   `gundi_action_runner.testing`; the `app/actions/core.py` deprecation shim picks
   the new names up automatically via the module swap).

## Out of scope

- cmore's four reference actions and tag-index code (used only as documentation
  examples).
- Rendering `description`/`truncated` in the Portal (portal-side work).
- Server-side `is_executable` enforcement (deliberately deferred in cdip).
- Response-envelope validation in the runner (Approach B — rejected: adds
  special-casing to generic dispatch for failures the Portal already degrades on;
  malformed envelopes fail visibly in dev).
