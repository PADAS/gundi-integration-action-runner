# Reference Data Actions Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Port reference-data action support from `gundi-integration-cmore` into this template repo on `main`, so any integration can define stateless "reference" actions that feed the Gundi Portal's live option comboboxes.

**Architecture:** A `ReferenceActionConfiguration` marker base makes a Pydantic config model act as the query for a stateless action; the runner registers such actions with `type: "reference"`, lets them execute without stored config, redacts auth secrets from their errors, suppresses their activity-log noise, and maps a new `ReferenceDataError` to HTTP 422. A `reference_annotation()` helper emits the `gundi:reference` ui_schema annotation the Portal consumes, and a drift-guard test utility keeps annotations and handlers in sync.

**Tech Stack:** Python 3.10+, FastAPI, **Pydantic v1** (`__fields__`, `parse_obj`, `.dict()`, `schema_json()`), pytest + pytest-asyncio + pytest-mock.

**Spec:** `docs/superpowers/specs/2026-08-11-reference-data-actions-design.md` — read it for the verified API/Portal contract. This plan targets `main` (legacy `app/` layout). The follow-up port into `src/gundi_action_runner/` on `design/action-runner-library` is out of scope here (spec §8).

## Global Constraints

- Base branch: `main`. Create feature branch `feature/reference-data-actions` (use an isolated worktree via superpowers:using-git-worktrees).
- Pydantic **v1** idioms only — `__fields__`, `field.required`, `.dict()`, `parse_obj`, `schema_json()`. No pydantic v2 APIs.
- The annotation key is the literal string `"gundi:reference"` placed at the field's ui_schema node. Never set `ui:widget` next to it — the Portal chooses the widget; portals without support must keep plain text fields.
- Register reference actions with `type: "reference"` and `is_periodic_action: false`. Never send `is_executable` for them.
- No feature flag — cmore's `REGISTER_REFERENCE_ACTIONS` is deliberately dropped (the platform accepts the type everywhere now).
- cmore's `_handle_error` has a `classify_heuristics` parameter; **main's does not**. Use main's signatures as shown in each task — do not copy cmore hunks blindly.
- Run tests from the repo root: `pytest <path> -v`. Before each commit, run the full suite: `pytest app -q`.
- Every commit message ends with:
  `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`

---

### Task 1: Contract types

**Files:**
- Modify: `app/actions/core.py` (add after `GenericActionConfiguration`, ~line 61)
- Create: `app/actions/tests/__init__.py` (empty)
- Test: `app/actions/tests/test_reference_actions.py`

**Interfaces:**
- Consumes: `ActionConfiguration`, `UISchemaModelMixin` (existing).
- Produces: `ReferenceActionConfiguration(ActionConfiguration)`, `ReferenceOption(BaseModel)` (fields `value: str`, `label/description/group: Optional[str]`), `ReferenceDataResponse(BaseModel)` (fields `options: List[ReferenceOption]`, `cache_ttl_seconds: int = 300`, `truncated: bool = False`). All later tasks import these from `app.actions.core`; they are also re-exported through `app.actions` via its existing `from .core import *`.

- [ ] **Step 1: Write the failing tests**

Create `app/actions/tests/__init__.py` (empty file), then `app/actions/tests/test_reference_actions.py`:

```python
import pydantic
import pytest

from app.actions.core import (
    ActionConfiguration,
    ReferenceActionConfiguration,
    ReferenceDataResponse,
    ReferenceOption,
)


def test_reference_action_configuration_is_an_action_configuration():
    class ListThingsQuery(ReferenceActionConfiguration):
        parent: str = ""

    assert issubclass(ReferenceActionConfiguration, ActionConfiguration)
    assert ListThingsQuery(parent="x").parent == "x"


def test_reference_option_requires_value():
    with pytest.raises(pydantic.ValidationError):
        ReferenceOption()


def test_reference_option_defaults():
    option = ReferenceOption(value="LAND")
    assert option.value == "LAND"
    assert option.label is None
    assert option.description is None
    assert option.group is None


def test_reference_data_response_defaults_and_serialization():
    response = ReferenceDataResponse(options=[ReferenceOption(value="a", label="A")])
    assert response.cache_ttl_seconds == 300
    assert response.truncated is False
    assert response.dict() == {
        "options": [{"value": "a", "label": "A", "description": None, "group": None}],
        "cache_ttl_seconds": 300,
        "truncated": False,
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest app/actions/tests/test_reference_actions.py -v`
Expected: FAIL with `ImportError: cannot import name 'ReferenceActionConfiguration'`

- [ ] **Step 3: Implement the types**

In `app/actions/core.py`: change the typing import at the top to `from typing import List, Optional`, then add after the `GenericActionConfiguration` class:

```python
class ReferenceActionConfiguration(ActionConfiguration):
    """Marker base for reference-data actions: the config model IS the query.

    Reference actions are stateless — they read the integration's auth config
    but store no configuration of their own; callers (the Gundi portal)
    supply query params via config_overrides. They return a
    ReferenceDataResponse dict. See
    docs/superpowers/specs/2026-08-11-reference-data-actions-design.md.
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

No changes to `discover_actions` — reference actions ride the existing `action_`-prefix discovery, and the existing `else: data_model = None` branch covers them.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest app/actions/tests/test_reference_actions.py -v`
Expected: 4 PASS

- [ ] **Step 5: Full suite, then commit**

Run: `pytest app -q` — expected: all pass.

```bash
git add app/actions/core.py app/actions/tests/__init__.py app/actions/tests/test_reference_actions.py
git commit -m "feat: reference-action contract types (marker config + options envelope)"
```

---

### Task 2: `reference_annotation()` helper

**Files:**
- Modify: `app/actions/core.py` (add after `ReferenceDataResponse` from Task 1)
- Test: `app/actions/tests/test_reference_actions.py` (append)

**Interfaces:**
- Produces: `reference_annotation(action: str, *, params: Optional[dict] = None, target: str = "self", allow_free_text: bool = True) -> dict`. Task 7's tests and the README (Task 8) use it. Re-exported through `app.actions` via `from .core import *`.

- [ ] **Step 1: Write the failing tests**

Append to `app/actions/tests/test_reference_actions.py`:

```python
def test_reference_annotation_defaults():
    from app.actions.core import reference_annotation

    annotation = reference_annotation("list_sites")
    assert annotation == {
        "action": "list_sites",
        "target": "self",
        "params": {},
        "allow_free_text": True,
    }


def test_reference_annotation_with_params_and_provider_target():
    from app.actions.core import reference_annotation

    annotation = reference_annotation(
        "list_event_types",
        params={"event_type": {"$data": "../../event_type"}},
        target="provider",
        allow_free_text=False,
    )
    assert annotation["target"] == "provider"
    assert annotation["params"] == {"event_type": {"$data": "../../event_type"}}
    assert annotation["allow_free_text"] is False


def test_reference_annotation_rejects_unknown_target():
    from app.actions.core import reference_annotation

    with pytest.raises(ValueError):
        reference_annotation("list_sites", target="destination")


def test_reference_annotation_never_sets_ui_widget():
    from app.actions.core import reference_annotation

    assert "ui:widget" not in reference_annotation("list_sites")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest app/actions/tests/test_reference_actions.py -v`
Expected: the 4 new tests FAIL with `ImportError: cannot import name 'reference_annotation'`

- [ ] **Step 3: Implement the helper**

Add to `app/actions/core.py` after `ReferenceDataResponse`:

```python
def reference_annotation(
    action: str,
    *,
    params: Optional[dict] = None,
    target: str = "self",
    allow_free_text: bool = True,
) -> dict:
    """Build the value for a ``"gundi:reference"`` ui_schema annotation.

    Place the result under the literal ``"gundi:reference"`` key at the
    field's ui_schema node (for array item fields: ``<array>.items.<field>``).
    The portal renders the field as a live combobox that fetches options by
    executing the named reference action. Deliberately never sets
    ``ui:widget`` — the portal chooses the widget, and portals without
    reference support must keep rendering plain text fields.

    :param action: value (id) of a reference action — one whose config model
        subclasses ReferenceActionConfiguration.
    :param params: query params passed to the action as config_overrides.
        Values are literals or ``{"$data": "<relative path>"}`` referencing
        current form state. ``$data`` paths resolve from the object containing
        the annotated field; array indices count as their own level and each
        ``../`` climbs exactly one level (a sibling inside the same array item
        is ``../../<name>``). Params resolving to an empty value block the
        fetch, and param *keys* are shown to the operator in the
        "Select ... first" hint — name them like the fields they reference.
    :param target: ``"self"`` executes the action on this integration;
        ``"provider"`` fans out to the connected provider integrations whose
        type registers the same action value with type "reference".
    :param allow_free_text: when True (default) the portal renders a combobox
        that also accepts free text; when False, a strict select.
    """
    if target not in ("self", "provider"):
        raise ValueError(f"target must be 'self' or 'provider', got {target!r}")
    return {
        "action": action,
        "target": target,
        "params": params or {},
        "allow_free_text": allow_free_text,
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest app/actions/tests/test_reference_actions.py -v`
Expected: 8 PASS

- [ ] **Step 5: Full suite, then commit**

Run: `pytest app -q` — expected: all pass.

```bash
git add app/actions/core.py app/actions/tests/test_reference_actions.py
git commit -m "feat: reference_annotation() helper for gundi:reference ui_schema annotations"
```

---

### Task 3: Register reference actions with type "reference"

**Files:**
- Modify: `app/services/core.py` (the `ActionTypeEnum`)
- Modify: `app/services/self_registration.py` (imports + the action-type branch chain, ~lines 8-15 and 55-63)
- Test: `app/services/tests/test_self_registration.py` (append)

**Interfaces:**
- Consumes: `ReferenceActionConfiguration` (Task 1).
- Produces: `ActionTypeEnum.REFERENCE` with value `"reference"`; registration payloads where reference actions carry `"type": "reference"` and `"is_periodic_action": False`.

- [ ] **Step 1: Write the failing test**

Append to `app/services/tests/test_self_registration.py`:

```python
def _mock_reference_action_handlers():
    from app.actions.core import ReferenceActionConfiguration

    class MockListThingsQuery(ReferenceActionConfiguration):
        parent: str

    async def action_list_things(integration, action_config: MockListThingsQuery):
        return {"options": []}

    return {"list_things": (action_list_things, MockListThingsQuery, None)}


@pytest.mark.asyncio
async def test_reference_actions_registered_with_reference_type(
    mocker,
    mock_gundi_client_v2,
    mock_get_webhook_handler_for_fixed_json_payload,
):
    mocker.patch("app.services.self_registration.INTEGRATION_TYPE_SLUG", "x_tracker")
    mocker.patch(
        "app.services.self_registration.action_handlers",
        _mock_reference_action_handlers(),
    )
    mocker.patch(
        "app.services.self_registration.get_webhook_handler",
        mock_get_webhook_handler_for_fixed_json_payload,
    )
    await register_integration_in_gundi(gundi_client=mock_gundi_client_v2)
    data = mock_gundi_client_v2.register_integration_type.call_args.args[0]
    assert len(data["actions"]) == 1
    action = data["actions"][0]
    assert action["value"] == "list_things"
    assert action["type"] == "reference"
    assert action["is_periodic_action"] is False
    # Reference actions must not claim executability — the cdip serializer
    # has no such registration field, and the schema flag would mislead.
    assert "is_executable" not in action["schema"]
```

(`pytest`, `register_integration_in_gundi`, and all three fixtures are already imported/available in this test module.)

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest app/services/tests/test_self_registration.py::test_reference_actions_registered_with_reference_type -v`
Expected: FAIL — `assert action["type"] == "reference"` gets `"generic"` (the current `else` branch)

- [ ] **Step 3: Implement enum + registration branch**

In `app/services/core.py`, add a member to `ActionTypeEnum`:

```python
class ActionTypeEnum(str, Enum):
    AUTHENTICATION = "auth"
    PULL_DATA = "pull"
    PUSH_DATA = "push"
    GENERIC = "generic"
    REFERENCE = "reference"
```

In `app/services/self_registration.py`, add `ReferenceActionConfiguration` to the existing `from app.actions import (...)` list:

```python
from app.actions import (
    action_handlers,
    AuthActionConfiguration,
    PullActionConfiguration,
    PushActionConfiguration,
    ExecutableActionMixin,
    InternalActionConfiguration,
    ReferenceActionConfiguration,
)
```

Then change the action-type branch chain inside the `for action_id, value in action_handlers.items():` loop. The reference check must come **first** — `ReferenceActionConfiguration` subclasses `ActionConfiguration`, and ordering is what routes it away from the other branches:

```python
        if issubclass(config_model, ReferenceActionConfiguration):
            action_type = ActionTypeEnum.REFERENCE.value
        elif issubclass(config_model, AuthActionConfiguration):
            action_type = ActionTypeEnum.AUTHENTICATION.value
        elif issubclass(config_model, PullActionConfiguration):
            action_type = ActionTypeEnum.PULL_DATA.value
        elif issubclass(config_model, PushActionConfiguration):
            action_type = ActionTypeEnum.PUSH_DATA.value
        else:
            action_type = ActionTypeEnum.GENERIC.value
```

Nothing else changes: reference actions fall into the existing `else: action["is_periodic_action"] = False` branch, and they don't inherit `ExecutableActionMixin` so no `is_executable` is set. No feature flag (deliberate divergence from cmore, per spec).

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest app/services/tests/test_self_registration.py::test_reference_actions_registered_with_reference_type -v`
Expected: PASS

- [ ] **Step 5: Full suite, then commit**

Run: `pytest app -q` — expected: all pass.

```bash
git add app/services/core.py app/services/self_registration.py app/services/tests/test_self_registration.py
git commit -m "feat: register reference actions with type 'reference'"
```

---

### Task 4: Stateless execution in the action runner

**Files:**
- Modify: `app/services/action_runner.py` (import ~line 19; `execute_action` around the `skippable_pull` assignment and the missing-config branch)
- Test: `app/services/tests/test_action_runner.py` (append)

**Interfaces:**
- Consumes: `ReferenceActionConfiguration` (Task 1).
- Produces: an `is_reference_action` local in `execute_action`, computed **before** the stored-config lookup — Tasks 5 and 6 reuse it.

- [ ] **Step 1: Write the failing tests**

Append to `app/services/tests/test_action_runner.py` (module already imports `json`, `pytest`, `execute_action`... — check its top; `async_return` comes from `app.conftest`, `api_client`/`_published_events_of_type` are module-level):

```python
@pytest.mark.asyncio
async def test_execute_reference_action_with_no_stored_config_and_no_overrides(
        mocker, mock_gundi_client_v2, mock_publish_event, mock_config_manager,
        integration_v2_as_dict,
):
    """A stateless reference action with an all-optional query must execute
    even when the integration stores no config for it and the caller sends
    no config_overrides (previously a 404 'configuration missing')."""
    from gundi_core.schemas.v2 import Integration
    from app.actions.core import ReferenceActionConfiguration
    from app.services.action_runner import execute_action

    class ListThingsQuery(ReferenceActionConfiguration):
        parent: str = ""

    captured = {}

    async def action_list_things(integration, action_config: ListThingsQuery):
        captured["config"] = action_config
        return {"options": [{"value": "a"}]}

    integration_no_config = Integration.parse_obj({**integration_v2_as_dict, "configurations": []})

    mocker.patch(
        "app.services.action_runner.action_handlers",
        {"list_things": (action_list_things, ListThingsQuery, None)},
    )
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mock_config_manager.get_integration_details.return_value = async_return(integration_no_config)
    mock_config_manager.get_action_configuration.return_value = async_return(None)

    result = await execute_action(
        integration_id=str(integration_no_config.id),
        action_id="list_things",
    )

    assert captured["config"].parent == ""
    assert result == {"options": [{"value": "a"}]}


@pytest.mark.asyncio
async def test_non_pull_non_reference_action_with_no_config_and_no_overrides_still_404s(
        mocker, mock_gundi_client_v2, integration_v2, mock_config_manager,
        mock_publish_event, mock_action_handlers,
):
    # Pins the three-way branch in execute_action: a plain (non-pull,
    # non-reference) action with neither stored config nor config_overrides
    # must still hit the strict "configuration missing" 404 path.
    mocker.patch("app.services.action_runner.action_handlers", mock_action_handlers)
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mock_config_manager.get_action_configuration.return_value = async_return(None)

    response = api_client.post(
        "/v1/actions/execute/",
        json={
            "integration_id": str(integration_v2.id),
            "action_id": "pull_observations_by_date",
        }
    )

    assert response.status_code == 404
    mock_action_handler, _, _ = mock_action_handlers["pull_observations_by_date"]
    assert not mock_action_handler.called
```

If `execute_action` or `async_return` is not already imported at the module top, add the imports there rather than inside the test (match the module's existing style — it imports `async_return` from `app.conftest`).

- [ ] **Step 2: Run tests to verify the first fails**

Run: `pytest app/services/tests/test_action_runner.py -k reference -v`
Expected: `test_execute_reference_action_with_no_stored_config_and_no_overrides` FAILS — `execute_action` returns a 404 `JSONResponse`, so `result == {...}` is False. The `still_404s` pin test PASSES already (it guards against over-loosening in Step 3).

- [ ] **Step 3: Implement stateless execution**

In `app/services/action_runner.py`, extend the import:

```python
from app.actions.core import PullActionConfiguration, ReferenceActionConfiguration
```

In `execute_action`, right after the `skippable_pull = is_pull_action and not is_manual` line, add:

```python
    # Reference actions are stateless: they never have stored config, and a
    # caller sending no config_overrides (e.g. a zero-param query like
    # list_tag_names) is a legitimate, complete request — not a 404. Required
    # query params still fail Pydantic validation below with a 422, which is
    # the correct signal to the portal.
    is_reference_action = isinstance(config_model, type) and issubclass(
        config_model, ReferenceActionConfiguration
    )
```

Then change the missing-config guard from:

```python
    if not action_config and not config_overrides:
```

to:

```python
    if not action_config and not config_overrides and not is_reference_action:
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest app/services/tests/test_action_runner.py -k "reference or still_404s" -v`
Expected: 2 PASS

- [ ] **Step 5: Full suite, then commit**

Run: `pytest app -q` — expected: all pass.

```bash
git add app/services/action_runner.py app/services/tests/test_action_runner.py
git commit -m "feat: allow stateless reference actions to execute without stored config"
```

---

### Task 5: Secret redaction + activity-log suppression for reference-action failures

**Files:**
- Modify: `app/services/action_runner.py` (`_handle_error` signature + event publication; `execute_action` handler-error paths)
- Test: `app/services/tests/test_action_runner.py` (append)

**Interfaces:**
- Consumes: `is_reference_action` (Task 4).
- Produces: `_handle_error(..., publish_activity_event: bool = True)` — named `publish_activity_event` (not `publish_event`) because `_handle_error`'s body calls the imported `publish_event()` function, which the spec's suggested name would shadow. Task 6 reuses this parameter and the `handler_error_config_data` local.

- [ ] **Step 1: Write the failing tests**

Append to `app/services/tests/test_action_runner.py`:

```python
@pytest.mark.asyncio
async def test_execute_reference_action_handler_error_does_not_leak_config_or_publish(
        mocker, mock_gundi_client_v2, mock_publish_event, mock_config_manager,
        integration_v2,
):
    """A reference action's handler failure (e.g. an unknown tag after a
    rename, or a 5xx from the third-party API) is routine at interactive-fetch
    frequency. It must NOT attach the integration's stored configurations
    (which include the raw auth token) to the JSON error response, and it
    must NOT publish an IntegrationActionFailed activity-log event at all —
    dropdown-open frequency would flood the activity feed."""
    from app.actions.core import ReferenceActionConfiguration
    from app.services.action_runner import execute_action

    class ListThingsQuery(ReferenceActionConfiguration):
        parent: str = ""

    async def action_list_things_boom(integration, action_config: ListThingsQuery):
        raise ValueError("boom")

    mocker.patch(
        "app.services.action_runner.action_handlers",
        {"list_things": (action_list_things_boom, ListThingsQuery, None)},
    )
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mock_config_manager.get_action_configuration.return_value = async_return(None)

    response = await execute_action(
        integration_id=str(integration_v2.id),
        action_id="list_things",
    )

    assert response.status_code == 500
    # The auth config in the shared integration_v2 fixture carries this token.
    auth_token = "testtoken2a97022f21732461ee103a08fac8a35"
    response_body = json.loads(response.body)
    assert auth_token not in json.dumps(response_body)
    assert "configurations" not in response_body["detail"]["config_data"]
    assert not _published_events_of_type(mock_publish_event, IntegrationActionFailed)


@pytest.mark.asyncio
async def test_non_reference_action_handler_error_still_publishes_failed_event(
        mocker, mock_gundi_client_v2, mock_publish_event, mock_config_manager,
        integration_v2,
):
    # Pins the suppression scope: a non-reference handler failure keeps the
    # pre-existing behavior — IntegrationActionFailed is published and carries
    # the integration's configurations for debugging.
    from app.actions.core import GenericActionConfiguration
    from app.services.action_runner import execute_action

    class DoThingConfig(GenericActionConfiguration):
        parent: str = ""

    async def action_do_thing_boom(integration, action_config: DoThingConfig):
        raise ValueError("boom")

    mocker.patch(
        "app.services.action_runner.action_handlers",
        {"do_thing": (action_do_thing_boom, DoThingConfig, None)},
    )
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mock_config_manager.get_action_configuration.return_value = async_return(None)

    response = await execute_action(
        integration_id=str(integration_v2.id),
        action_id="do_thing",
        config_overrides={"parent": "x"},
    )

    assert response.status_code == 500
    failed_events = _published_events_of_type(mock_publish_event, IntegrationActionFailed)
    assert len(failed_events) == 1
    assert failed_events[0].payload.config_data.get("configurations")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest app/services/tests/test_action_runner.py -k "does_not_leak or still_publishes" -v`
Expected: the leak test FAILS (token present in the response body and one IntegrationActionFailed published); the pin test PASSES already.

- [ ] **Step 3: Implement redaction + suppression**

In `app/services/action_runner.py`:

1. Extend `_handle_error`'s signature and gate the event publication:

```python
async def _handle_error(
        exc: Exception, integration_id: str, action_id: Optional[str] = None,
        config_data=None, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        publish_activity_event: bool = True
):
```

and wrap the existing `await publish_event(...)` block:

```python
    # Publish the error event (suppressed for reference actions: they are
    # portal-invoked at interactive-fetch frequency, so failures are routine
    # and would flood the activity feed — the local log line above is enough).
    if publish_activity_event:
        await publish_event(
            event=IntegrationActionFailed(
                payload=ActionExecutionFailed(**error_details)
            ),
            topic_name=settings.INTEGRATION_EVENTS_TOPIC,
        )
```

2. In `execute_action`, immediately before the `try:  # Execute the action handler with a timeout` block, add:

```python
    # Reference-action errors must not carry the integration's stored
    # configurations — which include raw auth secrets (e.g. a bearer token) —
    # into the JSON error response or any published event.
    handler_error_config_data = None if is_reference_action else {
        "configurations": [c.dict() for c in integration.configurations]
    }
```

3. Rewrite the two handler-failure handlers to use it:

```python
    except asyncio.TimeoutError:
        return await _handle_error(
            asyncio.TimeoutError(f"Action '{action_id}' timed out"),
            integration_id, action_id,
            config_data=handler_error_config_data,
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            publish_activity_event=not is_reference_action,
        )
    except Exception as e:
        return await _handle_error(e, integration_id, action_id,
                                   config_data=handler_error_config_data,
                                   publish_activity_event=not is_reference_action)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest app/services/tests/test_action_runner.py -k "does_not_leak or still_publishes" -v`
Expected: 2 PASS

- [ ] **Step 5: Full suite, then commit**

Run: `pytest app -q` — expected: all pass (existing error-path tests exercise `_handle_error` with its default, unchanged behavior).

```bash
git add app/services/action_runner.py app/services/tests/test_action_runner.py
git commit -m "feat: redact config and suppress activity-log events for reference-action failures"
```

---

### Task 6: `ReferenceDataError` → HTTP 422

**Files:**
- Modify: `app/services/errors.py`
- Modify: `app/services/action_runner.py` (import + one `except` clause)
- Test: `app/services/tests/test_action_runner.py` (append)

**Interfaces:**
- Consumes: `handler_error_config_data`, `is_reference_action`, `publish_activity_event` (Tasks 4-5).
- Produces: `ReferenceDataError(Exception)` in `app.services.errors` — the exception integration authors raise from reference handlers for unknown-value queries.

- [ ] **Step 1: Write the failing test**

Append to `app/services/tests/test_action_runner.py`:

```python
@pytest.mark.asyncio
async def test_reference_data_error_maps_to_422(
        mocker, mock_gundi_client_v2, mock_publish_event, mock_config_manager,
        integration_v2,
):
    """Unknown reference values (e.g. a stale tag name in a $data cascade)
    are an expected client-input condition, not a server fault — 422, not
    500. cmore surfaced these as 500s; the template fixes that."""
    from app.actions.core import ReferenceActionConfiguration
    from app.services.action_runner import execute_action
    from app.services.errors import ReferenceDataError

    class ListFieldsQuery(ReferenceActionConfiguration):
        tag: str = ""

    async def action_list_fields(integration, action_config: ListFieldsQuery):
        raise ReferenceDataError(f"Unknown tag '{action_config.tag}'")

    mocker.patch(
        "app.services.action_runner.action_handlers",
        {"list_fields": (action_list_fields, ListFieldsQuery, None)},
    )
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mock_config_manager.get_action_configuration.return_value = async_return(None)

    response = await execute_action(
        integration_id=str(integration_v2.id),
        action_id="list_fields",
        config_overrides={"tag": "renamed_tag"},
    )

    assert response.status_code == 422
    response_body = json.loads(response.body)
    assert "Unknown tag 'renamed_tag'" in response_body["detail"]["error"]
    assert "configurations" not in response_body["detail"]["config_data"]
    assert not _published_events_of_type(mock_publish_event, IntegrationActionFailed)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest app/services/tests/test_action_runner.py::test_reference_data_error_maps_to_422 -v`
Expected: FAIL with `ImportError: cannot import name 'ReferenceDataError'`

- [ ] **Step 3: Implement the exception and mapping**

Append to `app/services/errors.py`:

```python
class ReferenceDataError(Exception):
    """Raised by reference-action handlers when a query names an unknown
    value (e.g. an unknown tag or field in a $data cascade). The action
    runner maps it to HTTP 422 instead of a generic 500."""
    pass
```

In `app/services/action_runner.py`, add the import near the other relative imports:

```python
from .errors import ReferenceDataError
```

and insert a new `except` clause between the `asyncio.TimeoutError` and generic `Exception` handlers of the handler-execution `try` block:

```python
    except ReferenceDataError as e:
        # An expected client-input condition (stale/unknown query value), not
        # a server fault.
        return await _handle_error(
            e, integration_id, action_id,
            config_data=handler_error_config_data,
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            publish_activity_event=not is_reference_action,
        )
```

(Plain `ValueError` from a handler still surfaces as 500 — arbitrary exceptions are not reinterpreted.)

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest app/services/tests/test_action_runner.py::test_reference_data_error_maps_to_422 -v`
Expected: PASS

- [ ] **Step 5: Full suite, then commit**

Run: `pytest app -q` — expected: all pass.

```bash
git add app/services/errors.py app/services/action_runner.py app/services/tests/test_action_runner.py
git commit -m "feat: map ReferenceDataError to 422 for unknown reference-query values"
```

---

### Task 7: Drift-guard test utility

**Files:**
- Create: `app/services/testing.py`
- Test: `app/services/tests/test_testing_utils.py`

**Interfaces:**
- Consumes: `ReferenceActionConfiguration`, `reference_annotation` (Tasks 1-2).
- Produces: `collect_gundi_references(node, found=None) -> list[tuple[dict, dict]]` and `assert_reference_annotations_valid(ui_schema, handlers) -> list[tuple[dict, dict]]` in `app.services.testing`. Integrations call the latter from their own test suite with `discover_actions(...)` output as `handlers`; it returns the found `(host_node, annotation)` pairs so callers can add integration-specific assertions (e.g. pin the expected action set).

- [ ] **Step 1: Write the failing tests**

Create `app/services/tests/test_testing_utils.py`:

```python
import pytest

from app.actions.core import ReferenceActionConfiguration, reference_annotation
from app.services.testing import assert_reference_annotations_valid


class ListSitesQuery(ReferenceActionConfiguration):
    region: str = ""


class ListSiteFieldsQuery(ReferenceActionConfiguration):
    site: str  # required


async def action_list_sites(integration, action_config: ListSitesQuery):
    return {"options": []}


async def action_list_site_fields(integration, action_config: ListSiteFieldsQuery):
    return {"options": []}


HANDLERS = {
    "list_sites": (action_list_sites, ListSitesQuery, None),
    "list_site_fields": (action_list_site_fields, ListSiteFieldsQuery, None),
}


def test_valid_annotations_pass_and_are_returned():
    ui_schema = {
        "site_id": {"gundi:reference": reference_annotation("list_sites")},
        "mappings": {
            "items": {
                "field_name": {
                    "gundi:reference": reference_annotation(
                        "list_site_fields",
                        params={"site": {"$data": "../../site_id"}},
                    )
                },
            },
        },
    }
    found = assert_reference_annotations_valid(ui_schema, HANDLERS)
    assert len(found) == 2


def test_unknown_action_fails():
    ui_schema = {"site_id": {"gundi:reference": reference_annotation("list_planets")}}
    with pytest.raises(AssertionError, match="list_planets"):
        assert_reference_annotations_valid(ui_schema, HANDLERS)


def test_undeclared_param_fails():
    ui_schema = {
        "site_id": {
            "gundi:reference": reference_annotation(
                "list_sites", params={"regionn": "typo"}
            )
        }
    }
    with pytest.raises(AssertionError, match="regionn"):
        assert_reference_annotations_valid(ui_schema, HANDLERS)


def test_missing_required_param_fails():
    ui_schema = {
        "field_name": {"gundi:reference": reference_annotation("list_site_fields")}
    }
    with pytest.raises(AssertionError, match="site"):
        assert_reference_annotations_valid(ui_schema, HANDLERS)


def test_ui_widget_alongside_annotation_fails():
    ui_schema = {
        "site_id": {
            "ui:widget": "select",
            "gundi:reference": reference_annotation("list_sites"),
        }
    }
    with pytest.raises(AssertionError, match="ui:widget"):
        assert_reference_annotations_valid(ui_schema, HANDLERS)


def test_provider_target_skips_handler_checks():
    # Provider-target actions live on another integration type's runner, so
    # their query models can't be validated here — only the host-node rules
    # (no ui:widget, valid target) apply.
    ui_schema = {
        "subject_type": {
            "gundi:reference": reference_annotation(
                "list_subject_types", target="provider"
            )
        }
    }
    found = assert_reference_annotations_valid(ui_schema, HANDLERS)
    assert len(found) == 1


def test_non_reference_config_model_fails():
    from app.actions.core import GenericActionConfiguration

    class NotAQuery(GenericActionConfiguration):
        pass

    async def action_not_a_query(integration, action_config: NotAQuery):
        return {}

    handlers = {"not_a_query": (action_not_a_query, NotAQuery, None)}
    ui_schema = {"site_id": {"gundi:reference": reference_annotation("not_a_query")}}
    with pytest.raises(AssertionError, match="not_a_query"):
        assert_reference_annotations_valid(ui_schema, handlers)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest app/services/tests/test_testing_utils.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'app.services.testing'`

- [ ] **Step 3: Implement the utility**

Create `app/services/testing.py`:

```python
"""Test utilities for integrations built on this template."""

from app.actions.core import ReferenceActionConfiguration


def collect_gundi_references(node, found=None):
    """Recursively collect every ``"gundi:reference"`` annotation in a
    ui_schema tree. Returns a list of ``(host_node, annotation)`` pairs,
    where host_node is the dict the annotation sits on."""
    if found is None:
        found = []
    if isinstance(node, dict):
        if "gundi:reference" in node:
            found.append((node, node["gundi:reference"]))
        for value in node.values():
            collect_gundi_references(value, found)
    return found


def assert_reference_annotations_valid(ui_schema, handlers):
    """Drift guard for ``gundi:reference`` annotations in a config model's
    ui_schema. Call from an integration's test suite:

        handlers = discover_actions(module_name="app.actions.handlers", prefix="action_")
        assert_reference_annotations_valid(MyConfig.ui_schema(), handlers)

    Asserts, for every annotation found:
    - ``target`` is "self" or "provider";
    - the host node never sets ``ui:widget`` (forward-compat: portals
      without reference support must keep rendering plain text fields);
    and additionally for ``target="self"`` annotations:
    - the named action exists in ``handlers`` and its config model
      subclasses ReferenceActionConfiguration;
    - declared params are a subset of the query model's fields;
    - the query model's required fields are all declared as params
      (otherwise the portal's fetch would always 422).

    Provider-target annotations name actions on another integration type's
    runner, so only the host-node rules apply to them.

    Returns the ``(host_node, annotation)`` pairs so callers can add
    integration-specific assertions (e.g. pin the expected action set).
    """
    found = collect_gundi_references(ui_schema)
    for host_node, ref in found:
        assert "action" in ref, f"gundi:reference annotation missing 'action': {ref}"
        target = ref.get("target", "self")
        assert target in ("self", "provider"), (ref["action"], target)
        assert "ui:widget" not in host_node, (
            f"'{ref['action']}': never set ui:widget next to gundi:reference; "
            "the portal chooses the widget."
        )
        if target != "self":
            continue
        assert ref["action"] in handlers, f"unknown reference action '{ref['action']}'"
        _, config_model, _ = handlers[ref["action"]]
        assert issubclass(config_model, ReferenceActionConfiguration), (
            f"'{ref['action']}' is annotated as a reference action but its "
            "config model does not subclass ReferenceActionConfiguration"
        )
        declared = set(ref.get("params", {}))
        model_fields = set(config_model.__fields__)
        assert declared <= model_fields, (ref["action"], declared - model_fields)
        required = {
            name for name, field in config_model.__fields__.items() if field.required
        }
        assert required <= declared, (ref["action"], required - declared)
    return found
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest app/services/tests/test_testing_utils.py -v`
Expected: 7 PASS

- [ ] **Step 5: Full suite, then commit**

Run: `pytest app -q` — expected: all pass.

```bash
git add app/services/testing.py app/services/tests/test_testing_utils.py
git commit -m "feat: drift-guard test utility for gundi:reference annotations"
```

---

### Task 8: README documentation

**Files:**
- Modify: `README.md` (new section after the "## Action Examples:" section, before "## Webhooks Usage:")

**Interfaces:**
- Consumes: everything from Tasks 1-7 (names must match exactly).
- Produces: user-facing docs. The docs-site page and CLAUDE.md guidance land with the library-branch port (spec §8) — `main` has neither mkdocs nor a tracked CLAUDE.md, so README is the docs home here.

- [ ] **Step 1: Add the README section**

Insert into `README.md` after the Action Examples section:

````markdown
## Reference Data Actions

Reference actions feed the Gundi Portal's live option comboboxes: when an
operator configures another action, annotated fields fetch their options by
executing a reference action on your integration. The config model IS the
query, and the handler returns an options envelope. Reference actions are
stateless — they read the integration's auth config but never store
configuration of their own, and they are hidden from the Portal's
configuration accordions.

```python
# actions/configurations.py
from .core import PushActionConfiguration, ReferenceActionConfiguration, reference_annotation
from app.services.utils import FieldWithUIOptions


class ListSitesQuery(ReferenceActionConfiguration):
    region: str = ""  # optional query param — sent by the portal as config_overrides


class DeliverConfig(PushActionConfiguration):
    site_id: str = FieldWithUIOptions(..., title="Site")

    @classmethod
    def ui_schema(cls, *args, **kwargs):
        schema = super().ui_schema(*args, **kwargs)
        # Renders site_id as a combobox fed by action_list_sites.
        schema.setdefault("site_id", {})["gundi:reference"] = reference_annotation("list_sites")
        return schema
```

```python
# actions/handlers.py
from app.actions.core import ReferenceDataResponse, ReferenceOption
from app.services.errors import ReferenceDataError

async def action_list_sites(integration, action_config: ListSitesQuery):
    auth_config = get_auth_config(integration)
    async with SiteClient(auth_config) as client:
        sites = await client.get_sites(region=action_config.region or None)
    return ReferenceDataResponse(
        options=[ReferenceOption(value=s["id"], label=s["name"]) for s in sites]
    ).dict()
```

Notes:

- Cascading params: pass `params={"site": {"$data": "../../site_id"}}` to
  `reference_annotation()` to fill a query param from the current form state.
  Paths resolve from the object containing the annotated field; array indices
  count as their own level and each `../` climbs one level (a sibling inside
  the same array item is `../../<name>`). Param keys are shown to operators
  in the "Select ... first" hint, so name them like the fields they reference.
- `target="provider"` fetches options from the connected provider
  integrations instead: the provider's integration type must register the
  same action value with type `reference`, and results from multiple
  providers are merged and deduplicated by `value`.
- Raise `ReferenceDataError` for unknown query values (stale tag names etc.);
  the runner returns 422. Prefer returning an empty `options` list for
  recoverable "nothing found" cases — the Portal renders all errors the same
  way (a plain text input with a Retry button).
- `label` defaults to `value` in the Portal; `description` and `truncated`
  are accepted but not rendered yet. Options are cached portal-side for
  `cache_ttl_seconds` (default 300).
- Guard against drift in your tests:

```python
from app.actions.core import discover_actions
from app.services.testing import assert_reference_annotations_valid

def test_reference_annotations_are_valid():
    handlers = discover_actions(module_name="app.actions.handlers", prefix="action_")
    assert_reference_annotations_valid(DeliverConfig.ui_schema(), handlers)
```
````

- [ ] **Step 2: Verify the README examples against the implementation**

Check every name in the section against the code: `ReferenceActionConfiguration`, `ReferenceOption`, `ReferenceDataResponse`, `reference_annotation`, `ReferenceDataError`, `assert_reference_annotations_valid`, `collect_gundi_references`, `discover_actions` import paths. Fix any mismatch in the README (not the code).

- [ ] **Step 3: Full suite, then commit**

Run: `pytest app -q` — expected: all pass.

```bash
git add README.md
git commit -m "docs: document reference data actions in the README"
```

---

## Completion

After Task 8: push the branch and open a PR against `main` titled "Reference data actions: portal-driven option lookups" summarizing the spec's verified-contract highlights (no flag; redaction + suppression; `ReferenceDataError` → 422; drift guard; helper). The library-branch port (spec §8) is a separate follow-up after this PR merges.
