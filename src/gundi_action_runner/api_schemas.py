from typing import List, Optional

from pydantic import BaseModel, Field


# Same constraints gundi_core puts on IntegrationType.value and
# IntegrationAction.value. Enforcing them here means a bad natural key is a
# request-validation 422 that names `integration_state.type_value`, instead of
# surfacing later from Integration.parse_obj against synthesized fields such
# as `type.actions.0.value` that never appeared in the request.
_NATURAL_KEY = dict(min_length=2, max_length=200, regex=r"^[a-z0-9_]+$")


class DraftActionConfig(BaseModel):
    action_value: str = Field(..., **_NATURAL_KEY)
    data: dict


class IntegrationState(BaseModel):
    type_value: str = Field(..., **_NATURAL_KEY)
    base_url: Optional[str] = None
    configurations: List[DraftActionConfig] = Field(default_factory=list)


class ActionRequest(BaseModel):
    # `integration_id` is optional: when absent, `integration_state` must be
    # present and the action must be reference or auth (the ephemerally-safe
    # whitelist enforced in action_runner._execute_action_impl).
    integration_id: Optional[str] = None
    action_id: str
    run_in_background: bool = False
    config_overrides: dict = None
    # How the run was initiated. The /execute endpoint is an explicit, direct
    # invocation, so it defaults to "manual" when unset (see the router) —
    # keeping the strict 404/422 behavior for misconfigured pull actions.
    triggered_by: Optional[str] = None
    integration_state: Optional[IntegrationState] = None
