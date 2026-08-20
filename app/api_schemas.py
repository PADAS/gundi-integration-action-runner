from typing import List, Optional

from pydantic import BaseModel


class DraftActionConfig(BaseModel):
    action_value: str
    data: dict


class IntegrationState(BaseModel):
    type_value: str
    base_url: Optional[str] = None
    configurations: List[DraftActionConfig] = []


class ActionRequest(BaseModel):
    # `integration_id` is optional: when absent, `integration_state` must be
    # present and the action must be a reference action.
    integration_id: Optional[str] = None
    action_id: str
    run_in_background: bool = False
    config_overrides: dict = None
    # How the run was initiated. The /execute endpoint is an explicit, direct
    # invocation, so it defaults to "manual" when unset (see the router) —
    # keeping the strict 404/422 behavior for misconfigured pull actions.
    triggered_by: Optional[str] = None
    integration_state: Optional[IntegrationState] = None
