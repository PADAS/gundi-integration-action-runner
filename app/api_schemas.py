from typing import Optional

from pydantic import BaseModel


class ActionRequest(BaseModel):
    integration_id: str
    action_id: str
    run_in_background: bool = False
    config_overrides: dict = None
    # How the run was initiated. The /execute endpoint is an explicit, direct
    # invocation, so it defaults to "manual" when unset (see the router) —
    # keeping the strict 404/422 behavior for misconfigured pull actions.
    triggered_by: Optional[str] = None

