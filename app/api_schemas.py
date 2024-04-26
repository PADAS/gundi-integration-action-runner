from pydantic import BaseModel


class ActionRequest(BaseModel):
    integration_id: str
    action_id: str
    run_in_background: bool = False
    config_overrides: dict = None

