import logging
from typing import List
import app.settings
from fastapi import APIRouter, BackgroundTasks
from app.actions import get_actions
from app.services.action_runner import execute_action, ActionTrigger, _request_error_response
from app.api_schemas import ActionRequest

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get(
    "/",
    summary="Execute an action with given settings",
    response_model=List[str]
)
async def list_actions():
    return get_actions()

@router.post(
    "/execute",
    summary="Execute an action with given settings",
)
async def execute(
    request: ActionRequest,
    background_tasks: BackgroundTasks
):
    # Direct /execute calls are explicit invocations → manual by default, so a
    # misconfigured pull action surfaces a 404/422 here rather than skipping.
    triggered_by = request.triggered_by or ActionTrigger.MANUAL.value
    # Request-shape rejections share the runner's {"detail": {"action_id",
    # "error"}} response and, like the runner's, publish no activity event.
    if request.integration_id is None and request.integration_state is None:
        return _request_error_response(request.action_id, "Provide either integration_id or integration_state.")
    if request.integration_id is not None and request.integration_state is not None:
        return _request_error_response(request.action_id, "Provide either integration_id or integration_state, not both.")
    if request.integration_state is not None and request.run_in_background:
        return _request_error_response(request.action_id, "Ephemeral executions cannot run in background.")
    if request.run_in_background:
        background_tasks.add_task(
            execute_action,
            integration_id=request.integration_id,
            action_id=request.action_id,
            config_overrides=request.config_overrides,
            triggered_by=triggered_by,
        )
        return {"message": "Action execution started in background"}
    else:
        return await execute_action(
            integration_id=request.integration_id,
            action_id=request.action_id,
            config_overrides=request.config_overrides,
            triggered_by=triggered_by,
            integration_state=request.integration_state,
        )
