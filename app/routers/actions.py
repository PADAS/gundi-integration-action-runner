import logging
from typing import List
import app.settings
from fastapi import APIRouter, BackgroundTasks
from app.actions import get_actions
from app.services.action_runner import execute_action
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
    if request.run_in_background:
        background_tasks.add_task(
            execute_action,
            integration_id=request.integration_id,
            action_id=request.action_id,
            config_overrides=request.config_overrides
        )
        return {"message": "Action execution started in background"}
    else:
        return await execute_action(
            integration_id=request.integration_id,
            action_id=request.action_id,
            config_overrides=request.config_overrides
        )
