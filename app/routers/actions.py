import logging
from typing import List, Dict, Any
import app.settings
from fastapi import APIRouter, BackgroundTasks, HTTPException
from app.actions import get_actions
from app.actions.core import discover_actions
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

@router.get(
    "/{action_id}/schema",
    summary="Get configuration schema for an action",
)
async def get_action_schema(action_id: str) -> Dict[str, Any]:
    """Get the configuration schema and UI schema for a specific action"""
    action_handlers = discover_actions(module_name="app.actions.handlers", prefix="action_")
    
    if action_id not in action_handlers:
        raise HTTPException(status_code=404, detail=f"Action '{action_id}' not found")
    
    func, config_model, data_model = action_handlers[action_id]
    
    # Get the JSON schema for the configuration model
    config_schema = config_model.schema()
    
    # Get the UI schema if the model supports it
    ui_schema = {}
    if hasattr(config_model, 'ui_schema'):
        ui_schema = config_model.ui_schema()
    
    return {
        "action_id": action_id,
        "config_schema": config_schema,
        "ui_schema": ui_schema,
        "description": func.__doc__ or f"Configuration for {action_id} action"
    }


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
