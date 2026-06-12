import asyncio
import logging
import time
import traceback
from enum import Enum
from typing import Optional

import httpx
import pydantic
import stamina
from gundi_client_v2 import GundiClient

from app.actions import action_handlers, get_action_handler_by_data_type
from app import settings
from fastapi import status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from gundi_core.events import IntegrationActionFailed, ActionExecutionFailed, LogLevel

from app.actions.core import PullActionConfiguration
from .config_manager import IntegrationConfigurationManager
from .utils import find_config_for_action
from .activity_logger import publish_event, log_action_activity

_portal = GundiClient()
config_manager = IntegrationConfigurationManager()
logger = logging.getLogger(__name__)


class ActionTrigger(str, Enum):
    """Where an action invocation originated.

    AUTO covers the portal's scheduler — and, by default, anything that
    doesn't say otherwise. For automated pull-action runs, a missing/invalid
    config (or a paused `run_on_schedule`) is a clean no-op, because pull
    actions are scheduled type-wide and fire even for destination-only
    integrations that never get a pull config.

    MANUAL is an explicit, operator-initiated run. Those keep the strict
    404/422 behavior so a real misconfiguration surfaces immediately, and they
    ignore the `run_on_schedule` pause toggle (a manual run is not "on
    schedule").
    """
    AUTO = "auto"
    MANUAL = "manual"


async def _handle_error(
        exc: Exception, integration_id: str, action_id: Optional[str] = None,
        config_data=None, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
):
    """
    Handles errors by logging, extracting details as available, and publishing events for activity logs.
    Returns a JSON response with error details too.
    """

    message = f"Error in action '{action_id}' for integration '{integration_id}': {type(exc).__name__}: {exc}"
    logger.exception(message)

    error_details = {
        "integration_id": integration_id,
        "action_id": action_id,
        "config_data": config_data or {},
        "error": message,
        "error_traceback": traceback.format_exc()
    }

    # Extract additional request/response details if available
    if (request := getattr(exc, "request", None)) is not None:
        error_details.update({
            "request_verb": str(request.method),
            "request_url": str(request.url),
            "request_data": str(getattr(request, "content", getattr(request, "body", None)) or "")
        })
    if (response := getattr(exc, "response", None)) is not None:  # bool(response) on status errors returns False
        error_details.update({
            "server_response_status": getattr(response, "status_code", None),
            "server_response_body": str(getattr(response, "text", getattr(response, "content", None)) or "")
        })

    # Publish the error event
    await publish_event(
        event=IntegrationActionFailed(
            payload=ActionExecutionFailed(**error_details)
        ),
        topic_name=settings.INTEGRATION_EVENTS_TOPIC,
    )

    # Return the JSON response
    return JSONResponse(
        status_code=status_code,
        content=jsonable_encoder({"detail": error_details}),
    )


async def _skip_pull_action(integration_id, action_id, *, title, level, reason, data=None):
    """Record a clean, non-error skip of a pull action.

    Destination-only integrations get pull actions scheduled type-wide but
    have no usable config. Rather than publishing an `IntegrationActionFailed`
    error, we log the skip to the activity feed (INFO for the expected
    "not configured as a source" cases, WARNING when a present config is
    invalid) and return a benign result.
    """
    logger.info(f"{title} (integration '{integration_id}')")
    await log_action_activity(
        integration_id=integration_id,
        action_id=action_id,
        title=title,
        level=level,
        data=data,
    )
    return {"skipped": True, "reason": reason}


async def execute_action(
        integration_id: str, action_id: Optional[str] = None, config_overrides: dict = None,
        data: dict = None, metadata: dict = None, triggered_by: Optional[str] = None
):
    try:  # Get the integration details to pass it to the action handler
        integration = await config_manager.get_integration_details(integration_id)
    except Exception as e:
        return await _handle_error(e, integration_id, action_id)

    # Find the action handler based on the action ID or data type
    if action_id:
        try:  # There must be one action handler implemented for the action
            handler, config_model, DataModel = action_handlers[action_id]
        except KeyError:
            return await _handle_error(
                KeyError(f"Action '{action_id}' is not supported"),
                integration_id, action_id,
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY
            )
    elif data and (data_type := data.get("event_type")):  # Push data actions
        try:  # Get the action handler by data type
            action_id, handler, config_model, DataModel = get_action_handler_by_data_type(type_name=data_type)
        except ValueError:
            return await _handle_error(
                ValueError(f"Data type '{data_type}' is not supported"),
                integration_id, action_id,
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY
            )
    else:
        return await _handle_error(
            ValueError("No action handler found by action ID or data type"),
            integration_id, action_id,
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY
        )

    logger.info(f"Executing action '{action_id}' for integration '{integration_id}'...")

    # Pull actions are scheduled type-wide, so the portal fires them for every
    # integration of this type — including destination-only ones that never get
    # a pull config. For an *automated* run, "no usable config" (or a paused
    # toggle) means "nothing to pull" — a clean no-op rather than a failure. A
    # *manual* run keeps the strict 404/422 behavior so misconfigurations
    # surface immediately, and ignores the pause toggle.
    is_pull_action = isinstance(config_model, type) and issubclass(config_model, PullActionConfiguration)
    is_manual = triggered_by == ActionTrigger.MANUAL
    skippable_pull = is_pull_action and not is_manual

    # Get the configuration needed to execute the action
    action_config = await config_manager.get_action_configuration(integration_id, action_id)
    if not action_config and not config_overrides:
        if skippable_pull:
            return await _skip_pull_action(
                integration_id, action_id,
                title=f"Skipping '{action_id}': integration is not configured for this action.",
                level=LogLevel.INFO,
                reason="no_configuration",
            )
        message = f"Configuration for action '{action_id}' for integration {str(integration.id)} is missing."
        logger.error(message)
        return await _handle_error(
            ValueError(message), integration_id, action_id,
            config_data={"configurations": [i.dict() for i in integration.configurations]},
            status_code=status.HTTP_404_NOT_FOUND
        )

    try:  # Parse the action configuration
        config_data = action_config.data if action_config else {}
        if config_overrides:
            config_data.update(config_overrides)
        parsed_config = config_model.parse_obj(config_data)
    except pydantic.ValidationError as e:
        # An automated pull whose config doesn't validate has nothing it can
        # safely pull. Skip rather than raise — but log at WARNING with the
        # validation detail so a genuinely misconfigured source stays noticeable.
        if skippable_pull:
            return await _skip_pull_action(
                integration_id, action_id,
                title=f"Skipping '{action_id}': configuration is missing or invalid.",
                level=LogLevel.WARNING,
                reason="invalid_configuration",
                data={"validation_error": str(e)},
            )
        return await _handle_error(e, integration_id, action_id, config_data, status.HTTP_422_UNPROCESSABLE_ENTITY)

    # Respect the operator's explicit pause toggle — only for scheduled runs.
    if skippable_pull and not getattr(parsed_config, "run_on_schedule", True):
        return await _skip_pull_action(
            integration_id, action_id,
            title=f"Skipping '{action_id}': 'run_on_schedule' is turned off for this integration.",
            level=LogLevel.INFO,
            reason="run_on_schedule_disabled",
        )

    parsed_data = None
    if data and DataModel:
        try:  # Parse the input data if a data model is defined for the action
            parsed_data = DataModel(**data)
        except pydantic.ValidationError as e:
            return await _handle_error(e, integration_id, action_id, data, status.HTTP_422_UNPROCESSABLE_ENTITY)

    try:  # Execute the action handler with a timeout
        start_time = time.monotonic()
        handler_kwargs = {
            "integration": integration,
            "action_config": parsed_config,
        }
        if parsed_data:
            handler_kwargs["data"] = parsed_data
        if metadata:
            handler_kwargs["metadata"] = metadata
        result = await asyncio.wait_for(
            handler(**handler_kwargs),
            timeout=settings.MAX_ACTION_EXECUTION_TIME
        )
    except asyncio.TimeoutError:
        return await _handle_error(
            asyncio.TimeoutError(f"Action '{action_id}' timed out"),
            integration_id, action_id,
            config_data={"configurations": [c.dict() for c in integration.configurations]},
            status_code=status.HTTP_504_GATEWAY_TIMEOUT
        )
    except Exception as e:
        return await _handle_error(e, integration_id, action_id,
                                   config_data={"configurations": [c.dict() for c in integration.configurations]})

    # Success. Log the execution time and return the result
    end_time = time.monotonic()
    execution_time = end_time - start_time
    logger.debug(
        f"Action '{action_id}' executed successfully for integration {integration_id} in {execution_time:.2f} seconds."
    )
    return result
