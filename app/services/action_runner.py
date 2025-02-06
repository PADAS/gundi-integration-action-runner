import asyncio
import logging
import time
import traceback

import httpx
import pydantic
import stamina
from gundi_client_v2 import GundiClient

from app.actions import action_handlers
from app import settings
from fastapi import status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from gundi_core.events import IntegrationActionFailed, ActionExecutionFailed

from .config_manager import IntegrationConfigurationManager
from .utils import find_config_for_action
from .activity_logger import publish_event

_portal = GundiClient()
config_manager = IntegrationConfigurationManager()
logger = logging.getLogger(__name__)


async def _handle_error(
        exc: Exception, integration_id: str, action_id: str,
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


async def execute_action(integration_id: str, action_id: str, config_overrides: dict = None):
    logger.info(f"Executing action '{action_id}' for integration '{integration_id}'...")

    try:  # Get the integration details to pass it to the action handler
        integration = await config_manager.get_integration_details(integration_id)
    except Exception as e:
        return await _handle_error(e, integration_id, action_id)

    # Get the configuration needed to execute the action
    action_config = await config_manager.get_action_configuration(integration_id, action_id)
    if not action_config and not config_overrides:
        message = f"Configuration for action '{action_id}' for integration {str(integration.id)} is missing."
        logger.error(message)
        return await _handle_error(
            ValueError(message), integration_id, action_id,
            config_data={"configurations": [i.dict() for i in integration.configurations]},
            status_code=status.HTTP_404_NOT_FOUND
        )

    try:  # There must be one action handler implemented for the action
        handler, config_model = action_handlers[action_id]
    except KeyError:
        return await _handle_error(
            KeyError(f"Action '{action_id}' is not supported"),
            integration_id, action_id,
            config_data=action_config,
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY
        )

    try:  # Parse the action configuration
        config_data = action_config.data if action_config else {}
        if config_overrides:
            config_data.update(config_overrides)
        parsed_config = config_model.parse_obj(config_data)
    except pydantic.ValidationError as e:
        return await _handle_error(e, integration_id, action_id, config_data, status.HTTP_422_UNPROCESSABLE_ENTITY)

    try:  # Execute the action handler with a timeout
        start_time = time.monotonic()
        result = await asyncio.wait_for(
            handler(integration=integration, action_config=parsed_config),
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
