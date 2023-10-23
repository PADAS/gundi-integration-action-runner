import logging
from typing import Optional
from gundi_client_v2 import GundiClient
from app.actions import action_handlers
from app.actions import ActionConfiguration
from fastapi import status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from .errors import ActionNotFound, ConfigurationNotFound, ConfigurationValidationError, ActionExecutionError
from .utils import find_config_for_action


_portal = GundiClient()
logger = logging.getLogger(__name__)


async def execute_action(integration_id: str, action_id: str):
    """
    Interface for executing actions.
    :param integration_id: The UUID of the integration
    :param action_id: "test_auth", "pull_observations", "pull_events"
    :return: action result if any, or raise an exception
    """
    logger.info(f"Executing action '{action_id}' for integration '{integration_id}'...")
    try:  # Get the integration config from the portal
        integration = await _portal.get_integration_details(integration_id=integration_id)
    except Exception as e:
        message = f"Error retrieving configuration for integration '{integration_id}': {e}"
        logger.exception(message)
        return JSONResponse(
            status_code=e.response.status_code,
            content=jsonable_encoder({"detail": message}),
        )

    # Look for the configuration of the action being executed
    action_config = find_config_for_action(
        configurations=integration.configurations,
        action_id=action_id
    )
    if not action_config:
        message = f"Configuration for action '{action_id}' for integration {str(integration.id)} " \
                  f"is missing. Please fix the integration setup in the portal."
        logger.error(message)
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content=jsonable_encoder({"detail": message}),
        )
    try:  # Execute the action
        handler = action_handlers[action_id]
        result = await handler(integration, action_config)
    except KeyError as e:
        message = f"Action '{action_id}' is not supported for this integration"
        logger.exception(message)
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content=jsonable_encoder({"detail": message}),
        )
    except Exception as e:
        message = f"Internal error executing action '{action_id}': {e}"
        logger.exception(message)
        return JSONResponse(
            status_code=e.response.status_code,
            content=jsonable_encoder({"detail": message}),
        )
    else:
        # ToDo: emit events on execution completion (success or error) once we move forward with the EDA
        return result
