import asyncio
import logging
import time

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


async def execute_action(integration_id: str, action_id: str, config_overrides: dict = None):
    """
    Interface for executing actions.
    :param integration_id: The UUID of the integration
    :param action_id: "test_auth", "pull_observations", "pull_events"
    :param config_overrides: Optional dictionary with configuration overrides
    :return: action result if any, or raise an exception
    """
    logger.info(f"Executing action '{action_id}' for integration '{integration_id}'...")
    try:  # Get the integration config
        integration = await config_manager.get_integration_details(integration_id)
    except Exception as e:
        message = f"Error retrieving integration '{integration_id}': {type(e)}: {e}"
        logger.exception(message)
        await publish_event(
            event=IntegrationActionFailed(
                payload=ActionExecutionFailed(
                    integration_id=integration_id,
                    action_id=action_id,
                    error=message
                )
            ),
            topic_name=settings.INTEGRATION_EVENTS_TOPIC,
        )
        return JSONResponse(
            status_code=e.response.status_code if hasattr(e, "response") else status.HTTP_500_INTERNAL_SERVER_ERROR,
            content=jsonable_encoder({"detail": message}),
        )

    # Look for the configuration of the action being executed
    action_config = await config_manager.get_action_configuration(integration_id, action_id)
    if not action_config and not config_overrides:
        message = f"Configuration for action '{action_id}' for integration {str(integration.id)} " \
                  f"is missing. Please fix the integration setup in the portal or provide a valid integration config in the request."
        logger.error(message)
        await publish_event(
            event=IntegrationActionFailed(
                payload=ActionExecutionFailed(
                    integration_id=integration_id,
                    action_id=action_id,
                    error=f"Configuration missing for action '{action_id}'",
                    config_data={"configurations": [i.dict() for i in integration.configurations]},
                )
            ),
            topic_name=settings.INTEGRATION_EVENTS_TOPIC,
        )
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content=jsonable_encoder({"detail": message}),
        )
    try:  # Execute the action
        handler, config_model = action_handlers[action_id]
        config_data = action_config.data if action_config else {}
        if config_overrides:
            config_data.update(config_overrides)
        parsed_config = config_model.parse_obj(config_data)
        start_time = time.monotonic()
        result = await asyncio.wait_for(
            handler(integration=integration, action_config=parsed_config),
            timeout=settings.MAX_ACTION_EXECUTION_TIME
        )
    except pydantic.ValidationError as e:
        message = f"Invalid configuration for action '{action_id}' and integration '{integration_id}': {e.errors()}"
        logger.error(message)
        await publish_event(
            event=IntegrationActionFailed(
                payload=ActionExecutionFailed(
                    integration_id=integration_id,
                    action_id=action_id,
                    config_data=config_data,
                    error=message
                )
            ),
            topic_name=settings.INTEGRATION_EVENTS_TOPIC,
        )
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content=jsonable_encoder({"detail": message}),
        )
    except KeyError as e:
        message = f"Action '{action_id}' is not supported for this integration"
        logger.exception(message)
        await publish_event(
            event=IntegrationActionFailed(
                payload=ActionExecutionFailed(
                    integration_id=integration_id,
                    action_id=action_id,
                    config_data=action_config,
                    error=message
                )
            ),
            topic_name=settings.INTEGRATION_EVENTS_TOPIC,
        )
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content=jsonable_encoder({"detail": message}),
        )
    except asyncio.TimeoutError:
        message = f"Action '{action_id}' timed out for integration {integration_id} after {settings.MAX_ACTION_EXECUTION_TIME} seconds. Please consider splitting the workload in sub-actions."
        logger.exception(message)
        await publish_event(
            event=IntegrationActionFailed(
                payload=ActionExecutionFailed(
                    integration_id=integration_id,
                    action_id=action_id,
                    config_data={"configurations": [c.dict() for c in integration.configurations]},
                    error=message
                )
            ),
            topic_name=settings.INTEGRATION_EVENTS_TOPIC,
        )
        return JSONResponse(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            content=jsonable_encoder({"detail": message}),
        )
    except Exception as e:
        message = f"Internal error executing action '{action_id}' for integration '{integration_id}': {type(e)}: {e}"
        logger.exception(message)
        await publish_event(
            event=IntegrationActionFailed(
                payload=ActionExecutionFailed(
                    integration_id=integration_id,
                    action_id=action_id,
                    config_data={"configurations": [c.dict() for c in integration.configurations]},
                    error=message
                )
            ),
            topic_name=settings.INTEGRATION_EVENTS_TOPIC,
        )
        return JSONResponse(
            status_code=e.response.status_code if hasattr(e, "response") else status.HTTP_500_INTERNAL_SERVER_ERROR,
            content=jsonable_encoder({"detail": message}),
        )
    else:
        end_time = time.monotonic()
        execution_time = end_time - start_time
        logger.debug(f"Action '{action_id}' executed successfully for integration {integration_id} in {execution_time:.2f} seconds.")
        return result




