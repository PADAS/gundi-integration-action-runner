import datetime
import logging
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
from .utils import find_config_for_action
from .activity_logger import publish_event


_portal = GundiClient()
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
    try:  # Get the integration config from the portal
        async for attempt in stamina.retry_context(on=httpx.HTTPError, wait_initial=datetime.timedelta(seconds=1), attempts=3):
            with attempt:
                integration = await _portal.get_integration_details(integration_id=integration_id)
    except Exception as e:
        message = f"Error retrieving configuration for integration '{integration_id}': {e}"
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
    action_config = find_config_for_action(
        configurations=integration.configurations,
        action_id=action_id
    )
    if not action_config:
        message = f"Configuration for action '{action_id}' for integration {str(integration.id)} " \
                  f"is missing. Please fix the integration setup in the portal."
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
        config_data = action_config.data
        if config_overrides:
            config_data.update(config_overrides)
        parsed_config = config_model.parse_obj(config_data)
        result = await handler(integration=integration, action_config=parsed_config)
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
    except Exception as e:
        message = f"Internal error executing action '{action_id}': {e}"
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
        return result
