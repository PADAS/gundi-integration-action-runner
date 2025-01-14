import json
import datetime
import logging

import stamina
import httpx

from app.actions import (
    action_handlers,
    AuthActionConfiguration,
    PullActionConfiguration,
    PushActionConfiguration,
    ExecutableActionMixin,
    InternalActionConfiguration,
)
from app.settings import INTEGRATION_TYPE_SLUG, INTEGRATION_SERVICE_URL
from .core import ActionTypeEnum
from app.webhooks.core import get_webhook_handler, GenericJsonTransformConfig

logger = logging.getLogger(__name__)


async def register_integration_in_gundi(gundi_client, type_slug=None, service_url=None, action_schedules=None):
    # Prepare the integration name and value
    integration_type_slug = type_slug or INTEGRATION_TYPE_SLUG
    if not integration_type_slug:
        raise ValueError(
            "Please define a slug id for this integration type, either passing it in the type_slug argument or setting it in the INTEGRATION_TYPE_SLUG setting."
        )
    integration_type_slug = integration_type_slug.strip().lower()
    integration_type_name = integration_type_slug.replace("_", " ").title()
    logger.info(f"Registering integration type '{integration_type_slug}'...")
    data = {
        "name": integration_type_name,
        "value": integration_type_slug,
        "description": f"Default type for integrations with {integration_type_name}",
    }
    if integration_service_url := service_url or INTEGRATION_SERVICE_URL:
        logger.info(
            f"Registering '{integration_type_slug}' with service_url: '{integration_service_url}'"
        )
        data["service_url"] = integration_service_url

    # Prepare the actions and schemas
    actions = []
    for action_id, handler in action_handlers.items():
        func, config_model = handler
        if issubclass(config_model, InternalActionConfiguration):
            logger.info(f"Skipping internal action '{action_id}'.")
            continue  # Internal actions are not registered in Gundi
        action_name = action_id.replace("_", " ").title()
        action_schema = json.loads(config_model.schema_json())
        action_ui_schema = config_model.ui_schema()
        if issubclass(config_model, AuthActionConfiguration):
            action_type = ActionTypeEnum.AUTHENTICATION.value
        elif issubclass(config_model, PullActionConfiguration):
            action_type = ActionTypeEnum.PULL_DATA.value
        elif issubclass(config_model, PushActionConfiguration):
            action_type = ActionTypeEnum.PUSH_DATA.value
        else:
            action_type = ActionTypeEnum.GENERIC.value

        if issubclass(config_model, ExecutableActionMixin):
            action_schema["is_executable"] = True

        action = {
            "type": action_type,
            "name": action_name,
            "value": action_id,
            "description": f"{integration_type_name} {action_name} action",
            "schema": action_schema,
            "ui_schema": action_ui_schema,
        }

        if issubclass(config_model, PullActionConfiguration):
            action["is_periodic_action"] = True
            # Schedules can be specified by argument or using a decorator
            if action_schedules and action_id in action_schedules:
                action["crontab_schedule"] = action_schedules[action_id].dict()
            elif hasattr(func, "crontab_schedule"):
                crontab_schedule = getattr(func, "crontab_schedule")
                action["crontab_schedule"] = crontab_schedule.dict()
        else:
            action["is_periodic_action"] = False

        actions.append(action)

    data["actions"] = actions

    try:  # Register webhook config if available
        webhook_handler, payload_model, config_model = get_webhook_handler()
    except (ImportError, AttributeError, NotImplementedError) as e:
        logger.info(f"Webhook handler not found. Skipping webhook registration.")
    except Exception as e:
        logger.warning(
            f"Error getting webhook handler: {e}. Skipping webhook registration."
        )
    else:
        data["webhook"] = {
            "name": f"{integration_type_name} Webhook",
            "value": f"{integration_type_slug}_webhook",
            "description": f"Webhook Integration with {integration_type_name}",
            "schema": json.loads(config_model.schema_json()),
            "ui_schema": config_model.ui_schema(),
        }

    logger.info(f"Registering '{integration_type_slug}' with actions: '{actions}'")
    # Register the integration type and actions in Gundi
    async for attempt in stamina.retry_context(
        on=httpx.HTTPError, wait_initial=datetime.timedelta(seconds=1), attempts=3
    ):
        with attempt:
            response = await gundi_client.register_integration_type(data)
    logger.info(f"Registering integration type '{integration_type_slug}'...DONE")
    return response
