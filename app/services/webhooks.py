import importlib
import logging
from fastapi import Request
from app.services.activity_logger import log_activity
from gundi_client_v2 import GundiClient
from app.webhooks.core import get_webhook_handler

_portal = GundiClient()
logger = logging.getLogger(__name__)


async def get_integration(request):
    integration = None
    consumer_username = request.headers.get("x-consumer-username")
    consumer_integration = consumer_username.split(":")[-1] if consumer_username and consumer_username != "anonymous" else None
    integration_id = consumer_integration or request.headers.get("x-gundi-integration-id") or request.query_params.get("integration_id")
    if integration_id:
        try:
            integration = await _portal.get_integration_details(integration_id=integration_id)
        except Exception as e:
            logger.warning(f"Error retrieving integration '{integration_id}' from the portal: {e}")
    return integration


async def process_webhook(request: Request):
    try:
        # Try to relate the request o an the integration
        integration = await get_integration(request=request)
        # Look for the handler function in webhooks/handlers.py
        webhook_handler, payload_model, config_model = get_webhook_handler()
        raw_content = await request.body()
        # Parse payload if a model was defined in webhooks/configurations.py
        parsed_payload = payload_model.parse_raw(raw_content) if payload_model else raw_content
        # Parse config if a model was defined in webhooks/configurations.py
        parsed_config = config_model.parse_obj(integration.configurations) if config_model else {}
        await webhook_handler(payload=parsed_payload, integration=integration, webhook_config=parsed_config)
    except (ImportError, AttributeError, NotImplementedError) as e:
        message = "Webhooks handler not found. Please implement a 'webhook_handler' function in app/webhooks/handlers.py"
        logger.exception(message)
        # await log_activity(
        #     level="error",
        #     title=message,
        # )
    except Exception as e:
        message = f"Error loading webhooks handler: {str(e)}"
        logger.exception(message)
        # await log_activity(
        #     level="error",
        #     title=message,
        # )
    return {}

