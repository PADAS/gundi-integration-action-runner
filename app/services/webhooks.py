import importlib
import logging
import traceback
import httpx
import stamina
from fastapi import Request
from app import settings
from app.services.activity_logger import log_activity, publish_event
from gundi_client_v2 import GundiClient
from gundi_core.events import IntegrationWebhookFailed, WebhookExecutionFailed
from app.services.utils import DyntamicFactory
from app.webhooks.core import get_webhook_handler, DynamicSchemaConfig, HexStringConfig, GenericJsonPayload
from app.services.config_manager import IntegrationConfigurationManager

config_manager = IntegrationConfigurationManager()
logger = logging.getLogger(__name__)


async def get_integration(request):
    integration = None
    consumer_username = request.headers.get("x-consumer-username")
    consumer_integration = consumer_username.split(":")[-1] if consumer_username and consumer_username != "anonymous" else None
    integration_id = consumer_integration or request.headers.get("x-gundi-integration-id") or request.query_params.get("integration_id")
    if integration_id:
        try:
            # Retry on httpx.HTTPError (StatusError, Timeout, ConnectError, etc.)
            for attempt in stamina.retry_context(on=httpx.HTTPError, wait_initial=10.0, wait_jitter=10.0, wait_max=300.0):
                with attempt:
                    # Cache the integration details and webhook config for 60 seconds. 
                    # ToDo: Refactor to event-driven webhook config updates (as in actions)
                    integration = await config_manager.get_integration_details(integration_id, ttl=60)
        except Exception as e:
            error_message = f"Error retrieving integration '{integration_id}': {type(e).__name__}: {e}"
            logger.exception(error_message)
            await publish_event(
                event=IntegrationWebhookFailed(
                    payload=WebhookExecutionFailed(
                        integration_id=str(integration_id),
                        webhook_id=None,
                        config_data={},
                        error=error_message
                    )
                ),
                topic_name=settings.INTEGRATION_EVENTS_TOPIC,
            )
    return integration


async def process_webhook(request: Request):
    try:
        # Try to relate the request to an integration
        integration = await get_integration(request=request)
        if not integration:
            logger.warning(f"No integration found for webhook request: headers: {request.headers}, query_params: {request.query_params}")
            return {}
        # Look for the handler function in webhooks/handlers.py
        webhook_handler, payload_model, config_model = get_webhook_handler()
        json_content = await request.json()
        # Parse config if a model was defined in webhooks/configurations.py
        webhook_config_data = integration.webhook_configuration.data if integration and integration.webhook_configuration else {}
        parsed_config = config_model.parse_obj(webhook_config_data) if config_model else {}
        if parsed_config and issubclass(config_model, HexStringConfig):
            json_content["hex_data_field"] = json_content.get("hex_data_field", parsed_config.hex_data_field)
            json_content["hex_format"] = json_content.get("hex_format", parsed_config.hex_format)
        # Parse payload if a model was defined in webhooks/configurations.py
        if payload_model:
            try:
                if issubclass(payload_model, GenericJsonPayload) and issubclass(config_model, DynamicSchemaConfig):
                    # Build the model from a json schema
                    model_factory = DyntamicFactory(
                        json_schema=parsed_config.json_schema,
                        base_model=payload_model,
                        ref_template="definitions"
                    )
                    dynamic_payload_model = model_factory.make()
                    if isinstance(json_content, list):
                        parsed_payload = [dynamic_payload_model.parse_obj(d) for d in json_content]
                    else:
                        parsed_payload = dynamic_payload_model.parse_obj(json_content)
                else:
                    parsed_payload = payload_model.parse_obj(json_content)
            except Exception as e:
                message = f"Error parsing payload: {type(e).__name__}: {str(e)}. Please review configurations."
                logger.exception(message)
                await publish_event(
                    event=IntegrationWebhookFailed(
                        payload=WebhookExecutionFailed(
                            integration_id=str(integration.id),
                            webhook_id=str(integration.type.webhook.value),
                            config_data=webhook_config_data,
                            error=message
                        )
                    ),
                    topic_name=settings.INTEGRATION_EVENTS_TOPIC,
                )
                return {}
        else:  # Pass the raw payload
            parsed_payload = json_content
        await webhook_handler(payload=parsed_payload, integration=integration, webhook_config=parsed_config)
    except (ImportError, AttributeError, NotImplementedError) as e:
        message = "Webhooks handler not found. Please implement a 'webhook_handler' function in app/webhooks/handlers.py"
        logger.exception(message)
        await publish_event(
            event=IntegrationWebhookFailed(
                payload=WebhookExecutionFailed(
                    integration_id=str(integration.id),
                    webhook_id=str(integration.type.webhook.value),
                    error=message
                )
            ),
            topic_name=settings.INTEGRATION_EVENTS_TOPIC,
        )
    except Exception as e:
        message = f"Error processing webhook: {type(e).__name__}: {str(e)}"
        logger.exception(message)
        await publish_event(
            event=IntegrationWebhookFailed(
                payload=WebhookExecutionFailed(
                    integration_id=str(integration.id) if integration else None,
                    webhook_id=str(integration.type.webhook.value) if integration and integration.type.webhook else None,
                    config_data=webhook_config_data,
                    error=message  # ToDo: Support storing the error traceback and other details as in action errors
                )
            ),
            topic_name=settings.INTEGRATION_EVENTS_TOPIC,
        )
    return {}

