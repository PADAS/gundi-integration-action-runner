import asyncio
import datetime
import importlib
import ipaddress
import logging
import traceback
from urllib.parse import urlparse
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
_diagnostic_client: httpx.AsyncClient | None = None


def _get_diagnostic_client() -> httpx.AsyncClient:
    global _diagnostic_client
    if _diagnostic_client is None:
        _diagnostic_client = httpx.AsyncClient(timeout=10.0)
    return _diagnostic_client


async def close_diagnostic_client() -> None:
    global _diagnostic_client
    if _diagnostic_client is not None:
        await _diagnostic_client.aclose()
        _diagnostic_client = None

_BLOCKED_NETWORKS = [
    ipaddress.ip_network("127.0.0.0/8"),    # loopback
    ipaddress.ip_network("::1/128"),          # IPv6 loopback
    ipaddress.ip_network("10.0.0.0/8"),       # RFC 1918 private
    ipaddress.ip_network("172.16.0.0/12"),    # RFC 1918 private
    ipaddress.ip_network("192.168.0.0/16"),   # RFC 1918 private
    ipaddress.ip_network("169.254.0.0/16"),   # link-local / cloud metadata (AWS, GCP)
    ipaddress.ip_network("fe80::/10"),        # IPv6 link-local
    ipaddress.ip_network("fc00::/7"),         # IPv6 unique local
    ipaddress.ip_network("0.0.0.0/8"),        # unspecified
    ipaddress.ip_network("100.64.0.0/10"),    # carrier-grade NAT
    ipaddress.ip_network("224.0.0.0/4"),      # multicast
    ipaddress.ip_network("240.0.0.0/4"),      # reserved
]


async def _validate_diagnostic_url(url: str) -> None:
    """Raise ValueError if url fails SSRF safety checks.

    Note: this validation is a best-effort defence. Because DNS is re-resolved
    by httpx at request time, a DNS-rebinding attack could cause the actual
    connection to reach a private address even after this check passes (TOCTOU).
    Operators should also restrict outbound network access at the infrastructure
    level for a complete mitigation.
    """
    parsed = urlparse(url)
    if parsed.scheme != "https":
        raise ValueError(
            f"Diagnostic URL scheme '{parsed.scheme}' is not allowed; only 'https' is permitted."
        )
    hostname = (parsed.hostname or "").rstrip(".").lower()
    if not hostname:
        raise ValueError("Diagnostic URL has no hostname.")
    allowlist = settings.DIAGNOSTIC_URL_ALLOWLIST
    if allowlist and hostname not in [h.rstrip(".").lower() for h in allowlist]:
        raise ValueError(
            f"Diagnostic URL hostname '{hostname}' is not in the configured allowlist."
        )
    loop = asyncio.get_running_loop()
    try:
        addr_infos = await loop.getaddrinfo(hostname, None)
    except OSError as e:
        raise ValueError(f"Cannot resolve diagnostic URL hostname '{hostname}': {e}")
    for _, _, _, _, sockaddr in addr_infos:
        ip = ipaddress.ip_address(sockaddr[0])
        if any(ip in net for net in _BLOCKED_NETWORKS):
            raise ValueError(
                f"Diagnostic URL resolves to a private or reserved address ({ip}), "
                "which is blocked to prevent SSRF."
            )


async def forward_payload_to_diagnostic_url(
    destination_url: str,
    integration_id: str,
    json_content,
):
    try:
        await _validate_diagnostic_url(destination_url)
        metadata = {
            "integration_id": integration_id,
            "received_at": datetime.datetime.utcnow().isoformat() + "Z",
        }
        if isinstance(json_content, dict):
            body = {**json_content, "__gundi_diagnostic_metadata": metadata}
        else:
            body = {"payload": json_content, "__gundi_diagnostic_metadata": metadata}
        response = await _get_diagnostic_client().post(destination_url, json=body)
        response.raise_for_status()
        logger.debug(
            f"Diagnostic payload forwarded to '{destination_url}' "
            f"for integration '{integration_id}'. Status: {response.status_code}"
        )
    except Exception as e:
        logger.warning(
            f"Diagnostic forwarding to '{destination_url}' failed for integration "
            f"'{integration_id}': {type(e).__name__}: {e}"
        )


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
        # Forward raw payload to diagnostic URL before any transformation or validation
        diag_url = getattr(parsed_config, "diagnostic_destination_url", None)
        if diag_url:
            asyncio.ensure_future(
                forward_payload_to_diagnostic_url(
                    destination_url=diag_url,
                    integration_id=str(integration.id),
                    json_content=json_content,
                )
            )
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

