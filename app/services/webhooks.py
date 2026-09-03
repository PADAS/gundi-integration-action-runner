import asyncio
import datetime
import importlib
import ipaddress
import logging
from urllib.parse import urlparse
import httpx
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


# asyncio keeps only a weak reference to a running task, so a fire-and-forget
# task can be garbage-collected mid-flight and vanish without a trace. Hold a
# strong reference until it finishes.
_background_tasks: set[asyncio.Task] = set()


def _spawn_background_task(coro) -> asyncio.Task:
    task = asyncio.ensure_future(coro)
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)
    return task


# How long lifespan shutdown waits for in-flight diagnostic forwards before
# cancelling them. Must stay well inside the platform's termination grace
# period (Cloud Run and Kubernetes default to 10 s, then SIGKILL), or the
# aclose() the drain protects never runs at all.
_SHUTDOWN_DRAIN_TIMEOUT_SECONDS = 5.0
# Bound on the resolver call in _validate_diagnostic_url. httpx's timeout=10.0
# is per phase of its own request and does not cover getaddrinfo, which runs
# in the default executor with no deadline of its own.
_DNS_RESOLUTION_TIMEOUT_SECONDS = 5.0


async def close_diagnostic_client() -> None:
    global _diagnostic_client
    # Drain in-flight forwards first; closing the client out from under them
    # would fail every request still on the wire. Bounded: a hung resolver or
    # an unresponsive diagnostic endpoint must not hold shutdown open.
    pending = list(_background_tasks)
    if pending:
        _, still_pending = await asyncio.wait(pending, timeout=_SHUTDOWN_DRAIN_TIMEOUT_SECONDS)
        if still_pending:
            logger.warning(
                f"Cancelling {len(still_pending)} diagnostic forward(s) still in flight "
                f"{_SHUTDOWN_DRAIN_TIMEOUT_SECONDS:g}s into shutdown."
            )
            for task in still_pending:
                task.cancel()
            await asyncio.gather(*still_pending, return_exceptions=True)
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
    ipaddress.ip_network("::/128"),           # IPv6 unspecified
    ipaddress.ip_network("ff00::/8"),         # IPv6 multicast
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
        addr_infos = await asyncio.wait_for(
            loop.getaddrinfo(hostname, None), timeout=_DNS_RESOLUTION_TIMEOUT_SECONDS,
        )
    except asyncio.TimeoutError:
        raise ValueError(
            f"Timed out resolving diagnostic URL hostname '{hostname}' "
            f"after {_DNS_RESOLUTION_TIMEOUT_SECONDS:g}s."
        )
    except OSError as e:
        raise ValueError(f"Cannot resolve diagnostic URL hostname '{hostname}': {e}")
    for _, _, _, _, sockaddr in addr_infos:
        ip = ipaddress.ip_address(sockaddr[0])
        # An IPv4-mapped IPv6 address (::ffff:a.b.c.d) parses as IPv6 and would
        # sail past the IPv4 blocklist entries; check the embedded IPv4 instead.
        if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped:
            ip = ip.ipv4_mapped
        if any(ip in net for net in _BLOCKED_NETWORKS):
            raise ValueError(
                f"Diagnostic URL resolves to a private or reserved address ({ip}), "
                "which is blocked to prevent SSRF."
            )


def _redact_url(url: str) -> str:
    """Host only — diagnostic URLs can carry credentials or tokens in the
    userinfo, the query string, or the path, none of which may reach the logs.

    The path is deliberately dropped rather than kept: Slack, Discord and Teams
    incoming webhooks all put the shared secret *in the path*
    (https://hooks.slack.com/services/T.../B.../<secret>), so keeping it would
    leak the very credential this function exists to protect.
    """
    try:
        parsed = urlparse(url)
        host = parsed.hostname or "<no-host>"
        if parsed.port:
            host = f"{host}:{parsed.port}"
        return host
    except Exception:
        return "<unparseable url>"


async def forward_payload_to_diagnostic_url(
    destination_url: str,
    integration_id: str,
    json_content,
):
    try:
        await _validate_diagnostic_url(destination_url)
        metadata = {
            "integration_id": integration_id,
            "received_at": datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z"),
        }
        if isinstance(json_content, dict):
            body = {**json_content, "__gundi_diagnostic_metadata": metadata}
        else:
            body = {"payload": json_content, "__gundi_diagnostic_metadata": metadata}
        response = await _get_diagnostic_client().post(destination_url, json=body)
        response.raise_for_status()
        logger.debug(
            f"Diagnostic payload forwarded to '{_redact_url(destination_url)}' "
            f"for integration '{integration_id}'. Status: {response.status_code}"
        )
    except Exception as e:
        # Never str(e) for transport/status errors: httpx embeds the request URL
        # in its messages, path secret included, which would undo _redact_url.
        # ValueError is ours (the SSRF/allowlist checks) and names only the host.
        if isinstance(e, httpx.HTTPStatusError):
            detail = f"HTTPStatusError: HTTP {e.response.status_code}"
        elif isinstance(e, ValueError):
            detail = f"ValueError: {e}"
        else:
            detail = type(e).__name__
        logger.warning(
            f"Diagnostic forwarding to '{_redact_url(destination_url)}' failed for integration "
            f"'{integration_id}': {detail}"
        )


async def get_integration(request):
    integration = None
    consumer_username = request.headers.get("x-consumer-username")
    consumer_integration = consumer_username.split(":")[-1] if consumer_username and consumer_username != "anonymous" else None
    integration_id = consumer_integration or request.headers.get("x-gundi-integration-id") or request.query_params.get("integration_id")
    if integration_id:
        try:
            # No retry loop here: on a cache miss get_integration_details reloads
            # from the Gundi API under GUNDI_API_RETRY already, and the loop this
            # replaced nested a second policy on top of it (multiplying the wall
            # time) while iterating stamina synchronously inside a coroutine,
            # which sleeps the whole event loop between attempts.
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
            logger.warning(
                "No integration found for webhook request: "
                f"consumer_username: {request.headers.get('x-consumer-username')}, "
                f"integration_id header: {request.headers.get('x-gundi-integration-id')}, "
                f"integration_id param: {request.query_params.get('integration_id')}"
            )
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
        if diag_url := getattr(parsed_config, "diagnostic_destination_url", None):
            _spawn_background_task(
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

