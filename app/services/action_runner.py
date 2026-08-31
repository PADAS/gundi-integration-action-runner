import asyncio
import logging
import time
import traceback
import uuid
from enum import Enum
from typing import Optional

import httpx
import pydantic
import stamina
from gundi_client_v2 import GundiClient
from gundi_core.schemas.v2 import Integration

from app.actions import action_handlers, get_action_handler_by_data_type
from app import settings
from fastapi import status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from gundi_core.events import IntegrationActionFailed, ActionExecutionFailed, LogLevel

from app.actions.core import AuthActionConfiguration, PullActionConfiguration, ReferenceActionConfiguration
from app.api_schemas import IntegrationState
from .config_manager import IntegrationConfigurationManager
from .state import IntegrationStateManager
from .utils import find_config_for_action
from .activity_logger import publish_event, log_action_activity, ephemeral_run
from .errors import classify_error, format_classified_error, IntegrationError

_portal = GundiClient()
config_manager = IntegrationConfigurationManager()
state_manager = IntegrationStateManager()
logger = logging.getLogger(__name__)


# How often (seconds) to publish a portal activity-log WARNING for a pull
# action that keeps skipping on an invalid config. Pull actions are scheduled
# type-wide and fire on every tick; without throttling a persistently
# misconfigured source would emit a WARNING every run. The skip itself is
# always recorded in the local application log — this only rate-limits the
# portal-facing activity-feed entry.
SKIP_WARNING_THROTTLE_SECONDS = 3600


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


def _build_synthetic_integration(state: IntegrationState) -> Integration:
    # Unique per run so concurrent drafts by different users don't collide on
    # any state_manager keys downstream handlers might set under
    # `integration.id` (`integration_state.{integration_id}.{action_id}.…`).
    run_id = str(uuid.uuid4())
    return Integration.parse_obj({
        "id": run_id,
        "name": "(ephemeral)",
        "base_url": state.base_url or "",
        "enabled": True,
        "type": {
            "id": run_id,
            "name": state.type_value,
            "value": state.type_value,
            "description": "",
            "actions": [
                {
                    "id": run_id,
                    "type": "generic",
                    "name": cfg.action_value,
                    "value": cfg.action_value,
                    "schema": {},
                }
                for cfg in state.configurations
            ],
        },
        "owner": {
            "id": run_id,
            "name": "(ephemeral)",
            "description": "",
        },
        "configurations": [
            {
                "id": run_id,
                "integration": run_id,
                "action": {
                    "id": run_id,
                    "type": "generic",
                    "name": cfg.action_value,
                    "value": cfg.action_value,
                },
                "data": cfg.data,
            }
            for cfg in state.configurations
        ],
        "additional": {},
        "default_route": None,
        "status": "healthy",
        "status_details": "",
    })


async def _handle_error(
        exc: Exception, integration_id: Optional[str], action_id: Optional[str] = None,
        config_data=None, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        *, classify_heuristics: bool = False,
):
    """
    Log the error and return a JSON response with details. On non-ephemeral
    runs, also publishes an IntegrationActionFailed event for the activity
    feed; ephemeral runs skip the publish (no integration to log against).
    """
    is_ephemeral = ephemeral_run.get()

    log_message = f"Error in action '{action_id}' for integration '{integration_id}': {type(exc).__name__}: {exc}"
    logger.exception(log_message)

    if is_ephemeral:
        # Third-party exceptions may embed our outgoing request (with auth
        # headers) or the source's response body. Return only the exception
        # class name to the caller — full details stay in the server log
        # above — and skip the activity-log publish entirely.
        return JSONResponse(
            status_code=status_code,
            content=jsonable_encoder({"detail": {
                "action_id": action_id,
                "error": type(exc).__name__,
            }}),
        )

    # Classified errors (auth, connectivity, rate limit, bad response) get
    # short human-first text — the portal prepends "Error running action
    # '<id>': " and truncates, so the useful part must come first. Anything
    # unclassified keeps the verbose format. Full details always remain in
    # error_traceback and the request/response fields below.
    #
    # Explicit IntegrationError subclasses classify everywhere — they're
    # unambiguous. Heuristic classification (status codes / connection
    # exception types) is scoped to action-handler execution failures only
    # (classify_heuristics=True), because the same signals mean something
    # different elsewhere — e.g. a 401 from the Gundi portal's own
    # get_integration_details call is a portal auth problem, not a
    # third-party provider one, and must not render as "Authentication
    # failed" (which would misdirect operators at the provider).
    classified = classify_error(exc) if (classify_heuristics or isinstance(exc, IntegrationError)) else None
    message = format_classified_error(classified) if classified else log_message

    error_details = {
        "integration_id": integration_id,
        "action_id": action_id,
        "config_data": config_data or {},
        "error": message,
        # Machine-readable category. Only reaches the JSON response below;
        # ActionExecutionFailed is a gundi-core model that drops unknown fields.
        "error_type": classified.error_type if classified else None,
        "error_traceback": traceback.format_exc()
    }

    # Extract additional request/response details if available.
    # httpx exceptions expose .request as a property that raises RuntimeError
    # when the error was constructed without one — treat that as "no request".
    try:
        request = getattr(exc, "request", None)
    except RuntimeError:
        request = None
    if request is not None:
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

    await publish_event(
        event=IntegrationActionFailed(
            payload=ActionExecutionFailed(**error_details)
        ),
        topic_name=settings.INTEGRATION_EVENTS_TOPIC,
    )

    return JSONResponse(
        status_code=status_code,
        content=jsonable_encoder({"detail": error_details}),
    )


def _skip_quietly(integration_id, action_id, *, reason, message, log_level=logging.INFO):
    """Record an expected pull-action skip in the local log only.

    Destination-only integrations get pull actions scheduled type-wide but
    have no usable config, and operators may deliberately pause a pull. These
    are expected, steady-state no-ops, so we keep them out of the portal
    activity feed entirely (no `IntegrationActionFailed`, no custom log) to
    avoid per-tick noise — the local application log is enough for debugging.
    """
    logger.log(log_level, f"{message} (integration '{integration_id}')")
    return {"skipped": True, "reason": reason}


async def _skip_invalid_config(integration_id, action_id, *, error):
    """Record a skip caused by a missing/invalid pull config.

    Unlike the expected skips, an invalid (rather than absent) config usually
    means a real source with a misconfiguration, so it IS worth surfacing in
    the portal activity feed — but only at WARNING and throttled to at most
    once per `SKIP_WARNING_THROTTLE_SECONDS`, so a persistently broken source
    doesn't emit a WARNING on every scheduled tick. The skip is always written
    to the local application log regardless.
    """
    logger.warning(
        f"Skipping '{action_id}': configuration is missing or invalid "
        f"(integration '{integration_id}'): {error}"
    )
    try:
        first_in_window = await state_manager.set_if_absent(
            integration_id=integration_id,
            action_id=action_id,
            source_id="skip-invalid-config-warning",
            ttl_seconds=SKIP_WARNING_THROTTLE_SECONDS,
        )
    except Exception as throttle_error:
        # The throttle is best-effort noise control. If the state store is
        # unavailable, don't let it crash the skip — that would turn a benign
        # no-op into an unhandled error (500 / PubSub redelivery). Fail open:
        # surface the misconfiguration this time rather than hiding it.
        logger.warning(
            f"Skip-warning throttle unavailable for '{action_id}' "
            f"(integration '{integration_id}'): {throttle_error}. Publishing the warning."
        )
        first_in_window = True
    if first_in_window:
        await log_action_activity(
            integration_id=integration_id,
            action_id=action_id,
            title=f"Skipping '{action_id}': configuration is missing or invalid.",
            level=LogLevel.WARNING,
            data={"validation_error": str(error)},
        )
    return {"skipped": True, "reason": "invalid_configuration"}


async def execute_action(
        integration_id: Optional[str], action_id: Optional[str] = None, config_overrides: dict = None,
        data: dict = None, metadata: dict = None, triggered_by: Optional[str] = None,
        integration_state: Optional[IntegrationState] = None,
):
    if integration_id is not None and integration_state is not None:
        # Same reasoning as the "neither provided" branch below — request-shape
        # error, so return a direct 422 rather than publishing an
        # IntegrationActionFailed event against a real integration_id for what
        # is really a malformed caller payload.
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content=jsonable_encoder({"detail": {
                "action_id": action_id,
                "error": "Provide either integration_id or integration_state, not both",
            }}),
        )
    is_ephemeral = integration_id is None and integration_state is not None
    # OR-fold the contextvar so a nested saved-integration call inside an
    # ephemeral outer run doesn't re-enable publishing (e.g. a reference
    # handler that triggers a helper action). The finally block still
    # restores whatever value was here on entry.
    ephemeral_token = ephemeral_run.set(is_ephemeral or ephemeral_run.get())
    try:
        return await _execute_action_impl(
            integration_id=integration_id,
            action_id=action_id,
            config_overrides=config_overrides,
            data=data,
            metadata=metadata,
            triggered_by=triggered_by,
            integration_state=integration_state,
            is_ephemeral=is_ephemeral,
        )
    finally:
        ephemeral_run.reset(ephemeral_token)


async def _execute_action_impl(
        integration_id: Optional[str], action_id: Optional[str], config_overrides: Optional[dict],
        data: Optional[dict], metadata: Optional[dict], triggered_by: Optional[str],
        integration_state: Optional[IntegrationState], is_ephemeral: bool,
):
    if is_ephemeral:
        try:
            integration = _build_synthetic_integration(integration_state)
        except Exception as e:
            return await _handle_error(
                e, integration_id=None, action_id=action_id,
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            )
    elif integration_id is None:
        # Request-shape error, not an action failure. Return a direct 422
        # instead of routing through _handle_error so we don't publish a
        # phantom IntegrationActionFailed event with integration_id=None
        # to the activity feed for what is really a malformed caller payload.
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content=jsonable_encoder({"detail": {
                "action_id": action_id,
                "error": "Either integration_id or integration_state must be provided",
            }}),
        )
    else:
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

    # Only read-only actions allowed ephemerally. cdip enforces independently.
    if is_ephemeral:
        is_ephemerally_safe = isinstance(config_model, type) and issubclass(
            config_model, (ReferenceActionConfiguration, AuthActionConfiguration),
        )
        if not is_ephemerally_safe:
            return await _handle_error(
                ValueError(
                    f"Action '{action_id}' cannot be executed ephemerally; "
                    "only reference and auth actions are supported."
                ),
                integration_id=None, action_id=action_id,
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            )

    logger.info(f"Executing action '{action_id}' for integration '{integration_id or '(ephemeral)'}'...")

    # Pull actions are scheduled type-wide, so the portal fires them for every
    # integration of this type — including destination-only ones that never get
    # a pull config. For an *automated* run, "no usable config" (or a paused
    # toggle) means "nothing to pull" — a clean no-op rather than a failure. A
    # *manual* run keeps the strict 404/422 behavior so misconfigurations
    # surface immediately, and ignores the pause toggle.
    is_pull_action = isinstance(config_model, type) and issubclass(config_model, PullActionConfiguration)
    # Normalize the marker so casing/whitespace from the caller (e.g. the
    # portal) doesn't silently fall through to the automated default.
    is_manual = (triggered_by or "").strip().lower() == ActionTrigger.MANUAL.value
    skippable_pull = is_pull_action and not is_manual

    # Get the configuration needed to execute the action
    if is_ephemeral:
        action_config = find_config_for_action(integration.configurations, action_id)
    else:
        action_config = await config_manager.get_action_configuration(integration_id, action_id)
    # Ephemeral runs skip the missing-config 404 entirely: reference actions
    # are frequently parameter-less (ER's list_event_types /
    # list_event_categories both declare `params: {}`), and the wizard only
    # forwards `configurations` for the sections the user edited (auth) —
    # never a config row for the reference action itself. `config_model
    # .parse_obj({})` below already decides whether params are required and
    # raises ValidationError → 422 if they are, so the 404 branch was pure
    # feature-blocking noise on this path.
    if not is_ephemeral and not action_config and not config_overrides:
        if skippable_pull:
            return _skip_quietly(
                integration_id, action_id,
                reason="no_configuration",
                message=f"Skipping '{action_id}': integration is not configured for this action.",
                log_level=logging.DEBUG,
            )
        message = f"Configuration for action '{action_id}' for integration {str(integration.id)} is missing."
        logger.error(message)
        return await _handle_error(
            ValueError(message), integration_id, action_id,
            # Reached only for non-ephemeral runs (see the guard above), so
            # dumping the saved integration's configurations here is safe.
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
        # safely pull. Skip rather than raise — surfaced at WARNING in the
        # activity feed (throttled) so a genuinely misconfigured source stays
        # noticeable without spamming a warning on every tick.
        if skippable_pull:
            return await _skip_invalid_config(integration_id, action_id, error=e)
        return await _handle_error(
            e, integration_id, action_id,
            config_data=None if is_ephemeral else config_data,
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        )

    # Respect the operator's explicit pause toggle — only for scheduled runs.
    if skippable_pull and not getattr(parsed_config, "run_on_schedule", True):
        return _skip_quietly(
            integration_id, action_id,
            reason="run_on_schedule_disabled",
            message=f"Skipping '{action_id}': 'run_on_schedule' is turned off for this integration.",
            log_level=logging.INFO,
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
        if metadata is not None:
            handler_kwargs["metadata"] = metadata
        result = await asyncio.wait_for(
            handler(**handler_kwargs),
            timeout=settings.MAX_ACTION_EXECUTION_TIME
        )
    except asyncio.TimeoutError:
        return await _handle_error(
            asyncio.TimeoutError(f"Action '{action_id}' timed out"),
            integration_id, action_id,
            config_data=None if is_ephemeral else {"configurations": [c.dict() for c in integration.configurations]},
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            classify_heuristics=True,
        )
    except Exception as e:
        # On the ephemeral path only: forward the source system's HTTP status
        # so cdip's upstream_status reflects what the source returned. Two
        # shapes surface it: httpx.HTTPStatusError.response.status_code (the
        # common raise_for_status path) and IntegrationError.status_code (a
        # handler wrapping the source error semantically). Other exception
        # types with a stray .response.status_code attribute do NOT propagate.
        if is_ephemeral:
            if isinstance(e, httpx.HTTPStatusError) and e.response is not None:
                ephemeral_status = e.response.status_code
            elif isinstance(e, IntegrationError) and getattr(e, "status_code", None):
                ephemeral_status = e.status_code
            else:
                ephemeral_status = status.HTTP_500_INTERNAL_SERVER_ERROR
        else:
            ephemeral_status = status.HTTP_500_INTERNAL_SERVER_ERROR
        return await _handle_error(
            e, integration_id, action_id,
            config_data=None if is_ephemeral else {"configurations": [c.dict() for c in integration.configurations]},
            status_code=ephemeral_status,
            classify_heuristics=True,
        )

    # Success. Log the execution time and return the result
    end_time = time.monotonic()
    execution_time = end_time - start_time
    logger.debug(
        f"Action '{action_id}' executed successfully for integration {integration_id or '(ephemeral)'} in {execution_time:.2f} seconds."
    )
    return result
