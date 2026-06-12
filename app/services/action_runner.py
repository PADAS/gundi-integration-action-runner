import asyncio
import logging
import time
import traceback
from enum import Enum
from typing import Optional

import httpx
import pydantic
import stamina
from gundi_client_v2 import GundiClient

from app.actions import action_handlers, get_action_handler_by_data_type
from app import settings
from fastapi import status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from gundi_core.events import IntegrationActionFailed, ActionExecutionFailed, LogLevel

from app.actions.core import PullActionConfiguration
from .config_manager import IntegrationConfigurationManager
from .state import IntegrationStateManager
from .utils import find_config_for_action
from .activity_logger import publish_event, log_action_activity

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


async def _handle_error(
        exc: Exception, integration_id: str, action_id: Optional[str] = None,
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
        integration_id: str, action_id: Optional[str] = None, config_overrides: dict = None,
        data: dict = None, metadata: dict = None, triggered_by: Optional[str] = None
):
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

    logger.info(f"Executing action '{action_id}' for integration '{integration_id}'...")

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
    action_config = await config_manager.get_action_configuration(integration_id, action_id)
    if not action_config and not config_overrides:
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
        return await _handle_error(e, integration_id, action_id, config_data, status.HTTP_422_UNPROCESSABLE_ENTITY)

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
        if metadata:
            handler_kwargs["metadata"] = metadata
        result = await asyncio.wait_for(
            handler(**handler_kwargs),
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
