import asyncio
import logging
import time
import traceback
import uuid
from enum import Enum
from typing import Optional

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
from .errors import classify_error, format_classified_error, source_status_code, IntegrationError
from .gundi import EphemeralWriteBlocked

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


def _request_error_response(action_id: Optional[str], message: str,
                            status_code: int = status.HTTP_422_UNPROCESSABLE_ENTITY) -> JSONResponse:
    """The one error shape shared by router-level and runner-level rejections
    that must not publish an activity event: {"detail": {"action_id", "error"}}."""
    return JSONResponse(
        status_code=status_code,
        content=jsonable_encoder({"detail": {"action_id": action_id, "error": message}}),
    )


# Server-owned text per pydantic error type, the only validation messages the
# ephemeral path forwards. A dotted type does not prove pydantic authored the
# message: a connector's PydanticValueError subclass gets one too
# (value_error.<code>) and its msg_template can interpolate the submitted
# value. Everything outside this allowlist reads "invalid value".
_SAFE_VALIDATION_MESSAGES = {
    "value_error.missing": "field required",
    "value_error.extra": "extra fields not permitted",
}


def _ephemeral_error_text(exc: Exception, *, classified, expose_message: bool) -> str:
    """Portal-facing text for a failure on the ephemeral path.

    Redaction is by provenance, not by path. Runner-authored errors keep
    their message: the runner built it, so it cannot contain draft
    credentials. That covers expose_message=True (unknown action, whitelist
    rejection, ...) and EphemeralWriteBlocked, which is raised inside the
    handler frame but names only the blocked operation. A pydantic
    ValidationError exposes field locations plus server-owned text from
    _SAFE_VALIDATION_MESSAGES; any other message, connector-authored or
    not, is replaced, since validators run on draft credentials. Anything
    else raised by a handler is reduced to the curated classification
    title plus the HTTP status, never str(exc), which can embed our
    outgoing request (auth headers) or the source's response body.
    `classified` is the caller's classify_error verdict, or None when the
    failure is not the handler's (see _handle_error).
    """
    if isinstance(exc, pydantic.ValidationError):
        fields = "; ".join(
            f"{'.'.join(str(part) for part in err['loc'])}: "
            f"{_SAFE_VALIDATION_MESSAGES.get(err.get('type', ''), 'invalid value')}"
            for err in exc.errors()
        )
        return f"{type(exc).__name__}: {fields}" if fields else type(exc).__name__
    if expose_message or isinstance(exc, EphemeralWriteBlocked):
        message = exc.args[0] if exc.args else str(exc)
        return f"{type(exc).__name__}: {message}"
    if classified:
        text = classified.title
        if classified.status_code:
            text = f"{text} (HTTP {classified.status_code})"
        return text
    return type(exc).__name__


def _ephemeral_status_for(exc: Exception, fallback: int) -> int:
    """HTTP status for a handler failure on the ephemeral path.

    Forward the source system's verdict so cdip's upstream_status matches it
    and the portal can tell bad credentials from a broken source. The status
    is read by errors.source_status_code, the same reader the classifier
    uses, so the body text and the response status always agree; an
    IntegrationAuthError with no explicit code is still a 401. Only 4xx/5xx
    are forwarded: statuses below 400 are not failures the portal can
    classify (a redirect surfaced by raise_for_status with redirects off is
    the common one), and anything outside the HTTP range is a connector bug,
    not a status the runner should answer with. Everything else keeps the
    caller's `fallback` (500 for a handler exception, 504 for a timeout).
    """
    source_status = source_status_code(exc)
    if source_status is None and isinstance(exc, IntegrationError) and exc.error_type == "auth":
        return status.HTTP_401_UNAUTHORIZED
    if source_status is not None and 400 <= source_status <= 599:
        return source_status
    return fallback


async def _handle_error(
        exc: Exception, integration_id: Optional[str], action_id: Optional[str] = None,
        config_data=None, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        *, classify_heuristics: bool = False, expose_message: bool = False,
):
    """
    Log the error and return a JSON response with details. On non-ephemeral
    runs, also publishes an IntegrationActionFailed event for the activity
    feed; ephemeral runs skip the publish (no integration to log against).

    expose_message marks an exception the runner itself constructed (unknown
    action, whitelist rejection, ...). Only the ephemeral branch reads it:
    saved-integration runs always report the full message.
    """
    is_ephemeral = ephemeral_run.get()

    # Explicit IntegrationError subclasses classify everywhere — they're
    # unambiguous. Heuristic classification (status codes / connection
    # exception types) is scoped to action-handler execution failures only
    # (classify_heuristics=True), because the same signals mean something
    # different elsewhere — e.g. a 401 from the Gundi portal's own
    # get_integration_details call is a portal auth problem, not a
    # third-party provider one, and must not render as "Authentication
    # failed" (which would misdirect operators at the provider). The same
    # scoping applies on both branches below: a nested saved-integration
    # call under an ephemeral context reaches the ephemeral branch with
    # classify_heuristics=False and must not get the provider wording either.
    classified = classify_error(exc) if (classify_heuristics or isinstance(exc, IntegrationError)) else None

    if is_ephemeral:
        # Draft credentials must reach neither PubSub nor the application log,
        # which outlives and outreaches the request. Log the same redacted
        # text the caller gets plus the traceback frames (source locations,
        # no values); str(exc) and the traceback's final line stay out.
        # config_data is dropped here regardless of what the caller passed.
        safe_text = _ephemeral_error_text(exc, classified=classified, expose_message=expose_message)
        if classify_heuristics:
            # A handler failure: forward the source system's verdict instead
            # of the caller's generic status.
            status_code = _ephemeral_status_for(exc, fallback=status_code)
        type_name = type(exc).__name__
        # Runner-authored and validation texts already lead with the type name.
        logged = safe_text if safe_text.startswith(type_name) else f"{type_name}: {safe_text}"
        frames = "".join(traceback.format_tb(exc.__traceback__)) if exc.__traceback__ else ""
        logger.error(f"Error in ephemeral action '{action_id}': {logged}\n{frames}".rstrip())
        return _request_error_response(action_id, safe_text, status_code)

    log_message = f"Error in action '{action_id}' for integration '{integration_id}': {type(exc).__name__}: {exc}"
    logger.exception(log_message)

    # Classified errors (auth, connectivity, rate limit, bad response) get
    # short human-first text — the portal prepends "Error running action
    # '<id>': " and truncates, so the useful part must come first. Anything
    # unclassified keeps the verbose format. Full details always remain in
    # error_traceback and the request/response fields below.
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
        try:
            await log_action_activity(
                integration_id=integration_id,
                action_id=action_id,
                title=f"Skipping '{action_id}': configuration is missing or invalid.",
                level=LogLevel.WARNING,
                data={"validation_error": str(error)},
            )
        except Exception as log_error:
            # Best-effort, like the throttle above. If the event publisher is
            # unavailable, don't turn a benign skip into an unhandled error
            # (500 / PubSub redelivery) -- the warning is already in the logs.
            logger.warning(
                f"Could not publish the skip warning for '{action_id}' "
                f"(integration '{integration_id}'): {log_error}"
            )
    return {"skipped": True, "reason": "invalid_configuration"}


async def execute_action(
        integration_id: Optional[str], action_id: Optional[str] = None, config_overrides: dict = None,
        data: dict = None, metadata: dict = None, triggered_by: Optional[str] = None,
        integration_state: Optional[IntegrationState] = None,
):
    # The router already rejects "both provided"; no other caller passes
    # integration_state, so a saved integration_id wins here by construction.
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
        # Request-shape error, not an action failure: reachable from the PubSub
        # route, which forwards whatever the message carried. Don't publish a
        # phantom IntegrationActionFailed with integration_id=None, but do
        # leave a trace, since main.py acks the message regardless.
        logger.error(f"Cannot execute action '{action_id}': no integration_id or integration_state provided.")
        return _request_error_response(action_id, "Provide either integration_id or integration_state.")
    else:
        try:  # Get the integration details to pass it to the action handler
            # Actions never read the webhook config; skipping it keeps a warm
            # action run off the portal (see get_integration_details).
            integration = await config_manager.get_integration_details(
                integration_id, include_webhook_config=False,
            )
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
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, expose_message=True,
            )
    elif data and (data_type := data.get("event_type")):  # Push data actions
        try:  # Get the action handler by data type
            action_id, handler, config_model, DataModel = get_action_handler_by_data_type(type_name=data_type)
        except ValueError:
            return await _handle_error(
                ValueError(f"Data type '{data_type}' is not supported"),
                integration_id, action_id,
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, expose_message=True,
            )
    else:
        return await _handle_error(
            ValueError("No action handler found by action ID or data type"),
            integration_id, action_id,
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, expose_message=True,
        )

    # Only read-only actions run under an ephemeral context. Key on the
    # effective (OR-folded) contextvar, not this call's own flag: a reference
    # or auth handler that calls execute_action(integration_id=...) for a push
    # action gets a nested run whose is_ephemeral is False, and the whitelist
    # must still apply to it. This is the runner's own check; cdip can enforce
    # it too, since reference actions register with the "reference" type (see
    # self_registration).
    if ephemeral_run.get():
        is_ephemerally_safe = isinstance(config_model, type) and issubclass(
            config_model, (ReferenceActionConfiguration, AuthActionConfiguration),
        )
        if not is_ephemerally_safe:
            return await _handle_error(
                ValueError(
                    f"Action '{action_id}' cannot be executed ephemerally; "
                    "only reference and auth actions are supported."
                ),
                integration_id, action_id,
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, expose_message=True,
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

    # Reference actions are stateless: they never have a stored config row, and
    # a caller sending no config_overrides (e.g. ER's zero-param
    # list_event_types) is a legitimate, complete request — not a 404. The
    # ephemeral path has the same property for every action: the wizard only
    # forwards `configurations` for the sections the user edited (auth), never
    # a row for the action being executed. Either way `config_model
    # .parse_obj({})` below decides whether params are actually required and
    # raises ValidationError → 422 if they are.
    is_reference_action = isinstance(config_model, type) and issubclass(
        config_model, ReferenceActionConfiguration
    )
    skip_missing_config = is_ephemeral or is_reference_action

    # Get the configuration needed to execute the action
    if is_ephemeral:
        action_config = find_config_for_action(integration.configurations, action_id)
    elif is_reference_action:
        # Stateless by contract, so there is no row to find. Looking one up
        # anyway would miss redis every time, and get_action_configuration
        # reloads the integration from the portal on a miss: a portal call on
        # every dropdown open.
        action_config = None
    else:
        action_config = await config_manager.get_action_configuration(integration_id, action_id)
    if not skip_missing_config and not action_config and not config_overrides:
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
            # Reached only for non-ephemeral, non-reference runs (see the
            # guard above). Those keep the pre-existing activity-log contract
            # of attaching the saved configurations to the failure event.
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
        # _handle_error drops config_data on the ephemeral path itself.
        return await _handle_error(
            e, integration_id, action_id,
            config_data=config_data,
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

    def handler_error_config_data():
        # Built only on failure: serializing every configuration row on each
        # successful run would be wasted work on the hot path. Reference
        # actions are portal-invoked at interactive-fetch frequency (every
        # dropdown open), so a handler failure is routine rather than
        # exceptional, and like every ephemeral error theirs must not carry
        # the integration's configurations — which include raw auth secrets —
        # into the published IntegrationActionFailed event or the JSON error
        # response. (_handle_error drops config_data on the ephemeral path.)
        if is_reference_action:
            return None
        return {"configurations": [c.dict() for c in integration.configurations]}

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
            config_data=handler_error_config_data(),
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            classify_heuristics=True,
        )
    except Exception as e:
        # Saved-integration runs keep the historical 500; the ephemeral path
        # forwards the source system's verdict instead (_handle_error applies
        # _ephemeral_status_for to handler failures).
        return await _handle_error(
            e, integration_id, action_id,
            config_data=handler_error_config_data(),
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            classify_heuristics=True,
        )

    # Success. Log the execution time and return the result
    end_time = time.monotonic()
    execution_time = end_time - start_time
    logger.debug(
        f"Action '{action_id}' executed successfully for integration {integration_id or '(ephemeral)'} in {execution_time:.2f} seconds."
    )
    return result
