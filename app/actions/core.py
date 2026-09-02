import importlib
import inspect
import logging
from typing import Optional

from pydantic import BaseModel, Field
from app.services.utils import UISchemaModelMixin

logger = logging.getLogger(__name__)


class ActionConfiguration(UISchemaModelMixin, BaseModel):
    pass


class InternalActionConfiguration(BaseModel):
    pass


class PullActionConfiguration(ActionConfiguration):
    # Pull actions are scheduled at the integration-type level, so the portal
    # fires them for every integration of this type. This toggle lets an
    # operator pause scheduled execution for a given integration without
    # deleting its configuration — useful when the integration is used only as
    # a destination and the pull is not intended to run. The action_runner also
    # treats a missing or invalid pull config as a clean no-op rather than an
    # error, so destination-only integrations stay quiet by default.
    run_on_schedule: bool = Field(
        True,
        title="Run On Schedule",
        description=(
            "When enabled, this action runs automatically on its configured schedule. "
            "Turn it off to pause scheduled execution for this integration without deleting "
            "the configuration."
        ),
    )


class ExecutableActionMixin:
    pass


def action_title(title: str):
    """Set the display name used when registering the action in Gundi,
    instead of the default derived from the handler function name."""
    def decorator(func):
        setattr(func, "action_title", title)
        return func
    return decorator


class PushActionConfiguration(ActionConfiguration):
    pass


class AuthActionConfiguration(ActionConfiguration):
    pass


class GenericActionConfiguration(ActionConfiguration):
    pass


class ReferenceActionConfiguration(ActionConfiguration):
    """Marker base for reference-data actions: the config model IS the query.

    Reference actions are stateless — they read the integration's auth config
    but store no configuration of their own; callers (the Gundi portal)
    supply query params via config_overrides. The runner therefore executes
    them without a stored config row, and they are one of the two action
    types (with auth) allowed to run ephemerally against a draft integration.
    """


def discover_actions(module_name, prefix):
    action_handlers = {}
    # Import the module using importlib
    module = importlib.import_module(module_name)
    all_members = inspect.getmembers(module)

    # Iterate through the members and filter functions by prefix
    for name, func in all_members:
        if name.startswith(prefix) and inspect.isfunction(func):
            if func is action_title:
                continue  # The decorator itself, often imported alongside handlers
            signature = inspect.signature(func)
            key = name[len(prefix):]  # Remove prefix
            if "action_config" not in signature.parameters:
                logger.warning(
                    f"Ignoring '{name}' in '{module_name}': functions prefixed with "
                    f"'{prefix}' must accept an 'action_config' argument to be registered as actions."
                )
                continue
            if (config_annotation := signature.parameters.get("action_config").annotation) != inspect._empty:
                config_model = config_annotation
            else:
                config_model = GenericActionConfiguration
            if issubclass(config_model, PushActionConfiguration):  # Push actions
                if data_param := signature.parameters.get("data"):
                    if (data_annotation := data_param.annotation) != inspect._empty:
                        data_model = data_annotation
                    else:
                        raise ValueError(f"The 'data' parameter in action '{key}' must be annotated with a data model.")
                else:
                    raise ValueError(f"Push action '{key}' must accept a 'data' parameter.")
                if not signature.parameters.get("metadata"):
                    raise ValueError(f"Push action '{key}' must accept a 'metadata' parameter.")
            else:
                data_model = None
            action_handlers[key] = (func, config_model, data_model)

    return action_handlers



def get_actions():
    return list(discover_actions(module_name="app.actions.handlers", prefix="action_").keys())
