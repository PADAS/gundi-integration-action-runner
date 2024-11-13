import importlib
import inspect
from typing import Optional

from pydantic import BaseModel
from app.services.utils import UISchemaModelMixin


class ActionConfiguration(UISchemaModelMixin, BaseModel):
    pass


class PullActionConfiguration(ActionConfiguration):
    pass


class PushActionConfiguration(ActionConfiguration):
    pass


class AuthActionConfiguration(ActionConfiguration):
    pass


class GenericActionConfiguration(ActionConfiguration):
    pass


def discover_actions(module_name, prefix):
    action_handlers = {}

    # Import the module using importlib
    module = importlib.import_module(module_name)
    all_members = inspect.getmembers(module)

    # Iterate through the members and filter functions by prefix
    for name, func in all_members:
        if name.startswith(prefix) and inspect.isfunction(func):
            key = name[len(prefix):]  # Remove prefix
            if (config_annotation := inspect.signature(func).parameters.get("action_config").annotation) != inspect._empty:
                config_model = config_annotation
            else:
                config_model = GenericActionConfiguration
            action_handlers[key] = (func, config_model)

    return action_handlers


def get_actions():
    return list(discover_actions(module_name="app.actions.handlers", prefix="action_").keys())
