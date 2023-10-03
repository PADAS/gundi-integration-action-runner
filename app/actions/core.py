import importlib
import inspect
from pydantic import BaseModel


class ActionConfiguration(BaseModel):
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
            action_handlers[key] = func

    return action_handlers


def get_actions():
    return list(discover_actions(module_name="app.actions.handlers", prefix="action_").keys())
