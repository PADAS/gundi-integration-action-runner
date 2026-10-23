# Settings first: handler discovery below executes the connector's handlers
# module at import, and gundi_action_runner.settings installs the shared token
# cache URL into gundi-client-v2's settings, so a GundiClient() built at module
# scope there only picks it up if this runs first. Done here rather than in the
# package __init__ so lightweight imports (services.utils, actions.core) stay light.
from gundi_action_runner import settings  # noqa: F401
from .core import *  # noqa: F401,F403
from gundi_action_runner.registry import registry

# The live handler mapping. Binding the registry's dict here (same mutable
# object) keeps the template convention working: services and forks do
# `from ...actions import action_handlers` and see registrations as they land.
action_handlers = registry.action_handlers


def setup_action_handlers():
    """Legacy template convention: scan app.actions.handlers into the registry."""
    from gundi_action_runner import settings
    registry.load_legacy_actions(module_name=settings.GUNDI_LEGACY_ACTIONS_MODULE)
    return registry.action_handlers


def get_actions():
    return list(registry.action_handlers.keys())


def get_action_handler_by_data_type(type_name: str):
    for action_id, value in action_handlers.items():
        func, config_model, data_model = value
        if data_model and data_model.__name__ == type_name.strip():
            return action_id, func, config_model, data_model
    else:
        raise ValueError(f"No action handler found for data type '{type_name}'.")
