from .core import *


def setup_action_handlers():
    return discover_actions(module_name="app.actions.handlers", prefix="action_")


action_handlers = setup_action_handlers()
