from .core import *


def setup_action_handlers():
    return discover_actions(module_name="app.actions.handlers", prefix="action_")


def get_action_handler_by_data_type(type_name: str):
    for action_id, value in action_handlers.items():
        func, config_model, data_model = value
        if data_model and data_model.__name__ == type_name.strip():
            return action_id, func, config_model, data_model
    else:
        raise ValueError(f"No action handler found for data type '{type_name}'.")


action_handlers = setup_action_handlers()
