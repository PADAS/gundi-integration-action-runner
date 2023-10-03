from typing import Optional
from gundi_client_v2 import GundiClient
from app.actions import action_handlers
from app.actions import ActionConfiguration
from .errors import ActionNotFound, ConfigurationNotFound, ConfigurationValidationError, ActionExecutionError
from .utils import find_config_for_action


_portal = GundiClient()


async def execute_action(integration_id: str, action_id: str):
    """
    Interface for executing actions.

    :param integration_id: The UUID of the integration
    :param action_id: "test_auth", "pull_observations", "pull_events"
    :return: action result if any, or raise an exception
    """
    print(f"Executing action '{action_id}' for integration '{integration_id}'...")
    try:  # Get the integration config from the portal
        integration = await _portal.get_integration_details(integration_id=integration_id)
    except Exception as e:
        raise ActionExecutionError(f"Error retrieving configuration for integration '{integration_id}': {e}")

    # Look for the configuration of the action being executed
    action_config = find_config_for_action(
        configurations=integration.configurations,
        action_id=action_id
    )
    if not action_config:
        raise ConfigurationNotFound(
            f"Configuration for action '{action_id}' for integration {str(integration.id)} is missing. Please fix the integration setup in the portal."
        )

    try:  # Execute the action
        handler = action_handlers[action_id]
        result = await handler(integration, action_config)
    except KeyError as e:
        raise ActionNotFound(f"Action '{action_id}' is not supported for this integration")
    except Exception as e:
        raise ActionExecutionError(f"Internal error executing action '{action_id}': {e}")
    else:
        return result
