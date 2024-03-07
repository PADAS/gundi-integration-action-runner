import json

from app.actions import action_handlers, AuthActionConfiguration, PullActionConfiguration, PushActionConfiguration
from app.settings.integration import INTEGRATION_TYPE_SLUG


async def register_integration_in_gundi(gundi_client):
    # Prepare the integration name and value
    integration_type_slug = INTEGRATION_TYPE_SLUG.strip().lower()
    integration_type_name = integration_type_slug.replace("_", " ").title()
    data = {
        "name": integration_type_name,
        "value": integration_type_slug,
        "description": f"Default type for integrations with {integration_type_name}",
    }
    # Prepare the actions and schemas
    actions = []
    for action_id, handler in action_handlers.items():
        _, config_model = handler
        action_name = action_id.replace("_", " ").title()
        action_schema = json.loads(config_model.schema_json())
        if issubclass(config_model, AuthActionConfiguration):
            action_type = "auth"
        elif issubclass(config_model, PullActionConfiguration):
            action_type = "pull"
        elif issubclass(config_model, PushActionConfiguration):
            action_type = "push"
        else:
            action_type = "generic"
        actions.append(
            {
                "type": action_type,
                "name": action_name,
                "value": action_id,
                "description": f"{integration_type_name} {action_name} action",
                "schema": action_schema,
            }
        )
    data["actions"] = actions
    # Register the integration type and actions in Gundi
    response = await gundi_client.register_integration_type(data)
    return response
