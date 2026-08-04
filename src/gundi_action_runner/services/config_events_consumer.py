import logging

from gundi_core.events import (
    SystemEventBaseModel,
    IntegrationCreated,
    IntegrationUpdated,
    IntegrationDeleted,
    ActionConfigCreated,
    ActionConfigUpdated,
    ActionConfigDeleted
)


from .config_manager import IntegrationConfigurationManager


logger = logging.getLogger(__name__)
config_manager = IntegrationConfigurationManager()


async def handle_integration_created_event(event: IntegrationCreated):
    await config_manager.set_integration(integration=event.payload)


async def handle_integration_updated_event(event: IntegrationUpdated):
    event_data = event.payload
    integration = await config_manager.get_integration(integration_id=event_data.id)
    for key, value in event_data.changes.items():
        if hasattr(integration, key):
            setattr(integration, key, value)
    await config_manager.set_integration(integration=integration)


async def handle_integration_deleted_event(event: IntegrationDeleted):
    await config_manager.delete_integration(integration_id=event.payload.id)


async def handle_action_config_created_event(event: ActionConfigCreated):
    action_config = event.payload
    await config_manager.set_action_configuration(
        integration_id=action_config.integration,
        action_id=action_config.action.value,
        config=action_config
    )


async def handle_action_config_updated_event(event: ActionConfigUpdated):
    event_data = event.payload
    integration_id = event_data.integration_id
    action_id = event_data.alt_id
    action_config = await config_manager.get_action_configuration(
        integration_id=integration_id,
        action_id=action_id
    )
    for key, value in event_data.changes.items():
        setattr(action_config, key, value)
    await config_manager.set_action_configuration(
        integration_id=integration_id,
        action_id=action_id,
        config=action_config
    )


async def handle_action_config_deleted_event(event: ActionConfigDeleted):
    event_data = event.payload
    integration_id = event_data.integration_id
    action_id = event_data.alt_id
    await config_manager.delete_action_configuration(
        integration_id=integration_id,
        action_id=action_id
    )


event_handlers = {
    "IntegrationCreated": handle_integration_created_event,
    "IntegrationUpdated": handle_integration_updated_event,
    "IntegrationDeleted": handle_integration_deleted_event,
    "ActionConfigCreated": handle_action_config_created_event,
    "ActionConfigUpdated": handle_action_config_updated_event,
    "ActionConfigDeleted": handle_action_config_deleted_event,
}

event_schemas = {
    "IntegrationCreated": IntegrationCreated,
    "IntegrationUpdated": IntegrationUpdated,
    "IntegrationDeleted": IntegrationDeleted,
    "ActionConfigCreated": ActionConfigCreated,
    "ActionConfigUpdated": ActionConfigUpdated,
    "ActionConfigDeleted": ActionConfigDeleted,
}


async def process_config_event(event_data: dict, attributes: dict = None):
    try:
        logger.info(f"Received Configuration Event. data: {event_data}, attributes: {attributes}.")
        event = SystemEventBaseModel.parse_obj(event_data)
        schema_version = event.schema_version
        if schema_version != "v1":
            logger.warning(
                f"Schema version '{schema_version}' is not supported. Message discarded."
            )
            return {"status": "error", "message": "Unsupported schema version"}
        try:
            event_type = attributes.get("event_type")
            handler = event_handlers[event_type]
        except KeyError:
            logger.warning(f"Event of type '{event_type}' unknown. Message discarded.")
            return {"status": "error", "message": "Unknown event type"}
        try:
            schema = event_schemas[event_type]
        except KeyError:
            logger.warning(f"Event Schema for '{event_type}' not found. Message discarded.")
            return
        parsed_event = schema.parse_obj(event_data)
        await handler(event=parsed_event)
    except Exception as e:  # ToDo: handle more specific exceptions
        logger.exception(f"Error processing event: {type(e)}:{e}",)
        return {"status": "error", "message": f"Internal error: {str(e)}"}
    else:
        logger.info(f"Configuration event {event_type} ({parsed_event.event_id}) processed successfully.")
        return {"status": "success", "message": "Event processed successfully"}
