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
    # Not the payload, and not unconditionally: a delayed Created can arrive
    # after an ActionConfigDeleted installed a fresh tombstone, and a plain SET
    # of the payload would resurrect the deleted row permanently. The portal is
    # read after everything, so its row (or its absence) is the truth, and it
    # is installed only over exactly what the cache held when we looked.
    integration_id = event.payload.integration
    action_id = event.payload.action.value
    _, observed = await config_manager.read_cached_action_configuration(
        integration_id=integration_id,
        action_id=action_id
    )
    integration = await config_manager._fetch_integration_from_gundi(integration_id)
    row = integration.get_action_config(action_id)
    if row is None:
        # Deleted again already; the cache holds (or will get) the tombstone.
        logger.info(
            f"Ignoring ActionConfigCreated for action '{action_id}' of integration "
            f"'{integration_id}': the portal no longer has a configuration for it."
        )
        return
    if observed is None:
        written = await config_manager.install_action_configuration_if_missing(integration_id, action_id, config=row)
    else:
        written = await config_manager.replace_cached_entry(integration_id, action_id, config=row, observed=observed)
    if not written:
        # The winner is later in cache time, not necessarily newer portal state
        # (a delayed older Updated can land after the read above). Reconcile the
        # way the Updated handler does: fresh portal row over the newest token,
        # bounded, stopping if the winner is a tombstone.
        reconciled, outcome = await _reconcile_from_portal(integration_id, action_id)
        message = (
            f"ActionConfigCreated for action '{action_id}' of integration '{integration_id}' "
            f"lost to a concurrent write; {outcome}."
        )
        (logger.info if reconciled else logger.error)(message)


# Deliveries run concurrently, so an update is a compare-and-set loop: read,
# apply, write only if the key still holds what was read; on a lost race,
# re-read and apply to the value that won. Bounded so a pathological stream
# cannot spin. Giving up reconciles from the portal, still conditionally,
# rather than leaving a stale permanent config behind an acked event.
UPDATE_ATTEMPTS = 3


async def handle_action_config_updated_event(event: ActionConfigUpdated):
    event_data = event.payload
    integration_id = event_data.integration_id
    action_id = event_data.alt_id
    for attempt in range(UPDATE_ATTEMPTS):
        # One read of the cache, never a reload: every write below compares-and-
        # sets against exactly what this read saw. A second read could observe
        # a fresh tombstone written by a concurrent ActionConfigDeleted in
        # between, and a reload rewrites the cache from a snapshot before
        # returning, leaving this handler holding a token for a value that is
        # gone.
        action_config, observed = await config_manager.read_cached_action_configuration(
            integration_id=integration_id,
            action_id=action_id
        )
        if action_config is None:
            if attempt > 0:
                # An absence seen after a failed write (a tombstone, or nothing)
                # means a newer delete, or the sentinel expired under us. Stop:
                # fetching the portal now and installing over that tombstone is
                # exactly the delete race this handler exists to prevent. Only
                # an absence on the first read is a recovery.
                logger.warning(
                    f"Ignoring ActionConfigUpdated for action '{action_id}' of integration "
                    f"'{integration_id}': its cached configuration went away while the update was being applied."
                )
                return
            # Nothing usable cached: a recorded absence (`observed` is the exact
            # sentinel) or nothing at all (cold cache, or an expired sentinel).
            # An Updated event still arriving means the portal has the row and
            # its Created event was lost or delivered out of order; fetch the
            # row without touching the cache (a full reload would SET every
            # action from one snapshot) and install it conditionally below.
            integration = await config_manager._fetch_integration_from_gundi(integration_id)
            action_config = integration.get_action_config(action_id)
            if action_config is None:
                logger.warning(
                    f"Ignoring ActionConfigUpdated for action '{action_id}' of integration "
                    f"'{integration_id}': the portal has no configuration for it."
                )
                return
        _apply_changes(action_config, event_data.changes)
        if observed is None:
            written = await config_manager.install_action_configuration_if_missing(
                integration_id, action_id, config=action_config,
            )
        else:
            written = await config_manager.replace_cached_entry(
                integration_id, action_id, config=action_config, observed=observed,
            )
        if written:
            return
        # The key changed under us (a newer value, a newer tombstone, or it
        # expired): go round and apply this event's changes to what is there now.
    # process_config_event acks the event whatever happens here, so a stale
    # permanent config left behind would never self-heal. Reconcile from the
    # portal, which holds the truth about whatever won the race, but still
    # conditionally: a blind write or a DEL could clobber a tombstone a
    # concurrent delete wrote a moment ago, and a later reload whose fetch
    # predates that delete would then resurrect the config. If this write
    # loses too, the cache holds a value newer than everything we saw; leave it.
    reconciled, outcome = await _reconcile_from_portal(integration_id, action_id)
    message = (
        f"Gave up applying ActionConfigUpdated for action '{action_id}' of integration "
        f"'{integration_id}' after {UPDATE_ATTEMPTS} attempts: the cached configuration kept changing "
        f"underneath it; {outcome}."
    )
    if reconciled:
        logger.warning(message)
    else:
        # A competing write that beat the reconciliation is only later in cache
        # time, not necessarily newer portal state (a delayed older Updated can
        # land after the final portal row was fetched), and the endpoint acks
        # this event regardless. Nothing later repairs a stale permanent key,
        # so say so where it will be seen.
        logger.error(message)


async def _reconcile_from_portal(integration_id, action_id):
    """Write the portal's truth over exactly what the cache holds, retrying
    against the newest token when a competing write lands in between (that
    write is later in cache time, not necessarily newer portal state), for
    up to UPDATE_ATTEMPTS. The portal is re-read on every retry: the write
    that won may have come from a newer event whose row is also in the portal
    now, and reusing the first snapshot would write stale state over it.
    Returns (reconciled, description) for the log."""
    for _ in range(UPDATE_ATTEMPTS):
        action_config, observed = await config_manager.read_cached_action_configuration(
            integration_id=integration_id,
            action_id=action_id
        )
        if action_config is None:
            # The same rule as inside the loop: no cached configuration after a
            # failed write means a newer delete (or an expired entry). Fetching
            # a possibly stale portal row and installing it over that tombstone
            # is the delete race this handler exists to prevent, so stop here.
            return True, "the cache now records no configuration for it, so it was left alone"
        integration = await config_manager._fetch_integration_from_gundi(integration_id)
        row = integration.get_action_config(action_id)
        if row is None:
            written = await config_manager.replace_cached_entry_with_absence(integration_id, action_id, observed=observed)
        else:
            written = await config_manager.replace_cached_entry(integration_id, action_id, config=row, observed=observed)
        if written:
            return True, "reconciled it from the portal instead"
    return False, (
        f"the reconciling write from the portal lost {UPDATE_ATTEMPTS} times as well, so the cached "
        "configuration may be stale until the next event for this action"
    )


def _apply_changes(action_config, changes: dict) -> None:
    for key, value in changes.items():
        setattr(action_config, key, value)


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
