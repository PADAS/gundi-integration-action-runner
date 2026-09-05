from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

from app.main import app


api_client = TestClient(app)


@pytest.mark.asyncio
async def test_process_event_integration_created_from_pubsub(
        mocker, mock_gundi_client_v2, mock_publish_event, mock_action_handlers, mock_config_manager,
        pubsub_message_request_headers, integration_created_event_as_pubsub_message
):

    mocker.patch("app.services.config_events_consumer.config_manager", mock_config_manager)

    response = api_client.post(
        "/config-events/",
        headers=pubsub_message_request_headers,
        json=integration_created_event_as_pubsub_message,
    )

    assert response.status_code == 200
    assert mock_config_manager.set_integration.called


@pytest.mark.asyncio
async def test_process_event_integration_updated_from_pubsub(
        mocker, mock_gundi_client_v2, mock_publish_event, mock_action_handlers, mock_config_manager,
        pubsub_message_request_headers, integration_updated_event_as_pubsub_message
):

    mocker.patch("app.services.config_events_consumer.config_manager", mock_config_manager)

    response = api_client.post(
        "/config-events/",
        headers=pubsub_message_request_headers,
        json=integration_updated_event_as_pubsub_message,
    )

    assert response.status_code == 200
    assert mock_config_manager.get_integration.called
    assert mock_config_manager.set_integration.called


@pytest.mark.asyncio
async def test_process_event_integration_deleted_from_pubsub(
        mocker, mock_gundi_client_v2, mock_publish_event, mock_action_handlers, mock_config_manager,
        pubsub_message_request_headers, integration_deleted_event_as_pubsub_message
):

    mocker.patch("app.services.config_events_consumer.config_manager", mock_config_manager)

    response = api_client.post(
        "/config-events/",
        headers=pubsub_message_request_headers,
        json=integration_deleted_event_as_pubsub_message,
    )

    assert response.status_code == 200
    assert mock_config_manager.delete_integration.called


@pytest.mark.asyncio
async def test_process_event_action_config_created_from_pubsub(
        mocker, mock_gundi_client_v2, mock_publish_event, mock_action_handlers, mock_config_manager,
        pubsub_message_request_headers, action_config_created_event_as_pubsub_message
):

    mocker.patch("app.services.config_events_consumer.config_manager", mock_config_manager)

    response = api_client.post(
        "/config-events/",
        headers=pubsub_message_request_headers,
        json=action_config_created_event_as_pubsub_message,
    )

    assert response.status_code == 200
    # Created installs the portal row conditionally over what the cache held.
    assert mock_config_manager._fetch_integration_from_gundi.called
    assert mock_config_manager.replace_cached_entry.called
    assert not mock_config_manager.set_action_configuration.called


@pytest.mark.asyncio
async def test_process_event_action_config_updated_from_pubsub(
        mocker, mock_gundi_client_v2, mock_publish_event, mock_action_handlers, mock_config_manager,
        pubsub_message_request_headers, action_config_updated_event_as_pubsub_message
):

    mocker.patch("app.services.config_events_consumer.config_manager", mock_config_manager)

    response = api_client.post(
        "/config-events/",
        headers=pubsub_message_request_headers,
        json=action_config_updated_event_as_pubsub_message,
    )

    assert response.status_code == 200
    assert mock_config_manager.read_cached_action_configuration.called
    assert mock_config_manager.replace_cached_entry.called


@pytest.mark.asyncio
async def test_process_event_action_config_deleted_from_pubsub(
        mocker, mock_gundi_client_v2, mock_publish_event, mock_action_handlers, mock_config_manager,
        pubsub_message_request_headers, action_config_deleted_event_as_pubsub_message
):

    mocker.patch("app.services.config_events_consumer.config_manager", mock_config_manager)

    response = api_client.post(
        "/config-events/",
        headers=pubsub_message_request_headers,
        json=action_config_deleted_event_as_pubsub_message,
    )

    assert response.status_code == 200
    assert mock_config_manager.delete_action_configuration.called



def _updated_event(integration_id, action_id, changes):
    from gundi_core.events import ActionConfigUpdated

    return ActionConfigUpdated.parse_obj({"payload": {
        "id": "81344345-f691-4230-8fab-6d2464729085",
        "alt_id": action_id,
        "changes": changes,
        "integration_id": integration_id,
    }})


@pytest.mark.asyncio
async def test_action_config_updated_reloads_from_the_portal_when_the_cache_records_absence(
        mocker, mock_config_manager, integration_v2,
):
    """get_action_configuration answers None from the absence sentinel without
    touching the portal. An Updated event can still arrive for that action
    (its Created was lost or delivered out of order): applying the changes
    to None raised AttributeError, which process_config_event logged and
    acked, so the change was silently dropped and the sentinel stayed.

    The recovery fetches the portal row without rewriting the cache: a full
    reload SETs every action's config from one snapshot and would overwrite a
    newer value another event cached while the fetch was in flight. Only this
    action's key is written, by the handler's own final set."""
    from app.services import config_events_consumer

    existing = integration_v2.configurations[0]
    mock_config_manager.read_cached_action_configuration = AsyncMock(return_value=(None, "null:gen1"))
    mock_config_manager._fetch_integration_from_gundi = AsyncMock(return_value=integration_v2)
    mock_config_manager._reload_integration_from_gundi = AsyncMock(return_value=integration_v2)
    mock_config_manager.set_action_configuration = AsyncMock()
    mock_config_manager.replace_cached_entry = AsyncMock(return_value=True)
    mocker.patch.object(config_events_consumer, "config_manager", mock_config_manager)

    await config_events_consumer.handle_action_config_updated_event(
        _updated_event(str(integration_v2.id), existing.action.value, {"data": {"lookback_days": 2}})
    )

    # The payload parses integration_id as a UUID; the key builders stringify it.
    (fetched_id,), _ = mock_config_manager._fetch_integration_from_gundi.await_args
    assert str(fetched_id) == str(integration_v2.id)
    assert not mock_config_manager._reload_integration_from_gundi.called, "must not rewrite the whole cache"
    # The recovered value replaces the exact sentinel generation this handler
    # observed on the read that established the absence, in one server-side
    # step: a plain SET would let the slower of two concurrent recoveries
    # restore its stale snapshot over the newer one, comparing against a bare
    # "null" would let it overwrite a newer tombstone from a concurrent
    # ActionConfigDeleted, and a second read to pick up the generation could
    # observe that newer tombstone and compare against it instead.
    assert mock_config_manager.read_cached_action_configuration.await_count == 1
    assert not mock_config_manager.set_action_configuration.called
    kwargs = mock_config_manager.replace_cached_entry.await_args.kwargs
    assert kwargs["observed"] == "null:gen1"
    assert kwargs["config"].id == existing.id
    assert kwargs["config"].data == {"lookback_days": 2}


@pytest.mark.asyncio
async def test_action_config_updated_recovery_that_loses_the_race_applies_its_changes_to_the_newer_value(
        mocker, mock_config_manager, integration_v2,
):
    """Two Updated events for the same action both see the sentinel and fetch.
    The one whose conditional write finds the sentinel gone must not overwrite
    the newer value; it re-reads the cache and applies its own changes on top,
    the way every ordinary Updated does."""
    from app.services import config_events_consumer

    existing = integration_v2.configurations[0]
    newer = existing.copy(update={"data": {**existing.data, "lookback_days": 9, "fresh": True}})
    newer_raw = newer.json()  # before the handler applies its changes to this object
    mock_config_manager.read_cached_action_configuration = AsyncMock(
        side_effect=[(None, "null:gen1"), (newer, newer_raw)],
    )
    mock_config_manager._fetch_integration_from_gundi = AsyncMock(return_value=integration_v2)
    mock_config_manager.set_action_configuration = AsyncMock()
    # First compare-and-set loses (against the sentinel); the second, against
    # the newer value's raw JSON, wins.
    mock_config_manager.replace_cached_entry = AsyncMock(side_effect=[False, True])
    mocker.patch.object(config_events_consumer, "config_manager", mock_config_manager)

    await config_events_consumer.handle_action_config_updated_event(
        _updated_event(str(integration_v2.id), existing.action.value, {"data": {"lookback_days": 2}})
    )

    assert mock_config_manager.read_cached_action_configuration.await_count == 2
    assert not mock_config_manager.set_action_configuration.called, "never a plain SET: a delete can land in between"
    kwargs = mock_config_manager.replace_cached_entry.await_args.kwargs
    assert kwargs["observed"] == newer_raw, "compare-and-set against exactly the value re-read"
    assert kwargs["config"].data == {"lookback_days": 2}, "this event's changes, applied to the value that won"
    assert kwargs["config"].id == newer.id


@pytest.mark.asyncio
async def test_action_config_updated_for_an_action_the_portal_has_no_config_for_is_a_no_op(
        mocker, mock_config_manager, integration_v2,
):
    from app.services import config_events_consumer

    configured = {c.action.value for c in integration_v2.configurations}
    unconfigured = next(a.value for a in integration_v2.type.actions if a.value not in configured)
    mock_config_manager.read_cached_action_configuration = AsyncMock(return_value=(None, "null:gen1"))
    mock_config_manager._fetch_integration_from_gundi = AsyncMock(return_value=integration_v2)
    mock_config_manager._reload_integration_from_gundi = AsyncMock(return_value=integration_v2)
    mock_config_manager.set_action_configuration = AsyncMock()
    mock_config_manager.replace_cached_entry = AsyncMock(return_value=True)
    mocker.patch.object(config_events_consumer, "config_manager", mock_config_manager)

    await config_events_consumer.handle_action_config_updated_event(
        _updated_event(str(integration_v2.id), unconfigured, {"data": {"lookback_days": 2}})
    )

    assert not mock_config_manager.set_action_configuration.called
    assert not mock_config_manager.replace_cached_entry.called


@pytest.mark.asyncio
async def test_action_config_updated_recovery_does_not_undo_a_concurrent_delete(
        mocker, mock_config_manager, integration_v2,
):
    """Recovery observes sentinel generation 1 and fetches the portal. Before
    it writes, an ActionConfigDeleted lands and writes generation 2. The
    compare-and-set against generation 1 fails, the re-read still says
    absent, and the recovery stops: the newer delete wins, and the deleted
    config is not resurrected as a permanent key."""
    from app.services import config_events_consumer

    existing = integration_v2.configurations[0]
    mock_config_manager.read_cached_action_configuration = AsyncMock(
        side_effect=[(None, "null:gen1"), (None, "null:gen2")],
    )
    mock_config_manager._fetch_integration_from_gundi = AsyncMock(return_value=integration_v2)
    mock_config_manager.set_action_configuration = AsyncMock()
    mock_config_manager.replace_cached_entry = AsyncMock(return_value=False)
    mocker.patch.object(config_events_consumer, "config_manager", mock_config_manager)

    await config_events_consumer.handle_action_config_updated_event(
        _updated_event(str(integration_v2.id), existing.action.value, {"data": {"lookback_days": 2}})
    )

    assert mock_config_manager.replace_cached_entry.await_args.kwargs["observed"] == "null:gen1"
    assert mock_config_manager._fetch_integration_from_gundi.await_count == 1, "a tombstone on the re-read ends the recovery"
    assert not mock_config_manager.set_action_configuration.called


@pytest.mark.asyncio
async def test_action_config_updated_on_a_cold_cache_installs_the_fetched_row_only_if_still_missing(
        mocker, mock_config_manager, integration_v2,
):
    """Nothing cached for this action (cold cache or an expired sentinel). The
    ordinary lookup would run the full portal reload, which rewrites the cache
    from a snapshot and would leave this handler without the "still missing"
    state its conditional write depends on. The recovery instead fetches the
    row without touching the cache and installs it with SET NX, so anything
    written meanwhile (a config or a tombstone) wins."""
    from app.services import config_events_consumer

    existing = integration_v2.configurations[0]
    mock_config_manager.read_cached_action_configuration = AsyncMock(return_value=(None, None))
    mock_config_manager._fetch_integration_from_gundi = AsyncMock(return_value=integration_v2)
    mock_config_manager._reload_integration_from_gundi = AsyncMock(return_value=integration_v2)
    mock_config_manager.install_action_configuration_if_missing = AsyncMock(return_value=True)
    mock_config_manager.replace_cached_entry = AsyncMock(return_value=True)
    mock_config_manager.set_action_configuration = AsyncMock()
    mocker.patch.object(config_events_consumer, "config_manager", mock_config_manager)

    await config_events_consumer.handle_action_config_updated_event(
        _updated_event(str(integration_v2.id), existing.action.value, {"data": {"lookback_days": 2}})
    )

    assert not mock_config_manager._reload_integration_from_gundi.called
    assert not mock_config_manager.replace_cached_entry.called
    assert not mock_config_manager.set_action_configuration.called
    installed = mock_config_manager.install_action_configuration_if_missing.await_args.kwargs["config"]
    assert installed.id == existing.id and installed.data == {"lookback_days": 2}


@pytest.mark.asyncio
async def test_action_config_updated_on_a_cold_cache_yields_to_a_delete_that_landed_meanwhile(
        mocker, mock_config_manager, integration_v2,
):
    from app.services import config_events_consumer

    existing = integration_v2.configurations[0]
    mock_config_manager.read_cached_action_configuration = AsyncMock(side_effect=[(None, None), (None, "null:gen2")])
    mock_config_manager._fetch_integration_from_gundi = AsyncMock(return_value=integration_v2)
    mock_config_manager.install_action_configuration_if_missing = AsyncMock(return_value=False)
    mock_config_manager.set_action_configuration = AsyncMock()
    mocker.patch.object(config_events_consumer, "config_manager", mock_config_manager)

    await config_events_consumer.handle_action_config_updated_event(
        _updated_event(str(integration_v2.id), existing.action.value, {"data": {"lookback_days": 2}})
    )

    assert mock_config_manager.read_cached_action_configuration.await_count == 2
    assert not mock_config_manager.set_action_configuration.called


@pytest.mark.asyncio
async def test_action_config_updated_on_a_cached_config_is_a_compare_and_set(
        mocker, mock_config_manager, integration_v2,
):
    """The ordinary path is read-modify-write too. A plain SET after the read
    would overwrite a tombstone (or a newer value) another concurrent delivery
    wrote in between; the write compares against the raw value read."""
    from app.services import config_events_consumer

    existing = integration_v2.configurations[0]
    mock_config_manager.read_cached_action_configuration = AsyncMock(return_value=(existing.copy(), existing.json()))
    mock_config_manager.replace_cached_entry = AsyncMock(return_value=True)
    mock_config_manager.set_action_configuration = AsyncMock()
    mocker.patch.object(config_events_consumer, "config_manager", mock_config_manager)

    await config_events_consumer.handle_action_config_updated_event(
        _updated_event(str(integration_v2.id), existing.action.value, {"data": {"lookback_days": 2}})
    )

    assert not mock_config_manager.set_action_configuration.called
    kwargs = mock_config_manager.replace_cached_entry.await_args.kwargs
    assert kwargs["observed"] == existing.json()
    assert kwargs["config"].data == {"lookback_days": 2}


@pytest.mark.asyncio
async def test_action_config_updated_retries_a_bounded_number_of_times_then_gives_up(
        mocker, mock_config_manager, integration_v2, caplog,
):
    from app.services import config_events_consumer

    existing = integration_v2.configurations[0]
    """After the attempts, reconcile from the portal rather than write blind or
    delete: a DEL could erase a tombstone a concurrent delete just wrote, and
    a later reload whose fetch predates that delete would then resurrect the
    config. The portal row (authoritative, read after everything) is installed
    with one more compare-and-set against exactly what the cache holds now."""
    portal_row = integration_v2.configurations[0]
    latest_raw = '{"id": "latest"}'
    mock_config_manager.read_cached_action_configuration = AsyncMock(
        side_effect=[(existing.copy(), existing.json())] * config_events_consumer.UPDATE_ATTEMPTS
        + [(existing.copy(), latest_raw)],
    )
    mock_config_manager.replace_cached_entry = AsyncMock(
        side_effect=[False] * config_events_consumer.UPDATE_ATTEMPTS + [True],
    )
    mock_config_manager._fetch_integration_from_gundi = AsyncMock(return_value=integration_v2)
    mock_config_manager.set_action_configuration = AsyncMock()
    mock_config_manager.invalidate_action_configuration = AsyncMock()
    mocker.patch.object(config_events_consumer, "config_manager", mock_config_manager)

    await config_events_consumer.handle_action_config_updated_event(
        _updated_event(str(integration_v2.id), existing.action.value, {"data": {"lookback_days": 2}})
    )

    assert mock_config_manager.replace_cached_entry.await_count == config_events_consumer.UPDATE_ATTEMPTS + 1
    assert not mock_config_manager.set_action_configuration.called
    assert not mock_config_manager.invalidate_action_configuration.called, "never delete an unobserved value"
    final = mock_config_manager.replace_cached_entry.await_args.kwargs
    assert final["observed"] == latest_raw
    assert final["config"].json() == portal_row.json(), "the portal row as fetched, not this event's changes"
    assert any("Gave up" in r.getMessage() and "portal" in r.getMessage() for r in caplog.records)


@pytest.mark.asyncio
async def test_action_config_updated_give_up_records_an_absence_when_the_portal_has_no_row(
        mocker, mock_config_manager, integration_v2,
):
    from app.services import config_events_consumer

    existing = integration_v2.configurations[0]
    gone = integration_v2.copy(update={"configurations": []})
    mock_config_manager.read_cached_action_configuration = AsyncMock(return_value=(existing.copy(), existing.json()))
    mock_config_manager.replace_cached_entry = AsyncMock(return_value=False)
    mock_config_manager.replace_cached_entry_with_absence = AsyncMock(return_value=True)
    mock_config_manager._fetch_integration_from_gundi = AsyncMock(return_value=gone)
    mock_config_manager.set_action_configuration = AsyncMock()
    mock_config_manager.invalidate_action_configuration = AsyncMock()
    mocker.patch.object(config_events_consumer, "config_manager", mock_config_manager)

    await config_events_consumer.handle_action_config_updated_event(
        _updated_event(str(integration_v2.id), existing.action.value, {"data": {"lookback_days": 2}})
    )

    mock_config_manager.replace_cached_entry_with_absence.assert_awaited_once()
    assert mock_config_manager.replace_cached_entry_with_absence.await_args.kwargs["observed"] == existing.json()
    assert not mock_config_manager.invalidate_action_configuration.called


@pytest.mark.asyncio
async def test_action_config_updated_warning_on_a_lost_race_does_not_claim_a_delete(
        mocker, mock_config_manager, integration_v2, caplog,
):
    """(None, None) on the re-read can be an expired sentinel or a cold-cache
    winner that vanished, not only a delete; the operator must not be sent
    looking for a delete event that never happened."""
    from app.services import config_events_consumer

    existing = integration_v2.configurations[0]
    mock_config_manager.read_cached_action_configuration = AsyncMock(side_effect=[(None, None), (None, None)])
    mock_config_manager._fetch_integration_from_gundi = AsyncMock(return_value=integration_v2)
    mock_config_manager.install_action_configuration_if_missing = AsyncMock(return_value=False)
    mock_config_manager.set_action_configuration = AsyncMock()
    mocker.patch.object(config_events_consumer, "config_manager", mock_config_manager)

    await config_events_consumer.handle_action_config_updated_event(
        _updated_event(str(integration_v2.id), existing.action.value, {"data": {"lookback_days": 2}})
    )

    assert not mock_config_manager.set_action_configuration.called
    messages = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
    assert messages and all("deleted" not in m for m in messages)


@pytest.mark.asyncio
async def test_action_config_updated_on_a_cached_config_stops_when_a_delete_wins_the_race(
        mocker, mock_config_manager, integration_v2, caplog,
):
    """An ordinary update reads a config, loses its compare-and-set to a
    concurrent ActionConfigDeleted, and re-reads a tombstone. That absence
    must end the update: treating it as an initial absence would fetch the
    portal and replace that exact tombstone with the fetched config, the
    delete race this recovery exists to prevent. Only an absence seen on the
    first read may trigger a recovery."""
    from app.services import config_events_consumer

    existing = integration_v2.configurations[0]
    mock_config_manager.read_cached_action_configuration = AsyncMock(
        side_effect=[(existing.copy(), existing.json()), (None, "null:gen2")],
    )
    mock_config_manager.replace_cached_entry = AsyncMock(return_value=False)
    mock_config_manager._fetch_integration_from_gundi = AsyncMock(return_value=integration_v2)
    mock_config_manager.install_action_configuration_if_missing = AsyncMock(return_value=True)
    mock_config_manager.set_action_configuration = AsyncMock()
    mocker.patch.object(config_events_consumer, "config_manager", mock_config_manager)

    await config_events_consumer.handle_action_config_updated_event(
        _updated_event(str(integration_v2.id), existing.action.value, {"data": {"lookback_days": 2}})
    )

    assert not mock_config_manager._fetch_integration_from_gundi.called, "no recovery after a failed write"
    assert mock_config_manager.replace_cached_entry.await_count == 1
    assert not mock_config_manager.install_action_configuration_if_missing.called
    assert not mock_config_manager.set_action_configuration.called


@pytest.mark.asyncio
async def test_action_config_updated_give_up_stops_when_the_re_read_shows_an_absence(
        mocker, mock_config_manager, integration_v2, caplog,
):
    """The same rule as inside the loop applies after it: if the last attempt
    lost to a delete, the reconciliation's re-read shows that fresh tombstone,
    and fetching a (possibly stale) portal row to install over it would
    resurrect the deleted configuration. No cached configuration on the
    re-read means stop, without fetching or writing."""
    from app.services import config_events_consumer

    existing = integration_v2.configurations[0]
    mock_config_manager.read_cached_action_configuration = AsyncMock(
        side_effect=[(existing.copy(), existing.json())] * config_events_consumer.UPDATE_ATTEMPTS + [(None, "null:e:9:x")],
    )
    mock_config_manager.replace_cached_entry = AsyncMock(return_value=False)
    mock_config_manager.replace_cached_entry_with_absence = AsyncMock(return_value=True)
    mock_config_manager.install_action_configuration_if_missing = AsyncMock(return_value=True)
    mock_config_manager._fetch_integration_from_gundi = AsyncMock(return_value=integration_v2)
    mock_config_manager.set_action_configuration = AsyncMock()
    mocker.patch.object(config_events_consumer, "config_manager", mock_config_manager)

    await config_events_consumer.handle_action_config_updated_event(
        _updated_event(str(integration_v2.id), existing.action.value, {"data": {"lookback_days": 2}})
    )

    assert not mock_config_manager._fetch_integration_from_gundi.called, "never fetch over a fresh tombstone"
    assert not mock_config_manager.replace_cached_entry_with_absence.called
    assert not mock_config_manager.install_action_configuration_if_missing.called
    assert mock_config_manager.replace_cached_entry.await_count == config_events_consumer.UPDATE_ATTEMPTS
    assert any("Gave up" in r.getMessage() for r in caplog.records)


def _created_event(integration_id, config):
    from gundi_core.events import ActionConfigCreated

    return ActionConfigCreated.parse_obj({"payload": {**config.dict(), "integration": integration_id}})


@pytest.mark.asyncio
async def test_action_config_created_installs_the_portal_row_over_the_observed_sentinel(
        mocker, mock_config_manager, integration_v2,
):
    """A delayed Created delivery can arrive after ActionConfigDeleted installed a
    fresh tombstone; an unconditional SET would resurrect the deleted row
    permanently. Created now reads the cache, fetches the portal (authoritative,
    read after everything), and installs the portal row only over exactly what
    the cache held."""
    from app.services import config_events_consumer

    existing = integration_v2.configurations[0]
    mock_config_manager.read_cached_action_configuration = AsyncMock(return_value=(None, "null:e:3:x"))
    mock_config_manager._fetch_integration_from_gundi = AsyncMock(return_value=integration_v2)
    mock_config_manager.replace_cached_entry = AsyncMock(return_value=True)
    mock_config_manager.install_action_configuration_if_missing = AsyncMock(return_value=True)
    mock_config_manager.set_action_configuration = AsyncMock()
    mocker.patch.object(config_events_consumer, "config_manager", mock_config_manager)

    await config_events_consumer.handle_action_config_created_event(_created_event(str(integration_v2.id), existing))

    assert not mock_config_manager.set_action_configuration.called, "never an unconditional write"
    kwargs = mock_config_manager.replace_cached_entry.await_args.kwargs
    assert kwargs["observed"] == "null:e:3:x"
    assert kwargs["config"].json() == existing.json(), "the portal row, not the event payload"


@pytest.mark.asyncio
async def test_action_config_created_does_nothing_when_the_portal_no_longer_has_the_row(
        mocker, mock_config_manager, integration_v2,
):
    from app.services import config_events_consumer

    existing = integration_v2.configurations[0]
    gone = integration_v2.copy(update={"configurations": []})
    mock_config_manager.read_cached_action_configuration = AsyncMock(return_value=(None, "null:e:3:x"))
    mock_config_manager._fetch_integration_from_gundi = AsyncMock(return_value=gone)
    mock_config_manager.replace_cached_entry = AsyncMock(return_value=True)
    mock_config_manager.install_action_configuration_if_missing = AsyncMock(return_value=True)
    mock_config_manager.set_action_configuration = AsyncMock()
    mocker.patch.object(config_events_consumer, "config_manager", mock_config_manager)

    await config_events_consumer.handle_action_config_created_event(_created_event(str(integration_v2.id), existing))

    assert not mock_config_manager.set_action_configuration.called
    assert not mock_config_manager.replace_cached_entry.called
    assert not mock_config_manager.install_action_configuration_if_missing.called


@pytest.mark.asyncio
async def test_action_config_created_on_a_cold_cache_installs_only_if_still_missing(
        mocker, mock_config_manager, integration_v2,
):
    from app.services import config_events_consumer

    existing = integration_v2.configurations[0]
    mock_config_manager.read_cached_action_configuration = AsyncMock(return_value=(None, None))
    mock_config_manager._fetch_integration_from_gundi = AsyncMock(return_value=integration_v2)
    mock_config_manager.replace_cached_entry = AsyncMock(return_value=True)
    mock_config_manager.install_action_configuration_if_missing = AsyncMock(return_value=True)
    mock_config_manager.set_action_configuration = AsyncMock()
    mocker.patch.object(config_events_consumer, "config_manager", mock_config_manager)

    await config_events_consumer.handle_action_config_created_event(_created_event(str(integration_v2.id), existing))

    assert not mock_config_manager.set_action_configuration.called
    assert not mock_config_manager.replace_cached_entry.called
    installed = mock_config_manager.install_action_configuration_if_missing.await_args.kwargs["config"]
    assert installed.json() == existing.json()


@pytest.mark.asyncio
async def test_action_config_updated_reconciliation_keeps_trying_against_the_newest_token(
        mocker, mock_config_manager, integration_v2, caplog,
):
    """A competing write that beats the reconciling compare-and-set is only
    later in cache time, not necessarily newer portal state: a delayed older
    Updated can land after the reconciliation fetched the final portal row.
    Acknowledging the event there would leave that stale value permanent with
    nothing to repair it. The reconciliation re-reads and retries against the
    newest token, bounded, and reports at error level if it still loses."""
    from app.services import config_events_consumer

    existing = integration_v2.configurations[0]
    n = config_events_consumer.UPDATE_ATTEMPTS
    mock_config_manager.read_cached_action_configuration = AsyncMock(
        side_effect=[(existing.copy(), existing.json())] * n + [(existing.copy(), '{"id": "t1"}'), (existing.copy(), '{"id": "t2"}')],
    )
    mock_config_manager.replace_cached_entry = AsyncMock(side_effect=[False] * n + [False, True])
    mock_config_manager._fetch_integration_from_gundi = AsyncMock(return_value=integration_v2)
    mock_config_manager.set_action_configuration = AsyncMock()
    mocker.patch.object(config_events_consumer, "config_manager", mock_config_manager)

    await config_events_consumer.handle_action_config_updated_event(
        _updated_event(str(integration_v2.id), existing.action.value, {"data": {"lookback_days": 2}})
    )

    assert mock_config_manager.replace_cached_entry.await_count == n + 2
    assert [c.kwargs["observed"] for c in mock_config_manager.replace_cached_entry.await_args_list[n:]] == ['{"id": "t1"}', '{"id": "t2"}']
    # A lost compare-and-set means the portal may have moved too (the write
    # that won came from a newer event): re-fetch on every retry.
    assert mock_config_manager._fetch_integration_from_gundi.await_count == 2
    assert not any(r.levelname == "ERROR" for r in caplog.records)


@pytest.mark.asyncio
async def test_action_config_updated_reconciliation_that_keeps_losing_is_reported_as_an_error(
        mocker, mock_config_manager, integration_v2, caplog,
):
    from app.services import config_events_consumer

    existing = integration_v2.configurations[0]
    mock_config_manager.read_cached_action_configuration = AsyncMock(return_value=(existing.copy(), existing.json()))
    mock_config_manager.replace_cached_entry = AsyncMock(return_value=False)
    mock_config_manager._fetch_integration_from_gundi = AsyncMock(return_value=integration_v2)
    mock_config_manager.set_action_configuration = AsyncMock()
    mocker.patch.object(config_events_consumer, "config_manager", mock_config_manager)

    await config_events_consumer.handle_action_config_updated_event(
        _updated_event(str(integration_v2.id), existing.action.value, {"data": {"lookback_days": 2}})
    )

    n = config_events_consumer.UPDATE_ATTEMPTS
    assert mock_config_manager.replace_cached_entry.await_count == 2 * n, "the update's attempts, then the reconciliation's"
    errors = [r.getMessage() for r in caplog.records if r.levelname == "ERROR"]
    assert errors and "may be stale" in errors[-1]


@pytest.mark.asyncio
async def test_action_config_updated_reconciliation_refetches_the_portal_on_every_retry(
        mocker, mock_config_manager, integration_v2,
):
    """Reusing the first portal snapshot across retries would let the loop write
    stale row A over row B after B's Created/Updated landed in both the portal
    and the cache and made the first compare-and-set lose."""
    from app.services import config_events_consumer

    existing = integration_v2.configurations[0]
    row_a = existing
    row_b = existing.copy(update={"data": {**existing.data, "lookback_days": 42}})
    portal_a = integration_v2
    portal_b = integration_v2.copy(update={"configurations": [row_b] + list(integration_v2.configurations[1:])})
    n = config_events_consumer.UPDATE_ATTEMPTS
    mock_config_manager.read_cached_action_configuration = AsyncMock(
        side_effect=[(existing.copy(), existing.json())] * n + [(existing.copy(), "token-1"), (row_b.copy(), row_b.json())],
    )
    mock_config_manager.replace_cached_entry = AsyncMock(side_effect=[False] * n + [False, True])
    mock_config_manager._fetch_integration_from_gundi = AsyncMock(side_effect=[portal_a, portal_b])
    mock_config_manager.set_action_configuration = AsyncMock()
    mocker.patch.object(config_events_consumer, "config_manager", mock_config_manager)

    await config_events_consumer.handle_action_config_updated_event(
        _updated_event(str(integration_v2.id), existing.action.value, {"data": {"lookback_days": 2}})
    )

    final = mock_config_manager.replace_cached_entry.await_args.kwargs
    assert final["observed"] == row_b.json()
    assert final["config"].json() == row_b.json(), "the portal as re-read on the retry, not the first snapshot"


@pytest.mark.asyncio
async def test_action_config_created_that_loses_its_write_reconciles_when_the_winner_is_a_config(
        mocker, mock_config_manager, integration_v2,
):
    """A failed Created compare-and-set is not proof that the winner is newer
    portal state: a delayed older Updated can land after this handler's read.
    Re-read the winner; if it is a configuration, run the same bounded fresh-
    portal reconciliation the Updated handler uses."""
    from app.services import config_events_consumer

    existing = integration_v2.configurations[0]
    stale = existing.copy(update={"data": {**existing.data, "lookback_days": 1}})
    mock_config_manager.read_cached_action_configuration = AsyncMock(
        side_effect=[(None, "null:e:1:x"), (stale.copy(), stale.json())],
    )
    mock_config_manager._fetch_integration_from_gundi = AsyncMock(return_value=integration_v2)
    mock_config_manager.replace_cached_entry = AsyncMock(side_effect=[False, True])
    mock_config_manager.install_action_configuration_if_missing = AsyncMock(return_value=True)
    mock_config_manager.set_action_configuration = AsyncMock()
    mocker.patch.object(config_events_consumer, "config_manager", mock_config_manager)

    await config_events_consumer.handle_action_config_created_event(_created_event(str(integration_v2.id), existing))

    assert mock_config_manager.replace_cached_entry.await_count == 2
    second = mock_config_manager.replace_cached_entry.await_args_list[1].kwargs
    assert second["observed"] == stale.json()
    assert second["config"].json() == existing.json(), "the portal row over the stale winner"


@pytest.mark.asyncio
async def test_action_config_created_that_loses_its_write_stops_when_the_winner_is_a_tombstone(
        mocker, mock_config_manager, integration_v2,
):
    from app.services import config_events_consumer

    existing = integration_v2.configurations[0]
    mock_config_manager.read_cached_action_configuration = AsyncMock(
        side_effect=[(None, "null:e:1:x"), (None, "null:e:2:y")],
    )
    mock_config_manager._fetch_integration_from_gundi = AsyncMock(return_value=integration_v2)
    mock_config_manager.replace_cached_entry = AsyncMock(return_value=False)
    mock_config_manager.install_action_configuration_if_missing = AsyncMock(return_value=True)
    mock_config_manager.set_action_configuration = AsyncMock()
    mocker.patch.object(config_events_consumer, "config_manager", mock_config_manager)

    await config_events_consumer.handle_action_config_created_event(_created_event(str(integration_v2.id), existing))

    assert mock_config_manager.replace_cached_entry.await_count == 1
    assert mock_config_manager._fetch_integration_from_gundi.await_count == 1, "no second fetch over a fresh tombstone"
    assert not mock_config_manager.install_action_configuration_if_missing.called
