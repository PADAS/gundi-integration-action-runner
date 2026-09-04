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
    assert mock_config_manager.set_action_configuration.called


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
    assert mock_config_manager.set_action_configuration.called


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
    mock_config_manager.replace_absence_sentinel = AsyncMock(return_value=True)
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
    kwargs = mock_config_manager.replace_absence_sentinel.await_args.kwargs
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
    mock_config_manager.read_cached_action_configuration = AsyncMock(side_effect=[(None, "null:gen1"), (newer, None)])
    mock_config_manager._fetch_integration_from_gundi = AsyncMock(return_value=integration_v2)
    mock_config_manager.set_action_configuration = AsyncMock()
    mock_config_manager.replace_absence_sentinel = AsyncMock(return_value=False)
    mocker.patch.object(config_events_consumer, "config_manager", mock_config_manager)

    await config_events_consumer.handle_action_config_updated_event(
        _updated_event(str(integration_v2.id), existing.action.value, {"data": {"lookback_days": 2}})
    )

    assert mock_config_manager.read_cached_action_configuration.await_count == 2
    saved = mock_config_manager.set_action_configuration.await_args.kwargs["config"]
    assert saved.data == {"lookback_days": 2}, "this event's changes, applied to the value that won"
    assert saved.id == newer.id


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
    mock_config_manager.replace_absence_sentinel = AsyncMock(return_value=True)
    mocker.patch.object(config_events_consumer, "config_manager", mock_config_manager)

    await config_events_consumer.handle_action_config_updated_event(
        _updated_event(str(integration_v2.id), unconfigured, {"data": {"lookback_days": 2}})
    )

    assert not mock_config_manager.set_action_configuration.called
    assert not mock_config_manager.replace_absence_sentinel.called


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
    mock_config_manager.replace_absence_sentinel = AsyncMock(return_value=False)
    mocker.patch.object(config_events_consumer, "config_manager", mock_config_manager)

    await config_events_consumer.handle_action_config_updated_event(
        _updated_event(str(integration_v2.id), existing.action.value, {"data": {"lookback_days": 2}})
    )

    assert mock_config_manager.replace_absence_sentinel.await_args.kwargs["observed"] == "null:gen1"
    assert not mock_config_manager.set_action_configuration.called


@pytest.mark.asyncio
async def test_action_config_updated_on_a_cold_cache_installs_the_fetched_row_only_if_still_missing(
        mocker, mock_config_manager, integration_v2,
):
    """Nothing cached for this action (cold cache or an expired sentinel). The
    ordinary lookup would run the full portal reload, whose unconditional SETs
    of configured actions can overwrite a tombstone written by a concurrent
    ActionConfigDeleted after the reload's fetch; the update would then
    persist that stale config. The recovery instead fetches the row without
    touching the cache and installs it with SET NX, so anything written
    meanwhile wins."""
    from app.services import config_events_consumer

    existing = integration_v2.configurations[0]
    mock_config_manager.read_cached_action_configuration = AsyncMock(return_value=(None, None))
    mock_config_manager._fetch_integration_from_gundi = AsyncMock(return_value=integration_v2)
    mock_config_manager._reload_integration_from_gundi = AsyncMock(return_value=integration_v2)
    mock_config_manager.install_action_configuration_if_missing = AsyncMock(return_value=True)
    mock_config_manager.replace_absence_sentinel = AsyncMock(return_value=True)
    mock_config_manager.set_action_configuration = AsyncMock()
    mocker.patch.object(config_events_consumer, "config_manager", mock_config_manager)

    await config_events_consumer.handle_action_config_updated_event(
        _updated_event(str(integration_v2.id), existing.action.value, {"data": {"lookback_days": 2}})
    )

    assert not mock_config_manager._reload_integration_from_gundi.called
    assert not mock_config_manager.replace_absence_sentinel.called
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
