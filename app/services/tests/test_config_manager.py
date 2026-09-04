from app.conftest import async_return
from unittest.mock import AsyncMock
import pytest

from gundi_core.schemas.v2 import IntegrationSummary, IntegrationActionConfiguration, Integration, WebhookConfiguration
from app.services.config_manager import (
    ACTION_ABSENCE_SENTINEL_TTL_SECONDS,
    IntegrationConfigurationManager,
    WEBHOOK_CONFIG_DEFAULT_TTL_SECONDS,
)


@pytest.mark.asyncio
async def test_get_integration_from_redis(
        mocker, mock_redis_with_integration_config, mock_gundi_client_v2_class, integration_v2,
):
    mocker.patch("app.services.config_manager.redis", mock_redis_with_integration_config)
    mocker.patch("app.services.config_manager.GundiClient", mock_gundi_client_v2_class)
    config_manager = IntegrationConfigurationManager()
    integration_id = str(integration_v2.id)

    integration = await config_manager.get_integration(integration_id)

    assert integration
    assert isinstance(integration, IntegrationSummary)
    assert integration.id == integration_v2.id
    mock_redis_with_integration_config.Redis.return_value.get.assert_called_once_with(f"integration.{integration_id}")
    assert not mock_gundi_client_v2_class.return_value.get_integration_details.called


@pytest.mark.asyncio
async def test_get_integration_from_gundi(
        mocker, mock_redis_empty, mock_gundi_client_v2_class, integration_v2,
):
    mocker.patch("app.services.config_manager.redis", mock_redis_empty)
    mocker.patch("app.services.config_manager.GundiClient", mock_gundi_client_v2_class)
    config_manager = IntegrationConfigurationManager()
    integration_id = str(integration_v2.id)

    integration = await config_manager.get_integration(integration_id)

    assert integration
    assert isinstance(integration, IntegrationSummary)
    assert integration.id == integration_v2.id
    mock_redis_empty.Redis.return_value.get.assert_called_once_with(f"integration.{integration_id}")
    mock_gundi_client_v2_class.return_value.get_integration_details.assert_called_once_with(integration_id)


@pytest.mark.asyncio
async def test_set_integration(mocker, mock_redis_empty, mock_gundi_client_v2_class, integration_v2):
    mocker.patch("app.services.config_manager.redis", mock_redis_empty)
    mocker.patch("app.services.config_manager.GundiClient", mock_gundi_client_v2_class)
    config_manager = IntegrationConfigurationManager()

    await config_manager.set_integration(integration_v2)

    mock_redis_empty.Redis.return_value.set.assert_called_once_with(
        f"integration.{integration_v2.id}",
        integration_v2.json(),
        None  # Never expire
    )


@pytest.mark.asyncio
async def test_get_action_configuration_from_redis(
        mocker, mock_redis_with_action_config, mock_gundi_client_v2_class, integration_v2,
):
    mocker.patch("app.services.config_manager.redis", mock_redis_with_action_config)
    mocker.patch("app.services.config_manager.GundiClient", mock_gundi_client_v2_class)
    config_manager = IntegrationConfigurationManager()
    integration_id = str(integration_v2.id)
    action_v2 = integration_v2.configurations[0].action
    action_id = action_v2.value

    action_config = await config_manager.get_action_configuration(integration_id, action_id)

    assert action_config
    assert isinstance(action_config, IntegrationActionConfiguration)
    mock_redis_with_action_config.Redis.return_value.get.assert_called_once_with(f"integrationconfig.{integration_id}.{action_id}")


@pytest.mark.asyncio
async def test_get_action_configuration_from_gundi(
        mocker, mock_redis_empty, mock_gundi_client_v2_class, integration_v2,
):
    mocker.patch("app.services.config_manager.redis", mock_redis_empty)
    mocker.patch("app.services.config_manager.GundiClient", mock_gundi_client_v2_class)
    config_manager = IntegrationConfigurationManager()
    integration_id = str(integration_v2.id)
    action_v2 = integration_v2.configurations[0].action
    action_id = action_v2.value

    action_config = await config_manager.get_action_configuration(integration_id, action_id)

    assert action_config
    assert isinstance(action_config, IntegrationActionConfiguration)
    mock_redis_empty.Redis.return_value.get.assert_called_once_with(f"integrationconfig.{integration_id}.{action_id}")
    mock_gundi_client_v2_class.return_value.get_integration_details.assert_called_once_with(integration_id)


@pytest.mark.asyncio
async def test_get_integration_details_with_empty_redis_db(
        mocker, mock_redis_empty, mock_gundi_client_v2_class, integration_v2,
):
    mocker.patch("app.services.config_manager.redis", mock_redis_empty)
    mocker.patch("app.services.config_manager.GundiClient", mock_gundi_client_v2_class)
    config_manager = IntegrationConfigurationManager()
    integration_id = str(integration_v2.id)

    integration = await config_manager.get_integration_details(integration_id)

    assert integration
    assert isinstance(integration, Integration)
    assert len(integration.configurations) == len(integration_v2.configurations)
    assert integration.id == integration_v2.id
    mock_gundi_client_v2_class.return_value.get_integration_details.assert_called_with(integration_id)
    for config in integration_v2.configurations:
        action_id = config.action.value
        mock_redis_empty.Redis.return_value.get.assert_any_call(f"integrationconfig.{integration_id}.{action_id}")


# TTL Feature Tests

@pytest.mark.asyncio
async def test_get_integration_with_ttl(
        mocker, mock_redis_empty, mock_gundi_client_v2_class, integration_v2,
):
    mocker.patch("app.services.config_manager.redis", mock_redis_empty)
    mocker.patch("app.services.config_manager.GundiClient", mock_gundi_client_v2_class)
    config_manager = IntegrationConfigurationManager()
    integration_id = str(integration_v2.id)
    ttl = 3600

    integration = await config_manager.get_integration(integration_id, ttl=ttl)

    assert integration
    assert isinstance(integration, IntegrationSummary)
    assert integration.id == integration_v2.id
    # Verify that set was called with TTL for integration
    mock_redis_empty.Redis.return_value.set.assert_any_call(
        f"integration.{integration_id}",
        integration.json(),
        ttl
    )


@pytest.mark.asyncio
async def test_get_action_configuration_with_ttl(
        mocker, mock_redis_empty, mock_gundi_client_v2_class, integration_v2,
):
    mocker.patch("app.services.config_manager.redis", mock_redis_empty)
    mocker.patch("app.services.config_manager.GundiClient", mock_gundi_client_v2_class)
    config_manager = IntegrationConfigurationManager()
    integration_id = str(integration_v2.id)
    action_v2 = integration_v2.configurations[0].action
    action_id = action_v2.value
    ttl = 1800

    action_config = await config_manager.get_action_configuration(integration_id, action_id, ttl=ttl)

    assert action_config
    assert isinstance(action_config, IntegrationActionConfiguration)
    # Verify the configured action was written through the snapshot script
    # with the caller's TTL (the script preserves a newer tombstone, so this
    # write is an EVAL rather than a plain SET).
    snapshot_writes = {c.args[2]: c.args for c in mock_redis_empty.Redis.return_value.eval.call_args_list}
    _, _, _, written_json, _, written_ttl = snapshot_writes[f"integrationconfig.{integration_id}.{action_id}"]
    assert (written_json, written_ttl) == (action_config.json(), ttl)
    # Verify that integration was also saved with TTL
    mock_redis_empty.Redis.return_value.set.assert_any_call(
        f"integration.{integration_id}",
        mocker.ANY,  # The integration summary JSON
        ttl
    )


@pytest.mark.asyncio
async def test_set_action_configuration_with_ttl(
        mocker, mock_redis_empty, mock_gundi_client_v2_class, integration_v2,
):
    mocker.patch("app.services.config_manager.redis", mock_redis_empty)
    mocker.patch("app.services.config_manager.GundiClient", mock_gundi_client_v2_class)
    config_manager = IntegrationConfigurationManager()
    integration_id = str(integration_v2.id)
    action_v2 = integration_v2.configurations[0]
    action_id = action_v2.action.value
    ttl = 7200

    await config_manager.set_action_configuration(integration_id, action_id, action_v2, ttl=ttl)

    mock_redis_empty.Redis.return_value.set.assert_called_once_with(
        f"integrationconfig.{integration_id}.{action_id}",
        action_v2.json(),
        ttl
    )


@pytest.mark.asyncio
async def test_set_integration_with_ttl(
        mocker, mock_redis_empty, mock_gundi_client_v2_class, integration_v2,
):
    mocker.patch("app.services.config_manager.redis", mock_redis_empty)
    mocker.patch("app.services.config_manager.GundiClient", mock_gundi_client_v2_class)
    config_manager = IntegrationConfigurationManager()
    ttl = 900

    await config_manager.set_integration(integration_v2, ttl=ttl)

    mock_redis_empty.Redis.return_value.set.assert_called_once_with(
        f"integration.{integration_v2.id}",
        integration_v2.json(),
        ttl
    )


# Webhook Configuration Tests

@pytest.mark.asyncio
async def test_get_webhook_configuration_from_redis(
        mocker, mock_redis_with_webhook_config, mock_gundi_client_v2_class, integration_v2_with_webhook,
):
    mocker.patch("app.services.config_manager.redis", mock_redis_with_webhook_config)
    mocker.patch("app.services.config_manager.GundiClient", mock_gundi_client_v2_class)
    config_manager = IntegrationConfigurationManager()
    integration_id = str(integration_v2_with_webhook.id)

    webhook_config = await config_manager.get_webhook_configuration(integration_id)

    assert webhook_config
    assert isinstance(webhook_config, WebhookConfiguration)
    mock_redis_with_webhook_config.Redis.return_value.get.assert_called_once_with(
        f"integrationconfig.{integration_id}.webhook"
    )
    assert not mock_gundi_client_v2_class.return_value.get_integration_details.called


@pytest.mark.asyncio
async def test_get_webhook_configuration_from_gundi(
        mocker, mock_redis_empty, mock_gundi_client_v2_class, integration_v2_with_webhook,
):
    mocker.patch("app.services.config_manager.redis", mock_redis_empty)
    mocker.patch("app.services.config_manager.GundiClient", mock_gundi_client_v2_class)
    # Override the get_integration_details method to return webhook integration
    mock_gundi_client_v2_class.return_value.get_integration_details = mocker.AsyncMock(return_value=integration_v2_with_webhook)
    
    config_manager = IntegrationConfigurationManager()
    integration_id = str(integration_v2_with_webhook.id)

    webhook_config = await config_manager.get_webhook_configuration(integration_id)

    assert webhook_config
    assert isinstance(webhook_config, WebhookConfiguration)
    mock_redis_empty.Redis.return_value.get.assert_called_once_with(
        f"integrationconfig.{integration_id}.webhook"
    )
    mock_gundi_client_v2_class.return_value.get_integration_details.assert_called_once_with(integration_id)


@pytest.mark.asyncio
async def test_get_webhook_configuration_with_ttl(
        mocker, mock_redis_empty, mock_gundi_client_v2_class, integration_v2_with_webhook,
):
    mocker.patch("app.services.config_manager.redis", mock_redis_empty)
    mocker.patch("app.services.config_manager.GundiClient", mock_gundi_client_v2_class)
    # Override the get_integration_details method to return webhook integration
    mock_gundi_client_v2_class.return_value.get_integration_details = mocker.AsyncMock(return_value=integration_v2_with_webhook)
    
    config_manager = IntegrationConfigurationManager()
    integration_id = str(integration_v2_with_webhook.id)
    ttl = 2400

    webhook_config = await config_manager.get_webhook_configuration(integration_id, ttl=ttl)

    assert webhook_config
    assert isinstance(webhook_config, WebhookConfiguration)
    # Verify that set was called with TTL for webhook config
    mock_redis_empty.Redis.return_value.set.assert_any_call(
        f"integrationconfig.{integration_id}.webhook",
        webhook_config.json(),
        ttl
    )


@pytest.mark.asyncio
async def test_get_integration_details_with_webhook_configuration(
        mocker, mock_redis_empty, mock_gundi_client_v2_class, integration_v2_with_webhook,
):
    mocker.patch("app.services.config_manager.redis", mock_redis_empty)
    mocker.patch("app.services.config_manager.GundiClient", mock_gundi_client_v2_class)
    # Override the get_integration_details method to return webhook integration
    mock_gundi_client_v2_class.return_value.get_integration_details = mocker.AsyncMock(return_value=integration_v2_with_webhook)
    
    config_manager = IntegrationConfigurationManager()
    integration_id = str(integration_v2_with_webhook.id)

    integration = await config_manager.get_integration_details(integration_id)

    assert integration
    assert isinstance(integration, Integration)
    assert integration.webhook_configuration is not None
    assert isinstance(integration.webhook_configuration, WebhookConfiguration)
    # Verify webhook config was fetched
    mock_redis_empty.Redis.return_value.get.assert_any_call(
        f"integrationconfig.{integration_id}.webhook"
    )


@pytest.mark.asyncio
async def test_get_integration_details_with_ttl(
        mocker, mock_redis_empty, mock_gundi_client_v2_class, integration_v2_with_webhook,
):
    mocker.patch("app.services.config_manager.redis", mock_redis_empty)
    mocker.patch("app.services.config_manager.GundiClient", mock_gundi_client_v2_class)
    # Override the get_integration_details method to return webhook integration
    mock_gundi_client_v2_class.return_value.get_integration_details = mocker.AsyncMock(return_value=integration_v2_with_webhook)
    
    config_manager = IntegrationConfigurationManager()
    integration_id = str(integration_v2_with_webhook.id)
    ttl = 3000

    integration = await config_manager.get_integration_details(integration_id, ttl=ttl)

    assert integration
    assert isinstance(integration, Integration)
    # Verify that all set operations were called with TTL
    set_calls = mock_redis_empty.Redis.return_value.set.call_args_list
    for call in set_calls:
        assert call[0][2] == ttl  # TTL is the third argument



@pytest.mark.asyncio
async def test_get_webhook_configuration_caches_absence_sentinel(
        mocker, mock_redis_empty, mock_gundi_client_v2_class, integration_v2,
):
    mocker.patch("app.services.config_manager.redis", mock_redis_empty)
    mocker.patch("app.services.config_manager.GundiClient", mock_gundi_client_v2_class)
    config_manager = IntegrationConfigurationManager()
    integration_id = str(integration_v2.id)

    webhook_config = await config_manager.get_webhook_configuration(integration_id)

    assert webhook_config is None
    # The absence is cached, so the next cold lookup won't reload from Gundi --
    # but always with an expiry even though this caller passed no TTL (the
    # action path's full reload reaches here with ttl=None): there is no
    # WebhookConfig* event to invalidate the sentinel on, so a permanent one
    # would keep serving "no webhook" after an operator adds a config in the
    # portal, breaking every delivery until the key was deleted by hand.
    mock_redis_empty.Redis.return_value.set.assert_any_call(
        f"integrationconfig.{integration_id}.webhook", "null", WEBHOOK_CONFIG_DEFAULT_TTL_SECONDS
    )


@pytest.mark.asyncio
async def test_get_webhook_configuration_reads_cached_absence_sentinel(
        mocker, mock_redis_empty, mock_gundi_client_v2_class, integration_v2,
):
    import asyncio as _asyncio
    fut = _asyncio.get_running_loop().create_future()
    fut.set_result(b"null")
    mock_redis_empty.Redis.return_value.get.return_value = fut
    mocker.patch("app.services.config_manager.redis", mock_redis_empty)
    mocker.patch("app.services.config_manager.GundiClient", mock_gundi_client_v2_class)
    config_manager = IntegrationConfigurationManager()

    webhook_config = await config_manager.get_webhook_configuration(str(integration_v2.id))

    assert webhook_config is None
    # Sentinel hit — no reload from the Gundi API.
    assert not mock_gundi_client_v2_class.return_value.get_integration_details.called


@pytest.mark.asyncio
async def test_present_webhook_config_gets_the_default_ttl_when_caller_passes_none(
        mocker, mock_redis_empty, mock_gundi_client_v2_class_for_webhooks, integration_v2_with_webhook,
):
    """Same reasoning as the sentinel: no WebhookConfig* event invalidates the
    key, so an unbounded write from the action path (ttl=None) would serve a
    stale config after an operator edits it -- and, since SET clears any TTL,
    would also undo the webhook path's 60 s cache of the same key."""
    mocker.patch("app.services.config_manager.redis", mock_redis_empty)
    mocker.patch("app.services.config_manager.GundiClient", mock_gundi_client_v2_class_for_webhooks)
    config_manager = IntegrationConfigurationManager()
    integration_id = str(integration_v2_with_webhook.id)

    webhook_config = await config_manager.get_webhook_configuration(integration_id, ttl=None)

    assert webhook_config == integration_v2_with_webhook.webhook_configuration
    mock_redis_empty.Redis.return_value.set.assert_any_call(
        f"integrationconfig.{integration_id}.webhook",
        integration_v2_with_webhook.webhook_configuration.json(),
        WEBHOOK_CONFIG_DEFAULT_TTL_SECONDS,
    )


@pytest.mark.asyncio
async def test_deleting_an_integration_drops_every_derived_key_in_one_call(
        mocker, mock_redis_with_integration_config, mock_gundi_client_v2_class, integration_v2,
):
    """The webhook key and the per-action config keys are addressed separately
    from the integration key, so they would otherwise outlive their owner -- a
    stale absence sentinel would be served to whatever integration next claimed
    the id. One variadic DEL leaves no window in which some keys are gone and
    others orphaned."""
    mocker.patch("app.services.config_manager.redis", mock_redis_with_integration_config)
    mocker.patch("app.services.config_manager.GundiClient", mock_gundi_client_v2_class)
    config_manager = IntegrationConfigurationManager()
    integration_id = str(integration_v2.id)

    await config_manager.delete_integration(integration_id)

    delete = mock_redis_with_integration_config.Redis.return_value.delete
    assert delete.call_count == 1
    deleted = set(delete.call_args.args)
    assert f"integration.{integration_id}" in deleted
    assert f"integrationconfig.{integration_id}.webhook" in deleted
    expected_action_keys = {f"integrationconfig.{integration_id}.{a.value}" for a in integration_v2.type.actions}
    assert expected_action_keys and expected_action_keys <= deleted


@pytest.mark.asyncio
async def test_deleting_an_uncached_integration_still_drops_the_keys_it_can_name(
        mocker, mock_redis_empty, mock_gundi_client_v2_class, integration_v2,
):
    mocker.patch("app.services.config_manager.redis", mock_redis_empty)
    mocker.patch("app.services.config_manager.GundiClient", mock_gundi_client_v2_class)
    config_manager = IntegrationConfigurationManager()
    integration_id = str(integration_v2.id)

    await config_manager.delete_integration(integration_id)

    delete = mock_redis_empty.Redis.return_value.delete
    assert delete.call_count == 1
    assert set(delete.call_args.args) == {
        f"integration.{integration_id}", f"integrationconfig.{integration_id}.webhook",
    }


@pytest.mark.asyncio
async def test_get_integration_details_can_skip_the_webhook_key(
        mocker, mock_redis_empty, mock_gundi_client_v2_class, integration_v2, integration_v2_as_json,
        pull_observations_config_as_json,
):
    """The webhook key is the only cache entry with a TTL nothing invalidates, so
    it is the only one that can miss on a warm integration. The action runner
    never reads it; letting it opt out keeps warm action runs off the portal."""
    def cached(key):
        if key.startswith("integration."):
            return async_return(integration_v2_as_json)
        if key.endswith(".webhook"):
            return async_return(None)  # expired
        return async_return(pull_observations_config_as_json)
    mock_redis_empty.Redis.return_value.get.side_effect = cached
    mocker.patch("app.services.config_manager.redis", mock_redis_empty)
    mocker.patch("app.services.config_manager.GundiClient", mock_gundi_client_v2_class)
    config_manager = IntegrationConfigurationManager()

    integration = await config_manager.get_integration_details(str(integration_v2.id), include_webhook_config=False)

    assert integration.webhook_configuration is None
    assert not mock_gundi_client_v2_class.return_value.get_integration_details.called


@pytest.mark.asyncio
async def test_redis_retry_backoff_does_not_block_the_event_loop(
        mocker, mock_redis_empty, mock_gundi_client_v2_class, integration_v2,
):
    """stamina's sync iterator sleeps with time.sleep between attempts; inside a
    coroutine that freezes every other request on the worker for the length of
    the back-off. Every Redis loop here must iterate asynchronously."""
    import redis.asyncio as redis
    calls = {"n": 0}
    def flaky_get(key):
        calls["n"] += 1
        if calls["n"] == 1:
            raise redis.RedisError("flap")
        return async_return(None)
    mock_redis_empty.Redis.return_value.get.side_effect = flaky_get
    mocker.patch("app.services.config_manager.redis", mock_redis_empty)
    mocker.patch("app.services.config_manager.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("time.sleep", side_effect=AssertionError("retry back-off blocked the event loop"))
    mocker.patch("asyncio.sleep", AsyncMock())
    config_manager = IntegrationConfigurationManager()

    integration = await config_manager.get_integration(str(integration_v2.id))

    assert integration.id == integration_v2.id
    assert calls["n"] == 2


@pytest.mark.asyncio
async def test_deleting_an_integration_survives_a_failed_summary_read(
        mocker, mock_redis_empty, mock_gundi_client_v2_class, integration_v2,
):
    """If the GET that looks up the action list exhausts its retries, the two
    keys that can be named without it must still be deleted; otherwise the
    IntegrationDeleted event is acked and the permanent summary key outlives
    the integration."""
    import redis.asyncio as redis
    mock_redis_empty.Redis.return_value.get.side_effect = redis.RedisError("down")
    mocker.patch("app.services.config_manager.redis", mock_redis_empty)
    mocker.patch("app.services.config_manager.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("asyncio.sleep", AsyncMock())
    config_manager = IntegrationConfigurationManager()
    integration_id = str(integration_v2.id)

    await config_manager.delete_integration(integration_id)

    delete = mock_redis_empty.Redis.return_value.delete
    assert delete.call_count == 1
    assert set(delete.call_args.args) == {
        f"integration.{integration_id}", f"integrationconfig.{integration_id}.webhook",
    }


@pytest.mark.asyncio
async def test_webhook_key_miss_refreshes_only_the_webhook_key(
        mocker, mock_redis_empty, mock_gundi_client_v2_class, integration_v2_with_webhook,
):
    """The webhook key expires on its own; refreshing it through the full reload
    would rewrite the summary and action keys with the webhook path's ttl=60,
    downgrading the action path's permanent keys and sending the next action
    run back to the portal."""
    mocker.patch("app.services.config_manager.redis", mock_redis_empty)
    mocker.patch("app.services.config_manager.GundiClient", mock_gundi_client_v2_class)
    mock_gundi_client_v2_class.return_value.get_integration_details = mocker.AsyncMock(return_value=integration_v2_with_webhook)
    config_manager = IntegrationConfigurationManager()
    integration_id = str(integration_v2_with_webhook.id)

    await config_manager.get_webhook_configuration(integration_id, ttl=60)

    written = [c.args[0] for c in mock_redis_empty.Redis.return_value.set.call_args_list]
    assert written == [f"integrationconfig.{integration_id}.webhook"]


@pytest.mark.asyncio
async def test_reload_writes_absence_sentinels_for_unconfigured_actions(
        mocker, mock_redis_empty, mock_gundi_client_v2_class, integration_v2,
):
    """An integration configured for only some of its type's actions must not
    reload from the Gundi API on every run because the others keep missing."""
    mocker.patch("app.services.config_manager.redis", mock_redis_empty)
    mocker.patch("app.services.config_manager.GundiClient", mock_gundi_client_v2_class)
    config_manager = IntegrationConfigurationManager()
    integration_id = str(integration_v2.id)
    configured = {c.action.value for c in integration_v2.configurations}
    unconfigured = {a.value for a in integration_v2.type.actions} - configured
    assert unconfigured, "fixture must declare an action without a configuration"

    await config_manager.get_integration(integration_id)

    set_calls = {c.args[0]: c for c in mock_redis_empty.Redis.return_value.set.call_args_list}
    snapshot_writes = {c.args[2]: c for c in mock_redis_empty.Redis.return_value.eval.call_args_list}
    for action_id in unconfigured:
        call = set_calls[f"integrationconfig.{integration_id}.{action_id}"]
        assert call.args[1].startswith("null:"), "an absence sentinel with a per-write generation"
        assert call.kwargs.get("nx") is True, "absence sentinels must never replace an existing key"
    for action_id in configured:
        # Written through the snapshot script, which preserves a newer tombstone.
        assert not snapshot_writes[f"integrationconfig.{integration_id}.{action_id}"].args[3].startswith("null")


class _FakeRedis:
    """Just enough of redis.asyncio for the reload/event interleaving tests: a
    dict with SET honouring nx, and an eval that emulates the module's scripts
    (matched by identity) so the interleavings exercise their real decisions."""
    def __init__(self):
        self.data = {}

    async def get(self, key):
        return self.data.get(key)

    async def set(self, key, value, ex=None, nx=False):
        if nx and key in self.data:
            return None
        self.data[key] = value
        return True

    async def delete(self, *keys):
        return sum(self.data.pop(k, None) is not None for k in keys)

    async def ttl(self, key):
        return -1 if key in self.data else -2

    async def eval(self, script, numkeys, key, *args):
        import re
        from app.services import config_manager as cm
        current = self.data.get(key)
        if script is cm._WRITE_SNAPSHOT_CONFIG_SCRIPT:
            value, fetch_started_ns, ttl = args
            m = re.match(r"^null:(\d+):", current) if isinstance(current, str) else None
            if m and int(m.group(1)) >= int(fetch_started_ns):
                return 0
            self.data[key] = value
            return 1
        if script is cm._REPLACE_CACHED_ENTRY_SCRIPT:
            observed, value = args[0], args[1]
            if current != observed:
                return 0
            self.data[key] = value
            return 1
        if script is cm._EXPIRE_LEGACY_SENTINEL_SCRIPT:
            return 0
        raise AssertionError(f"unexpected script: {script[:40]}")


@pytest.mark.asyncio
async def test_stale_reload_does_not_overwrite_a_concurrently_created_action_config(
        mocker, mock_gundi_client_v2_class, integration_v2, pull_observations_config_as_json,
):
    """Copilot on #102: a reload reads the portal before an action config exists,
    the ActionConfigCreated event caches the new config, then the reload's
    sentinel loop runs. The sentinel must not replace the config: nothing later
    would correct a permanent "null" for an action the portal now has."""
    mocker.patch("app.services.config_manager.GundiClient", mock_gundi_client_v2_class)
    config_manager = IntegrationConfigurationManager()
    fake = _FakeRedis()
    config_manager.db_client = fake
    integration_id = str(integration_v2.id)
    configured = {c.action.value for c in integration_v2.configurations}
    newly_created = next(a.value for a in integration_v2.type.actions if a.value not in configured)
    created_key = f"integrationconfig.{integration_id}.{newly_created}"

    # The portal snapshot the reload works from predates the creation...
    reload = config_manager._reload_integration_from_gundi(integration_id)
    # ...and the event handler's write lands before the reload's sentinel loop.
    created = IntegrationActionConfiguration.parse_raw(pull_observations_config_as_json)
    await config_manager.set_action_configuration(integration_id, newly_created, created)
    await reload

    assert fake.data[created_key] == created.json()
    assert await config_manager.get_action_configuration(integration_id, newly_created) == created


@pytest.mark.asyncio
async def test_cached_action_absence_returns_none_without_reloading(
        mocker, mock_redis_empty, mock_gundi_client_v2_class, integration_v2,
):
    mock_redis_empty.Redis.return_value.get.return_value = async_return(b"null")
    mocker.patch("app.services.config_manager.redis", mock_redis_empty)
    mocker.patch("app.services.config_manager.GundiClient", mock_gundi_client_v2_class)
    config_manager = IntegrationConfigurationManager()

    config = await config_manager.get_action_configuration(str(integration_v2.id), "pull_events")

    assert config is None
    assert not mock_gundi_client_v2_class.return_value.get_integration_details.called


@pytest.mark.asyncio
async def test_deleting_an_action_configuration_records_its_absence(
        mocker, mock_redis_empty, mock_gundi_client_v2_class, integration_v2,
):
    """A dropped key would miss on the next lookup and reload the whole
    integration from the Gundi API, which would then report the same absence."""
    mocker.patch("app.services.config_manager.redis", mock_redis_empty)
    mocker.patch("app.services.config_manager.GundiClient", mock_gundi_client_v2_class)
    config_manager = IntegrationConfigurationManager()
    integration_id = str(integration_v2.id)

    await config_manager.delete_action_configuration(integration_id, "pull_events")

    (key, value, ttl), _ = mock_redis_empty.Redis.return_value.set.call_args
    assert (key, ttl) == (f"integrationconfig.{integration_id}.pull_events", ACTION_ABSENCE_SENTINEL_TTL_SECONDS)
    assert value.startswith("null:")
    assert not mock_redis_empty.Redis.return_value.delete.called


@pytest.mark.asyncio
async def test_every_absence_sentinel_write_has_its_own_generation(
        mocker, mock_redis_empty, mock_gundi_client_v2_class, integration_v2,
):
    """A recovery reads a sentinel, fetches the portal, then replaces the
    sentinel if it is still there. If a concurrent ActionConfigDeleted wrote a
    new sentinel in between and both read "null", the recovery could not tell
    the newer tombstone from the one it observed and would resurrect the
    deleted config, permanently. Each write carries a generation instead."""
    mocker.patch("app.services.config_manager.redis", mock_redis_empty)
    mocker.patch("app.services.config_manager.GundiClient", mock_gundi_client_v2_class)
    config_manager = IntegrationConfigurationManager()
    integration_id = str(integration_v2.id)

    await config_manager.delete_action_configuration(integration_id, "pull_events")
    await config_manager.delete_action_configuration(integration_id, "pull_events")

    first, second = [c.args[1] for c in mock_redis_empty.Redis.return_value.set.call_args_list]
    assert first != second
    assert first.startswith("null:") and second.startswith("null:")


@pytest.mark.parametrize("stored", [b"null", b"null:0123abcd"])
@pytest.mark.asyncio
async def test_any_generation_of_the_sentinel_reads_as_absence(
        mocker, mock_redis_empty, mock_gundi_client_v2_class, integration_v2, stored,
):
    # Legacy sentinels are the bare value; both shapes mean "the portal has no
    # config for this action" and neither triggers a reload.
    client = mock_redis_empty.Redis.return_value
    client.get.return_value = async_return(stored)
    mocker.patch("app.services.config_manager.redis", mock_redis_empty)
    mocker.patch("app.services.config_manager.GundiClient", mock_gundi_client_v2_class)
    config_manager = IntegrationConfigurationManager()

    assert await config_manager.get_action_configuration(str(integration_v2.id), "pull_events") is None
    assert not mock_gundi_client_v2_class.return_value.get_integration_details.called


@pytest.mark.asyncio
async def test_read_cached_action_configuration_never_reloads_and_reports_what_the_cache_holds(
        mocker, mock_redis_empty, mock_gundi_client_v2_class, integration_v2, pull_observations_config_as_json,
):
    """The consumer's recovery compares-and-sets against what it saw in the
    cache, so its read must (a) come from one Redis GET, since a second read
    could observe a fresh tombstone from a concurrent ActionConfigDeleted, and
    (b) never fall through to the full portal reload, whose unconditional SETs
    of configured actions would overwrite a tombstone written after the
    reload's fetch. Three answers, each with the exact stored value as the
    token to compare-and-set against: a config, a sentinel, or nothing."""
    client = mock_redis_empty.Redis.return_value
    mocker.patch("app.services.config_manager.redis", mock_redis_empty)
    mocker.patch("app.services.config_manager.GundiClient", mock_gundi_client_v2_class)
    config_manager = IntegrationConfigurationManager()
    integration_id = str(integration_v2.id)

    client.get.return_value = async_return(b"null:0123abcd")
    assert await config_manager.read_cached_action_configuration(integration_id, "pull_events") == (None, "null:0123abcd")
    assert client.get.call_count == 1, "one read establishes both the absence and the generation"

    client.get.return_value = async_return(pull_observations_config_as_json.encode())
    config, token = await config_manager.read_cached_action_configuration(integration_id, "pull_observations")
    assert (config.json(), token) == (pull_observations_config_as_json, pull_observations_config_as_json), \
        "the raw stored value, so an update can compare-and-set against exactly what it read"

    client.get.return_value = async_return(None)
    assert await config_manager.read_cached_action_configuration(integration_id, "pull_observations") == (None, None)
    assert not mock_gundi_client_v2_class.return_value.get_integration_details.called, "never reloads"
@pytest.mark.asyncio
async def test_reload_absence_sentinels_keep_an_explicit_caller_ttl(
        mocker, mock_redis_empty, mock_gundi_client_v2_class, integration_v2,
):
    mocker.patch("app.services.config_manager.redis", mock_redis_empty)
    mocker.patch("app.services.config_manager.GundiClient", mock_gundi_client_v2_class)
    config_manager = IntegrationConfigurationManager()
    integration_id = str(integration_v2.id)
    configured = {c.action.value for c in integration_v2.configurations}
    unconfigured = {a.value for a in integration_v2.type.actions} - configured

    await config_manager.get_integration(integration_id, ttl=60)

    set_calls = {c.args[0]: c for c in mock_redis_empty.Redis.return_value.set.call_args_list}
    for action_id in unconfigured:
        assert set_calls[f"integrationconfig.{integration_id}.{action_id}"].args[2] == 60


@pytest.mark.asyncio
async def test_portal_reloads_are_blocked_on_the_ephemeral_path(
        mocker, mock_redis_empty, mock_gundi_client_v2_class, integration_v2,
):
    """A reference/auth handler running against a draft integration has no
    portal row: a reload would 404 and spin GUNDI_API_RETRY for up to two
    minutes on the request. Same guard _get_gundi_api_key applies, for the
    same reason, so the invariant holds by construction rather than because
    no handler happens to call the config manager today."""
    from app.services.activity_logger import ephemeral_run
    from app.services.gundi import EphemeralWriteBlocked

    mocker.patch("app.services.config_manager.redis", mock_redis_empty)
    mocker.patch("app.services.config_manager.GundiClient", mock_gundi_client_v2_class)
    config_manager = IntegrationConfigurationManager()
    integration_id = str(integration_v2.id)

    token = ephemeral_run.set(True)
    try:
        with pytest.raises(EphemeralWriteBlocked):
            await config_manager._reload_integration_from_gundi(integration_id)
        with pytest.raises(EphemeralWriteBlocked):
            await config_manager._reload_webhook_configuration_from_gundi(integration_id)
    finally:
        ephemeral_run.reset(token)

    assert not mock_gundi_client_v2_class.called
    assert not mock_redis_empty.Redis.return_value.set.called


@pytest.mark.asyncio
async def test_a_legacy_permanent_sentinel_gets_the_ttl_on_its_next_hit_atomically(
        mocker, mock_redis_empty, mock_gundi_client_v2_class, integration_v2,
):
    """Sentinels written before this release have no expiry, and the reload's
    NX write never touches an existing key, so without a read-time fix an
    integration already hit by the lost-event bug would stay "unconfigured"
    after rollout. A hit on a sentinel attaches the bounded TTL, in one
    server-side step: a separate TTL check and EXPIRE could race an
    ActionConfigCreated write in between and put the TTL on the real config,
    which must stay permanent. The value check, the "still permanent" check
    and the expiry therefore run as one script that only touches a key still
    holding the sentinel with no TTL; the sentinel still answers this lookup."""
    client = mock_redis_empty.Redis.return_value
    client.get.return_value = async_return(b"null")
    client.eval.return_value = async_return(1)
    mocker.patch("app.services.config_manager.redis", mock_redis_empty)
    mocker.patch("app.services.config_manager.GundiClient", mock_gundi_client_v2_class)
    config_manager = IntegrationConfigurationManager()
    integration_id = str(integration_v2.id)
    key = f"integrationconfig.{integration_id}.pull_events"

    config = await config_manager.get_action_configuration(integration_id, "pull_events")

    assert config is None
    assert not client.ttl.called and not client.expire.called, "must not be two separate commands"
    (script, numkeys, called_key, value, ttl), _ = client.eval.call_args
    assert (numkeys, called_key, value, ttl) == (1, key, "null", ACTION_ABSENCE_SENTINEL_TTL_SECONDS), \
        "the script compares against the value this read observed"
    # The script itself: expire only a key that still holds the sentinel and has no TTL.
    assert "GET" in script and "TTL" in script and "EXPIRE" in script and "-1" in script
    assert not mock_gundi_client_v2_class.return_value.get_integration_details.called


@pytest.mark.asyncio
async def test_replace_cached_entry_requires_the_exact_observed_value(
        mocker, mock_redis_empty, mock_gundi_client_v2_class, integration_v2, pull_observations_config_as_json,
):
    """Every consumer write is read-modify-write: it must not land on top of a
    value another concurrent delivery wrote in between, so the value check and
    the SET are one script, and the key must equal exactly what the caller
    read, a sentinel generation or a config's raw JSON. An expired key is not
    a match either: a newer tombstone written during a slow fetch can itself
    have expired by the time the script runs, and treating "missing" as a
    match would install the stale config. The return value tells the caller
    whether its value won."""
    client = mock_redis_empty.Redis.return_value
    client.eval.return_value = async_return(1)
    mocker.patch("app.services.config_manager.redis", mock_redis_empty)
    mocker.patch("app.services.config_manager.GundiClient", mock_gundi_client_v2_class)
    config_manager = IntegrationConfigurationManager()
    integration_id = str(integration_v2.id)
    config = IntegrationActionConfiguration.parse_raw(pull_observations_config_as_json)

    won = await config_manager.replace_cached_entry(
        integration_id, "pull_observations", config=config, observed="null:0123abcd",
    )

    assert won is True
    (script, numkeys, key, sentinel, value, ttl), _ = client.eval.call_args
    assert (numkeys, key, sentinel, value, ttl) == (
        1, f"integrationconfig.{integration_id}.pull_observations", "null:0123abcd", config.json(), "",
    )
    assert "GET" in script and "SET" in script
    assert "false" not in script, "a missing (expired) key must not count as the observed sentinel"
    assert not client.set.called

    client.eval.return_value = async_return(0)
    assert await config_manager.replace_cached_entry(
        integration_id, "pull_observations", config=config, observed="null:0123abcd",
    ) is False

    # The same primitive updates a real configuration against its raw JSON.
    client.eval.return_value = async_return(1)
    stale_raw = '{"id": "x"}'
    assert await config_manager.replace_cached_entry(
        integration_id, "pull_observations", config=config, observed=stale_raw,
    ) is True
    assert client.eval.call_args.args[3] == stale_raw


@pytest.mark.asyncio
async def test_install_action_configuration_if_missing_is_a_single_conditional_write(
        mocker, mock_redis_empty, mock_gundi_client_v2_class, integration_v2, pull_observations_config_as_json,
):
    """Cold-cache recovery: the consumer saw nothing cached, fetched the portal
    row, and installs it only if the key is still missing (SET NX), so a
    tombstone or config written meanwhile wins. Permanent, like every real
    configuration; the caller learns whether its write happened."""
    client = mock_redis_empty.Redis.return_value
    mocker.patch("app.services.config_manager.redis", mock_redis_empty)
    mocker.patch("app.services.config_manager.GundiClient", mock_gundi_client_v2_class)
    config_manager = IntegrationConfigurationManager()
    integration_id = str(integration_v2.id)
    config = IntegrationActionConfiguration.parse_raw(pull_observations_config_as_json)

    client.set.return_value = async_return(True)
    assert await config_manager.install_action_configuration_if_missing(integration_id, "pull_observations", config=config) is True
    client.set.assert_called_once_with(f"integrationconfig.{integration_id}.pull_observations", config.json(), None, nx=True)
    assert not client.eval.called

    client.set.return_value = async_return(None)  # redis: NX not applied
    assert await config_manager.install_action_configuration_if_missing(integration_id, "pull_observations", config=config) is False
@pytest.mark.parametrize("cached", ["webhook-config", "sentinel"])
@pytest.mark.asyncio
async def test_a_legacy_permanent_webhook_entry_gets_the_default_ttl_on_its_next_hit(
        mocker, mock_redis_empty, mock_gundi_client_v2_class, integration_v2_with_webhook, cached,
):
    """Before the webhook key carried a TTL, the action path's reload
    (ttl=None) wrote both real webhook configs and the "null" sentinel
    permanently, and a cache hit never refreshes the key, so integrations with
    such an entry would serve a stale or missing webhook configuration
    indefinitely after rollout. A hit attaches the default TTL to an entry
    that has none, in one server-side step (a separate TTL check and EXPIRE
    could race a fresh write and clobber its expiry), whatever the value: a
    real config that then expires is simply refreshed from the portal."""
    from app.services.config_manager import WEBHOOK_CONFIG_DEFAULT_TTL_SECONDS

    client = mock_redis_empty.Redis.return_value
    value = integration_v2_with_webhook.webhook_configuration.json() if cached == "webhook-config" else "null"
    client.get.return_value = async_return(value.encode())
    client.eval.return_value = async_return(1)
    mocker.patch("app.services.config_manager.redis", mock_redis_empty)
    mocker.patch("app.services.config_manager.GundiClient", mock_gundi_client_v2_class)
    config_manager = IntegrationConfigurationManager()
    integration_id = str(integration_v2_with_webhook.id)

    result = await config_manager.get_webhook_configuration(integration_id)

    assert (result is None) == (cached == "sentinel")
    assert not client.ttl.called and not client.expire.called, "must not be two separate commands"
    (script, numkeys, key, ttl), _ = client.eval.call_args
    assert (numkeys, key, ttl) == (1, f"integrationconfig.{integration_id}.webhook", WEBHOOK_CONFIG_DEFAULT_TTL_SECONDS)
    assert "TTL" in script and "EXPIRE" in script and "-1" in script
    assert not mock_gundi_client_v2_class.return_value.get_integration_details.called
