import pytest

from gundi_core.schemas.v2 import IntegrationSummary, IntegrationActionConfiguration, Integration, WebhookConfiguration
from app.services.config_manager import (
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
    # Verify that set was called with TTL for action config
    mock_redis_empty.Redis.return_value.set.assert_any_call(
        f"integrationconfig.{integration_id}.{action_id}",
        action_config.json(),
        ttl
    )
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
    # but always with an expiry, so it can't outlive a webhook config being added.
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
async def test_absence_sentinel_always_gets_a_ttl_even_when_caller_passes_none(
        mocker, mock_redis_empty, mock_gundi_client_v2_class, integration_v2,
):
    """There is no WebhookConfigCreated event to invalidate the sentinel on, so a
    permanent one would keep serving "no webhook" after an operator adds a
    webhook config in the portal -- breaking every delivery until the key was
    deleted by hand. execute_action reaches here with ttl=None."""
    mocker.patch("app.services.config_manager.redis", mock_redis_empty)
    mocker.patch("app.services.config_manager.GundiClient", mock_gundi_client_v2_class)
    config_manager = IntegrationConfigurationManager()
    integration_id = str(integration_v2.id)

    await config_manager.get_webhook_configuration(integration_id, ttl=None)

    mock_redis_empty.Redis.return_value.set.assert_any_call(
        f"integrationconfig.{integration_id}.webhook",
        "null",
        WEBHOOK_CONFIG_DEFAULT_TTL_SECONDS,
    )


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
