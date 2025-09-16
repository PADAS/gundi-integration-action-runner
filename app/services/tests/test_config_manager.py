import pytest

from gundi_core.schemas.v2 import IntegrationSummary, IntegrationActionConfiguration, Integration, WebhookConfiguration
from app.services.config_manager import IntegrationConfigurationManager


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

