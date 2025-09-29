import json
import stamina
import httpx
import redis.asyncio as redis
from gundi_core.schemas.v2 import Integration, IntegrationSummary, IntegrationActionConfiguration, WebhookConfiguration
from gundi_client_v2 import GundiClient
from app import settings


class IntegrationConfigurationManager:
    # ToDo: Add support for webhook configs

    def __init__(self, **kwargs):
        host = kwargs.get("host", settings.REDIS_HOST)
        port = kwargs.get("port", settings.REDIS_PORT)
        db = kwargs.get("db", settings.REDIS_CONFIGS_DB)
        self.db_client = redis.Redis(host=host, port=port, db=db)

    def _get_integration_key(self, integration_id: str) -> str:
        return f"integration.{integration_id}"

    def _get_action_config_key(self, integration_id: str, action_id: str) -> str:
        return f"integrationconfig.{integration_id}.{action_id}"

    def _get_webhook_config_key(self, integration_id: str) -> str:
        return f"integrationconfig.{integration_id}.webhook"

    async def _reload_integration_from_gundi(self, integration_id: str, ttl=None) -> Integration:
        key = self._get_integration_key(integration_id)
        async with GundiClient() as gundi:
            async for attempt in stamina.retry_context(on=httpx.HTTPError, wait_initial=1.0, wait_jitter=5.0,  wait_max=32.0):
                with attempt:
                    integration_details = await gundi.get_integration_details(integration_id)
            integration = IntegrationSummary.from_integration(integration_details)
            await self.db_client.set(key, integration.json(), ttl)
            # Save configurations for individual actions
            for config in integration_details.configurations:
                config_key = self._get_action_config_key(integration_id, config.action.value)
                await self.db_client.set(config_key, config.json(), ttl)
            # Save webhook configuration if present
            if webhook_configuration := integration_details.webhook_configuration:
                webhook_key = self._get_webhook_config_key(integration_id)
                await self.db_client.set(webhook_key, webhook_configuration.json(), ttl)
            return integration_details

    async def get_action_configuration(self, integration_id: str, action_id: str, ttl=None) -> IntegrationActionConfiguration:
        key = self._get_action_config_key(integration_id, action_id)
        for attempt in stamina.retry_context(on=redis.RedisError, attempts=5, wait_initial=1.0, wait_max=30, wait_jitter=3.0):
            with attempt:
                data = await self.db_client.get(key)
        if data:
            return IntegrationActionConfiguration.parse_raw(data)
        # If not found in the redis db, try reloading data from Gundi API
        integration_details = await self._reload_integration_from_gundi(integration_id, ttl)
        return integration_details.get_action_config(action_id)

    async def get_webhook_configuration(self, integration_id: str, ttl=None) -> WebhookConfiguration:
        key = self._get_webhook_config_key(integration_id)
        for attempt in stamina.retry_context(on=redis.RedisError, attempts=5, wait_initial=1.0, wait_max=30, wait_jitter=3.0):
            with attempt:
                data = await self.db_client.get(key)
        if data:
            return WebhookConfiguration.parse_raw(data)
        # If not found in the redis db, try reloading data from Gundi API
        integration_details = await self._reload_integration_from_gundi(integration_id, ttl)
        return integration_details.webhook_configuration


    async def set_action_configuration(self, integration_id: str, action_id: str, config: IntegrationActionConfiguration, ttl=None):
        key = self._get_action_config_key(integration_id, action_id)
        for attempt in stamina.retry_context(on=redis.RedisError, attempts=5, wait_initial=1.0, wait_max=30, wait_jitter=3.0):
            with attempt:
                await self.db_client.set(key, config.json(), ttl)

    async def delete_action_configuration(self, integration_id: str, action_id: str):
        key = self._get_action_config_key(integration_id, action_id)
        for attempt in stamina.retry_context(on=redis.RedisError, attempts=5, wait_initial=1.0, wait_max=30, wait_jitter=3.0):
            with attempt:
                return await self.db_client.delete(key)

    async def get_integration(self, integration_id: str, ttl=None) -> IntegrationSummary:
        key = self._get_integration_key(integration_id)
        for attempt in stamina.retry_context(on=redis.RedisError, attempts=5, wait_initial=1.0, wait_max=30, wait_jitter=3.0):
            with attempt:
                integration_data = await self.db_client.get(key)
        if integration_data:
            # Looks for configurations
            return IntegrationSummary.parse_raw(integration_data)
        # If not found in cache, reload from Gundi
        integration_details = await self._reload_integration_from_gundi(integration_id, ttl)
        return IntegrationSummary.from_integration(integration_details)

    async def set_integration(self, integration: IntegrationSummary, ttl=None):
        key = self._get_integration_key(integration.id)
        for attempt in stamina.retry_context(on=redis.RedisError, attempts=5, wait_initial=1.0, wait_max=30, wait_jitter=3.0):
            with attempt:
                await self.db_client.set(key, integration.json(), ttl)

    async def delete_integration(self, integration_id: str):
        key = self._get_integration_key(integration_id)
        for attempt in stamina.retry_context(on=redis.RedisError, attempts=5, wait_initial=1.0, wait_max=30, wait_jitter=3.0):
            with attempt:
                await self.db_client.delete(key)

    async def get_integration_details(self, integration_id: str, ttl=None) -> Integration:
        integration_summary = await self.get_integration(integration_id, ttl)
        configurations = []
        for action in integration_summary.type.actions:
            config = await self.get_action_configuration(integration_id, action.value, ttl)
            if config:
                configurations.append(config)
        webhook_configuration = await self.get_webhook_configuration(integration_id, ttl)
        return Integration(
            id=integration_summary.id,
            name=integration_summary.name,
            type=integration_summary.type,
            base_url=integration_summary.base_url,
            enabled=integration_summary.enabled,
            owner=integration_summary.owner,
            default_route=integration_summary.default_route,
            additional=integration_summary.additional,
            configurations=configurations,
            webhook_configuration=webhook_configuration
        )