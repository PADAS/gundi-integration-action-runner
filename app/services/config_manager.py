import json
import stamina
import httpx
import redis.asyncio as redis
from gundi_core.schemas.v2 import Integration, IntegrationActionConfiguration
from gundi_client_v2 import GundiClient
from app import settings


class IntegrationConfigurationManager:

    def __init__(self, **kwargs):
        host = kwargs.get("host", settings.REDIS_HOST)
        port = kwargs.get("port", settings.REDIS_PORT)
        db = kwargs.get("db", settings.REDIS_CONFIGS_DB)
        self.db_client = redis.Redis(host=host, port=port, db=db)

    def _get_integration_key(self, integration_id: str) -> str:
        return f"integration.{integration_id}"

    def _get_integration_config_key(self, integration_id: str, actions_id: str) -> str:
        return f"integration.{integration_id}.{actions_id}"

    async def _reload_integration_from_gundi(self, integration_id: str) -> Integration:
        key = self._get_integration_key(integration_id)
        async with GundiClient() as gundi:
            async for attempt in stamina.retry_context(on=httpx.HTTPError, wait_initial=1.0, wait_jitter=5.0,  wait_max=32.0):
                with attempt:
                    integration = await gundi.get_integration_details(integration_id)
            await self.db_client.set(key, integration.json())
            # Save configurations for individual actions
            for config in integration.configurations:
                config_key = self._get_integration_config_key(integration_id, config.action.value)
                await self.db_client.set(config_key, config.json())
            return integration

    async def get_action_configuration(self, integration_id: str, action_id: str) -> IntegrationActionConfiguration:
        key = self._get_integration_config_key(integration_id, action_id)
        for attempt in stamina.retry_context(on=redis.RedisError, attempts=5, wait_initial=1.0, wait_max=30, wait_jitter=3.0):
            with attempt:
                data = await self.db_client.get(key)
        if data:
            return IntegrationActionConfiguration.parse_raw(data)
        # If not found in cache, try reloading data from Gundi
        integration = await self._reload_integration_from_gundi(integration_id)
        for config in integration.configurations:
            if config.action.value == action_id:
                return config

    async def set_action_configuration(self, integration_id: str, action_id: str, config: IntegrationActionConfiguration):
        key = self._get_integration_config_key(integration_id, action_id)
        for attempt in stamina.retry_context(on=redis.RedisError, attempts=5, wait_initial=1.0, wait_max=30, wait_jitter=3.0):
            with attempt:
                await self.db_client.set(key, config.json())

    async def get_integration(self, integration_id: str) -> Integration:
        key = self._get_integration_key(integration_id)
        for attempt in stamina.retry_context(on=redis.RedisError, attempts=5, wait_initial=1.0, wait_max=30, wait_jitter=3.0):
            with attempt:
                data = await self.db_client.get(key)
        if data:
            return Integration.parse_raw(data)
        # If not found in cache, reload from Gundi
        return await self._reload_integration_from_gundi(integration_id)

    async def set_integration(self, integration: Integration):
        key = self._get_integration_key(integration.id)
        for attempt in stamina.retry_context(on=redis.RedisError, attempts=5, wait_initial=1.0, wait_max=30, wait_jitter=3.0):
            with attempt:
                await self.db_client.set(key, integration.json())

