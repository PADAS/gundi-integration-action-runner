import json
import logging
from typing import Optional

import pydantic
import stamina
import redis.asyncio as redis
# Imported by name, not as `redis.RedisError`: tests replace the `redis` module
# attribute with a mock, and an except clause naming a mock attribute would
# raise TypeError instead of catching.
from redis.exceptions import RedisError
from gundi_core.schemas.v2 import Integration, IntegrationSummary, IntegrationActionConfiguration, WebhookConfiguration
from gundi_client_v2 import GundiClient
from app import settings
from .gundi import GUNDI_API_RETRY
from .retry_policies import REDIS_RETRY

logger = logging.getLogger(__name__)


# Cached marker meaning "this integration has no configuration for this
# action" / "no webhook configuration", so a cold cache doesn't trigger a
# Gundi API reload on every lookup of something the portal says is absent.
# Action-config sentinels are invalidated by the same ActionConfig* events as
# real configs; the webhook sentinel has no event and relies on its TTL.
_ABSENCE_SENTINEL = "null"
_NO_WEBHOOK_CONFIG_SENTINEL = _ABSENCE_SENTINEL
# Default expiry for the webhook key (the config itself or the absence
# sentinel) when the caller asks for no TTL, which the action path does. There
# is no WebhookConfig* event to invalidate that key on, no consumer handler
# touches it, and a cache hit never refreshes it, so a permanent entry would
# serve a stale or missing webhook config until someone deleted it by hand.
# Bounded so it self-heals; five minutes is the longest an operator's webhook
# config change can take to reach a connector that is not also receiving
# webhooks (the webhook path caches for 60 s on its own).
WEBHOOK_CONFIG_DEFAULT_TTL_SECONDS = 300


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
            async for attempt in stamina.retry_context(**GUNDI_API_RETRY):
                with attempt:
                    integration_details = await gundi.get_integration_details(integration_id)
            integration = IntegrationSummary.from_integration(integration_details)
            await self.db_client.set(key, integration.json(), ttl)
            # Save configurations for individual actions, and a sentinel for each
            # action of the type that has none: otherwise every lookup of an
            # unconfigured action misses and reloads from the Gundi API, so an
            # integration configured for two of its type's three actions would
            # hit the portal on every single run.
            configured = set()
            for config in integration_details.configurations:
                configured.add(config.action.value)
                config_key = self._get_action_config_key(integration_id, config.action.value)
                await self.db_client.set(config_key, config.json(), ttl)
            # Sentinels are written with NX so they never replace a key that is
            # already there. A reload reads the portal, then writes; if an
            # ActionConfigCreated event for one of these actions lands in
            # between, its config would otherwise be overwritten with a
            # permanent "null" that no later event corrects. With NX the event's
            # write wins in either order: written first, it is left alone;
            # written after, it replaces the sentinel as a plain SET.
            for action in integration_details.type.actions or []:
                if action.value not in configured:
                    await self.db_client.set(
                        self._get_action_config_key(integration_id, action.value), _ABSENCE_SENTINEL, ttl, nx=True,
                    )
            await self._cache_webhook_configuration(integration_id, integration_details.webhook_configuration, ttl)
            return integration_details

    async def _cache_webhook_configuration(self, integration_id: str, webhook_configuration, ttl=None):
        # Save the webhook configuration, or a sentinel marking its absence, so
        # integrations without one don't reload from the Gundi API on every
        # lookup. Both always expire, even when the caller asked for no TTL
        # (see WEBHOOK_CONFIG_DEFAULT_TTL_SECONDS). A plain SET also clears any
        # TTL the webhook path had put on the same key, so an unbounded write
        # here would undo its 60 s cache.
        webhook_key = self._get_webhook_config_key(integration_id)
        webhook_ttl = ttl if ttl is not None else WEBHOOK_CONFIG_DEFAULT_TTL_SECONDS
        if webhook_configuration:
            await self.db_client.set(webhook_key, webhook_configuration.json(), webhook_ttl)
        else:
            await self.db_client.set(webhook_key, _NO_WEBHOOK_CONFIG_SENTINEL, webhook_ttl)

    async def _reload_webhook_configuration_from_gundi(self, integration_id: str, ttl=None) -> Optional[WebhookConfiguration]:
        """Refresh only the webhook key from the Gundi API.

        The webhook key is the one entry that expires on its own, so it misses
        while the summary and action keys are still warm. Refreshing it through
        the full reload would rewrite those keys with this caller's ttl, and the
        webhook path calls with ttl=60: the action path's permanent,
        event-invalidated keys would be downgraded to a 60 s expiry and the next
        action run would go back to the portal.
        """
        async with GundiClient() as gundi:
            async for attempt in stamina.retry_context(**GUNDI_API_RETRY):
                with attempt:
                    integration_details = await gundi.get_integration_details(integration_id)
        await self._cache_webhook_configuration(integration_id, integration_details.webhook_configuration, ttl)
        return integration_details.webhook_configuration

    async def get_action_configuration(self, integration_id: str, action_id: str, ttl=None) -> Optional[IntegrationActionConfiguration]:
        key = self._get_action_config_key(integration_id, action_id)
        async for attempt in stamina.retry_context(**REDIS_RETRY):
            with attempt:
                data = await self.db_client.get(key)
        if data:
            if data in (_ABSENCE_SENTINEL, _ABSENCE_SENTINEL.encode()):
                return None  # cached absence: the portal has no config for this action
            return IntegrationActionConfiguration.parse_raw(data)
        # If not found in the redis db, try reloading data from Gundi API
        integration_details = await self._reload_integration_from_gundi(integration_id, ttl)
        return integration_details.get_action_config(action_id)

    async def get_webhook_configuration(self, integration_id: str, ttl=None) -> Optional[WebhookConfiguration]:
        key = self._get_webhook_config_key(integration_id)
        async for attempt in stamina.retry_context(**REDIS_RETRY):
            with attempt:
                data = await self.db_client.get(key)
        if data:
            if data in (_NO_WEBHOOK_CONFIG_SENTINEL, _NO_WEBHOOK_CONFIG_SENTINEL.encode()):
                return None  # cached absence — this integration has no webhook config
            return WebhookConfiguration.parse_raw(data)
        # Missing or expired: refresh this key only (see the method's docstring).
        return await self._reload_webhook_configuration_from_gundi(integration_id, ttl)


    async def set_action_configuration(self, integration_id: str, action_id: str, config: IntegrationActionConfiguration, ttl=None):
        key = self._get_action_config_key(integration_id, action_id)
        async for attempt in stamina.retry_context(**REDIS_RETRY):
            with attempt:
                await self.db_client.set(key, config.json(), ttl)

    async def delete_action_configuration(self, integration_id: str, action_id: str):
        # Record the absence rather than dropping the key: a deleted key misses on
        # the next lookup and reloads the whole integration from the Gundi API,
        # which would then report the same absence.
        key = self._get_action_config_key(integration_id, action_id)
        async for attempt in stamina.retry_context(**REDIS_RETRY):
            with attempt:
                await self.db_client.set(key, _ABSENCE_SENTINEL)

    async def get_integration(self, integration_id: str, ttl=None) -> IntegrationSummary:
        key = self._get_integration_key(integration_id)
        async for attempt in stamina.retry_context(**REDIS_RETRY):
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
        async for attempt in stamina.retry_context(**REDIS_RETRY):
            with attempt:
                await self.db_client.set(key, integration.json(), ttl)

    async def delete_integration(self, integration_id: str):
        # Every key derived from this integration goes in one variadic DEL: the
        # webhook key (config or absence sentinel) and the per-action config
        # keys are addressed separately, so leaving them behind lets them
        # outlive their owner, and a single round-trip has no partial-failure
        # window between the keys. The action list lives in the summary that is
        # about to go, so read it first; if it is gone or unreadable, the two
        # keys we can name still get deleted.
        integration_key = self._get_integration_key(integration_id)
        keys = [integration_key, self._get_webhook_config_key(integration_id)]
        try:
            async for attempt in stamina.retry_context(**REDIS_RETRY):
                with attempt:
                    summary_data = await self.db_client.get(integration_key)
            if summary_data:
                summary = IntegrationSummary.parse_raw(summary_data)
                keys += [self._get_action_config_key(integration_id, a.value) for a in (summary.type.actions or [])]
        except (RedisError, pydantic.ValidationError) as e:
            logger.warning(
                f"Could not read the cached summary for integration '{integration_id}' "
                f"to drop its action keys ({type(e).__name__}); deleting the keys that can be named."
            )
        async for attempt in stamina.retry_context(**REDIS_RETRY):
            with attempt:
                await self.db_client.delete(*keys)

    async def get_integration_details(
            self, integration_id: str, ttl=None, *, include_webhook_config: bool = True,
    ) -> Integration:
        """Assemble an Integration from the cached summary, action configs and,
        unless include_webhook_config is False, the webhook config.

        The webhook key is the one piece of the cache that no config event
        invalidates, so it carries a bounded TTL and reloads from the Gundi API
        when it expires. The action runner never reads webhook_configuration,
        so it opts out: otherwise every action run would go back to the portal
        once per TTL, and a portal outage would fail actions whose own summary
        and configs were still warm.
        """
        integration_summary = await self.get_integration(integration_id, ttl)
        configurations = []
        for action in integration_summary.type.actions:
            config = await self.get_action_configuration(integration_id, action.value, ttl)
            if config:
                configurations.append(config)
        webhook_configuration = (
            await self.get_webhook_configuration(integration_id, ttl) if include_webhook_config else None
        )
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