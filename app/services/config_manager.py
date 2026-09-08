import json
import logging
import uuid
from typing import Optional, Tuple

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
from .gundi import GUNDI_API_RETRY, _block_if_ephemeral
from .retry_policies import REDIS_RETRY

logger = logging.getLogger(__name__)


# Cached marker meaning "this integration has no configuration for this
# action" / "no webhook configuration", so a cold cache doesn't trigger a
# Gundi API reload on every lookup of something the portal says is absent.
# Action-config sentinels are replaced by the ActionConfig* events for that
# action and, as a backstop, expire (see ACTION_ABSENCE_SENTINEL_TTL_SECONDS);
# the webhook sentinel has no event and relies on its TTL alone.
#
# Every action sentinel write carries its own generation
# ("null:<epoch>:<n>:<hex>"), issued by one Redis counter (_GENERATION_KEY)
# inside the write itself. Two readers depend on it. The consumer's recovery
# reads the sentinel, fetches the portal, then replaces the sentinel only if it
# is still the one it observed; were every sentinel the same bare value, a
# concurrent ActionConfigDeleted's fresh tombstone would compare equal and the
# recovery would resurrect the deleted config as a permanent key. The reload
# takes a generation before its portal fetch and refuses to overwrite a
# tombstone with a higher one: that tombstone was written while the fetch was
# in flight, so it is newer than the snapshot (see
# _WRITE_SNAPSHOT_CONFIG_SCRIPT). Redis hands out the numbers, so the ordering
# holds across runner instances whatever their clocks say.
#
# The counter is an ordinary key: a flush or an eviction restarts it, and
# numbers from after a restart must never be compared with numbers from before
# it (a fresh tombstone numbered 1 would look older than an in-flight reload's
# 100). So a generation is qualified by an epoch (_GENERATION_EPOCH_KEY), minted
# whenever the counter or the epoch is found missing, and the reload preserves
# any tombstone from an epoch other than its own.
#
# Generated tombstones are opt-in (settings.CONFIG_CACHE_SENTINEL_GENERATIONS).
# A rolling deployment runs old and new replicas side by side, and a replica on
# a release without this reader parses anything but the bare "null" as a
# configuration, failing every lookup of that action; so the reader ships
# first and the writer is turned on once every replica has it. Until then, and
# for sentinels written before generations existed, the value is the bare
# "null", which still reads as absence; the reload's ordering guard simply has
# nothing to compare and behaves as before.
_ABSENCE_SENTINEL_PREFIX = "null"
_NO_WEBHOOK_CONFIG_SENTINEL = "null"
_GENERATION_KEY = "integrationconfig.generation"
_GENERATION_EPOCH_KEY = "integrationconfig.generation.epoch"


def _is_absence_sentinel(data) -> bool:
    if isinstance(data, bytes):
        data = data.decode()
    return data == _ABSENCE_SENTINEL_PREFIX or data.startswith(_ABSENCE_SENTINEL_PREFIX + ":")


def _is_unorderable_token(observed: str) -> bool:
    """A bare "null" while generations are enabled. During the rolling restart
    that enables them, a not-yet-enabled replica can still write a bare "null"
    for a delete, and a compare-and-set that observed a bare "null" earlier
    cannot tell that fresh delete from the one it saw; equality would resurrect
    it. So while the setting is on, a bare sentinel is not a token anyone may
    write over: the caller's re-read treats it as an absence and stops, and the
    sentinel's TTL takes it from there. With the setting off every replica
    writes bare sentinels and there is nothing to tell apart."""
    return settings.CONFIG_CACHE_SENTINEL_GENERATIONS and observed == _ABSENCE_SENTINEL_PREFIX
# Default expiry for the webhook key (the config itself or the absence
# sentinel) when the caller asks for no TTL, which the action path does. There
# is no WebhookConfig* event to invalidate that key on, no consumer handler
# touches it, and a cache hit never refreshes it, so a permanent entry would
# serve a stale or missing webhook config until someone deleted it by hand.
# Bounded so it self-heals; five minutes is the longest an operator's webhook
# config change can take to reach a connector that is not also receiving
# webhooks (the webhook path caches for 60 s on its own).
WEBHOOK_CONFIG_DEFAULT_TTL_SECONDS = 300
# Expiry for an action absence sentinel when the caller asks for no TTL (the
# action path). Real configs stay permanent because every change to them
# arrives as an event; a sentinel's only correction is the ActionConfigCreated
# event for that action, and the consumer acks a failed or lost delivery, so a
# permanent "null" would keep the action "unconfigured" until someone flushed
# redis. Bounded, the next lookup after expiry misses and the reload writes
# whatever the portal now says. It also lets an orphan sentinel (a cascade
# ActionConfigDeleted arriving after IntegrationDeleted) age out on its own.
ACTION_ABSENCE_SENTINEL_TTL_SECONDS = 300
# Attach a TTL to a key only while it still holds the exact sentinel the read
# observed and has no expiry (TTL == -1). One server-side step on purpose: a
# client-side TTL check followed by EXPIRE could race an ActionConfigCreated
# write in between and put the TTL on the real configuration, which must stay
# permanent.
_EXPIRE_LEGACY_SENTINEL_SCRIPT = """
if redis.call('GET', KEYS[1]) == ARGV[1] and redis.call('TTL', KEYS[1]) == -1 then
    return redis.call('EXPIRE', KEYS[1], ARGV[2])
end
return 0
"""
# Attach a TTL to a key that has none, whatever it holds. Used for webhook
# entries written before the key carried a TTL (see _expire_legacy_webhook_entry).
# One server-side step: a client-side TTL check followed by EXPIRE could race a
# fresh write and replace the expiry it came with.
_EXPIRE_IF_PERMANENT_SCRIPT = """
if redis.call('TTL', KEYS[1]) == -1 then
    return redis.call('EXPIRE', KEYS[1], ARGV[1])
end
return 0
"""
# Replace exactly the value the caller read (a sentinel generation or a
# config's raw JSON) with a real configuration, but never anything that landed
# in between: a newer configuration, a newer sentinel generation from a
# concurrent ActionConfigDeleted, or nothing at all. A missing key is not a
# match either: a tombstone written during a slow portal fetch can itself have
# expired by the time this runs, and installing over "missing" would resurrect
# the deleted config. Every consumer write of an action config goes through it.
_REPLACE_CACHED_ENTRY_SCRIPT = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    if ARGV[3] == '' then
        redis.call('SET', KEYS[1], ARGV[2])
    else
        redis.call('SET', KEYS[1], ARGV[2], 'EX', ARGV[3])
    end
    return 1
end
return 0
"""
# Shared by the two scripts below: the current epoch, minting a new one (the
# caller's candidate) when the counter or the epoch key is missing, then the
# next number. Returns "<epoch>:<n>".
_NEXT_GENERATION_LUA = """
local function next_generation(counter_key, epoch_key, candidate_epoch)
    local epoch = redis.call('GET', epoch_key)
    if not epoch or redis.call('EXISTS', counter_key) == 0 then
        epoch = candidate_epoch
        redis.call('SET', epoch_key, epoch)
    end
    return epoch .. ':' .. redis.call('INCR', counter_key)
end
"""
# The reload's generation, taken before its portal fetch so every tombstone
# written during the fetch compares higher (within the same epoch). KEYS[1] the
# counter, KEYS[2] the epoch key; ARGV[1] a candidate epoch.
_NEXT_GENERATION_SCRIPT = _NEXT_GENERATION_LUA + """
return next_generation(KEYS[1], KEYS[2], ARGV[1])
"""
# Write an absence sentinel, atomically with the INCR that issues its generation
# (a generation taken separately could be written after a higher one, breaking
# the ordering the reload relies on). KEYS[1] is the action key, KEYS[2] the
# counter, KEYS[3] the epoch key; ARGV[1] a hex nonce, ARGV[2] the TTL in
# seconds, ARGV[3] the precondition ('any', 'missing', or 'equals' the value in
# ARGV[4]), ARGV[5] a candidate epoch, ARGV[6] '1' to write a generated
# tombstone or '0' for the bare value (CONFIG_CACHE_SENTINEL_GENERATIONS).
_WRITE_TOMBSTONE_SCRIPT = _NEXT_GENERATION_LUA + """
local current = redis.call('GET', KEYS[1])
if ARGV[3] == 'missing' and current then
    return 0
end
if ARGV[3] == 'equals' and current ~= ARGV[4] then
    return 0
end
local value = 'null'
if ARGV[6] == '1' then
    value = 'null:' .. next_generation(KEYS[2], KEYS[3], ARGV[5]) .. ':' .. ARGV[1]
end
redis.call('SET', KEYS[1], value, 'EX', ARGV[2])
return 1
"""
# The reload's write of one configured action from its portal snapshot. Refuses
# to overwrite an absence sentinel from another epoch, or from this epoch with a
# generation above the one the reload took before its fetch: that is a
# concurrent ActionConfigDeleted's tombstone, newer than the snapshot, and an
# unconditional SET would resurrect the deleted config permanently. While
# generations are enabled (ARGV[4] == '1') a bare "null" is preserved as well:
# enabling the setting is itself a rolling restart, and a not-yet-enabled
# replica writes bare tombstones that cannot be ordered against the reload's
# generation; a bare sentinel that merely predates the fetch expires on its
# TTL and the next miss reloads. Anything else (a real config, an older
# generated sentinel, nothing) is replaced by the snapshot, which is the
# portal's truth as of the fetch. ARGV[2] is the reload's "<epoch>:<n>" token.
_WRITE_SNAPSHOT_CONFIG_SCRIPT = """
local current = redis.call('GET', KEYS[1])
if current then
    local epoch, generation = string.match(current, '^null:([^:]+):(%d+):')
    if epoch then
        local fetch_epoch, fetch_generation = string.match(ARGV[2], '^([^:]+):(%d+)$')
        if epoch ~= fetch_epoch or tonumber(generation) > tonumber(fetch_generation) then
            return 0
        end
    elseif ARGV[4] == '1' and string.sub(current, 1, 4) == 'null' then
        return 0
    end
end
if ARGV[3] == '' then
    redis.call('SET', KEYS[1], ARGV[1])
else
    redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[3])
end
return 1
"""


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

    async def _fetch_integration_from_gundi(self, integration_id: str) -> Integration:
        # The one portal read both reloads share. Blocked on the ephemeral
        # path: a draft integration has no portal row, so the read would 404
        # and spin GUNDI_API_RETRY for up to two minutes on the request (the
        # same guard, for the same reason, as gundi._get_gundi_api_key).
        _block_if_ephemeral("IntegrationConfigurationManager reload")
        async with GundiClient() as gundi:
            async for attempt in stamina.retry_context(**GUNDI_API_RETRY):
                with attempt:
                    integration_details = await gundi.get_integration_details(integration_id)
        return integration_details

    async def _write_absence_sentinel(self, key: str, *, ttl: int, mode: str, expected: str = "") -> bool:
        async for attempt in stamina.retry_context(**REDIS_RETRY):
            with attempt:
                written = await self.db_client.eval(
                    _WRITE_TOMBSTONE_SCRIPT, 3, key, _GENERATION_KEY, _GENERATION_EPOCH_KEY,
                    uuid.uuid4().hex, ttl, mode, expected, uuid.uuid4().hex,
                    "1" if settings.CONFIG_CACHE_SENTINEL_GENERATIONS else "0",
                )
        return bool(written)

    async def _reload_integration_from_gundi(self, integration_id: str, ttl=None) -> Integration:
        key = self._get_integration_key(integration_id)
        async for attempt in stamina.retry_context(**REDIS_RETRY):
            with attempt:
                fetch_generation = await self.db_client.eval(
                    _NEXT_GENERATION_SCRIPT, 2, _GENERATION_KEY, _GENERATION_EPOCH_KEY, uuid.uuid4().hex,
                )
        integration_details = await self._fetch_integration_from_gundi(integration_id)
        integration = IntegrationSummary.from_integration(integration_details)
        await self.db_client.set(key, integration.json(), ttl)
        # Save configurations for individual actions, and a sentinel for each
        # action of the type that has none: otherwise every lookup of an
        # unconfigured action misses and reloads from the Gundi API, so an
        # integration configured for two of its type's three actions would
        # hit the portal on every single run. Configured actions are written
        # through a script that preserves a tombstone newer than the fetch
        # (see _WRITE_SNAPSHOT_CONFIG_SCRIPT).
        configured = set()
        for config in integration_details.configurations:
            configured.add(config.action.value)
            config_key = self._get_action_config_key(integration_id, config.action.value)
            await self.db_client.eval(
                _WRITE_SNAPSHOT_CONFIG_SCRIPT, 1, config_key, config.json(), fetch_generation,
                ttl if ttl is not None else "", "1" if settings.CONFIG_CACHE_SENTINEL_GENERATIONS else "0",
            )
        # Sentinels are written only if the key is missing, so they never
        # replace a key that is already there. A reload reads the portal, then
        # writes; if an ActionConfigCreated event for one of these actions
        # lands in between, its config would otherwise be overwritten with a
        # "null" that no later event corrects. This way the event's write wins
        # in either order: written first, it is left alone; written after, it
        # replaces the sentinel. The sentinel also expires
        # (ACTION_ABSENCE_SENTINEL_TTL_SECONDS) for the case where that event
        # never arrives at all.
        sentinel_ttl = ttl if ttl is not None else ACTION_ABSENCE_SENTINEL_TTL_SECONDS
        for action in integration_details.type.actions or []:
            if action.value not in configured:
                await self._write_absence_sentinel(
                    self._get_action_config_key(integration_id, action.value), ttl=sentinel_ttl, mode="missing",
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
        integration_details = await self._fetch_integration_from_gundi(integration_id)
        await self._cache_webhook_configuration(integration_id, integration_details.webhook_configuration, ttl)
        return integration_details.webhook_configuration

    async def get_action_configuration(self, integration_id: str, action_id: str, ttl=None) -> Optional[IntegrationActionConfiguration]:
        config, sentinel = await self.read_cached_action_configuration(integration_id, action_id)
        if config is not None or sentinel is not None:
            return config  # a cached config, or a cached absence
        # If not found in the redis db, try reloading data from Gundi API
        integration_details = await self._reload_integration_from_gundi(integration_id, ttl)
        # Answer from the cache, not from the snapshot: a delete that landed
        # during the reload's fetch left a tombstone the snapshot write
        # preserved, and returning the fetched row anyway would have the
        # runner execute the deleted action once. Nothing cached at all right
        # after the reload means the key changed under it (an IntegrationDeleted
        # dropped every action key, or a tombstone expired); the snapshot is
        # stale either way, so that reads as absent too.
        config, _ = await self.read_cached_action_configuration(integration_id, action_id)
        return config

    async def read_cached_action_configuration(
            self, integration_id: str, action_id: str,
    ) -> Tuple[Optional[IntegrationActionConfiguration], Optional[str]]:
        """What the cache holds for this action, from one read and without ever
        reloading from the portal, with the exact stored value as a token:
        (config, raw_json), (None, sentinel) for a recorded absence, or
        (None, None) when nothing is cached.

        For the consumer, which then compares-and-sets against the token it
        saw (replace_cached_entry / install_action_configuration_if_missing).
        The token must come from the very read that established the state,
        since a second read could observe a fresh tombstone from a concurrent
        ActionConfigDeleted; and the full reload is off limits here because it
        rewrites the cache from a snapshot before returning, so the token this
        caller holds would no longer describe what the key contains."""
        key = self._get_action_config_key(integration_id, action_id)
        async for attempt in stamina.retry_context(**REDIS_RETRY):
            with attempt:
                data = await self.db_client.get(key)
        if not data:
            return None, None
        raw = data.decode() if isinstance(data, bytes) else data
        if _is_absence_sentinel(data):
            await self._expire_legacy_sentinel(key, data)
            return None, raw
        return IntegrationActionConfiguration.parse_raw(data), raw

    async def _expire_legacy_sentinel(self, key: str, observed) -> None:
        # Sentinels written before they carried a TTL are permanent, and the
        # reload's NX write never touches an existing key, so an integration
        # already hit by the lost-event bug would stay "unconfigured" after
        # rollout. Attach the TTL on a hit instead: one round trip per sentinel
        # hit (the compare-and-expire script), and a write only on the first
        # hit after rollout. Can go once every deployment has run a release
        # with sentinel TTLs.
        observed = observed.decode() if isinstance(observed, bytes) else observed
        async for attempt in stamina.retry_context(**REDIS_RETRY):
            with attempt:
                await self.db_client.eval(
                    _EXPIRE_LEGACY_SENTINEL_SCRIPT, 1, key, observed, ACTION_ABSENCE_SENTINEL_TTL_SECONDS,
                )

    async def get_webhook_configuration(self, integration_id: str, ttl=None) -> Optional[WebhookConfiguration]:
        key = self._get_webhook_config_key(integration_id)
        async for attempt in stamina.retry_context(**REDIS_RETRY):
            with attempt:
                data = await self.db_client.get(key)
        if data:
            await self._expire_legacy_webhook_entry(key, ttl)
            if data in (_NO_WEBHOOK_CONFIG_SENTINEL, _NO_WEBHOOK_CONFIG_SENTINEL.encode()):
                return None  # cached absence — this integration has no webhook config
            return WebhookConfiguration.parse_raw(data)
        # Missing or expired: refresh this key only (see the method's docstring).
        return await self._reload_webhook_configuration_from_gundi(integration_id, ttl)

    async def _expire_legacy_webhook_entry(self, key: str, ttl=None) -> None:
        # Before the webhook key always carried a TTL, the action path's reload
        # (ttl=None) wrote both real configs and the absence sentinel
        # permanently, and a hit never refreshes the key, so such an entry
        # would serve a stale or missing webhook config indefinitely. Attach
        # the bounded TTL on a hit to an entry that has none; a real config
        # that then expires is simply refreshed from the portal. Can go once
        # every deployment has run a release with webhook TTLs.
        webhook_ttl = ttl if ttl is not None else WEBHOOK_CONFIG_DEFAULT_TTL_SECONDS
        async for attempt in stamina.retry_context(**REDIS_RETRY):
            with attempt:
                await self.db_client.eval(_EXPIRE_IF_PERMANENT_SCRIPT, 1, key, webhook_ttl)


    async def set_action_configuration(self, integration_id: str, action_id: str, config: IntegrationActionConfiguration, ttl=None):
        key = self._get_action_config_key(integration_id, action_id)
        async for attempt in stamina.retry_context(**REDIS_RETRY):
            with attempt:
                await self.db_client.set(key, config.json(), ttl)

    async def replace_cached_entry(
            self, integration_id: str, action_id: str, *, config: IntegrationActionConfiguration, observed: str,
    ) -> bool:
        """Write `config` only while the key still holds `observed`, the exact
        token the caller read (see read_cached_action_configuration): a
        sentinel generation or a config's raw JSON. Returns whether the write
        happened; False means the key changed, to a newer configuration, a
        fresh tombstone from a concurrent ActionConfigDeleted, or nothing, and
        the caller must not overwrite it. Permanent, like every real
        configuration."""
        if _is_unorderable_token(observed):
            return False
        key = self._get_action_config_key(integration_id, action_id)
        async for attempt in stamina.retry_context(**REDIS_RETRY):
            with attempt:
                written = await self.db_client.eval(_REPLACE_CACHED_ENTRY_SCRIPT, 1, key, observed, config.json(), "")
        return bool(written)

    async def replace_cached_entry_with_absence(self, integration_id: str, action_id: str, *, observed: str) -> bool:
        """Record an absence (a fresh, expiring sentinel) only while the key
        still holds `observed`. The consumer's reconciliation uses it when the
        portal says the row is gone; the same exactness rule as
        replace_cached_entry applies."""
        if _is_unorderable_token(observed):
            return False
        key = self._get_action_config_key(integration_id, action_id)
        return await self._write_absence_sentinel(
            key, ttl=ACTION_ABSENCE_SENTINEL_TTL_SECONDS, mode="equals", expected=observed,
        )

    async def install_action_configuration_if_missing(
            self, integration_id: str, action_id: str, *, config: IntegrationActionConfiguration,
    ) -> bool:
        """Write `config` only if nothing is cached for this action (SET NX),
        for a recovery that saw a cold cache, fetched the portal row, and must
        not overwrite whatever landed meanwhile. Returns whether it was written.
        Permanent, like every real configuration."""
        key = self._get_action_config_key(integration_id, action_id)
        async for attempt in stamina.retry_context(**REDIS_RETRY):
            with attempt:
                written = await self.db_client.set(key, config.json(), None, nx=True)
        return bool(written)

    async def delete_action_configuration(self, integration_id: str, action_id: str):
        # Record the absence rather than dropping the key: a deleted key misses on
        # the next lookup and reloads the whole integration from the Gundi API,
        # which would then report the same absence. Bounded like the reload's
        # sentinels, so a Deleted that outlives its integration ages out.
        key = self._get_action_config_key(integration_id, action_id)
        await self._write_absence_sentinel(key, ttl=ACTION_ABSENCE_SENTINEL_TTL_SECONDS, mode="any")

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