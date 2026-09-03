import json
import logging
import stamina
import redis.asyncio as redis
from app import settings
from .activity_logger import ephemeral_run
from .retry_policies import REDIS_RETRY

logger = logging.getLogger(__name__)


def _skip_on_ephemeral_run(op: str, integration_id: str, action_id: str) -> bool:
    """Ephemeral runs get a fresh synthetic integration id and no TTL on state
    keys, so any write would be a permanent orphan. Every mutating method
    no-ops behind this, with a log line: a handler that writes then reads in
    the same run sees {} and fails in a way that looks unrelated otherwise."""
    if not ephemeral_run.get():
        return False
    logger.debug(
        f"Skipping {op} for action '{action_id}' on ephemeral integration '{integration_id}': "
        "state is not persisted on the ephemeral path."
    )
    return True


class IntegrationStateManager:

    def __init__(self, **kwargs):
        host = kwargs.get("host", settings.REDIS_HOST)
        port = kwargs.get("port", settings.REDIS_PORT)
        db = kwargs.get("db", settings.REDIS_STATE_DB)
        self.db_client = redis.Redis(host=host, port=port, db=db)

    async def get_state(self, integration_id: str, action_id: str, source_id: str = "no-source") -> dict:
        async for attempt in stamina.retry_context(**REDIS_RETRY):
            with attempt:
                json_value = await self.db_client.get(f"integration_state.{integration_id}.{action_id}.{source_id}")
        value = json.loads(json_value) if json_value else {}
        return value

    async def set_state(self, integration_id: str, action_id: str, state: dict, source_id: str = "no-source"):
        if _skip_on_ephemeral_run("set_state", integration_id, action_id):
            return
        async for attempt in stamina.retry_context(**REDIS_RETRY):
            with attempt:
                await self.db_client.set(
                    f"integration_state.{integration_id}.{action_id}.{source_id}",
                    json.dumps(state, default=str)
                )

    async def set_if_absent(
        self, integration_id: str, action_id: str, *, ttl_seconds: int, source_id: str = "no-source"
    ) -> bool:
        """Atomically set a key only if it does not already exist, with a TTL.

        Returns True if the key was set by this call (i.e. the caller is the
        first within the TTL window), or False if it already existed. Useful
        for rate-limiting/throttling repeated events: the first caller in each
        window gets True, the rest get False until the key expires. On the
        ephemeral path nothing is written and the answer is False, so a
        throttling caller treats the window as already taken.
        """
        if _skip_on_ephemeral_run("set_if_absent", integration_id, action_id):
            return False
        async for attempt in stamina.retry_context(**REDIS_RETRY):
            with attempt:
                was_set = await self.db_client.set(
                    f"integration_state.{integration_id}.{action_id}.{source_id}",
                    "1",
                    ex=ttl_seconds,
                    nx=True,
                )
        return bool(was_set)

    async def delete_state(self, integration_id: str, action_id: str, source_id: str = "no-source"):
        if _skip_on_ephemeral_run("delete_state", integration_id, action_id):
            return
        async for attempt in stamina.retry_context(**REDIS_RETRY):
            with attempt:
                await self.db_client.delete(
                    f"integration_state.{integration_id}.{action_id}.{source_id}"
                )

    def __str__(self):
        return f"IntegrationStateManager(host={self.db_client.host}, port={self.db_client.port}, db={self.db_client.db})"

    def __repr__(self):
        return self.__str__()
