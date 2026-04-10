"""
Utility classes and functions for action handlers.
"""
import logging
import redis.asyncio as redis
from app import settings

logger = logging.getLogger(__name__)


class FileProcessingLockManager:
    """
    Manages file processing locks to prevent concurrent processing of the same file.
    Uses Redis with expiration to ensure locks are automatically released.
    Lock operations are best-effort: if Redis is unavailable they fail open immediately
    with no retries, to avoid adding latency to the processing pipeline.
    """

    def __init__(self, **kwargs):
        host = kwargs.get("host", settings.REDIS_HOST)
        port = kwargs.get("port", settings.REDIS_PORT)
        db = kwargs.get("db", settings.REDIS_STATE_DB)
        self.db_client = redis.Redis(host=host, port=port, db=db)
        self.lock_timeout = 3600  # 1 hour expiry so locks don't stick if release is missed

    async def acquire_lock(self, integration_id: str, file_name: str) -> bool:
        """
        Try to acquire a lock for a file. Returns True if acquired, False if already locked.
        Returns True (fail open) if Redis is unavailable.
        """
        lock_key = f"file_processing_lock.{integration_id}.{file_name}"
        try:
            result = await self.db_client.set(lock_key, "locked", ex=self.lock_timeout, nx=True)
            return result is not None
        except Exception as e:
            logger.warning(f"Redis unavailable when acquiring lock for {file_name}, proceeding without lock: {str(e)}")
            return True  # fail open — prefer duplicate chunk risk over halting all processing

    async def release_lock(self, integration_id: str, file_name: str) -> bool:
        """
        Release a lock for a file. Returns False silently if Redis is unavailable
        (lock will expire automatically via the TTL).
        """
        lock_key = f"file_processing_lock.{integration_id}.{file_name}"
        try:
            result = await self.db_client.delete(lock_key)
            return result > 0
        except Exception as e:
            logger.warning(f"Redis unavailable when releasing lock for {file_name}: {str(e)}")
            return False

    async def is_locked(self, integration_id: str, file_name: str) -> bool:
        """
        Check if a file is currently locked. Returns False (fail open) if Redis is unavailable.
        """
        lock_key = f"file_processing_lock.{integration_id}.{file_name}"
        try:
            result = await self.db_client.exists(lock_key)
            return result > 0
        except Exception as e:
            logger.warning(f"Redis unavailable when checking lock for {file_name}: {str(e)}")
            return False
