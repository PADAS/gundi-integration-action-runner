"""Retry policies shared across the service layer.

Kept in a leaf module (no app imports) so config_manager and state can both
use REDIS_RETRY without importing each other. GUNDI_API_RETRY lives in
gundi.py next to the helpers it decorates.

Iterate stamina with `async for`: its synchronous iterator sleeps with
time.sleep, which inside a coroutine stalls the whole event loop for the
length of the back-off.
"""
from redis.exceptions import RedisError

REDIS_RETRY = dict(on=RedisError, attempts=5, wait_initial=1.0, wait_max=30, wait_jitter=3.0)
