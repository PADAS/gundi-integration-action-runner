"""Retry policies shared across the service layer.

Kept in a leaf module (imports only .errors, which loads app.settings and
gundi_client_v2's errors) so config_manager and state can both use REDIS_RETRY
without importing each other. GUNDI_API_RETRY lives in gundi.py next to the
helpers it decorates.

Iterate stamina with `async for`: its synchronous iterator sleeps with
time.sleep, which inside a coroutine stalls the whole event loop for the
length of the back-off.
"""
# app.settings before gundi_client_v2 (see app/services/errors.py).
from app import settings  # noqa: F401
import httpx
from gundi_client_v2.errors import AuthenticationError, GundiAPIError
from redis.exceptions import RedisError

from .errors import source_status_code

REDIS_RETRY = dict(on=RedisError, attempts=5, wait_initial=1.0, wait_max=30, wait_jitter=3.0)


def _retryable_status(status_code: int) -> bool:
    # 429 and 5xx are the server's problem for now. Any other status is a
    # definite answer that will not change on the next attempt.
    return status_code == 429 or status_code >= 500


def is_transient_gundi_error(exc: BaseException) -> bool:
    """Retry predicate for Gundi API calls (``on=`` for stamina).

    gundi-client-v2 3.x reports a non-2xx Gundi response as ``GundiAPIError``
    and a token-endpoint failure as ``AuthenticationError``; neither subclasses
    ``httpx.HTTPError``, which is all these policies used to retry on, so a
    transient 503 or a Keycloak outage would otherwise fail on the first
    attempt. Transport failures, 429 and 5xx get another try; a 4xx (a missing
    integration, rejected credentials) and a permanent OAuth misconfiguration
    fail at once instead of six times.
    """
    if isinstance(exc, AuthenticationError) and exc.status_code is None:
        # No status from the token endpoint. ``transport``: it never answered.
        # OIDC discovery failures are wrapped without the flag, with httpx's
        # error as the cause: judge the cause by the same rules. Anything else
        # (no token URL or credentials configured, a malformed token response)
        # has no httpx cause and is permanent.
        if exc.transport:
            return True
        return exc.__cause__ is not None and is_transient_gundi_error(exc.__cause__)
    if isinstance(exc, (httpx.HTTPError, GundiAPIError, AuthenticationError)):
        status_code = source_status_code(exc)
        # An httpx error without a status is a transport failure.
        return True if status_code is None else _retryable_status(status_code)
    return False
