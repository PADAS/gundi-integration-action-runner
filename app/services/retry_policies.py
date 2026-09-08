"""Retry policies shared across the service layer.

Kept in a leaf module (no app imports) so config_manager and state can both
use REDIS_RETRY without importing each other. GUNDI_API_RETRY lives in
gundi.py next to the helpers it decorates.

Iterate stamina with `async for`: its synchronous iterator sleeps with
time.sleep, which inside a coroutine stalls the whole event loop for the
length of the back-off.
"""
import httpx
from gundi_client_v2.errors import AuthenticationError, GundiAPIError
from redis.exceptions import RedisError

REDIS_RETRY = dict(on=RedisError, attempts=5, wait_initial=1.0, wait_max=30, wait_jitter=3.0)


def _retryable_status(status_code) -> bool:
    # No status: the request never got an answer (transport failure, malformed
    # body). 429 and 5xx are the server's problem for now. Any other 4xx is a
    # definite answer that will not change on the next attempt.
    return status_code is None or status_code == 429 or status_code >= 500


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
    if isinstance(exc, httpx.HTTPStatusError):
        return _retryable_status(exc.response.status_code)
    if isinstance(exc, httpx.HTTPError):
        return True
    if isinstance(exc, GundiAPIError):
        return _retryable_status(exc.status_code)
    if isinstance(exc, AuthenticationError):
        # A status-less AuthenticationError is usually permanent: no token URL or
        # credentials configured, a malformed token response. Only a token
        # endpoint that never answered (``transport``), or OIDC discovery that
        # failed on the network (wrapped without the flag; the cause is httpx's
        # transport error), is worth another attempt.
        if exc.transport:
            return True
        if exc.status_code is None:
            cause = exc.__cause__
            return isinstance(cause, httpx.HTTPError) and not isinstance(cause, httpx.HTTPStatusError)
        return exc.status_code == 429 or exc.status_code >= 500
    return False
