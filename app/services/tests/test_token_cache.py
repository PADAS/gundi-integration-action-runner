"""Contract tests for the shared OAuth token cache.

gundi-client-v2 3.7 keeps one token per set of credentials in process memory
and, when configured with a token cache URL, in Redis, so runner replicas stop
minting a fresh Keycloak token for nearly every portal call. These tests pin
that the runner derives that URL from its Redis settings, installs it as the
client library's default so every GundiClient() in the process uses it, and
never lets an unusable URL take the service down.

The tests read the loaded settings rather than reloading the settings module
against a patched environment: a reload re-runs env.read_env(), so a
developer's repo .env would leak into the assertions.
"""
import logging
import os

import pytest
from gundi_client_v2 import GundiClient
from gundi_client_v2 import settings as client_settings
from gundi_client_v2.token_cache import RedisTokenCache, token_cache_from_url

from app import settings
from app.settings.base import default_token_cache_url, validated_token_cache_url


def test_default_token_cache_url_derives_from_redis_settings():
    assert default_token_cache_url("cache.internal", 6380, 7) == "redis://cache.internal:6380/7"


def test_default_token_cache_url_brackets_ipv6_hosts():
    # redis.Redis(host="::1") is fine, but a URL needs the brackets or redis-py
    # reads ":1:6379" as the port.
    assert default_token_cache_url("::1", 6379, 2) == "redis://[::1]:6379/2"
    assert default_token_cache_url("[::1]", 6379, 2) == "redis://[::1]:6379/2"
    assert validated_token_cache_url(default_token_cache_url("::1", 6379, 2)) == "redis://[::1]:6379/2"


def test_token_cache_db_defaults_next_to_state_and_config_dbs():
    if os.environ.get("REDIS_TOKEN_CACHE_DB") is not None:
        pytest.skip("REDIS_TOKEN_CACHE_DB is set in this environment")
    assert settings.REDIS_TOKEN_CACHE_DB == 2


def test_settings_url_is_the_derived_default_or_the_env_override():
    override = os.environ.get("GUNDI_TOKEN_CACHE_URL")
    if override is not None:
        assert settings.GUNDI_TOKEN_CACHE_URL == validated_token_cache_url(override)
    else:
        assert settings.GUNDI_TOKEN_CACHE_URL == default_token_cache_url(
            settings.REDIS_HOST, settings.REDIS_PORT, settings.REDIS_TOKEN_CACHE_DB
        )


@pytest.mark.parametrize("good", ["redis://localhost:6379/2", "rediss://cache.internal:6380/0", "file:///var/cache/gundi-tokens"])
def test_validated_token_cache_url_keeps_a_usable_url(good):
    assert validated_token_cache_url(good) == good


def test_validated_token_cache_url_empty_means_in_process_only():
    assert validated_token_cache_url("") == ""
    assert validated_token_cache_url(None) == ""


@pytest.mark.parametrize("bad", [
    "localhost:6379",           # no scheme: TokenCacheConfigError
    "redis://::1:6379/2",       # unbracketed IPv6: redis-py ValueError
    "file://relative/dir",      # file URL with a host: TokenCacheConfigError
])
def test_validated_token_cache_url_falls_back_to_memory_with_one_warning(bad, caplog):
    """The portal client is built at import, so a URL the client cannot parse
    must degrade to in-process sharing instead of crashing the service."""
    with caplog.at_level(logging.WARNING, logger="app.settings.base"):
        assert validated_token_cache_url(bad) == ""
    warnings = [r for r in caplog.records if "GUNDI_TOKEN_CACHE_URL is unusable" in r.getMessage()]
    assert len(warnings) == 1
    assert bad not in warnings[0].getMessage(), "a cache URL may carry a password; never log it"


def test_runner_url_is_installed_as_the_client_library_default():
    """gundi-client-v2 reads GUNDI_TOKEN_CACHE_URL from its own settings module
    at construction time. The runner installs its derived URL there so a bare
    GundiClient() anywhere in the process (connector code included) shares the
    backend, not only the sites that pass a kwarg."""
    assert client_settings.GUNDI_TOKEN_CACHE_URL == settings.GUNDI_TOKEN_CACHE_URL


def _expected_backend():
    # None when the developer opted out of a backend.
    return token_cache_from_url(settings.GUNDI_TOKEN_CACHE_URL)


def _backend_signature(backend):
    """What a backend points at, comparable across instances. The conftest
    fixture clears the library's memoized backends between tests, so a client
    built at import (the portal) and one built now hold different objects for
    the same URL; object identity is the wrong comparison."""
    if backend is None:
        return None
    if isinstance(backend, RedisTokenCache):
        kwargs = backend._client.connection_pool.connection_kwargs
        return ("redis", kwargs.get("host"), kwargs.get("port"), kwargs.get("db"))
    return (type(backend).__name__, vars(backend))


def test_bare_client_uses_the_runner_token_cache_backend():
    """``_token_store._backend`` is gundi-client-v2 internal API; if this breaks
    on an upgrade, re-check that the library still reads its settings module
    for the constructor default."""
    client = GundiClient(oauth_client_id="svc", oauth_token_url="https://auth.example.com/token")

    assert _backend_signature(client._token_store._backend) == _backend_signature(_expected_backend())


def test_portal_client_is_bare_and_shares_the_token_cache_backend():
    """Credentials must stay env-driven: bareness is what lets
    GUNDI_USERNAME/GUNDI_PASSWORD select the password grant."""
    from app.services.action_runner import _portal

    assert isinstance(_portal, GundiClient)
    assert _portal.username == client_settings.GUNDI_USERNAME
    assert _portal.password == client_settings.GUNDI_PASSWORD
    assert _portal.client_id == client_settings.OAUTH_CLIENT_ID
    assert _portal.client_secret == client_settings.OAUTH_CLIENT_SECRET
    assert _backend_signature(_portal._token_store._backend) == _backend_signature(_expected_backend())


def test_default_backend_is_redis():
    if os.environ.get("GUNDI_TOKEN_CACHE_URL") is not None:
        pytest.skip("GUNDI_TOKEN_CACHE_URL is overridden in this environment")
    assert isinstance(_expected_backend(), RedisTokenCache)


def test_portal_singleton_token_is_reset_between_tests():
    """The autouse fixture in conftest resets the module-level portal client's
    instance token, which get_access_token consults before the shared cache."""
    from app.services.action_runner import _portal

    assert _portal.cached_token is None
    _portal.cached_token = "leaked-if-seen-by-the-next-test"
