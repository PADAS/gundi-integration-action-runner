"""Contract tests for the shared OAuth token cache.

gundi-client-v2 3.7 keeps one token per set of credentials in process memory
and, when a client is given a ``token_cache_url``, in Redis, so runner replicas
stop minting a fresh Keycloak token for nearly every portal call. These tests
pin that the runner derives that URL from its Redis settings and hands it to
every GundiClient it builds.
"""
import importlib

import pytest
from gundi_client_v2 import GundiClient
from gundi_client_v2.token_cache import RedisTokenCache

from app import settings


def _reload_settings():
    # app.settings re-exports app.settings.base, so the base module has to be
    # re-evaluated first and the package reloaded to pick up the new values.
    import app.settings.base

    importlib.reload(app.settings.base)
    importlib.reload(settings)


@pytest.fixture
def reload_settings():
    """Re-evaluate app.settings against the patched environment and restore it
    afterwards. Request it BEFORE ``monkeypatch`` so that the environment is
    restored before the final reload."""
    yield _reload_settings
    _reload_settings()


def test_token_cache_url_is_derived_from_redis_settings(reload_settings, monkeypatch):
    monkeypatch.delenv("GUNDI_TOKEN_CACHE_URL", raising=False)
    monkeypatch.setenv("REDIS_HOST", "cache.internal")
    monkeypatch.setenv("REDIS_PORT", "6380")
    monkeypatch.setenv("REDIS_TOKEN_CACHE_DB", "7")

    reload_settings()

    assert settings.REDIS_TOKEN_CACHE_DB == 7
    assert settings.GUNDI_TOKEN_CACHE_URL == "redis://cache.internal:6380/7"


def test_token_cache_db_defaults_next_to_state_and_config_dbs(reload_settings, monkeypatch):
    monkeypatch.delenv("REDIS_TOKEN_CACHE_DB", raising=False)
    monkeypatch.delenv("GUNDI_TOKEN_CACHE_URL", raising=False)

    reload_settings()

    assert settings.REDIS_TOKEN_CACHE_DB == 2
    assert settings.GUNDI_TOKEN_CACHE_URL.endswith("/2")


def test_token_cache_url_env_override_wins(reload_settings, monkeypatch):
    monkeypatch.setenv("GUNDI_TOKEN_CACHE_URL", "file:///var/cache/gundi-tokens")

    reload_settings()

    assert settings.GUNDI_TOKEN_CACHE_URL == "file:///var/cache/gundi-tokens"


@pytest.mark.asyncio
async def test_api_key_lookup_client_uses_shared_token_cache(mocker, mock_gundi_client_v2_class):
    mock_gundi_client_v2_class.return_value.get_integration_api_key = mocker.AsyncMock(return_value="an-api-key")
    mocker.patch("app.services.gundi.GundiClient", mock_gundi_client_v2_class)
    from app.services.gundi import _get_gundi_api_key

    await _get_gundi_api_key(integration_id="8a0b8b2e-0a3c-4f7a-8f1b-6b0f5d6a4d11")

    mock_gundi_client_v2_class.assert_called_once_with(token_cache_url=settings.GUNDI_TOKEN_CACHE_URL)


@pytest.mark.asyncio
async def test_config_reload_client_uses_shared_token_cache(
        mocker, mock_redis_empty, mock_gundi_client_v2_class, integration_v2,
):
    mocker.patch("app.services.config_manager.redis", mock_redis_empty)
    mocker.patch("app.services.config_manager.GundiClient", mock_gundi_client_v2_class)
    from app.services.config_manager import IntegrationConfigurationManager

    await IntegrationConfigurationManager().get_integration(str(integration_v2.id))

    mock_gundi_client_v2_class.assert_called_once_with(token_cache_url=settings.GUNDI_TOKEN_CACHE_URL)


def test_portal_client_is_bare_and_has_redis_token_cache_backend():
    """The module-level portal client is built at import time, so the only
    observable is the backend it ended up with. ``_token_store._backend`` is
    gundi-client-v2 internal API; if this breaks on an upgrade, re-check that
    ``action_runner._portal`` still passes ``token_cache_url``. Credentials
    must stay env-driven: bareness is what lets GUNDI_USERNAME/GUNDI_PASSWORD
    select the password grant."""
    from gundi_client_v2 import settings as client_settings

    from app.services.action_runner import _portal

    assert isinstance(_portal, GundiClient)
    assert _portal.client_id == client_settings.OAUTH_CLIENT_ID
    assert _portal.client_secret == client_settings.OAUTH_CLIENT_SECRET
    assert settings.GUNDI_TOKEN_CACHE_URL.startswith("redis://")
    assert isinstance(_portal._token_store._backend, RedisTokenCache)
