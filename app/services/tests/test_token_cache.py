"""Contract tests for the shared OAuth token cache.

gundi-client-v2 3.7 keeps one token per set of credentials in process memory
and, when configured with a token cache URL, in Redis, so runner replicas stop
minting a fresh Keycloak token for nearly every portal call. These tests pin
that the runner derives that URL from its Redis settings, installs it as the
client library's default so every GundiClient() in the process uses it, and
never lets an unusable URL take the service down.

The tests read the loaded settings rather than reloading the settings module
against a patched environment: a reload re-runs env.read_env(), so a
developer's repo .env would leak into the assertions. Under pytest the runner
is configured for in-process sharing only (conftest sets GUNDI_TOKEN_CACHE_URL
to ""), so the Redis-backend assertions build their URLs explicitly.
"""
import itertools
import logging
import os
import pathlib
import subprocess
import sys
import tempfile

import httpx
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


_REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
_SUBPROCESS_KEYS = ("GUNDI_TOKEN_CACHE_URL", "REDIS_HOST", "REDIS_PORT", "REDIS_TOKEN_CACHE_DB")


def _settings_in_subprocess(env_overrides: dict, probe: str) -> str:
    """Run ``probe`` in a fresh interpreter with the runner's env adjusted, so
    a settings value can be checked without reloading the settings module in
    this process (a reload re-runs env.read_env()). Runs from the repo root,
    with gundi-client-v2's own .env loader pointed at an empty file. The
    runner's read_env() still finds a repo-root .env, which environs would let
    win over the keys removed here, so such a .env skips these tests instead
    of failing them. Returns the last stdout line; the settings module logs to
    stdout, so a warning may precede it."""
    repo_dotenv = _REPO_ROOT / ".env"
    if repo_dotenv.exists():
        from dotenv import dotenv_values

        clashing = sorted(k for k in dotenv_values(repo_dotenv) if k in _SUBPROCESS_KEYS)
        if clashing:
            pytest.skip(f"{repo_dotenv} sets {clashing}; the runner's .env loader would override the test environment")
    env = {k: v for k, v in os.environ.items() if k not in _SUBPROCESS_KEYS}
    env.update(env_overrides)
    with tempfile.NamedTemporaryFile(prefix="gundi-empty-", suffix=".env") as empty:
        env["GUNDI_CLIENT_ENVFILE"] = empty.name
        result = subprocess.run(
            [sys.executable, "-c", probe], capture_output=True, text=True, timeout=60, env=env, cwd=_REPO_ROOT,
        )
    assert result.returncode == 0, result.stderr
    return result.stdout.strip().splitlines()[-1]


@pytest.mark.parametrize("entry_point", [
    "app.main", "app.register", "app.services.action_runner", "app.services.config_manager", "app.services.webhooks",
    # Leaf modules that import gundi_client_v2.errors: reaching the library's
    # package runs its .env loader, so they must load the runner's first too.
    "app.services.errors", "app.services.retry_policies", "app.services.gundi",
])
def test_runner_settings_load_before_the_client_library(entry_point):
    """Both the runner and gundi-client-v2 load a .env through environs'
    read_env(), and the first loader wins per key. app/settings/base.py imports
    the client only after its own read_env(), so the runner's .env wins wherever
    app.settings is reached first. Observed by spying on read_env itself:
    sys.modules order is completion order, so it cannot tell the two apart.

    Asserts which loader ran first and that the client's ran at all, not how
    many ran: a connector is free to add its own read_env() in
    app/settings/integration.py, and counting would fail it for doing so."""
    line = _settings_in_subprocess(
        {},
        # Env.read_env is a staticmethod in environs.
        "import inspect, environs; calls = []; orig = environs.Env.read_env\n"
        "def spy(*a, **k):\n"
        "    calls.append(inspect.stack()[1].filename); return orig(*a, **k)\n"
        "environs.Env.read_env = staticmethod(spy)\n"
        f"import {entry_point}\n"
        "paths = [c.replace('\\\\', '/') for c in calls]\n"
        "print(paths[0].endswith('app/settings/base.py'), "
        "any('gundi_client_v2/settings.py' in p for p in paths))",
    )
    assert line == "True True", (
        f"{entry_point}: expected the runner's .env loader first and the client's to run ({line})"
    )


def test_settings_derive_the_url_from_redis_settings_when_not_overridden():
    line = _settings_in_subprocess(
        {"REDIS_HOST": "cache.internal", "REDIS_PORT": "6380", "REDIS_TOKEN_CACHE_DB": "7"},
        "from app import settings; print(settings.REDIS_TOKEN_CACHE_DB, settings.GUNDI_TOKEN_CACHE_URL)",
    )
    assert line == "7 redis://cache.internal:6380/7"


def test_settings_honour_the_env_override():
    line = _settings_in_subprocess(
        {"GUNDI_TOKEN_CACHE_URL": "file:///var/cache/gundi-tokens"},
        "from app import settings; print(settings.GUNDI_TOKEN_CACHE_URL)",
    )
    assert line == "file:///var/cache/gundi-tokens"


@pytest.mark.parametrize("good", [
    "redis://localhost:6379/2",
    "redis://localhost:6379/2/",           # redis-py reads db 2
    "rediss://cache.internal:6380/0",
    "redis://localhost:6379?db=3",
    "redis://[::1]:6379/2",
    "file:///var/cache/gundi-tokens",
])
def test_validated_token_cache_url_keeps_a_usable_url(good):
    assert validated_token_cache_url(good) == good


def test_validated_token_cache_url_empty_means_in_process_only():
    assert validated_token_cache_url("") == ""
    assert validated_token_cache_url(None) == ""


@pytest.mark.parametrize("bad", [
    "localhost:6379",                 # no scheme: TokenCacheConfigError
    "redis://::1:6379/2",             # unbracketed IPv6: redis-py ValueError
    "file://relative/dir",            # file URL with a host: TokenCacheConfigError
    "redis://localhost:6379/tokens",  # redis-py would silently use db 0, the state database
    "redis://localhost:6379",         # no database at all: same
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


def test_default_url_builds_a_redis_backend_on_the_named_db():
    backend = token_cache_from_url(default_token_cache_url("cache.internal", 6380, 7))

    assert _backend_signature(backend) == ("redis", "cache.internal", 6380, 7)


def test_bare_client_reads_the_installed_url_at_construction_time(monkeypatch):
    """``_token_store._backend`` is gundi-client-v2 internal API; if this breaks
    on an upgrade, re-check that the library still reads its settings module
    for the constructor default (that is what lets the runner configure every
    GundiClient() in the process from app.settings)."""
    monkeypatch.setattr(client_settings, "GUNDI_TOKEN_CACHE_URL", "redis://[::1]:6379/9")

    client = GundiClient(oauth_client_id="svc", oauth_token_url="https://auth.example.com/token")

    assert _backend_signature(client._token_store._backend) == ("redis", "::1", 6379, 9)


def test_portal_client_is_bare_env_driven():
    """Credentials must stay env-driven: bareness is what lets
    GUNDI_USERNAME/GUNDI_PASSWORD select the password grant. (The backend it
    shares is not asserted here: tests run with no backend, see conftest; the
    subprocess test below covers that the URL is installed before the portal
    client is built.)"""
    from app.services.action_runner import _portal

    assert isinstance(_portal, GundiClient)
    assert _portal.username == client_settings.GUNDI_USERNAME
    assert _portal.password == client_settings.GUNDI_PASSWORD
    assert _portal.client_id == client_settings.OAUTH_CLIENT_ID
    assert _portal.client_secret == client_settings.OAUTH_CLIENT_SECRET


def test_settings_load_before_connector_code_runs():
    """app.actions executes the connector's handlers module at import. A
    GundiClient() built at module scope there must already see the runner's
    token cache URL, so importing app.actions alone has to load app.settings
    (and install the URL) first; the portal client built at import must then
    hold the Redis backend. A subprocess, so nothing here is pre-imported."""
    probe = (
        "import sys; import app.actions; "
        "assert 'app.settings' in sys.modules, 'app.settings not loaded by app.actions'; "
        "from app import settings; from gundi_client_v2 import settings as cs; "
        "assert cs.GUNDI_TOKEN_CACHE_URL == settings.GUNDI_TOKEN_CACHE_URL == 'redis://cache.internal:6380/7'; "
        "from app.services.action_runner import _portal; "
        "kw = _portal._token_store._backend._client.connection_pool.connection_kwargs; "
        "print(kw['host'], kw['port'], kw['db'])"
    )
    line = _settings_in_subprocess({"REDIS_HOST": "cache.internal", "REDIS_PORT": "6380", "REDIS_TOKEN_CACHE_DB": "7"}, probe)
    assert line == "cache.internal 6380 7"


def test_portal_singleton_token_is_reset_between_tests():
    """The autouse fixture in conftest resets the module-level portal client's
    instance token, which get_access_token consults before the shared cache."""
    from app.services.action_runner import _portal

    assert _portal.cached_token is None
    _portal.cached_token = "leaked-if-seen-by-the-next-test"


def _idp_and_api(accepted):
    """A transport standing in for the IdP and the Gundi API: the token endpoint
    issues a distinct token per call, and the API answers 401 to any bearer
    outside ``accepted`` — the shape of a token the IdP invalidated early."""
    issued = (f"tok-{n}" for n in itertools.count())
    calls = {"token": 0, "api": 0}

    def handler(request):
        if request.url.path.endswith("/token"):
            calls["token"] += 1
            return httpx.Response(200, json={
                "access_token": next(issued), "token_type": "Bearer", "expires_in": 1800,
                "refresh_token": "r", "refresh_expires_in": 3600,
            })
        calls["api"] += 1
        bearer = request.headers.get("authorization", "").split(" ")[-1]
        if bearer not in accepted:
            return httpx.Response(401, json={"detail": "Invalid token."})
        return httpx.Response(200, json={"api_key": "k"})

    return httpx.MockTransport(handler), calls


@pytest.mark.asyncio
async def test_a_portal_token_the_api_rejects_is_replaced_by_the_client(mocker):
    """The runner carries no 401 workaround of its own: gundi-client-v2 3.7.1
    replaces a token the API rejects with a plain 401 and retries once
    (PADAS/gundi-client#61). This pins both halves of relying on that — that
    the pinned client still does it, and that a runner portal call goes
    through the client path that does — so dropping either is a test failure
    rather than a stale token served to every replica until it expires.
    """
    from app.services import gundi as gundi_helpers

    transport, calls = _idp_and_api(accepted={"tok-1"})

    def build_client():
        client = GundiClient(
            base_url="https://api.example.org", oauth_client_id="svc", oauth_client_secret="s",
            oauth_token_url="https://auth.example.org/realms/x/protocol/openid-connect/token",
            token_cache_url="",
        )
        client._session = httpx.AsyncClient(transport=transport)
        return client

    mocker.patch.object(gundi_helpers, "GundiClient", build_client)

    assert await gundi_helpers._get_gundi_api_key("abc-123") == "k"
    assert calls == {"token": 2, "api": 2}, "one replacement, one retry"
