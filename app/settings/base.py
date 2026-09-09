import logging
import logging.config
import sys

from environs import Env

env = Env()
env.read_env()

# Imported after read_env(). gundi_client_v2.settings loads a .env of its own,
# walking up from the current working directory, while the runner's read_env()
# above walks up from this file; environs never overrides a key that is already
# set, so whichever loader runs first wins per key. The entry points (app.main,
# app.register, app.services.action_runner and the service modules) import
# app.settings before anything from gundi_client_v2, so the runner's .env wins
# there (pinned by a test). A module imported on its own that reaches
# gundi_client_v2 first lets the client's loader go first; the two only differ
# when the process runs from a directory outside the repo tree, or from one with
# its own .env, while another .env sits at the repo root.
from gundi_client_v2 import settings as gundi_client_settings  # noqa: E402
from gundi_client_v2.errors import TokenCacheConfigError  # noqa: E402
from gundi_client_v2.token_cache import token_cache_from_url  # noqa: E402
from redis.connection import parse_url as _parse_redis_url  # noqa: E402

LOGGING_LEVEL = env.str("LOGGING_LEVEL", "INFO")

DEFAULT_LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "handlers": {
        "console": {
            "level": LOGGING_LEVEL,
            "class": "logging.StreamHandler",
            "stream": sys.stdout
        },
    },
    "loggers": {
        "": {
            "handlers": ["console"],
            "level": LOGGING_LEVEL,
        },
    },
}
logging.config.dictConfig(DEFAULT_LOGGING)

DEFAULT_REQUESTS_TIMEOUT = (10, 20)  # Connect, Read

GUNDI_API_BASE_URL = env.str("GUNDI_API_BASE_URL", None)
GUNDI_API_SSL_VERIFY = env.bool("GUNDI_API_SSL_VERIFY", True)
SENSORS_API_BASE_URL = env.str("SENSORS_API_BASE_URL", None)

# Used in OTel traces/spans to set the 'environment' attribute, used on metrics calculation
TRACE_ENVIRONMENT = env.str("TRACE_ENVIRONMENT", "dev")

# GCP related settings
GCP_PROJECT_ID = env.str("GCP_PROJECT_ID", "cdip-78ca")


# Gundi API authentication is configured through gundi-client-v2's own settings
# (GUNDI_OAUTH_CLIENT_ID / GUNDI_OAUTH_CLIENT_SECRET / GUNDI_OAUTH_ISSUER, with the
# OAUTH_* and KEYCLOAK_* spellings as fallbacks); nothing here reads them.


# Redis settings for state & config managers
REDIS_HOST = env.str("REDIS_HOST", "localhost")
REDIS_PORT = env.int("REDIS_PORT", 6379)
REDIS_STATE_DB = env.int("REDIS_STATE_DB", 0)
REDIS_CONFIGS_DB = env.int("REDIS_CONFIGS_DB", 1)  # ToDo: define a convention for DB numbers across services
REDIS_TOKEN_CACHE_DB = env.int("REDIS_TOKEN_CACHE_DB", 2)


def default_token_cache_url(host: str, port: int, db: int) -> str:
    """The runner's Redis as a token cache URL. redis-py parses the URL with
    urllib, so an IPv6 literal (which redis.Redis(host=...) accepts bare) has to
    be bracketed here."""
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    return f"redis://{host}:{port}/{db}"


def _require_explicit_redis_db(url: str) -> None:
    """A redis:// URL must name its database as a number: redis-py maps a
    missing or non-numeric path (``redis://host:6379``, ``redis://host/tokens``)
    to db 0 without a word, which is the runner's state database. Judged with
    redis-py's own parser so the check is by construction what it will do
    (``/2/`` is db 2; ``?db=3`` beats the path)."""
    if not url.startswith(("redis://", "rediss://")):
        return
    if _parse_redis_url(url).get("db") is None:
        raise ValueError(
            "a redis:// token cache URL must name a numeric database index "
            "(e.g. redis://host:6379/2); a missing or non-numeric one would land "
            "tokens in db 0, the state database"
        )


def validated_token_cache_url(url: str) -> str:
    """Return ``url`` if gundi-client-v2 can build a token cache backend from
    it, else "" (tokens shared within the process only) after one warning.

    The client parses the URL when a GundiClient is constructed, and the portal
    client is constructed at import, so an unusable URL would otherwise take the
    whole service down (webhooks, health, config events) over a cache that is an
    optimization. The backend built here is memoized by the client library, so
    this costs no extra connection; redis connects lazily, so an unreachable
    Redis is not detected here and is handled by the client at request time."""
    if not url:
        return ""
    try:
        _require_explicit_redis_db(url)
        token_cache_from_url(url)
    except (TokenCacheConfigError, ValueError) as e:
        # Log the failure, not the URL: a redis:// URL may carry a password.
        logging.getLogger(__name__).warning(
            "GUNDI_TOKEN_CACHE_URL is unusable (%s: %s); Gundi OAuth tokens will be "
            "shared within this process only. Check REDIS_HOST/REDIS_PORT/"
            "REDIS_TOKEN_CACHE_DB or the GUNDI_TOKEN_CACHE_URL override.",
            type(e).__name__, e,
        )
        return ""
    return url


# Shared OAuth token cache (gundi-client-v2 >= 3.7). Every GundiClient the runner
# builds shares one token per set of credentials, in process memory and in this
# backend, so replicas stop minting a fresh Keycloak token per portal call.
# Defaults to the runner's Redis, next to the state (0) and config (1) databases.
# Override with a redis://, rediss:// or file:///dir URL; set it to an empty
# string to share tokens within the process only.
#
# The cache holds OAuth access AND refresh tokens as plaintext JSON for the
# refresh token's lifetime. Secure that database like the config cache (db 1),
# which already holds integration credentials, and prefer rediss:// for a Redis
# reached over a network.
GUNDI_TOKEN_CACHE_URL = validated_token_cache_url(
    env.str(
        "GUNDI_TOKEN_CACHE_URL",
        default_token_cache_url(REDIS_HOST, REDIS_PORT, REDIS_TOKEN_CACHE_DB),
    )
)
# gundi-client-v2 reads its own settings module for the constructor default (at
# construction time, not import time). Installing the runner's URL there means
# every GundiClient() in this process, including bare ones in connector code,
# uses this backend, instead of only the sites that remember a kwarg.
gundi_client_settings.GUNDI_TOKEN_CACHE_URL = GUNDI_TOKEN_CACHE_URL


REGISTER_ON_START = env.bool("REGISTER_ON_START", False)
INTEGRATION_TYPE_SLUG = env.str("INTEGRATION_TYPE_SLUG", None)  # Define a string id here e.g. "my_tracker"
INTEGRATION_TYPE_NAME = env.str("INTEGRATION_TYPE_NAME", None)  # Display name e.g. "My Tracker"; defaults to a name derived from the slug
INTEGRATION_SERVICE_URL = env.str("INTEGRATION_SERVICE_URL", None)  # Define a string id here e.g. "my_tracker"
PROCESS_PUBSUB_MESSAGES_IN_BACKGROUND = env.bool("PROCESS_PUBSUB_MESSAGES_IN_BACKGROUND", False)
PROCESS_WEBHOOKS_IN_BACKGROUND = env.bool("PROCESS_WEBHOOKS_IN_BACKGROUND", True)
MAX_ACTION_EXECUTION_TIME = env.int("MAX_ACTION_EXECUTION_TIME", 60 * 9)  # 10 minutes is the maximum ack timeout

# Settings for system events & commands (EDA)
INTEGRATION_EVENTS_TOPIC = env.str("INTEGRATION_EVENTS_TOPIC", "integration-events")
default_commands_topic = f"{INTEGRATION_TYPE_SLUG}-actions-topic" if INTEGRATION_TYPE_SLUG else None
INTEGRATION_COMMANDS_TOPIC = env.str("INTEGRATION_COMMANDS_TOPIC", default_commands_topic)
TRIGGER_ACTIONS_ALWAYS_SYNC = env.bool("TRIGGER_ACTIONS_ALWAYS_SYNC", False)

# SSRF protection for diagnostic URL forwarding.
# When non-empty, only the listed hostnames are permitted as diagnostic destinations.
# Example: "diagnostics.example.com,hooks.example.org"
DIAGNOSTIC_URL_ALLOWLIST = env.list("DIAGNOSTIC_URL_ALLOWLIST", [])

# SSRF protection for the ephemeral (draft-integration) path. The draft's
# base_url is request-controlled and reaches the connector's HTTP client
# unchanged, so with this on it must be https, resolve only to public
# addresses and, when the allowlist is non-empty, name one of those hosts.
# Off by default: a saved integration's base_url is operator-supplied with no
# host policy either, and the endpoint is reachable only by callers who can
# already reach the runner. Turn it on where the runner can reach addresses
# its callers should not (see app/services/url_policy.py for the caveats).
EPHEMERAL_BASE_URL_BLOCK_PRIVATE_ADDRESSES = env.bool("EPHEMERAL_BASE_URL_BLOCK_PRIVATE_ADDRESSES", False)
EPHEMERAL_BASE_URL_ALLOWLIST = env.list("EPHEMERAL_BASE_URL_ALLOWLIST", [])

# Config cache: write absence sentinels with a Redis-issued generation
# ("null:<epoch>:<n>:<hex>") instead of the bare "null". The generation lets a
# concurrent delete's tombstone win over an in-flight reload's stale snapshot
# and over the consumer's recovery writes (see config_manager). Off by default
# because a rolling deployment runs old and new replicas side by side, and a
# replica on a release without the tolerant reader parses anything but the
# bare "null" as a configuration and fails every lookup of that action. Roll
# a release with the reader out everywhere first, then turn this on.
CONFIG_CACHE_SENTINEL_GENERATIONS = env.bool("CONFIG_CACHE_SENTINEL_GENERATIONS", False)
