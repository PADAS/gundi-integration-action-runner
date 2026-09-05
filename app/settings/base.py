import logging.config
import sys
from environs import Env

env = Env()
env.read_env()

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

CDIP_API_ENDPOINT = env.str("CDIP_API_ENDPOINT", None)
CDIP_ADMIN_ENDPOINT = env.str("CDIP_ADMIN_ENDPOINT", None)
PORTAL_API_ENDPOINT = f"{CDIP_ADMIN_ENDPOINT}/api/v1.0"
PORTAL_OUTBOUND_INTEGRATIONS_ENDPOINT = (
    f"{PORTAL_API_ENDPOINT}/integrations/outbound/configurations"
)
PORTAL_INBOUND_INTEGRATIONS_ENDPOINT = (
    f"{PORTAL_API_ENDPOINT}/integrations/inbound/configurations"
)
GUNDI_API_BASE_URL = env.str("GUNDI_API_BASE_URL", None)
GUNDI_API_SSL_VERIFY = env.bool("GUNDI_API_SSL_VERIFY", True)
SENSORS_API_BASE_URL = env.str("SENSORS_API_BASE_URL", None)

# Used in OTel traces/spans to set the 'environment' attribute, used on metrics calculation
TRACE_ENVIRONMENT = env.str("TRACE_ENVIRONMENT", "dev")

# GCP related settings
GCP_PROJECT_ID = env.str("GCP_PROJECT_ID", "cdip-78ca")


KEYCLOAK_ALGORITHMS = env.list("KEYCLOAK_ALGORITHMS", ["RS256", "HS256"])
KEYCLOAK_AUDIENCE = env.str("KEYCLOAK_AUDIENCE", None)
KEYCLOAK_AUTH_SERVICE = env.str("KEYCLOAK_AUTH_SERVICE", None)
KEYCLOAK_REALM = env.str("KEYCLOAK_REALM", None)
KEYCLOAK_ISSUER = f"{KEYCLOAK_AUTH_SERVICE}/realms/{KEYCLOAK_REALM}"


# Redis settings for state & config managers
REDIS_HOST = env.str("REDIS_HOST", "localhost")
REDIS_PORT = env.int("REDIS_PORT", 6379)
REDIS_STATE_DB = env.int("REDIS_STATE_DB", 0)
REDIS_CONFIGS_DB = env.int("REDIS_CONFIGS_DB", 1)  # ToDo: define a convention for DB numbers across services


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
