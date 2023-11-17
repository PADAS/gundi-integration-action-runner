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

# Used in OTel traces/spans to set the 'environment' attribute, used on metrics calculation
TRACE_ENVIRONMENT = env.str("TRACE_ENVIRONMENT", "dev")

# GCP related settings
GCP_PROJECT_ID = env.str("GCP_PROJECT_ID", "cdip-78ca")


KEYCLOAK_ALGORITHMS = env.list("KEYCLOAK_ALGORITHMS", ["RS256", "HS256"])
KEYCLOAK_AUDIENCE = env.str("KEYCLOAK_AUDIENCE", None)
KEYCLOAK_AUTH_SERVICE = env.str("KEYCLOAK_AUTH_SERVICE", None)
KEYCLOAK_REALM = env.str("KEYCLOAK_REALM", None)
KEYCLOAK_ISSUER = f"{KEYCLOAK_AUTH_SERVICE}/realms/{KEYCLOAK_REALM}"

# Redis settings for state manager
REDIS_HOST = env.str("REDIS_HOST", "localhost")
REDIS_PORT = env.int("REDIS_PORT", 6379)
REDIS_STATE_DB = env.int("REDIS_STATE_DB", 0)
