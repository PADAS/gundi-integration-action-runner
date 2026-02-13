# Kineis/CLS API (CONNECTORS-836)
from .base import env

# Override base INTEGRATION_TYPE_SLUG for Kineis
INTEGRATION_TYPE_SLUG = env.str("INTEGRATION_TYPE_SLUG", "kineis")

KINEIS_AUTH_BASE_URL = env.str(
    "KINEIS_AUTH_BASE_URL",
    "https://account.groupcls.com",
)
KINEIS_API_BASE_URL = env.str(
    "KINEIS_API_BASE_URL",
    "https://api.groupcls.com",
)
KINEIS_AUTH_PATH = env.str("KINEIS_AUTH_PATH", "/auth/realms/cls/protocol/openid-connect/token")
