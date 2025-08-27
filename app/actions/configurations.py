from app.actions.client import OnyeshaDevice
from .core import InternalActionConfiguration, PullActionConfiguration, AuthActionConfiguration
import pydantic
from app.services.errors import ConfigurationNotFound
from app.services.utils import find_config_for_action, UIOptions, FieldWithUIOptions
class AuthenticateConfig(AuthActionConfiguration):
    username: str
    password: pydantic.SecretStr = pydantic.Field(..., title = "Password", 
                                description = "Password for Onyesha account",
                                format="password")


class PullObservationsConfig(PullActionConfiguration):
    endpoint: str = "mobile/vehicles"

class PullObservationsFromDeviceBatch(InternalActionConfiguration):
    devices: list[OnyeshaDevice]

def get_auth_config(integration):
    # Look for the login credentials, needed for any action
    auth_config = find_config_for_action(
        configurations=integration.configurations,
        action_id="auth"
    )
    if not auth_config:
        raise ConfigurationNotFound(
            f"Authentication settings for integration {str(integration.id)} "
            f"are missing. Please fix the integration setup in the portal."
        )
    return AuthenticateConfig.parse_obj(auth_config.data)

