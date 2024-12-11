import pydantic

from .core import AuthActionConfiguration, PullActionConfiguration, ExecutableActionMixin
from ..services.errors import ConfigurationNotFound
from ..services.utils import find_config_for_action


class AuthenticateConfig(AuthActionConfiguration, ExecutableActionMixin):
    username: str
    password: pydantic.SecretStr = pydantic.Field(..., format="password")
    grant_type: str = "password"


class PullObservationsConfig(PullActionConfiguration):
    observations_per_request: int = 200


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


def get_pull_config(integration):
    # Look for the login credentials, needed for any action
    pull_config = find_config_for_action(
        configurations=integration.configurations,
        action_id="pull_observations"
    )
    if not pull_config:
        raise ConfigurationNotFound(
            f"Authentication settings for integration {str(integration.id)} "
            f"are missing. Please fix the integration setup in the portal."
        )
    return PullObservationsConfig.parse_obj(pull_config.data)
