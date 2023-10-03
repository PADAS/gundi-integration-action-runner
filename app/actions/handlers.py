import httpx

from .configurations import AuthenticateConfig, PullObservationsConfig
from app.services.utils import find_config_for_action
from app.services.errors import ConfigurationNotFound


async def action_auth(integration, action_config):
    print(f"Executing auth action with integration {integration} and action_config {action_config}...")
    token = await _get_auth_token(
        integration=integration,
        config=_get_auth_config(integration)
    )
    print(f"Authenticated with success. token:{token}")
    return {"valid_credentials": token is not None}


async def action_pull_observations(integration, action_config):
    print(f"Executing pull_observations action with integration {integration} and action_config {action_config}...")
    token = await _get_auth_token(config=_get_auth_config(integration))
    # ToDo: Pull observations


def _get_auth_config(integration):
    # Look for the login credentials, needed for any action
    auth_config = find_config_for_action(
        configurations=integration.configurations,
        action_id="auth"
    )
    if not auth_config:
        raise ConfigurationNotFound(
            f"Authentication settings for integration {str(integration.id)} are missing. Please fix the integration setup in the portal."
        )
    return AuthenticateConfig.parse_obj(auth_config.data)

# ToDo: This could be part of a cellstop client
async def _get_auth_token(integration, config):
    token_endpoint = config.endpoint
    # Remove endpoint from request
    del config.endpoint

    url = f"{integration.base_url}{token_endpoint}"

    async with httpx.AsyncClient(timeout=120) as session:
        response = await session.post(url, json=config.dict())
        response.raise_for_status()

    json_response = response.json()
    return json_response["token"]
