import logging
import httpx
import dateparser
from datetime import datetime, timezone, timedelta
from app.services.activity_logger import activity_logger
from app.services.state import IntegrationStateManager
from gundi_core.schemas.v2 import Integration
from erclient import ERClient
from er_syncer import er_syncer
from app.actions.configurations import AuthenticateConfig, PullEventsConfig
from app.services.errors import ConfigurationNotFound, ConfigurationValidationError
from app.services.utils import find_config_for_action

logger = logging.getLogger(__name__)
state_manager = IntegrationStateManager()

DEFAULT_DAYS_TO_SYNC = 10

async def action_auth(integration:Integration, action_config: AuthenticateConfig):
    logger.info(f"Executing auth action with integration {integration} and action_config {action_config}...")

    try:
        # Use a request for region info as a proxy for verifying credentials.
        erclient = ERClient(service_root = action_config.source_server, token = action_config.source_token)
        erclient.pulse()

        ERClient(service_root = action_config.dest_server, token = action_config.dest_token)
        erclient.pulse()
        return {"valid_credentials": True}
    
    except httpx.HTTPStatusError as e:
        return {"valid_credentials": False, "status_code": e.response.status_code}


@activity_logger()
async def action_pull_events(integration:Integration, action_config: PullEventsConfig):
    auth_config = _get_auth_config(integration)
    syncer = er_syncer(auth_config, action_config)

    state = await state_manager.get_state(integration.id, "pull_events")
    last_run = state.get('updated_to') or state.get('last_run')

    load_since = state.get('last_run')
    now = datetime.now(tz=timezone.utc)
    if(last_run):
        load_since = dateparser.parse(last_run)
    else:
        load_since = now - timedelta(days=action_config.days_to_sync)
    if not start_date.tzinfo:
        start_date = start_date.replace(tzinfo=timezone.utc)

    syncer.sync(start_date = start_date)
    state['last_run'] = now.isoformat()
    

def _get_auth_config(integration):
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
