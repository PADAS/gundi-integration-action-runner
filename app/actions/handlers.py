import json
import logging
from datetime import datetime, timezone, timedelta
from app.actions.configurations import AuthenticateConfig, ReadDiscourseTopicsConfig
from app.services.activity_logger import activity_logger
from app.services.action_scheduler import crontab_schedule
from app.services.state import IntegrationStateManager
from app.services.errors import ConfigurationNotFound, ConfigurationValidationError
from app.services.utils import find_config_for_action
from gundi_core.schemas.v2 import Integration

from app.actions.discourse import get_topics_per_tag
from app.utils import store

logger = logging.getLogger(__name__)
state_manager = IntegrationStateManager()


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


async def action_auth(integration:Integration, action_config: AuthenticateConfig):
    logger.info(f"Executing auth action with integration {integration} and action_config {action_config}...")
    return {"valid_credentials": True} if all([action_config.username, action_config.apikey]) else {"valid_credentials": False}


@activity_logger()
@crontab_schedule(crontab='*/10 * * * *')
async def action_fetch_community_posts(integration:Integration, action_config: ReadDiscourseTopicsConfig):
    logger.info(f"Executing pull_observations action with integration {integration} and action_config {action_config}...")

    auth_config = get_auth_config(integration)

    topics = await get_topics_per_tag(topics_url=action_config.discourse_feed_url, username=auth_config.username, apikey=auth_config.apikey.get_secret_value())

    
    await store(bucket_name=action_config.storage_bucket, 
                destination_blob_name=action_config.storage_blob, data=json.dumps(topics, indent=2))
    return {"success": True}
