import json
import logging

from app.actions.configurations import AuthenticateConfig, PullObservationsConfig
from app.services.state import IntegrationStateManager


logger = logging.getLogger(__name__)


state_manager = IntegrationStateManager()


async def action_auth(integration, action_config: AuthenticateConfig):
    logger.info(f"Executing auth action with integration {integration} and action_config {action_config}...")
    if action_config.password == "valid_password" and action_config.username == "valid_username":
        return {"valid_credentials": True}
    else:
        return {"valid_credentials": False}


async def action_fetch_samples(integration, action_config: PullObservationsConfig):
    logger.info(f"Executing fetch_samples action with integration {integration} and action_config {action_config}...")
    vehicles = {
        "observations_extracted": [
            [
            {
                "object_id": "75848f54-312d-4e4b-a931-546880931f68",
                "location":{
                    "lat":27.192358,
                    "lon":13.273482
                }
            },
            {
                "object_id": "34236d0f-b02d-4bef-bb89-a7bb3bfafa97",
                "location":{
                    "lat":55.847321,
                    "lon":72.120293
                }
            },
            {
                "object_id": "41c7d231-7cf9-428f-8699-000723361e85",
                "location":{
                    "lat":31.847263,
                    "lon":44.758383
                }
            },
            {
                "object_id": "fe01afd6-3c18-487b-8359-6ad109ca4043",
                "location":{
                    "lat":29.925873,
                    "lon":75.473293
                }
            }]
        ]
    }
    if action_config.password == "valid_password" and action_config.username == "valid_username":
        return {
            "observations_extracted": len(vehicles),
            "observations": [vehicle for vehicle in vehicles]
        }
    else:
        return {"valid_credentials": False}
    
