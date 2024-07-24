import asyncio
import datetime
import httpx
import logging
import stamina
import random
import app.actions.client as client

from datetime import timezone

from app.actions.configurations import AuthenticateConfig, PullEventsConfig
from app.services.activity_logger import activity_logger
from app.services.gundi import send_events_to_gundi
from app.services.state import IntegrationStateManager


GFW_INTEGRATED_ALERTS = "gfwgladalert"
GFW_FIRE_ALERT = "gfwfirealert"


logger = logging.getLogger(__name__)
state_manager = IntegrationStateManager()


async def handle_transformed_data(transformed_data, integration_id, action_id):
    try:
        response = await send_events_to_gundi(
            events=transformed_data,
            integration_id=integration_id
        )
    except httpx.HTTPError as e:
        msg = f'Sensors API returned error for integration_id: {integration_id}. Exception: {e}'
        logger.exception(
            msg,
            extra={
                'needs_attention': True,
                'integration_id': integration_id,
                'action_id': action_id
            }
        )
        return [msg]
    else:
        return response


def transform_fire_alert(alert):
    event_time = alert.acq_date.replace(tzinfo=timezone.utc).isoformat()
    title = (
        "GFW VIIRS Alert"
        if alert.num_clustered_alerts < 2
        else f"GFW VIIRS Cluster ({alert.num_clustered_alerts} alerts)"
    )

    return dict(
        title=title,
        event_type=GFW_FIRE_ALERT,
        recorded_at=event_time,
        location={"lat": alert.latitude, "lon": alert.longitude},
        event_details=dict(
            bright_ti4=alert.bright_ti4,
            bright_ti5=alert.bright_ti5,
            scan=alert.scan,
            satellite=alert.satellite,
            frp=alert.frp,
            track=alert.track,
            clustered_alerts=alert.num_clustered_alerts,
        )
    )


def transform_integrated_alert(alert):
    title = (
        "GFW Integrated Deforestation Alert"
        if alert.num_clustered_alerts < 2
        else f"GFW Integrated Deforestation Cluster ({alert.num_clustered_alerts} alerts)"
    )

    return dict(
        title=title,
        event_type=GFW_INTEGRATED_ALERTS,
        recorded_at=alert.recorded_at,
        location={"lat": alert.latitude, "lon": alert.longitude},
        event_details=dict(
            confidence=alert.confidence,
            clustered_alerts=alert.num_clustered_alerts,
        )
    )


async def action_auth(integration, action_config: AuthenticateConfig):
    logger.info(f"Executing auth action with integration {integration} and action_config {action_config}...")
    try:
        token = await client.get_token(
            integration=integration,
            config=action_config
        )
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 401:
            return {"valid_credentials": False}
        else:
            logger.error(f"Error getting token from GFW API. status code: {e.response.status_code}")
            raise e
    except httpx.HTTPError as e:
        message = f"auth action returned error."
        logger.exception(message, extra={
            "integration_id": str(integration.id),
            "attention_needed": True
        })
        raise e
    else:
        logger.info(f"Authenticated with success. token: {token}")
        return {"valid_credentials": token is not None}


@activity_logger()
async def action_pull_events(integration, action_config: PullEventsConfig):

    if await state_manager.is_quiet_period(str(integration.id), "pull_events"):
        return {"message": 'Quiet period is active.'}


    logger.info(f"Executing 'pull_events' action with integration {integration} and action_config {action_config}...")

    # Get AOI data first.
    aoi_data = await client.get_aoi_data(integration=integration,config=action_config)

    # Some AOIs do not have an associated Geostore so we short-circuit here a report in the logs.
    if not aoi_data.attributes.geostore:
        msg = f"No Geostore associated with AOI {aoi_data.id}."
        logger.error(
            msg,
            extra={
                "needs_attention": True,    
                "integration_id": str(integration.id),
                "aoi_id": aoi_data.id,
                "gfw_url": integration.base_url,
            },
        )
        return [msg]

        # Create a list of tasks.
    tasklist = [asyncio.create_task(get_fire_alerts(aoi_data, integration, action_config)),
                 asyncio.create_task(get_integrated_alerts(aoi_data, integration, action_config))]
    
    # Wait until they're all finished.
    results = await asyncio.gather(*tasklist)

    quiet_minutes = random.randint(60, 180) # Todo: change to be more fair.
    await state_manager.set_quiet_period(str(integration.id), "pull_events", datetime.timedelta(minutes=quiet_minutes))

    # The results are in the order of the tasklist.
    return results
 

async def get_fire_alerts(aoi_data, integration, action_config):

    if not action_config.include_fire_alerts:
        return {'type': 'fire_alerts', "message": 'Not included in action config.'}

    fire_alerts = await client.get_fire_alerts(
        aoi_data=aoi_data,
        integration=integration,
        config=action_config
    )
    if fire_alerts:
        logger.info(f"Fire alerts pulled with success.")
        transformed_data = [transform_fire_alert(alert) for alert in fire_alerts]
        response = await handle_transformed_data(
            transformed_data,
            str(integration.id),
            "pull_events"
        )
        return {"type": 'fire_alerts', "response": response}
    
    return {"type": 'fire_alerts', "message": 'No new data available.'}


async def get_integrated_alerts(aoi_data, integration, action_config):


    if not action_config.include_integrated_alerts:
        return {'dataset': client.DATASET_GFW_INTEGRATED_ALERTS, "message": 'Not included in action config.'}

    integrated_alerts, dataset_status, dataset_metadata = await client.get_integrated_alerts(
        aoi_data=aoi_data,
        integration=integration,
        config=action_config
    )

    if integrated_alerts:
        logger.info(f"Integrated alerts pulled with success.")
        transformed_data = [transform_integrated_alert(alert)for alert in integrated_alerts]
        response = await handle_transformed_data(
            transformed_data,
            str(integration.id),
            "pull_events"
        )

        if response:
            # update states
            dataset_status.latest_updated_on = dataset_metadata.updated_on

            await state_manager.set_state(
                str(integration.id),
                "pull_events",
                dataset_status.json(),
                client.DATASET_GFW_INTEGRATED_ALERTS
            )
            message = f'Dataset updated at {dataset_status.latest_updated_on.isoformat()}'
            return {"dataset": client.DATASET_GFW_INTEGRATED_ALERTS, "message": message}

        return {"dataset": client.DATASET_GFW_INTEGRATED_ALERTS, "message": 'Failed sending to Sensors API.', 'response': response}
    else:
        return {"dataset": client.DATASET_GFW_INTEGRATED_ALERTS, "message": 'No data available.'}

            



