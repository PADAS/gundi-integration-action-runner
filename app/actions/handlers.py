import asyncio
import httpx
import logging
import random
import app.actions.client as client
from app.actions import utils
from app.actions.gfwclient import DataAPI, Geostore, DatasetStatus
from shapely.geometry import GeometryCollection, shape, mapping
from datetime import timezone, timedelta, datetime

from app.actions.configurations import AuthenticateConfig, PullEventsConfig
from app.services.activity_logger import activity_logger
from app.services.gundi import send_events_to_gundi
from app.services.state import IntegrationStateManager
from gundi_core.schemas.v2 import Integration, IntegrationAction, IntegrationType, UUID, IntegrationActionConfiguration, IntegrationActionSummary, ConnectionRoute, Organization


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
    title = "GFW VIIRS Alert"

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
        )
    )


def transform_integrated_alert(alert):
    title = ("GFW Integrated Deforestation Alert")

    return dict(
        title=title,
        event_type=GFW_INTEGRATED_ALERTS,
        recorded_at=alert.recorded_at,
        location={"lat": alert.latitude, "lon": alert.longitude},
        event_details=dict(
            confidence=alert.confidence
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
async def action_pull_events(integration:Integration, action_config: PullEventsConfig):

    if await state_manager.is_quiet_period(str(integration.id), "pull_events"):
        return {"message": 'Quiet period is active.'}


    logger.info(f"Executing 'pull_events' action with integration {integration} and action_config {action_config}...")

    auth_config = client.get_auth_config(integration)

    # Get AOI data first.
    gfwclient = DataAPI(username=auth_config.email, password=auth_config.password)

    aoi_id = await gfwclient.aoi_from_url(action_config.gfw_share_link_url)
    aoi_data = await gfwclient.get_aoi(aoi_id=aoi_id)

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
                 asyncio.create_task(get_integrated_alerts(integration, action_config))]
    
    # Wait until they're all finished.
    results = await asyncio.gather(*tasklist)

    quiet_minutes = random.randint(60, 180) # Todo: change to be more fair.
    await state_manager.set_quiet_period(str(integration.id), "pull_events", timedelta(minutes=quiet_minutes))

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


async def get_integrated_alerts(integration:Integration, action_config: PullEventsConfig):

    if not action_config.include_integrated_alerts:
        return {'dataset': client.DATASET_GFW_INTEGRATED_ALERTS, "message": 'Not included in action config.'}

    auth_config = client.get_auth_config(integration)

    gfwclient = DataAPI(username=auth_config.email, password=auth_config.password)

    dataset_metadata = await gfwclient.get_dataset_metadata(client.DATASET_GFW_INTEGRATED_ALERTS)
    dataset_status = await state_manager.get_state(
        str(integration.id),
        "pull_events",
        client.DATASET_GFW_INTEGRATED_ALERTS
    )

    if dataset_status:
        dataset_status = DatasetStatus.parse_raw(dataset_status)
    else:
        dataset_status = DatasetStatus(
            dataset=dataset_metadata.dataset,
            version=dataset_metadata.version,
        )

        # If I've saved a status for this dataset, compare 'updated_on' timestamp to avoid redundant queries.
    if dataset_status.latest_updated_on >= dataset_metadata.updated_on:
        logger.info(
            "No updates reported for dataset '%s' so skipping integrated_alerts queries",
            client.DATASET_GFW_INTEGRATED_ALERTS,
            extra={
                "integration_id": str(integration.id),
                "integration_login": auth_config.email,
                "dataset_updated_on": dataset_metadata.updated_on.isoformat(),
            },
        )
        return {"dataset": client.DATASET_GFW_INTEGRATED_ALERTS, "message": 'No new data available.'}

    aoi_id = await gfwclient.aoi_from_url(action_config.gfw_share_link_url)
    aoi_data = await gfwclient.get_aoi(aoi_id=aoi_id)

    val:Geostore = await gfwclient.get_geostore(geostore_id=aoi_data.attributes.geostore)

    # Date ranges are in whole days, so we round to next midnight.
    end_date = (datetime.now(tz=timezone.utc) + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = end_date - timedelta(days=2)

    geostore_ids = await state_manager.get_geostore_ids(aoi_data.id)

    if not geostore_ids:
        geometry_collection = GeometryCollection(
            [
                shape(feature["geometry"]).buffer(0)
                for feature in val.attributes.geojson["features"]
            ]
        )
        for partition in utils.generate_geometry_fragments(geometry_collection=geometry_collection):

            geostore = await gfwclient.create_geostore(geometry=mapping(partition))

            await state_manager.add_geostore_id(aoi_data.id, geostore.gfw_geostore_id)

        await state_manager.set_geostores_id_ttl(aoi_data.id, 86400*7)

    geostore_ids = await state_manager.get_geostore_ids(aoi_data.id)

    sema = asyncio.Semaphore(5)
    for t in asyncio.as_completed([gfwclient.get_gfw_integrated_alerts(geostore_id=geostore_id.decode('utf8'), date_range=(start_date, end_date), semaphore=sema) for geostore_id in geostore_ids]):
        integrated_alerts = await t
        if integrated_alerts:
            logger.info(f"Integrated alerts pulled with success.")
            transformed_data = [transform_integrated_alert(alert)for alert in integrated_alerts]
            await handle_transformed_data(
                transformed_data,
                str(integration.id),
                "pull_events"
            )


    dataset_status = DatasetStatus(
        dataset=dataset_metadata.dataset,
        version=dataset_metadata.version,
        latest_updated_on=dataset_metadata.updated_on
    )

    await state_manager.set_state(
        str(integration.id),
        "pull_events",
        dataset_status.json(),
        source_id=client.DATASET_GFW_INTEGRATED_ALERTS
    )

    return {"dataset": client.DATASET_GFW_INTEGRATED_ALERTS, "response": dataset_status.dict()}



