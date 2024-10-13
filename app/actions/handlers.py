import asyncio
import httpx
import logging
import random
import app.settings

from app.actions import utils
from app.actions.gfwclient import DataAPI, Geostore, DatasetStatus, \
    AOIData, DATASET_GFW_INTEGRATED_ALERTS, DATASET_NASA_VIIRS_FIRE_ALERTS
from shapely.geometry import GeometryCollection, shape, mapping
from datetime import timezone, timedelta, datetime

from app.actions.configurations import AuthenticateConfig, PullEventsConfig
from app.services.activity_logger import activity_logger
from app.services.gundi import send_events_to_gundi
from app.services.state import IntegrationStateManager
from app.services.errors import ConfigurationNotFound
from app.services.utils import find_config_for_action
from gundi_core.schemas.v2 import Integration


GFW_INTEGRATED_ALERTS = "gfwgladalert"
GFW_FIRE_ALERT = "gfwfirealert"

MAX_DAYS_PER_QUERY = 2

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
    event_time = alert.alert_date.replace(tzinfo=timezone.utc).isoformat()
    title = "GFW VIIRS Alert"

    return dict(
        title=title,
        event_type=GFW_FIRE_ALERT,
        recorded_at=event_time,
        location={"lat": alert.latitude, "lon": alert.longitude},
        event_details=dict(
            confidence=alert.confidence,
            alert_time=event_time
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
        dataapi = DataAPI(username=action_config.email, password=action_config.password)
        token = await dataapi.get_access_token()
    except Exception as e:
        message = f"auth action returned error."
        logger.exception(message, extra={
            "integration_id": str(integration.id),
            "attention_needed": True
        })
        raise e
    else:
        logger.info(f"Authenticated with success. token: {token}")
    
    return {"valid_credentials": token is not None}


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


@activity_logger()
async def action_pull_events(integration:Integration, action_config: PullEventsConfig):

    if not action_config.force_fetch and await state_manager.is_quiet_period(str(integration.id), "pull_events"):
        return {"message": 'Quiet period is active.'}


    logger.info(f"Executing 'pull_events' action with integration {integration} and action_config {action_config}...")

    auth_config = get_auth_config(integration)

    # Get AOI data first.
    dataapi = DataAPI(username=auth_config.email, password=auth_config.password)

    aoi_id = await dataapi.aoi_from_url(action_config.gfw_share_link_url)
    aoi_data = await dataapi.get_aoi(aoi_id=aoi_id)

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

    # Get AOI and Geostore data.
    aoi_id = await dataapi.aoi_from_url(action_config.gfw_share_link_url)
    aoi_data = await dataapi.get_aoi(aoi_id=aoi_id)

    geostore_ids = await state_manager.get_geostore_ids(aoi_data.id)

    if not geostore_ids:
        geostore:Geostore = await dataapi.get_geostore(geostore_id=aoi_data.attributes.geostore)

        geometry_collection = GeometryCollection(
            [
                shape(feature["geometry"]).buffer(0)
                for feature in geostore.attributes.geojson["features"]
            ]
        )
        for partition in utils.generate_geometry_fragments(geometry_collection=geometry_collection):

            geostore = await dataapi.create_geostore(geometry=mapping(partition))

            await state_manager.add_geostore_id(aoi_data.id, geostore.gfw_geostore_id)

        await state_manager.set_geostores_id_ttl(aoi_data.id, 86400*7)

    # This semaphore is meant to limit the concurrent requests to GFW's dataset API query endpoints.
    # When configuring a cloud run service, include this in a calculation so that 
    # GFW_DATASET_QUERY_CONCURRENCY * maximum-number-of-instances * maximum-concurrent-requests-per-instance <= N
    # where N is the maximum concurrent requests allowed by GFW's API. 
    # (ex. in practice, N is around 50)
    sema = asyncio.Semaphore(app.settings.GFW_DATASET_QUERY_CONCURRENCY)

    # Create a list of tasks.
    tasklist = [get_nasa_viirs_fire_alerts(integration, action_config, auth_config, aoi_data, sema),
                 get_integrated_alerts(integration, action_config, auth_config, aoi_data, sema)
                 ]
    
    # Wait until they're all finished.
    results = await asyncio.gather(*tasklist)

    quiet_minutes = random.randint(60, 180) # Todo: change to be more fair.
    await state_manager.set_quiet_period(str(integration.id), "pull_events", timedelta(minutes=quiet_minutes))

    # The results are in the order of the tasklist.
    return results


def generate_date_pairs(lower_date, upper_date, interval=MAX_DAYS_PER_QUERY):
    while upper_date > lower_date:
        yield max(lower_date, upper_date - timedelta(days=interval)), upper_date
        upper_date -= timedelta(days=interval)


async def get_integrated_alerts(integration:Integration, action_config: PullEventsConfig, auth_config: AuthenticateConfig,
                                aoi_data: AOIData, sema: asyncio.Semaphore):

    if not action_config.include_integrated_alerts:
        return {'dataset': DATASET_GFW_INTEGRATED_ALERTS, "message": 'Not included in action config.'}

    dataapi = DataAPI(username=auth_config.email, password=auth_config.password)

    dataset_metadata = await dataapi.get_dataset_metadata(DATASET_GFW_INTEGRATED_ALERTS)
    dataset_status = await state_manager.get_state(
        str(integration.id),
        "pull_events",
        DATASET_GFW_INTEGRATED_ALERTS
    )

    if dataset_status:
        dataset_status = DatasetStatus.parse_raw(dataset_status)
    else:
        dataset_status = DatasetStatus(
            dataset=dataset_metadata.dataset,
            version=dataset_metadata.version,
        )

        # If I've saved a status for this dataset, compare 'updated_on' timestamp to avoid redundant queries.
    if not action_config.force_fetch and dataset_status.latest_updated_on >= dataset_metadata.updated_on:
        logger.info(
            "No updates reported for dataset '%s' so skipping integrated_alerts queries",
            DATASET_GFW_INTEGRATED_ALERTS,
            extra={
                "integration_id": str(integration.id),
                "integration_login": auth_config.email,
                "dataset_updated_on": dataset_metadata.updated_on.isoformat(),
            },
        )
        return {"dataset": DATASET_GFW_INTEGRATED_ALERTS, "message": 'No new data available.'}

    aoi_id = await dataapi.aoi_from_url(action_config.gfw_share_link_url)
    aoi_data = await dataapi.get_aoi(aoi_id=aoi_id)

    geostore_ids = await state_manager.get_geostore_ids(aoi_data.id)

    # Date ranges are in whole days, so we round to next midnight.
    end_date = (datetime.now(tz=timezone.utc) + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = end_date - timedelta(days=action_config.integrated_alerts_lookback_days)

    geostore_ids = await state_manager.get_geostore_ids(aoi_data.id)

    # Generate tasks for each geostore_id and 48 hour partition.
    tasks = [dataapi.get_gfw_integrated_alerts(geostore_id=geostore_id.decode('utf8'), date_range=(lower, upper),
                                               lowest_confidence=action_config.integrated_alerts_lowest_confidence, semaphore=sema)
              for geostore_id in geostore_ids 
              for lower, upper in generate_date_pairs(start_date, end_date)]
    
    for t in asyncio.as_completed(tasks):
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
        source_id=DATASET_GFW_INTEGRATED_ALERTS
    )

    return {"dataset": DATASET_GFW_INTEGRATED_ALERTS, "response": dataset_status.dict()}


async def get_nasa_viirs_fire_alerts(integration:Integration, action_config: PullEventsConfig, auth_config: AuthenticateConfig,
                                     aoi_data: AOIData, sema: asyncio.Semaphore):

    if not action_config.include_fire_alerts:
        return {'dataset': DATASET_NASA_VIIRS_FIRE_ALERTS, "message": 'Not included in action config.'}

    dataapi = DataAPI(username=auth_config.email, password=auth_config.password)

    dataset_metadata = await dataapi.get_dataset_metadata(DATASET_NASA_VIIRS_FIRE_ALERTS)
    dataset_status = await state_manager.get_state(
        str(integration.id),
        "pull_events",
        DATASET_NASA_VIIRS_FIRE_ALERTS
    )

    if dataset_status:
        dataset_status = DatasetStatus.parse_raw(dataset_status)
    else:
        dataset_status = DatasetStatus(
            dataset=dataset_metadata.dataset,
            version=dataset_metadata.version,
        )

        # If I've saved a status for this dataset, compare 'updated_on' timestamp to avoid redundant queries.
    if not action_config.force_fetch and dataset_status.latest_updated_on >= dataset_metadata.updated_on:
        logger.info(
            "No updates reported for dataset '%s' so skipping integrated_alerts queries",
            DATASET_NASA_VIIRS_FIRE_ALERTS,
            extra={
                "integration_id": str(integration.id),
                "integration_login": auth_config.email,
                "dataset_updated_on": dataset_metadata.updated_on.isoformat(),
            },
        )
        return {"dataset": DATASET_NASA_VIIRS_FIRE_ALERTS, "message": 'No new data available.'}

    # aoi_id = await dataapi.aoi_from_url(action_config.gfw_share_link_url)
    # aoi_data = await dataapi.get_aoi(aoi_id=aoi_id)

    geostore_ids = await state_manager.get_geostore_ids(aoi_data.id)

    # Date ranges are in whole days, so we round to next midnight.
    end_date = (datetime.now(tz=timezone.utc) + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = end_date - timedelta(days=action_config.integrated_alerts_lookback_days)

    # Generate tasks for each geostore_id and 48 hour partition.
    tasks = [dataapi.get_nasa_viirs_fire_alerts(geostore_id=geostore_id.decode('utf8'), date_range=(lower, upper),
                                                lowest_confidence=action_config.fire_alerts_lowest_confidence, semaphore=sema)
              for geostore_id in geostore_ids 
              for lower, upper in generate_date_pairs(start_date, end_date)]
    for t in asyncio.as_completed(tasks):
        fire_alerts = await t
        if fire_alerts:
            logger.info(f"Fire alerts pulled with success.")
            transformed_data = [transform_fire_alert(alert) for alert in fire_alerts]
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
        source_id=DATASET_NASA_VIIRS_FIRE_ALERTS
    )

    return {"dataset": DATASET_NASA_VIIRS_FIRE_ALERTS, "response": dataset_status.dict()}


