import asyncio
import datetime
import httpx
import logging
import stamina
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
    async for attempt in stamina.retry_context(
            on=httpx.HTTPError,
            attempts=3,
            wait_initial=datetime.timedelta(seconds=10),
            wait_max=datetime.timedelta(seconds=10),
    ):
        with attempt:
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
                        'action_id': "pull_events"
                    }
                )
                return [msg]
            else:
                """
                for vehicle in transformed_data:
                    # Update state
                    state = {
                        "latest_device_timestamp": vehicle.get("recorded_at")
                    }
                    await state_manager.set_state(
                        str(integration.id),
                        "pull_observations",
                        state,
                        vehicle.get("source")
                    )
                """
                return response


async def transform_fire_alert(alert):
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


async def transform_integrated_alert(alert):
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
    logger.info(f"Executing 'pull_events' action with integration {integration} and action_config {action_config}...")
    try:
        async for attempt in stamina.retry_context(
                on=httpx.HTTPError,
                attempts=3,
                wait_initial=datetime.timedelta(seconds=10),
                wait_max=datetime.timedelta(seconds=10),
        ):
            with attempt:
                aoi_data = await client.get_aoi_data(
                    integration=integration,
                    config=action_config
                )

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

                response_per_type = []
                if action_config.include_fire_alerts:
                    fire_alerts_task = asyncio.create_task(
                        client.get_fire_alerts(
                            aoi_data=aoi_data,
                            integration=integration,
                            config=action_config
                        )
                    )
                    fire_alerts = await fire_alerts_task
                    if fire_alerts:
                        logger.info(f"Fire alerts pulled with success.")
                        transformed_data = [
                            await transform_fire_alert(alert)
                            for alert in fire_alerts
                        ]
                        response = await handle_transformed_data(
                            transformed_data,
                            str(integration.id),
                            "pull_events"
                        )
                        response_per_type.append({"type": "fire_alerts", "response": response})

                if action_config.include_integrated_alerts:
                    integrated_alerts_task = asyncio.create_task(
                        client.get_integrated_alerts(
                            aoi_data=aoi_data,
                            integration=integration,
                            config=action_config
                        )
                    )
                    integrated_alerts = await integrated_alerts_task
                    if integrated_alerts:
                        logger.info(f"Tree losses alerts pulled with success.")
                        transformed_data = [
                            await transform_integrated_alert(alert)
                            for alert in integrated_alerts
                        ]
                        response = await handle_transformed_data(
                            transformed_data,
                            str(integration.id),
                            "pull_events"
                        )
                        response_per_type.append({"type": "tree_losses", "response": response})
    except httpx.HTTPError as e:
        message = f"pull_observations action returned error."
        logger.exception(message, extra={
            "integration_id": str(integration.id),
            "attention_needed": True
        })
        raise e
    else:
        return response_per_type
