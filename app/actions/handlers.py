"""
Kineis actions: auth (credential verification) and pull_telemetry (CONNECTORS-836).
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import Dict

from app.actions.configurations import AuthenticateKineisConfig, PullTelemetryConfiguration, get_auth_config
from app.actions.transformers import telemetry_batch_to_observations
from app.services.activity_logger import activity_logger, log_action_activity
from app.services.action_scheduler import crontab_schedule
from app.services.gundi import send_observations_to_gundi
from app.services.kineis_client import fetch_device_list, fetch_telemetry, fetch_telemetry_realtime, get_access_token
from app.services.state import IntegrationStateManager
from app.services.utils import generate_batches
from gundi_core.events import LogLevel

logger = logging.getLogger(__name__)

OBSERVATION_BATCH_SIZE = 200
REALTIME_STATE_KEY = "kineis_realtime_checkpoint"


async def action_auth(integration, action_config: AuthenticateKineisConfig):
    """
    Verify Kineis/CLS credentials by obtaining a Bearer token.
    Used by the portal to validate credentials without running pull_telemetry.
    """
    try:
        result = await get_access_token(
            username=action_config.username,
            password=action_config.password.get_secret_value(),
            client_id=action_config.client_id,
        )
        return {"valid_credentials": True, "expires_in": result.get("expires_in")}
    except Exception as e:
        logger.exception("Kineis auth failed")
        return {"valid_credentials": False, "message": str(e)}


def _utc_now():
    return datetime.now(timezone.utc)


def _format_utc(dt: datetime) -> str:
    """Format datetime as YYYY-MM-DDTHH:mm:ss.SSSZ for Kineis API."""
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


@crontab_schedule("0 */4 * * *")  # Every 4 hours
@activity_logger()
async def action_pull_telemetry(integration, action_config: PullTelemetryConfiguration):
    """
    Pull telemetry from Kineis: when use_realtime is enabled and a checkpoint is stored,
    use the realtime API (only new data since last run). Otherwise use bulk API with
    lookback window. Map to Gundi observations and send. Credentials from Auth action config.
    """
    integration_id = str(integration.id)
    action_id = "pull_telemetry"
    auth_config = get_auth_config(integration)
    device_refs = action_config.device_refs or None
    device_uids = action_config.device_uids or None

    use_realtime = action_config.use_realtime
    checkpoint = 0
    if use_realtime:
        try:
            state_mgr = IntegrationStateManager()
            state = await state_mgr.get_state(integration_id, action_id)
            checkpoint = state.get(REALTIME_STATE_KEY, 0)
        except Exception as e:
            logger.warning("Could not load realtime checkpoint, using bulk: %s", e)
            use_realtime = False

    if use_realtime:
        await log_action_activity(
            integration_id=integration_id,
            action_id=action_id,
            title="Fetching telemetry from Kineis realtime API",
            level=LogLevel.INFO,
            data={"checkpoint": checkpoint},
        )
        messages, new_checkpoint = await fetch_telemetry_realtime(
            integration_id=integration_id,
            username=auth_config.username,
            password=auth_config.password.get_secret_value(),
            checkpoint=checkpoint,
            device_refs=device_refs,
            device_uids=device_uids,
            retrieve_metadata=action_config.retrieve_metadata,
            retrieve_raw_data=action_config.retrieve_raw_data,
            client_id=auth_config.client_id,
        )
        try:
            state_mgr = IntegrationStateManager()
            await state_mgr.set_state(
                integration_id, action_id, {REALTIME_STATE_KEY: new_checkpoint}
            )
        except Exception as e:
            logger.warning("Could not persist realtime checkpoint: %s", e)
    else:
        to_time = _utc_now()
        from_time = to_time - timedelta(hours=action_config.lookback_hours)
        from_str = _format_utc(from_time)
        to_str = _format_utc(to_time)
        await log_action_activity(
            integration_id=integration_id,
            action_id=action_id,
            title="Fetching telemetry from Kineis bulk API",
            level=LogLevel.INFO,
            data={"from": from_str, "to": to_str, "lookback_hours": action_config.lookback_hours},
        )
        messages = await fetch_telemetry(
            integration_id=integration_id,
            username=auth_config.username,
            password=auth_config.password.get_secret_value(),
            from_datetime=from_str,
            to_datetime=to_str,
            page_size=action_config.page_size,
            device_refs=device_refs,
            device_uids=device_uids,
            retrieve_metadata=action_config.retrieve_metadata,
            retrieve_raw_data=action_config.retrieve_raw_data,
            client_id=auth_config.client_id,
        )

    await log_action_activity(
        integration_id=integration_id,
        action_id=action_id,
        title="Fetched telemetry messages, mapping to observations",
        level=LogLevel.INFO,
        data={"messages_count": len(messages)},
    )

    device_uid_to_customer_name: Dict[int, str] = {}
    try:
        devices = await fetch_device_list(
            integration_id=integration_id,
            username=auth_config.username,
            password=auth_config.password.get_secret_value(),
            client_id=auth_config.client_id,
        )
        device_uid_to_customer_name = {
            d["deviceUid"]: d["customerName"]
            for d in devices
            if d.get("customerName")
        }
    except Exception as e:
        logger.warning("Could not fetch device list for source_name, using fallback: %s", e)

    observations = telemetry_batch_to_observations(
        messages,
        device_uid_to_customer_name=device_uid_to_customer_name,
    )
    skipped = len(messages) - len(observations)
    if skipped:
        await log_action_activity(
            integration_id=integration_id,
            action_id=action_id,
            title="Some messages skipped (missing location or timestamp)",
            level=LogLevel.INFO,
            data={"skipped": skipped},
        )

    sent_total = 0
    for batch in generate_batches(observations, OBSERVATION_BATCH_SIZE):
        await send_observations_to_gundi(
            observations=list(batch),
            integration_id=integration.id,
        )
        sent_total += len(batch)

    await log_action_activity(
        integration_id=integration_id,
        action_id=action_id,
        title="Sent observations to Gundi",
        level=LogLevel.INFO,
        data={"observations_sent": sent_total},
    )

    return {
        "messages_fetched": len(messages),
        "observations_sent": sent_total,
        "skipped": skipped,
    }
