import json
import pyjq
import logging
from app.services.gundi import send_observations_to_gundi, send_events_to_gundi
from app.services.activity_logger import webhook_activity_logger
from .core import GenericJsonPayload,  GenericJsonTransformConfig


logger = logging.getLogger(__name__)


@webhook_activity_logger()
async def webhook_handler(payload: GenericJsonPayload, integration=None, webhook_config: GenericJsonTransformConfig = None):
    logger.info(f"Webhook handler executed with integration: '{integration}'.")
    logger.info(f"Payload: '{payload}'.")
    logger.info(f"Config: '{webhook_config}'.")
    if isinstance(payload, list):
        input_data = [json.loads(i.json()) for i in payload]
    else:
        input_data = json.loads(payload.json())
    filter_expression = webhook_config.jq_filter.replace("\n", "")
    transformed_data = pyjq.all(filter_expression, input_data)
    logger.info(f"Transformed Data: {transformed_data}")
    # Check if a filter is present in the filtered data
    for data in transformed_data:
        if data.get("status", "OK") != 'OK':
            logger.info(f"'{data}' point received was filtered")
            transformed_data.remove(data)
    if transformed_data:
        if webhook_config.output_type == "obv":  # ToDo: Use an enum?

            try:
                response = await send_observations_to_gundi(
                    observations=transformed_data,
                    integration_id=integration.id
                )
            except Exception as e:
                logger.exception(f"Failed sending Observations. payload: {payload}, transformed_data: {transformed_data}, error: {e}")
                raise

        elif webhook_config.output_type == "ev":
            try:
                response = await send_events_to_gundi(
                    events=transformed_data,
                    integration_id=integration.id
                )
            except Exception as e:
                logger.exception(f"Failed sending Events. payload: {payload}, transformed_data: {transformed_data}, error: {e}")
                raise

        else:
            raise ValueError(f"Invalid output type: {webhook_config.output_type}. Please review the configuration.")
        data_points_qty = len(response)
        logger.info(f"'{data_points_qty}' data point(s) sent to Gundi.")
        return {"data_points_qty": data_points_qty}
    else:
        logger.info(f"No data point(s) sent to Gundi.")
        return {"data_points_qty": 0}
