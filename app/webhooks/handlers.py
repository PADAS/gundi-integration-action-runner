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
    if transformed_data:
        groups = {}
        for data in transformed_data:
            record = dict(data)
            output_type = record.pop("__gundi_output_type", None) or webhook_config.output_type
            if not output_type:
                raise ValueError(
                    "No output type for record and no default output_type in config. "
                    "Set output_type in the webhook configuration or emit '__gundi_output_type' per record."
                )
            groups.setdefault(output_type, []).append(record)

        data_points_qty = 0
        for output_type, records in groups.items():
            if output_type == "obv":
                try:
                    response = await send_observations_to_gundi(
                        observations=records,
                        integration_id=integration.id
                    )
                except Exception as e:
                    logger.exception(f"Failed sending Observations. error: {e}")
                    raise
            elif output_type == "ev":
                try:
                    response = await send_events_to_gundi(
                        events=records,
                        integration_id=integration.id
                    )
                except Exception as e:
                    logger.exception(f"Failed sending Events. error: {e}")
                    raise
            else:
                raise ValueError(f"Invalid output type: '{output_type}'. Valid values: 'obv', 'ev'.")
            data_points_qty += len(response)
        logger.info(f"'{data_points_qty}' data point(s) sent to Gundi.")
        return {"data_points_qty": data_points_qty}
    else:
        logger.info(f"No data point(s) sent to Gundi.")
        return {"data_points_qty": 0}
