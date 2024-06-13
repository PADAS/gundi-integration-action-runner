import json
import pyjq
from app.services.gundi import send_observations_to_gundi, send_events_to_gundi
from .core import GenericJsonPayload,  GenericJsonTransformConfig


async def webhook_handler(payload: GenericJsonPayload, integration=None, webhook_config: GenericJsonTransformConfig = None):
    print(f"Webhook handler executed with integration: {integration}. \nPayload: {payload}. \nConfig: {webhook_config}")
    if isinstance(payload, list):
        input_data = [json.loads(i.json()) for i in payload]
    else:
        input_data = json.loads(payload.json())
    filter_expression = webhook_config.jq_filter.replace("\n", ""). replace(" ", "")
    transformed_data = pyjq.all(filter_expression, input_data)
    print(f"Transformed Data:\n: {transformed_data}")
    if webhook_config.output_type == "obv":  # ToDo: Use an enum?
        response = await send_observations_to_gundi(
            observations=transformed_data,
            integration_id=integration.id
        )
    elif webhook_config.output_type == "ev":
        response = await send_events_to_gundi(
            events=transformed_data,
            integration_id=integration.id
        )
    else:
        raise ValueError(f"Invalid output type: {webhook_config.output_type}. Please review the configuration.")
    data_points_qty = len(transformed_data) if isinstance(transformed_data, list) else 1
    print(f"{data_points_qty} data point(s) sent to Gundi.")
    return {"data_points_qty": data_points_qty}
