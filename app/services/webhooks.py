import importlib
import logging
from fastapi import Request
from app.services.activity_logger import log_activity
from gundi_client_v2 import GundiClient

from app.services.utils import DyntamicFactory
from app.webhooks.core import get_webhook_handler, DynamicSchemaConfig, HexStringConfig, GenericJsonPayload, \
    WebhookPayload

_portal = GundiClient()
logger = logging.getLogger(__name__)


async def get_integration(request):
    integration = None
    consumer_username = request.headers.get("x-consumer-username")
    consumer_integration = consumer_username.split(":")[-1] if consumer_username and consumer_username != "anonymous" else None
    integration_id = consumer_integration or request.headers.get("x-gundi-integration-id") or request.query_params.get("integration_id")
    if integration_id:
        try:
            integration = await _portal.get_integration_details(integration_id=integration_id)
        except Exception as e:
            logger.warning(f"Error retrieving integration '{integration_id}' from the portal: {e}")
    return integration


async def process_webhook(request: Request):
    try:
        # Try to relate the request o an the integration
        integration = await get_integration(request=request)
        # Look for the handler function in webhooks/handlers.py
        webhook_handler, payload_model, config_model = get_webhook_handler()
        json_content = await request.json()
        # Parse config if a model was defined in webhooks/configurations.py
        # ToDo: Update the Gundi API ang client to get webhook config
        #webhook_config_data = integration.webhook_configuration.data if integration and integration.webhook_configuration else {}
        # Liquidtech example
        # webhook_config_data = {
        #     "hex_data_field": "data",
        #     "hex_format": {
        #         "byte_order": "<",
        #         "fields": [
        #             {
        #                 "name": "start_bit",
        #                 "format": "B",
        #                 "output_type": "int"
        #             },
        #             {
        #                 "name": "v",
        #                 "format": "I"
        #             },
        #             {
        #                 "name": "interval",
        #                 "format": "H",
        #                 "output_type": "int"
        #             },
        #             {
        #                 "name": "meter_state_1",
        #                 "format": "B",
        #             },
        #             {
        #                 "name": "meter_state_2",
        #                 "format": "B",
        #                 "bit_fields": [
        #                     {
        #                         "name": "meter_batter_alarm",
        #                         "start_bit": 0,
        #                         "end_bit": 0,
        #                         "output_type": "bool"
        #                     },
        #                     {
        #                         "name": "empty_pipe_alarm",
        #                         "start_bit": 1,
        #                         "end_bit": 1,
        #                         "output_type": "bool"
        #                     },
        #                     {
        #                         "name": "reverse_flow_alarm",
        #                         "start_bit": 2,
        #                         "end_bit": 2,
        #                         "output_type": "bool"
        #                     },
        #                     {
        #                         "name": "over_range_alarm",
        #                         "start_bit": 3,
        #                         "end_bit": 3,
        #                         "output_type": "bool"
        #                     },
        #                     {
        #                         "name": "temp_alarm",
        #                         "start_bit": 4,
        #                         "end_bit": 4,
        #                         "output_type": "bool"
        #                     },
        #                     {
        #                         "name": "ee_error",
        #                         "start_bit": 5,
        #                         "end_bit": 5,
        #                         "output_type": "bool"
        #                     },
        #                     {
        #                         "name": "transduce_in_error",
        #                         "start_bit": 6,
        #                         "end_bit": 6,
        #                         "output_type": "bool"
        #                     },
        #                     {
        #                         "name": "transduce_out_error",
        #                         "start_bit": 7,
        #                         "end_bit": 7,
        #                         "output_type": "bool"
        #                     },
        #                     {
        #                         "name": "transduce_out_error",
        #                         "start_bit": 7,
        #                         "end_bit": 7,
        #                         "output_type": "bool"
        #                     }
        #                 ]
        #             },
        #             {
        #                 "name": "r1",
        #                 "format": "B",
        #                 "output_type": "int"
        #             },
        #             {
        #                 "name": "r2",
        #                 "format": "B",
        #                 "output_type": "int"
        #             },
        #             {
        #                 "name": "crc",
        #                 "format": "B",
        #             },
        #         ]
        #     }
        # }
        # Everywhere Example
        schema_dict = {"title": "PayloadItem", "type": "object", "properties": {"deviceId": {"title": "Deviceid", "type": "integer"}, "teamId": {"title": "Teamid", "type": "integer"}, "trackPoint": {"$ref": "#/definitions/TrackPoint"}, "source": {"title": "Source", "type": "string"}, "entityId": {"title": "Entityid", "type": "integer"}, "deviceType": {"title": "Devicetype", "type": "string"}, "name": {"title": "Name", "type": "string"}}, "required": ["deviceId", "teamId", "trackPoint", "source", "entityId", "deviceType", "name"], "definitions": {"Point": {"title": "Point", "type": "object", "properties": {"x": {"title": "X", "type": "integer"}, "y": {"title": "Y", "type": "integer"}}, "required": ["x", "y"]}, "TrackPoint": {"title": "TrackPoint", "type": "object", "properties": {"point": {"$ref": "#/definitions/Point"}, "time": {"title": "Time", "type": "integer"}}, "required": ["point", "time"]}}}
        webhook_config_data = {
            "jq_filter": ".",  # JQ filter to transform JSON data
            "json_schema": schema_dict
        }
        parsed_config = config_model.parse_obj(webhook_config_data) if config_model else {}
        extra = {}
        if parsed_config and issubclass(config_model, HexStringConfig):
            json_content["hex_data_field"] = parsed_config.hex_data_field
            json_content["hex_format"] = parsed_config.hex_format
        # Parse payload if a model was defined in webhooks/configurations.py
        if payload_model:
            try:
                if issubclass(payload_model, GenericJsonPayload) and issubclass(config_model, DynamicSchemaConfig):
                    # Build the model from a json schema
                    model_factory = DyntamicFactory(
                        json_schema=parsed_config.json_schema,
                        base_model=WebhookPayload,
                        ref_template="definitions"
                    )
                    dynamic_payload_model = model_factory.make()
                    if isinstance(json_content, list):
                        parsed_payload = [dynamic_payload_model.parse_obj(d) for d in json_content]
                    else:
                        parsed_payload = dynamic_payload_model.parse_obj(json_content)
                else:
                    parsed_payload = payload_model.parse_obj(json_content)
            except Exception as e:
                message = f"Error parsing payload: {str(e)}"
                logger.exception(message)
                # await log_activity(
                #     level="error",
                #     title=message,
                # )
                return {}
        else:  # Pass the raw payload
            parsed_payload = json_content
        await webhook_handler(payload=parsed_payload, integration=integration, webhook_config=parsed_config)
    except (ImportError, AttributeError, NotImplementedError) as e:
        message = "Webhooks handler not found. Please implement a 'webhook_handler' function in app/webhooks/handlers.py"
        logger.exception(message)
        # ToDo: Update activity logger to support non action-related webhooks
        # await log_activity(
        #     level="error",
        #     title=message,
        # )
    except Exception as e:
        message = f"Error loading webhooks handler: {str(e)}"
        logger.exception(message)
        # await log_activity(
        #     level="error",
        #     title=message,
        # )
    return {}

