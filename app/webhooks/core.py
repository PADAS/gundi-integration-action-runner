import importlib
import inspect
import json
from typing import Optional, Union
from pydantic import BaseModel
from fastapi.encoders import jsonable_encoder
from app.services.utils import StructHexString, UISchemaModelMixin, FieldWithUIOptions, UIOptions


class WebhookConfiguration(UISchemaModelMixin, BaseModel):
    class Config:
        extra = "allow"


class HexStringConfig(WebhookConfiguration):
    hex_format: dict
    hex_data_field: str


class DynamicSchemaConfig(WebhookConfiguration):
    json_schema: dict = FieldWithUIOptions(
        default={},
        description="JSON Schema to validate the data.",
        ui_options=UIOptions(
            widget="textarea",  # ToDo: Use a better (custom) widget to render the JSON schema
        )
    )


class JQTransformConfig(UISchemaModelMixin, BaseModel):
    jq_filter: str = FieldWithUIOptions(
        default=".",
        description="JQ filter to transform JSON data.",
        example=". | map(select(.isActive))",
        ui_options=UIOptions(
            widget="textarea",  # ToDo: Use a better (custom) widget to render the JQ filter
        )
    )


class GenericJsonTransformConfig(JQTransformConfig, DynamicSchemaConfig):
    output_type: str = FieldWithUIOptions(
        ...,
        description="Output type for the transformed data: 'obv' or 'event'",
        ui_options=UIOptions(
            widget="text",  # ToDo: Use a select or a better widget to render the output type
        )
    )


class GenericJsonTransformWithHexStrConfig(HexStringConfig, GenericJsonTransformConfig):
    pass


class WebhookPayload(BaseModel):
    class Config:
        extra = "allow"


class HexStringPayload(WebhookPayload):
    hex_format: Optional[dict]
    hex_data_field: Optional[str]

    def dict(
        self,
        *,
        include: Optional[Union['AbstractSetIntStr', 'MappingIntStrAny']] = None,
        exclude: Optional[Union['AbstractSetIntStr', 'MappingIntStrAny']] = None,
        by_alias: bool = False,
        skip_defaults: Optional[bool] = None,
        exclude_unset: bool = False,
        exclude_defaults: bool = False,
        exclude_none: bool = False,
    ) -> 'DictStrAny':
        """
        Generate a dictionary representation of the model.
        This is overriden to be able to serialize StructHexString objects.
        """
        return json.loads(self.json())

    class Config:
        arbitrary_types_allowed = True
        json_encoders = {
            StructHexString: jsonable_encoder,
        }


class GenericJsonPayload(WebhookPayload):
    pass


class GenericJsonWithHexStrPayload(HexStringPayload, GenericJsonPayload):
    pass


def get_webhook_handler():

    # Import the module using importlib
    module = importlib.import_module("app.webhooks.handlers")
    handler = module.webhook_handler

    if (annotation := inspect.signature(handler).parameters.get("payload").annotation) != inspect._empty:
        payload_model = annotation
    else:
        payload_model = None

    # Introspect schemas
    if (annotation := inspect.signature(handler).parameters.get("webhook_config").annotation) != inspect._empty:
        config_model = annotation
    else:
        config_model = None

    return handler, payload_model, config_model
