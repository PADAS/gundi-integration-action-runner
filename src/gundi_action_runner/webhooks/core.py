import json
from typing import Optional, Union
from pydantic import BaseModel
from fastapi.encoders import jsonable_encoder
from gundi_action_runner.services.utils import StructHexString, UISchemaModelMixin, FieldWithUIOptions, UIOptions, OptionalStringType


class WebhookConfiguration(UISchemaModelMixin, BaseModel):
    diagnostic_destination_url: Optional[OptionalStringType] = FieldWithUIOptions(
        None,
        title="Diagnostic Destination URL",
        description=(
            "Optional URL to forward the raw incoming payload to for diagnostic purposes. "
            "When set, the original JSON payload is POST'd to this URL before any transformation."
        ),
        ui_options=UIOptions(
            widget="text",
            placeholder="https://your-diagnostic-app.example.com/webhook-dump",
        ),
    )

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
    output_type: Optional[str] = FieldWithUIOptions(
        None,
        description=(
            "Output type for all transformed records: 'obv' (observations) or 'ev' (events)."
        ),
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
    from gundi_action_runner.registry import registry  # deferred: avoids a module cycle

    if registry.webhook_handler is None:
        from gundi_action_runner import settings
        registry.load_legacy_webhook(settings.GUNDI_LEGACY_WEBHOOKS_MODULE)
    return registry.webhook_handler
