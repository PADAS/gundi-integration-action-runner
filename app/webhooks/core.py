import importlib
import inspect
import json
import struct
from typing import List

from pydantic import BaseModel
from pydantic.fields import Field


class StructHexString:
    def __init__(self, value: str, hex_format):
        self.value = value
        self.hex_format = hex_format
        self.format_spec = hex_format.get("byte_order", "<") + ''.join(f["format"] for f in hex_format["fields"])
        self.unpacked_data = self.unpacked_data()

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v: str, values, field):
        hex_format = values['hex_format']  # Assumes format is already set in the parent model
        format_spec = hex_format.get("byte_order", "<") + ''.join(d["format"] for d in hex_format["fields"])
        try:
            bytes_data = bytes.fromhex(v)
            if len(bytes_data) != struct.calcsize(format_spec):
                raise ValueError("Hex string does not match the expected length for format")
        except (ValueError, struct.error) as e:
            raise ValueError(f"Invalid hex string for format '{format_spec}': {str(e)}")

        return cls(v, hex_format)

    def unpacked_data(self):
        field_values = []
        unpacked_fields = struct.unpack(self.format_spec, bytes.fromhex(self.value))
        for s, v in zip(self.hex_format["fields"], unpacked_fields):
            field_values.append(self._cast_output(value=v, output_type=s.get("output_type", "int")))
        field_names = [f["name"] for f in self.hex_format["fields"]]
        fields_with_bitfields = [f for f in self.hex_format["fields"] if "bit_fields" in f]
        for field in fields_with_bitfields:
            bit_fields = field["bit_fields"]
            for bit_field in bit_fields:
                start_bit = bit_field["start_bit"]
                end_bit = bit_field["end_bit"]
                field_value = unpacked_fields[field_names.index(field["name"])]
                bits_value = (field_value >> start_bit) & (2 ** (end_bit - start_bit + 1) - 1)
                field_values.append(self._cast_output(value=bits_value, output_type=bit_field.get("output_type", "bool")))
                field_names.append(bit_field["name"])
        return dict(zip(field_names, field_values))

    def _cast_output(self, value, output_type="hex"):
        if output_type == "bool":
            return bool(value)
        elif output_type == "int":
            return int(value)
        else:  # hex string by default
            return hex(value)


class WebhookConfiguration(BaseModel):

    class Config:
        extra = "allow"


class HexStringConfig(WebhookConfiguration):
    hex_format: dict
    hex_data_field: str

    class Config:
        extra = "allow"


class DynamicSchemaConfig(WebhookConfiguration):
    json_schema: dict

    class Config:
        extra = "allow"


class JQTransformConfig(BaseModel):
    jq_filter: str = Field(
        default=".",
        description="JQ filter to transform JSON data.",
        example=". | map(select(.isActive))"
    )

    class Config:
        extra = "allow"


class GenericJsonTransformConfig(JQTransformConfig, DynamicSchemaConfig):

    class Config:
        extra = "allow"


class WebhookPayload(BaseModel):

    class Config:
        extra = "allow"


class HexStringPayload(WebhookPayload):
    hex_format: dict
    hex_data_field: str

    class Config:
        extra = "allow"

    def dict(self, *args, **kwargs):
        data = super().dict(*args, **kwargs)
        if self.hex_data_field and self.hex_format:
            data_field = getattr(self, self.hex_data_field)
            unpacked_data = data_field.unpacked_data()
            # FixMe unpacked_data is not a dictionary
            data.update(unpacked_data)  # Update the dictionary with unpacked data
        return data

    def json(self, *args, **kwargs):
        return json.dumps(self.dict(*args, **kwargs), default=str)


class GenericJsonPayload(WebhookPayload):

    class Config:
        extra = "allow"


class GenericJsonWithHexStrPayload(HexStringPayload, GenericJsonPayload):

    class Config:
        extra = "allow"


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
