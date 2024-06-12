import struct
from typing import Annotated, Union
import typing
from pydantic import create_model
from pydantic.fields import Field



def find_config_for_action(configurations, action_id):
    return next(
        (
            config for config in configurations
            if config.action.value == action_id
        ),
        None
    )


class StructHexString:
    def __init__(self, value: str, hex_format):
        self.value = value
        self.hex_format = hex_format
        self.format_spec = hex_format.get("byte_order", "<") + ''.join(f["format"] for f in hex_format["fields"])
        self.unpacked_data = self._unpack_data()

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

    @classmethod
    def __modify_schema__(cls, field_schema):
        field_schema.update(type="hex_string", example="123456789ABCDEF", description="Hex string data")

    def _unpack_data(self):
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

    def __repr__(self) -> str:
        return f"StructHexString(value={self.value}, hex_format={self.hex_format})"

    def to_dict(self) -> typing.Dict[str, typing.Any]:
        return {
            "value": self.value,
            "hex_format": self.hex_format,
            "unpacked_data": self.unpacked_data
        }


Model = typing.TypeVar('Model', bound='BaseModel')


class DyntamicFactory:
    """
    Modified version of the DyntamicFactory class from:
    https://github.com/c32168/dyntamic
    """

    TYPES = {
        'string': str,
        'array': list,
        'boolean': bool,
        'integer': int,
        'float': float,
        'number': float,
        'object': dict,
        'hex_string': StructHexString
        # ToDo: test with custom types such as StructHexString
    }

    def __init__(self,
                 json_schema: dict,
                 base_model: type[Model] | tuple[type[Model], ...] | None = None,
                 ref_template: str = "#/$defs/"
                 ) -> None:
        """
        Creates a dynamic pydantic model from a JSONSchema, dumped from and existing Pydantic model elsewhere.
            JSONSchema dump must be called with ref_template='{model}' like:

            SomeSampleModel.model_json_schema(ref_template='{model}')
            Use:
            >> _factory = DyntamicFactory(schema)
            >> _factory.make()
            >> _model = create_model(_factory.class_name, **_factory.model_fields)
            >> _instance = dynamic_model.model_validate(json_with_data)
            >> validated_data = model_instance.model_dump()
        """
        self.class_name = json_schema.get('title', "DataSchema")
        self.class_type = json_schema.get('type')
        self.required = json_schema.get('required', [])
        self.raw_fields = json_schema.get('properties', [])
        self.ref_template = ref_template
        self.definitions = json_schema.get(ref_template)
        self.fields = {}
        self.model_fields = {}
        self._base_model = base_model

    def make(self) -> Model:
        """Factory method, dynamically creates a pydantic model from JSON Schema"""
        for field in self.raw_fields:
            if '$ref' in self.raw_fields[field]:
                model_name = self.raw_fields[field].get('$ref')
                self._make_nested(model_name, field)
            else:
                factory = self.TYPES.get(self.raw_fields[field].get('type'))
                if factory == list:
                    items = self.raw_fields[field].get('items')
                    if self.ref_template in items:
                        self._make_nested(items.get(self.ref_template), field)
                self._make_field(factory, field, self.raw_fields.get('title'))
        return create_model(self.class_name, __base__=self._base_model, **self.model_fields)

    def _make_nested(self, model_name: str, field) -> None:
        """Create a nested model"""
        clean_model_name = model_name.split("/")[-1].strip()
        level = DyntamicFactory({self.ref_template: self.definitions} | self.definitions.get(clean_model_name),
                                ref_template=self.ref_template)
        level.make()
        model = create_model(clean_model_name, **level.model_fields)
        self._make_field(model, field, field)

    def _make_field(self, factory, field, alias) -> None:
        """Create an annotated field"""
        if field not in self.required:
            factory_annotation = Annotated[Union[factory | None], factory]
            self.model_fields[field] = (
                Annotated[factory_annotation, Field(default_factory=factory, alias=alias)],
                ...
            )
        else:
            self.model_fields[field] = (
                Annotated[factory, Field(..., alias=alias)],
                ...
            )

