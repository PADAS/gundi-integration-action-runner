import struct
import typing
from pydantic import create_model, BaseModel
from pydantic.fields import Field, FieldInfo, Undefined, NoArgAnyCallable
from typing import Any, Dict, Optional, Union, List, Annotated


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


class GlobalUISchemaOptions(BaseModel):
    order: Optional[List[str]]
    addable: Optional[bool]
    copyable: Optional[bool]
    orderable: Optional[bool]
    removable: Optional[bool]
    label: Optional[bool]
    duplicateKeySuffixSeparator: Optional[str]


class UIOptions(GlobalUISchemaOptions):
    classNames: Optional[str]
    style: Optional[Dict[str, Any]]  # Assuming style is a dictionary of CSS properties
    title: Optional[str]
    description: Optional[str]
    placeholder: Optional[str]
    help: Optional[str]
    autofocus: Optional[bool]
    autocomplete: Optional[str]  # Type of HTMLInputElement['autocomplete']
    disabled: Optional[bool]
    emptyValue: Optional[Any]
    enumDisabled: Optional[Union[List[Union[str, int, bool]], None]]  # List of disabled enum options
    hideError: Optional[bool]
    readonly: Optional[bool]
    filePreview: Optional[bool]
    inline: Optional[bool]
    inputType: Optional[str]
    rows: Optional[int]
    submitButtonOptions: Optional[Dict[str, Any]]  # Assuming UISchemaSubmitButtonOptions is a dict
    widget: Optional[Union[str, Any]]  # Either a widget implementation or its name
    enumNames: Optional[List[str]]  # List of labels for enum values


class FieldInfoWithUIOptions(FieldInfo):

    def __init__(self, *args, **kwargs):
        """
        Extends the Pydantic Field class to support ui:schema generation
        :param kwargs: ui_options: UIOptions
        """
        self.ui_options = kwargs.pop("ui_options", None)
        super().__init__(*args, **kwargs)

    def ui_schema(self, *args, **kwargs):
        """Generates a UI schema from model field ui_options"""
        if not self.ui_options:
            return {}
        ui_schema = {}
        ui_options = self.ui_options.__fields__
        for field_name, model_field in ui_options.items():
            if value := getattr(self.ui_options, field_name, model_field.default):
                ui_schema[f"ui:{field_name}"] = value
        return ui_schema


def FieldWithUIOptions(
    default: Any = Undefined,
    *,
    default_factory: Optional[NoArgAnyCallable] = None,
    alias: Optional[str] = None,
    title: Optional[str] = None,
    description: Optional[str] = None,
    exclude: Optional[Union['AbstractSetIntStr', 'MappingIntStrAny', Any]] = None,
    include: Optional[Union['AbstractSetIntStr', 'MappingIntStrAny', Any]] = None,
    const: Optional[bool] = None,
    gt: Optional[float] = None,
    ge: Optional[float] = None,
    lt: Optional[float] = None,
    le: Optional[float] = None,
    multiple_of: Optional[float] = None,
    allow_inf_nan: Optional[bool] = None,
    max_digits: Optional[int] = None,
    decimal_places: Optional[int] = None,
    min_items: Optional[int] = None,
    max_items: Optional[int] = None,
    unique_items: Optional[bool] = None,
    min_length: Optional[int] = None,
    max_length: Optional[int] = None,
    allow_mutation: bool = True,
    regex: Optional[str] = None,
    discriminator: Optional[str] = None,
    repr: bool = True,
    ui_options: UIOptions = None,
    **extra: Any,
) -> FieldInfoWithUIOptions:
    """
    Used to provide extra information about a field, either for the model schema or complex validation. Some arguments
    apply only to number fields (``int``, ``float``, ``Decimal``) and some apply only to ``str``.

    :param default: since this is replacing the field’s default, its first argument is used
      to set the default, use ellipsis (``...``) to indicate the field is required
    :param default_factory: callable that will be called when a default value is needed for this field
      If both `default` and `default_factory` are set, an error is raised.
    :param alias: the public name of the field
    :param title: can be any string, used in the schema
    :param description: can be any string, used in the schema
    :param exclude: exclude this field while dumping.
      Takes same values as the ``include`` and ``exclude`` arguments on the ``.dict`` method.
    :param include: include this field while dumping.
      Takes same values as the ``include`` and ``exclude`` arguments on the ``.dict`` method.
    :param const: this field is required and *must* take it's default value
    :param gt: only applies to numbers, requires the field to be "greater than". The schema
      will have an ``exclusiveMinimum`` validation keyword
    :param ge: only applies to numbers, requires the field to be "greater than or equal to". The
      schema will have a ``minimum`` validation keyword
    :param lt: only applies to numbers, requires the field to be "less than". The schema
      will have an ``exclusiveMaximum`` validation keyword
    :param le: only applies to numbers, requires the field to be "less than or equal to". The
      schema will have a ``maximum`` validation keyword
    :param multiple_of: only applies to numbers, requires the field to be "a multiple of". The
      schema will have a ``multipleOf`` validation keyword
    :param allow_inf_nan: only applies to numbers, allows the field to be NaN or infinity (+inf or -inf),
        which is a valid Python float. Default True, set to False for compatibility with JSON.
    :param max_digits: only applies to Decimals, requires the field to have a maximum number
      of digits within the decimal. It does not include a zero before the decimal point or trailing decimal zeroes.
    :param decimal_places: only applies to Decimals, requires the field to have at most a number of decimal places
      allowed. It does not include trailing decimal zeroes.
    :param min_items: only applies to lists, requires the field to have a minimum number of
      elements. The schema will have a ``minItems`` validation keyword
    :param max_items: only applies to lists, requires the field to have a maximum number of
      elements. The schema will have a ``maxItems`` validation keyword
    :param unique_items: only applies to lists, requires the field not to have duplicated
      elements. The schema will have a ``uniqueItems`` validation keyword
    :param min_length: only applies to strings, requires the field to have a minimum length. The
      schema will have a ``minLength`` validation keyword
    :param max_length: only applies to strings, requires the field to have a maximum length. The
      schema will have a ``maxLength`` validation keyword
    :param allow_mutation: a boolean which defaults to True. When False, the field raises a TypeError if the field is
      assigned on an instance.  The BaseModel Config must set validate_assignment to True
    :param regex: only applies to strings, requires the field match against a regular expression
      pattern string. The schema will have a ``pattern`` validation keyword
    :param discriminator: only useful with a (discriminated a.k.a. tagged) `Union` of sub models with a common field.
      The `discriminator` is the name of this common field to shorten validation and improve generated schema
    :param repr: show this field in the representation
    :param ui_options: UIOptions instance used to set ui properties for the ui schema
    :param **extra: any additional keyword arguments will be added as is to the schema
    """
    field_info = FieldInfoWithUIOptions(
        default,
        default_factory=default_factory,
        alias=alias,
        title=title,
        description=description,
        exclude=exclude,
        include=include,
        const=const,
        gt=gt,
        ge=ge,
        lt=lt,
        le=le,
        multiple_of=multiple_of,
        allow_inf_nan=allow_inf_nan,
        max_digits=max_digits,
        decimal_places=decimal_places,
        min_items=min_items,
        max_items=max_items,
        unique_items=unique_items,
        min_length=min_length,
        max_length=max_length,
        allow_mutation=allow_mutation,
        regex=regex,
        discriminator=discriminator,
        repr=repr,
        ui_options=ui_options,
        **extra,
    )
    field_info._validate()
    return field_info


class UISchemaModelMixin:

    @classmethod
    def ui_schema(cls, *args, **kwargs):
        """Generates a UI schema from model"""
        ui_schema = {}
        # Iterate through the fields and generate UI schema
        for field_name, model_field in cls.__fields__.items():
            if getattr(model_field.field_info, "ui_options", None):
                ui_schema[field_name] = model_field.field_info.ui_schema()
        # Include global options
        if global_options := cls.__fields__.get('ui_global_options'):
            if getattr(global_options, "type_", None) == GlobalUISchemaOptions:
                model = global_options.default
                for field_name, model_field in model.__fields__.items():
                    if value := getattr(model, field_name, model_field.default):
                        ui_schema[f"ui:{field_name}"] = value
        return ui_schema


    @classmethod
    def schema(cls, **kwargs):
        # Call the parent schema method to get the original schema
        json_schema_dict = super().schema(**kwargs)

        # Remove ui schema fields from the properties and definitions
        properties = json_schema_dict.get('properties', {})
        for field in ["ui_options", "ui_global_options"]:
            properties.pop(field, None)
        json_schema_dict['properties'] = properties
        definitions = json_schema_dict.get('definitions', {})
        for field in ["UIOptions", "GlobalUISchemaOptions"]:
            definitions.pop(field, None)
        json_schema_dict['definitions'] = definitions
        return json_schema_dict


class OptionalStringType(str):
    """
    A custom type that ensures JSON schema includes both 'string' and 'null'.
    This is a workaround to solve the following pydantic issue until we can upgrade to v2:
    https://github.com/pydantic/pydantic/issues/4111
    """

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, value):
        if value is None:
            return None
        if not isinstance(value, str):
            raise TypeError("String expected")
        return value

    @classmethod
    def __modify_schema__(cls, field_schema):
        field_schema["type"] = ["string", "null"]


def generate_batches(iterable, batch_size):
    for i in range(0, len(iterable), batch_size):
        yield iterable[i: i + batch_size]

