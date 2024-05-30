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
        self.class_name = json_schema.get('title')
        self.class_type = json_schema.get('type')
        self.required = json_schema.get('required', False)
        self.raw_fields = json_schema.get('properties')
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

