import pydantic

from typing import Optional


class BuoyConfig(pydantic.BaseModel):
    er_token: str = pydantic.Field(..., title='EarthRanger authorization token"')
    er_site: str = pydantic.Field(
        ..., title="EarthRanger site address (ex. 'develop.pamdas.org')"
    )
    event_source: Optional[str] = pydantic.Field(
        ..., title="The source identifier for the data (ex. mfg1)"
    )
    er_event_type: Optional[str] = pydantic.Field(
        ..., title="The EarthRanger Event Type (ex. gear_position_mfg1)"
    )


class State(pydantic.BaseModel):
    er_token: str = pydantic.Field(..., title='EarthRanger authorization token"')
    er_site: str = pydantic.Field(
        ..., title="EarthRanger site address (ex. 'develop.pamdas.org')"
    )
    event_source: Optional[str] = pydantic.Field(
        ..., title="The source identifier for the data (ex. mfg1)"
    )
    er_event_type: Optional[str] = pydantic.Field(
        ..., title="The EarthRanger Event Type (ex. gear_position_mfg1)"
    )

    # This will allow us to have two post-processors for the same integration, each one pointing to a different ER site.
    er_buoy_config: Optional[BuoyConfig] = pydantic.Field(
        None, title="Secondary configuration, used for ER Buoy processors."
    )
