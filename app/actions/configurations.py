from typing import Optional
import pydantic
from .core import PullActionConfiguration, AuthActionConfiguration, ExecutableActionMixin

class AuthenticateConfig(AuthActionConfiguration, ExecutableActionMixin):
    api_key: pydantic.SecretStr = pydantic.Field(..., title = "eBird API Key", 
                                  description = "API key generated from eBird's website at https://ebird.org/api/keygen",
                                  format="password")
    
class PullEventsConfig(PullActionConfiguration):

    latitude: float = pydantic.Field(0, title="Latitude",
        description="Latitude of point to search around.  If not present, a search region shoud be included instead.")
    longitude: float = pydantic.Field(0, title="Longitude",
        description="Longitude of point to search around.  If not present, a search region shoud be included instead.")
    distance: float = pydantic.Field(25, title="Distance",
        description="Distance in kilometers to search around.  Max: 50km.  Default: 25km.", ge=1, le=50)
    
    num_days_default: int = pydantic.Field(2, title="Number of Days",
        description = "Number of days of data to pull from eBird. If the integration state has a last_run parameter, this will be overriden by the number of days since the last run, rounded up to a whole day. Default: 2")

    region_code: Optional[str] = pydantic.Field('', title="Region Code",
        description="An eBird region code that should be used in the query.  Either a region code or a combination of latitude, longitude and distance should be included.")
    
    species_code: Optional[str] = pydantic.Field('', title="Species Code",
        description="An eBird species code that should be used in the query.  If not included, all species will be searched.")

    include_provisional: bool = pydantic.Field(False, title="Include Unreviewed", 
        description="Whether or not to include observations that have not yet been reviewed.  Default: False.")
    
    # Temporary validator to cope with a limitation in Gundi Portal.
    @pydantic.validator("region_code", "species_code", always=True)
    def validate_region_code(cls, v, values):
        if 'any' == str(v).lower():
            return None
        return v

    class Config:
        schema_extra = {
            "examples": [
                {
                    "latitude": 47.5218082,
                    "longitude": -122.3864506,
                    "distance": 30,
                    "num_days_default": 1
                }
            ],
            "required": ["latitude", "longitude", "distance", "num_days"]
        }