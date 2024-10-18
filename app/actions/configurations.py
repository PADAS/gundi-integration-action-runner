import pydantic
from .core import PullActionConfiguration, AuthActionConfiguration, ExecutableActionMixin

class AuthenticateConfig(AuthActionConfiguration, ExecutableActionMixin):
    api_key: str = pydantic.Field(..., title = "eBird API Key", 
                                  description = "API key generated from eBird's website at https://ebird.org/api/keygen")
    

class PullEventsConfig(PullActionConfiguration):

    latitude: float = pydantic.Field(0, title="Latitude",
        description="Latitude of point to search around.  If not present, a search region shoud be included instead.")
    longitude: float = pydantic.Field(0, title="Longitude",
        description="Longitude of point to search around.  If not present, a search region shoud be included instead.")
    distance: float = pydantic.Field(25, title="Distance",
        description="Distance in kilometers to search around.  Max: 50km.  Default: 25km.", ge=1, le=50)
    
    region_code: str = pydantic.Field(None, title="Region Code",
        description="An eBird region code that should be used in the query.  Either a region code or a combination of latitude, longitude and distance should be included.")
    
    species_code: str = pydantic.Field(None, title="Species Code",
        description="An eBird species code that should be used in the query.  If not included, all species will be searched.")

    include_provisional: bool = pydantic.Field(False, title="Include Unreviewed", 
        description="Whether or not to include observations that have not yet been reviewed.  Default: False.")