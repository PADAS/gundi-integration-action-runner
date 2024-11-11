from typing import Optional, List, Dict
import pydantic
from .core import PullActionConfiguration, AuthActionConfiguration, ExecutableActionMixin

class AuthenticateConfig(AuthActionConfiguration, ExecutableActionMixin):
    api_key: pydantic.SecretStr = pydantic.Field(..., title = "eBird API Key", 
                                  description = "API key generated from eBird's website at https://ebird.org/api/keygen",
                                  format="password")
    
class PullEventsConfig(PullActionConfiguration):

    event_type: Optional[str] = pydantic.Field("inat_observation", title="Event type",
        description="The event type to use in the returned event data.")

    event_prefix: str = pydantic.Field("iNat: ", title="Event prefix",
        description = "A string to prefix to the observed species to set a title when creating the event.  Default: 'iNat: '")

    days_to_load: int = pydantic.Field(30, title = "Default number of days to load",
        description="The number of days of data to load from iNaturalist.  If the integration state contains a last_run value, this parameter will be ignored and data will be loaded since the last_run value.")

    bounding_box: Optional[List[float]] = pydantic.Field(title = "Bounding box for search area.  Of the format [ne_latitude, ne_longitude, sw_latitude, sw_longitude]")

    projects: Optional[List[int]] = pydantic.Field(title = "Project IDs", 
        description="List of project IDs to pull from iNaturalist.")
    
    taxa: Optional[List[int]] = pydantic.Field(title = "Taxa IDs", 
        description="List of iNaturalist taxa IDs for which to load observations.")
    
    annotations: Optional[Dict[int,List[int]]] = pydantic.Field(title = "Annotations",
        description="Map of annotation terms and the values which to include.  For example, 22: [24, 25] would only include observations that had the Evidence of Presence annotation set to Organism or Scat.  Entries in the Dict are treated as ORs, whereas values in the Lists are treated as ANDs.")

    priorities: Optional[Dict[int,List[int]]] = pydantic.Field(title="Taxa Priorities",
        description="Dict of priority levels to use for ER events and, for each, a list of taxa whose observations should be given that priority.")
    
    include_photos: Optional[bool] = pydantic.Field(True, title="Include photos",
        description = "Whether or not to include the photos from iNaturalist observations.  Default: True")

    # Temporary validator to cope with a limitation in Gundi Portal.
    @pydantic.validator("event_type", "event_prefix", always=True)
    def validate_region_code(cls, v, values):
        if 'any' == str(v).lower():
            return None
        return v

    @pydantic.validator("bounding_box", always=True)
    def validate_bounding_box(cls, v):
        if(not v or len(v) != 4):
            raise ValueError("Did not receive four values in bounding box configuration.")
        return v

    class Config:
        schema_extra = {
            "examples": [
                {
                    "": 47.5218082,
                    "longitude": -122.3864506,
                    "distance": 30,
                    "num_days_default": 1
                }
            ],
            "required": ["latitude", "longitude", "distance", "num_days"]
        }