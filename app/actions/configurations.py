from typing import Optional, List, Dict
import json
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

    bounding_box: Optional[str] = pydantic.Field(title = "Bounding box for search area.  Of the format [ne_latitude, ne_longitude, sw_latitude, sw_longitude]")

    projects: Optional[List[int]] = pydantic.Field(title = "Project IDs", 
        description="List of project IDs to pull from iNaturalist.")
    
    taxa: Optional[List[str]] = pydantic.Field(title = "Taxa IDs", 
        description="List of iNaturalist taxa IDs for which to load observations.")
    
    quality_grade: Optional[List[str]] = pydantic.Field(title = "Quality Grade",
        description = "If present, only observations that have one of the entered quality grades will be included.  As of November, 2024, valid iNaturalist values are casual, needs_id and/or research.")

    annotations: Optional[str] = pydantic.Field(title = "Annotations",
        description='Map of annotation terms and the values which to include.  For example, {"22": ["24", "25"], "1": ["2"]} would only include observations of Adults (annotation 1 == 2) that had the Evidence of Presence annotation (22) set to Organism (24) or Scat (25).  Entries in the Dict are treated as ORs, whereas values in the Lists are treated as ANDs.')

    include_photos: Optional[bool] = pydantic.Field(True, title="Include photos",
        description = "Whether or not to include the photos from iNaturalist observations.  Default: True")

    # Temporary validator to cope with a limitation in Gundi Portal.
    @pydantic.validator("event_type", "event_prefix", always=True)
    def validate_region_code(cls, v, values):
        if 'any' == str(v).lower():
            return None
        return v

    @pydantic.validator("annotations", always=True)
    def validate_json(cls, v):
        v = v.strip()
        if(v == ""):
            return None
        try:
            v = json.loads(v)
        except:
            raise ValueError(f"Could not parse json: {v}")
        return v

    @pydantic.validator("bounding_box", always=True)
    def validate_bounding_box(cls, v):
        if(not v):
            raise ValueError("Did not receive a bounding box configuration.")
        coords = json.loads(v)
        if(len(coords) != 4):
            raise ValueError("Did not receive four values in bounding box configuration.")
        for coord in coords:
            try:
                float(coord)
            except:
                raise ValueError(f"Could not parse bounding box values {v}.")
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
            "required": ["bounding_box", "days_to_load"]
        }