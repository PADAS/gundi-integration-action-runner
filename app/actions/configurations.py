from .core import ActionConfiguration
import pydantic  

class AuthenticateConfig(ActionConfiguration):
    username: str
    password: pydantic.SecretStr = pydantic.Field(..., format="password")


class PullObservationsConfig(ActionConfiguration):
    data_endpoint: str
    transmissions_endpoint: str
    mortality_event_type: str = "mortality_event"
    observations_per_request: int = 200
