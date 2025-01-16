from .core import InternalActionConfiguration, PullActionConfiguration, AuthActionConfiguration
import pydantic

class AuthenticateConfig(AuthActionConfiguration):
    username: str
    password: pydantic.SecretStr = pydantic.Field(..., title = "Password", 
                                description = "Password for Bluetrax account",
                                format="password")


class PullObservationsConfig(PullActionConfiguration):
    endpoint: str = "mobile/vehicles"

class PullObservationsFromDeviceBatch(InternalActionConfiguration):
    devices: list[str]