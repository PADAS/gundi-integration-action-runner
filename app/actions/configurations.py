import pydantic
from app.services.utils import FieldWithUIOptions, GlobalUISchemaOptions, UIOptions
from .core import AuthActionConfiguration, PullActionConfiguration, PushActionConfiguration, ExecutableActionMixin


class AuthenticateConfig(AuthActionConfiguration, ExecutableActionMixin):
    username: str = pydantic.Field(..., title = "Username", description = "Username for Bluetrax account")
    password: pydantic.SecretStr = pydantic.Field(..., title = "Password", 
                                description = "Password for Bluetrax account",
                                format="password")


class PullEventsConfig(PullActionConfiguration, ExecutableActionMixin):
    pass