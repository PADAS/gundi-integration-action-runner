import pydantic
from .core import PullActionConfiguration, AuthActionConfiguration, ExecutableActionMixin

class AuthenticateConfig(AuthActionConfiguration, ExecutableActionMixin):
    username: str
    password: pydantic.SecretStr = pydantic.Field(..., 
                                                  format="password", 
                                                  title="Password",
                                                  description="Password for an eBird account.")

class PullEventsConfig(PullActionConfiguration):

    some_important_value: float = pydantic.Field(0.0,
        title="Some Important Value",
        description="This is a value of great importance."
    )
