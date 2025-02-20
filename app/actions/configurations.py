import pydantic

from .core import (
    AuthActionConfiguration,
    ExecutableActionMixin,
    PullActionConfiguration,
)


class EdgeTechAuthConfiguration(AuthActionConfiguration, ExecutableActionMixin):
    token_json: pydantic.SecretStr = pydantic.Field(
        ...,
        title="Token JSON",
        description="Token JSON for the Edge Tech API.",
    )
    token_url: pydantic.AnyHttpUrl = pydantic.Field(
        ...,
        title="Token URL",
        description="URL to get the Edge Tech API key.",
    )
    redirect_uri: pydantic.AnyHttpUrl = pydantic.Field(
        ...,
        title="Redirect URI",
        description="Redirect URI for the Edge Tech API key.",
    )
    client_id: str = pydantic.Field(
        ...,
        title="Client ID",
        description="Client ID for the Edge Tech API.",
    )

    @property
    def scope(self):
        return "offline_access database:dump openid profile email"


class EdgeTechConfiguration(PullActionConfiguration):
    api_base_url: pydantic.AnyHttpUrl = pydantic.Field(
        ...,
        title="API Base URL",
        description="Base URL for the Edge Tech API.",
    )

    num_get_retry: int = 60

    @property
    def v1_url(self):
        return self.api_base_url + "/v1"

    @property
    def database_dump_url(self):
        return self.v1_url + "/database-dump/tasks"
