import pydantic

from .core import PullActionConfiguration


class EdgeTechConfiguration(PullActionConfiguration):
    token_json: pydantic.SecretStr = pydantic.Field(
        ...,
        title="Token JSON",
        description="Token JSON for the Edge Tech API.",
    )
    api_base_url: pydantic.AnyHttpUrl = pydantic.Field(
        ...,
        title="API Base URL",
        description="Base URL for the Edge Tech API.",
    )
    client_id: str = pydantic.Field(
        ...,
        title="Client ID",
        description="Client ID for the Edge Tech API.",
    )
    redirect_uri: pydantic.AnyHttpUrl = pydantic.Field(
        ...,
        title="Redirect URI",
        description="Redirect URI for the Edge Tech API key.",
    )

    token_url: pydantic.AnyHttpUrl = pydantic.Field(
        ...,
        title="Token URL",
        description="URL to get the Edge Tech API key.",
    )

    num_get_retry: int = 60

    @property
    def v1_url(self):
        return self.api_base_url + "/v1"

    @property
    def database_dump_url(self):
        return self.v1_url + "/database-dump/tasks"

    @property
    def scope(self):
        return "offline_access database:dump openid profile email"
