# actions/configurations.py
import pydantic
from .core import (
    PullActionConfiguration,
    AuthActionConfiguration,
    ExecutableActionMixin,
)


class AuthenticateConfig(AuthActionConfiguration, ExecutableActionMixin):
    api_key: pydantic.SecretStr = pydantic.Field(
        ...,
        title="rmwHUB API Key",
        description="API key used to read/write data from rmwHUB services.",
        format="password",
    )


class PullRmwHubObservationsConfiguration(PullActionConfiguration):
    sync_interval_minutes: int = 5  # TODO: This doesn't affect Gundi's scheduler yet.
    api_key: pydantic.SecretStr = pydantic.Field(
        ...,
        title="rmwHUB API Key",
        description="API key used to read/write data from rmwHUB services.",
        format="password",
    )
    rmw_url: str = "https://test.ropeless.network/api/"
    er_site: str = "https://buoy.dev.pamdas.org/api/v1.0/"
    er_token: pydantic.SecretStr = pydantic.Field(
        ...,
        title="ER Token",
        description="ER token used to read/write data from buoy services.",
        format="password",
    )
