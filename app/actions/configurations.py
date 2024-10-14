import pydantic
from .core import PullActionConfiguration, AuthActionConfiguration, ExecutableActionMixin
from .gfwclient import IntegratedAlertsConfidenceEnum, NasaViirsFireAlertConfidenceEnum

class AuthenticateConfig(AuthActionConfiguration, ExecutableActionMixin):
    email: str
    password: pydantic.SecretStr = pydantic.Field(..., format="password", 
                                                  title="Password",
                                                  description="Password for the Global Forest Watch account.")

class PullEventsConfig(PullActionConfiguration):

    gfw_share_link_url: pydantic.HttpUrl = pydantic.Field(
        ...,
        title="Global Forest Watch AOI Share Link",
        description="AOI share link from your MyGFW dashboard."
    )
    include_fire_alerts: bool = pydantic.Field(
        True,
        title="Include fire alerts",
        description="Fetch fire alerts from Global Forest Watch and include them in this connection."
    )

    fire_alerts_lowest_confidence: NasaViirsFireAlertConfidenceEnum = pydantic.Field(
        NasaViirsFireAlertConfidenceEnum.high,
        title="Fire alerts lowest confidence",
        description="Lowest confidence level to include in the connection."
    )

    include_integrated_alerts: bool = pydantic.Field(
        True,
        title="Include integrated deforestation alerts",
        description="Fetch integrated deforestation alerts from Global Forest Watch and include them in the connection."
    )

    integrated_alerts_lowest_confidence: IntegratedAlertsConfidenceEnum = pydantic.Field(
        IntegratedAlertsConfidenceEnum.highest,
        title="Integrated deforestation alerts lowest confidence",
        description="Lowest confidence level to include in the connection."
    )

    fire_lookback_days: int = pydantic.Field(
        10,
        le=10,
        ge=1,
        title="Fire alerts lookback days",
        description="Number of days to look back for fire alerts."
    )
    integrated_alerts_lookback_days: int = pydantic.Field(
        30,
        le=30,
        ge=1,
        title="Integrated deforestation alerts lookback days",
        description="Number of days to look back for integrated deforestation alerts."
    )

    force_fetch: bool = pydantic.Field(
        False,
        title="Force fetch",
        description="Force fetch even if in a quiet period."
    )
