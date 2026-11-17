from gundi_action_runner.actions.core import AuthActionConfiguration, PullActionConfiguration
from gundi_action_runner.services.utils import (
    FieldWithUIOptions,
    GlobalUISchemaOptions,
    UIOptions,
)
from gundi_action_runner.webhooks.core import WebhookConfiguration, WebhookPayload


class AuthConfig(AuthActionConfiguration):
    api_key: str = FieldWithUIOptions(
        ...,
        title="API Key",
        description="API key for the reference tracking API",
        format="password",
        ui_options=UIOptions(widget="password"),
    )
    ui_global_options = GlobalUISchemaOptions(order=["api_key"])


class PullObservationsConfig(PullActionConfiguration):
    lookback_days: int = FieldWithUIOptions(
        7,
        ge=1,
        le=30,
        title="Lookback Days",
        description="How many days back to fetch on the first run",
        ui_options=UIOptions(widget="range"),
    )
    ui_global_options = GlobalUISchemaOptions(order=["lookback_days"])


class ReferenceWebhookPayload(WebhookPayload):
    device_id: str
    lat: float
    lon: float
    recorded_at: str


class ReferenceWebhookConfig(WebhookConfiguration):
    default_subject_type: str = "unknown"
