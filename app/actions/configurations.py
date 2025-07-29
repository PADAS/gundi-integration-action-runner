import pydantic
import datetime

from app.actions.core import AuthActionConfiguration, PullActionConfiguration, ExecutableActionMixin
from app.services.errors import ConfigurationNotFound
from app.services.utils import find_config_for_action, UIOptions, FieldWithUIOptions, GlobalUISchemaOptions


class AuthenticateConfig(AuthActionConfiguration, ExecutableActionMixin):
    username: str
    password: pydantic.SecretStr = pydantic.Field(..., format="password")

    ui_global_options: GlobalUISchemaOptions = GlobalUISchemaOptions(
        order=[
            "username",
            "password",
        ],
    )


class PullObservationsConfig(PullActionConfiguration):
    gmt_offset: int = FieldWithUIOptions(
        0,
        le=12,
        ge=-12,
        title="GMT Offset",
        ui_options=UIOptions(
            widget="range",
        ),
        description="Offset from GMT in hours (e.g., -5 for EST, +1 for CET). This is used to adjust the timestamps of the observations.",
    )


class PullHistoricalObservationsConfig(PullActionConfiguration, ExecutableActionMixin):
    start_date: datetime.datetime = FieldWithUIOptions(
        '',
        title="Start Date",
        description="The start date for the historical data pull. (Must be 7 days or less from end_date)",
    )
    end_date: datetime.datetime = FieldWithUIOptions(
        '',
        title="End Date",
        description="The end date for the historical data pull. (Must be 7 days or less from start_date)",
    )
    gmt_offset: int = FieldWithUIOptions(
        0,
        le=12,
        ge=-12,
        title="GMT Offset",
        ui_options=UIOptions(
            widget="range",
        ),
        description="Offset from GMT in hours (e.g., -5 for EST, +1 for CET). This is used to adjust the timestamps of the observations.",
    )

    ui_global_options: GlobalUISchemaOptions = GlobalUISchemaOptions(
        order=[
            "start_date",
            "end_date",
            "gmt_offset"
        ],
    )

    @pydantic.root_validator
    def check_date_range(cls, values):
        start = values.get("start_date")
        end = values.get("end_date")
        if start and end and (end - start).days > 7:
            raise ValueError("The date range between start_date and end_date must not exceed 7 days.")
        return values


def get_auth_config(integration):
    # Look for the login credentials, needed for any action
    auth_config = find_config_for_action(
        configurations=integration.configurations,
        action_id="auth"
    )
    if not auth_config:
        raise ConfigurationNotFound(
            f"Authentication settings for integration {str(integration.id)} "
            f"are missing. Please fix the integration setup in the portal."
        )
    return AuthenticateConfig.parse_obj(auth_config.data)


def get_pull_config(integration):
    # Look for the login credentials, needed for any action
    pull_config = find_config_for_action(
        configurations=integration.configurations,
        action_id="pull_observations"
    )
    if not pull_config:
        raise ConfigurationNotFound(
            f"Authentication settings for integration {str(integration.id)} "
            f"are missing. Please fix the integration setup in the portal."
        )
    return PullObservationsConfig.parse_obj(pull_config.data)
