from typing import List, Optional

import pydantic
from pydantic import root_validator

from app.actions.core import AuthActionConfiguration, ExecutableActionMixin, PullActionConfiguration
from app.services.errors import ConfigurationNotFound
from app.services.utils import FieldWithUIOptions, GlobalUISchemaOptions, UIOptions, find_config_for_action


class AuthenticateKineisConfig(AuthActionConfiguration, ExecutableActionMixin):
    """Configuration for the Kineis auth action (portal credential verification)."""

    username: str = pydantic.Field(
        ...,
        title="Username",
        description="CLS/Kineis API username"
    )
    password: pydantic.SecretStr = pydantic.Field(
        ...,
        title="Password",
        description="CLS/Kineis API password",
        format="password"
    )
    client_id: str = pydantic.Field(
        "api-telemetry",
        title="Client ID",
        description="OAuth client_id for token endpoint",
    )

    ui_global_options = GlobalUISchemaOptions(
        order=["username", "password", "client_id"],
    )


def get_auth_config(integration):
    """Get Kineis auth credentials from the integration's auth action config."""
    auth_config = find_config_for_action(
        configurations=integration.configurations,
        action_id="auth",
    )
    if not auth_config:
        raise ConfigurationNotFound(
            f"Authentication settings for integration {str(integration.id)} "
            "are missing. Please configure the Auth action in the portal."
        )
    return AuthenticateKineisConfig.parse_obj(auth_config.data)


class PullTelemetryConfiguration(PullActionConfiguration):
    """Configuration for the Kineis pull_telemetry action (CONNECTORS-836). Credentials come from the Auth action."""

    lookback_hours: int = FieldWithUIOptions(
        4,
        ge=1,
        le=168,
        title="Lookback hours",
        description="Hours to look back for telemetry (UTC time window)",
        ui_options=UIOptions(
            widget="range",  # slider
        )
    )
    page_size: int = FieldWithUIOptions(
        100,
        ge=1,
        le=500,
        title="Page size",
        description="Bulk API pagination page size",
        ui_options=UIOptions(
            widget="range",  # slider
        )

    )
    device_refs: Optional[List[str]] = FieldWithUIOptions(
        default=None,
        title="Device refs",
        description="Optional list of device refs (string IDs) to filter",
        # ui_options=UIOptions(widget="textarea"),
    )
    device_uids: Optional[List[int]] = FieldWithUIOptions(
        default=None,
        title="Device UIDs",
        description="Optional list of device UIDs (numeric) to filter",
        # ui_options=UIOptions(widget="textarea"),
    )
    retrieve_metadata: bool = pydantic.Field(
        True,
        title="Retrieve metadata",
        description="Include metadata in bulk response",
    )
    retrieve_raw_data: bool = pydantic.Field(
        True,
        title="Retrieve raw data",
        description="Include raw data in bulk response",
    )
    use_realtime: bool = pydantic.Field(
        True,
        title="Use realtime API",
        description="When enabled, use the realtime checkpoint API for scheduled pulls (only new data since last run). When disabled or on first run, use bulk API with lookback window.",
    )

    @root_validator
    def device_filter_single(cls, values):
        """API allows only one of deviceRefs or deviceUids (manual 1.3.1.2)."""
        refs = values.get("device_refs") or []
        uids = values.get("device_uids") or []
        if refs and uids:
            raise ValueError(
                "Provide only one of device_refs or device_uids; the API does not accept both."
            )
        return values

    ui_global_options = GlobalUISchemaOptions(
        order=[
            "lookback_hours",
            "page_size",
            "use_realtime",
            "device_refs",
            "device_uids",
            "retrieve_metadata",
            "retrieve_raw_data",
        ],
    )
