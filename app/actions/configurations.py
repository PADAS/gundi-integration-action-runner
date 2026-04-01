from typing import Optional
from .core import ExecutableActionMixin, PullActionConfiguration, InternalActionConfiguration
import pydantic


class ProcessTelemetryDataActionConfiguration(PullActionConfiguration, ExecutableActionMixin):
    bucket_path: str = pydantic.Field("", title="Bucket Path", description="Path within the bucket where telemetry files are stored")
    delete_after_archive_days: int = pydantic.Field(90, title="Delete After Archive Days", description="Number of days after archiving before files are deleted")
    historical_limit_days: int = pydantic.Field(30, title="Historical Limit Days", description="Number of days to look back for data")

    @pydantic.validator("bucket_path")
    def validate_bucket_path(cls, v):
        return v.strip().strip('/') if v else ""


class ProcessOrnitelaFileActionConfiguration(InternalActionConfiguration):
    bucket_path: str = pydantic.Field("", title="Bucket Path", description="Path within the bucket where telemetry files are stored")
    file_name: str = pydantic.Field(..., title="File Name", description="Name of the file to process")
    historical_limit_days: int = pydantic.Field(30, title="Historical Limit Days", description="Number of days to look back for data")
    delete_after_archive_days: int = pydantic.Field(90, title="Delete After Archive Days", description="Number of days after archiving before files are deleted")

    @pydantic.validator("bucket_path")
    def validate_bucket_path(cls, v):
        return v.strip().strip('/') if v else ""
