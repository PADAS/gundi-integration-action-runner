from typing import Optional
from .core import ExecutableActionMixin, PullActionConfiguration, InternalActionConfiguration
import pydantic


class ProcessTelemetryDataActionConfiguration(PullActionConfiguration, ExecutableActionMixin):
    bucket_path: str = pydantic.Field("ornitela/", title="Bucket Path", description="Path within the bucket where telemetry files are stored")
    delete_after_archive_days: int = pydantic.Field(5, title="Delete After Archive Days", description="Number of days after archiving before files are deleted")
    historical_limit_days: int = pydantic.Field(5, title="Historical Limit Days", description="Number of days to look back for data")
    max_files_per_run: int = pydantic.Field(10, title="Max Files Per Run", description="Maximum number of files to process per run")
    process_most_recent_first: bool = pydantic.Field(True, title="Process Most Recent First", description="Process the most recently modified files first")
    chunk_size: int = pydantic.Field(5000, title="Chunk Size", description="Number of rows per processing chunk")
    batch_size: int = pydantic.Field(500, title="Batch Size", description="Number of observations per batch when sending to Gundi")
    include_sensor_data: bool = pydantic.Field(True, title="Include Sensor Data", description="Include sensor (SEN_*) rows as observations. Disable to send only GPS position data.")

    @pydantic.validator("bucket_path")
    def validate_bucket_path(cls, v):
        return v.strip().strip('/') if v else ""


class ProcessOrnitelaFileActionConfiguration(InternalActionConfiguration):
    bucket_path: str = pydantic.Field("", title="Bucket Path", description="Path within the bucket where telemetry files are stored")
    file_name: str = pydantic.Field(..., title="File Name", description="Name of the file to process")
    source_file: Optional[str] = pydantic.Field(None, title="Source File", description="Original root file this chunk was carved from; when set, triggers the next chunk after archiving")
    chunk_size: int = pydantic.Field(5000, title="Chunk Size", description="Number of rows per processing chunk")
    historical_limit_days: int = pydantic.Field(5, title="Historical Limit Days", description="Number of days to look back for data")
    delete_after_archive_days: int = pydantic.Field(5, title="Delete After Archive Days", description="Number of days after archiving before files are deleted")
    batch_size: int = pydantic.Field(500, title="Batch Size", description="Number of observations per batch when sending to Gundi")
    include_sensor_data: bool = pydantic.Field(True, title="Include Sensor Data", description="Include sensor (SEN_*) rows as observations. Disable to send only GPS position data.")

    @pydantic.validator("bucket_path")
    def validate_bucket_path(cls, v):
        return v.strip().strip('/') if v else ""
