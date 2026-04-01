import aiohttp
import stamina
import asyncio
from datetime import datetime
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field, validator
from gcloud.aio.storage import Storage
from app import settings


class FileMetadata(BaseModel):
    """
    Pydantic model for Google Cloud Storage file metadata with validation and date parsing.
    Uses field names that match the Google Cloud Storage API response.
    """
    timeCreated: Optional[datetime] = Field(None, description="File creation timestamp")
    updated: Optional[datetime] = Field(None, description="Last modification timestamp")
    size: Optional[int] = Field(None, description="File size in bytes")
    contentType: Optional[str] = Field(None, description="MIME type of the file")
    md5Hash: Optional[str] = Field(None, description="MD5 hash for integrity checking")
    etag: Optional[str] = Field(None, description="Entity tag for caching")
    generation: Optional[str] = Field(None, description="File generation number")
    metageneration: Optional[str] = Field(None, description="Metadata generation number")
    storageClass: Optional[str] = Field(None, description="Storage class (STANDARD, NEARLINE, etc.)")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Custom metadata")

    class Config:
        """Pydantic configuration."""
        # Allow extra fields that might be present in the response
        extra = "ignore"
        # Use enum values instead of enum names
        use_enum_values = True


# ToDo. Move this to the template for other integrations needing file support
class CloudFileStorage:
    def __init__(self, bucket_name=None, root_prefix=None):
        self.root_prefix = root_prefix
        self.bucket_name = bucket_name or settings.INFILE_STORAGE_BUCKET
        self._storage_client = None  # Lazy initialization

    @property
    def storage_client(self):
        if self._storage_client is None:
            self._storage_client = Storage()
        return self._storage_client

    def get_file_fullname(self, integration_id, blob_name):
        # Remove integration_id from path - use only root_prefix and blob_name
        return f"{self.root_prefix}/{blob_name}"

    async def upload_file(self, integration_id, local_file_path, destination_blob_name, metadata=None):
        target_path = self.get_file_fullname(integration_id, destination_blob_name)
        custom_metadata = {"metadata": metadata} if metadata else None
        for attempt in stamina.retry_context(on=(aiohttp.ClientError, asyncio.TimeoutError),
                                             attempts=5, wait_initial=1.0, wait_max=30, wait_jitter=3.0):
            with attempt:
                await self.storage_client.upload_from_filename(
                    self.bucket_name, target_path, local_file_path, metadata=custom_metadata
                )

    async def download_file(self, integration_id, source_blob_name, destination_file_path):
        source_path = self.get_file_fullname(integration_id, source_blob_name)
        for attempt in stamina.retry_context(on=(aiohttp.ClientError, asyncio.TimeoutError),
                                             attempts=5, wait_initial=1.0, wait_max=30, wait_jitter=3.0):
            with attempt:
                await self.storage_client.download_to_filename(self.bucket_name, source_path, destination_file_path)

    async def delete_file(self, integration_id, blob_name):
        target_path = self.get_file_fullname(integration_id, blob_name)
        for attempt in stamina.retry_context(on=(aiohttp.ClientError, asyncio.TimeoutError),
                                             attempts=5, wait_initial=1.0, wait_max=30, wait_jitter=3.0):
            with attempt:
                await self.storage_client.delete(self.bucket_name, target_path)

    async def list_files(self, integration_id):
        # List files without integration_id in the path
        blobs = await self.storage_client.list_objects(self.bucket_name, params={"prefix": f"{self.root_prefix}"})
        for attempt in stamina.retry_context(on=(aiohttp.ClientError, asyncio.TimeoutError),
                                             attempts=5, wait_initial=1.0, wait_max=30, wait_jitter=3.0):
            with attempt:
                # Return only the blob names without the root_prefix
                items = blobs.get('items', [])
                results = [blob['name'].replace(f"{self.root_prefix}/", "") for blob in items if blob['name'].startswith(f"{self.root_prefix}/")]
                return results

    async def get_file_metadata(self, integration_id, blob_name) -> FileMetadata:
        """
        Get file metadata from Google Cloud Storage and return as a validated Pydantic model.
        
        Args:
            integration_id: Integration ID (used for path construction)
            blob_name: Name of the blob/file
            
        Returns:
            FileMetadata: Validated metadata model with parsed dates and types
        """
        target_path = self.get_file_fullname(integration_id, blob_name)
        for attempt in stamina.retry_context(on=(aiohttp.ClientError, asyncio.TimeoutError),
                                             attempts=5, wait_initial=1.0, wait_max=30, wait_jitter=3.0):
            with attempt:
                response = await self.storage_client.download_metadata(self.bucket_name, target_path)
                
                # Create and validate the Pydantic model directly from the response
                # Field names now match the Google Cloud Storage API response
                return FileMetadata(**response)

    async def update_file_metadata(self, integration_id, blob_name, metadata):
        target_path = self.get_file_fullname(integration_id, blob_name)
        custom_metadata = {"metadata": metadata}
        for attempt in stamina.retry_context(on=(aiohttp.ClientError, asyncio.TimeoutError),
                                             attempts=5, wait_initial=1.0, wait_max=30, wait_jitter=3.0):
            with attempt:
                await self.storage_client.patch_metadata(self.bucket_name, target_path, custom_metadata)

    async def move_file(self, integration_id, source_blob_name, destination_blob_name):
        """Move a file within the same bucket by copying then deleting the original."""
        source_path = self.get_file_fullname(integration_id, source_blob_name)
        dest_path = self.get_file_fullname(integration_id, destination_blob_name)
        for attempt in stamina.retry_context(on=(aiohttp.ClientError, asyncio.TimeoutError),
                                             attempts=5, wait_initial=1.0, wait_max=30, wait_jitter=3.0):
            with attempt:
                await self.storage_client.copy(self.bucket_name, source_path, self.bucket_name, new_name=dest_path)
                await self.storage_client.delete(self.bucket_name, source_path)

    async def stream_file(self, integration_id, blob_name):
        """
        Stream file contents from GCS as an async generator.
        This is memory-efficient for large files.
        """
        target_path = self.get_file_fullname(integration_id, blob_name)
        for attempt in stamina.retry_context(on=(aiohttp.ClientError, asyncio.TimeoutError),
                                             attempts=5, wait_initial=1.0, wait_max=30, wait_jitter=3.0):
            with attempt:
                stream_response = await self.storage_client.download_stream(self.bucket_name, target_path)
                chunk_size = 8192  # 8KB chunks
                while True:
                    chunk = await stream_response.read(chunk_size)
                    if not chunk:  # End of stream
                        break
                    yield chunk

