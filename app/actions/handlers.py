import asyncio
import json
import logging
import os
import csv
import io
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, AsyncGenerator, Generator, Optional
from app.services.gundi import send_observations_to_gundi, _get_sensors_api_client
from app.services.utils import batches_from_generator, find_config_for_action
from app.services.errors import ConfigurationNotFound
from app.services.activity_logger import activity_logger, log_action_activity
from app.services.action_scheduler import crontab_schedule, trigger_action
from gundi_core.schemas.v2.gundi import LogLevel
from app.actions.configurations import ProcessTelemetryDataActionConfiguration, ProcessOrnitelaFileActionConfiguration
from app.services.state import IntegrationStateManager
from app.services.file_storage import CloudFileStorage
from app.actions.utils import FileProcessingLockManager
from app import settings


try:
    from google.cloud import storage
    from google.oauth2 import service_account
    from google.cloud.exceptions import NotFound
    GCS_AVAILABLE = True
except ImportError:
    GCS_AVAILABLE = False
    # Create mock classes for when GCS is not available
    class MockStorage:
        class Client:
            def __init__(self, *args, **kwargs):
                pass
            def bucket(self, *args, **kwargs):
                return MockBucket()
    
    class MockBucket:
        def list_blobs(self, *args, **kwargs):
            return []
        def blob(self, *args, **kwargs):
            return MockBlob()
        def copy_blob(self, *args, **kwargs):
            pass
    
    class MockBlob:
        def __init__(self):
            self.name = ""
            self.size = 0
            self.time_created = datetime.utcnow()
            self.content_type = ""
        def download_as_text(self):
            return "{}"
        def delete(self):
            pass
    
    storage = MockStorage()
    service_account = type('MockServiceAccount', (), {
        'Credentials': type('MockCredentials', (), {
            'from_service_account_file': lambda x: None
        })
    })()

logger = logging.getLogger(__name__)

class OrnitelaFileProcessingError(Exception):
    pass


def _safe_float(value, default=None):
    """Safely convert a value to float, returning default if conversion fails."""
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def _safe_int(value, default=None):
    """Safely convert a value to int, returning default if conversion fails."""
    if value is None or value == "":
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def _detect_encoding(chunk: bytes) -> str:
    """Detect the encoding of a chunk of data."""
    # Try common encodings in order of likelihood
    encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
    
    for encoding in encodings:
        try:
            chunk.decode(encoding)
            return encoding
        except UnicodeDecodeError:
            continue
    
    # If all fail, return utf-8 with error replacement
    return 'utf-8'

def get_file_processing_config(integration):

    file_processing_config = find_config_for_action(
        configurations=integration.configurations,
        action_id="process_new_files"
    )
    if not file_processing_config:
        raise ConfigurationNotFound(
            f"File processing settings for integration {str(integration.id)} "
            f"are missing. Please fix the integration setup in the portal."
        )
    return ProcessTelemetryDataActionConfiguration.parse_obj(file_processing_config.data)


@activity_logger()
async def action_process_ornitela_file(integration, action_config: ProcessOrnitelaFileActionConfiguration):
    """
    Process a single Ornitela CSV file that has been moved to the in_progress/ prefix.
    Streams the file from GCS, parses telemetry records, sends observations to Gundi,
    then moves the file to archive/ on success. On any failure the file is moved to
    dead_letter/ so it is visible and does not get retried automatically.
    """
    integration_id = str(integration.id)
    file_storage = None
    in_progress_path = f"in_progress/{action_config.file_name}"
    tag = f"[{action_config.file_name}]"

    try:
        file_storage = CloudFileStorage(
            bucket_name=settings.INFILE_STORAGE_BUCKET,
            root_prefix=action_config.bucket_path
        )

        logger.info(f"{tag} Starting processing for integration {integration_id}")

        telemetry_data = await _process_csv_file(file_storage, integration_id, in_progress_path, action_config.include_sensor_data)

        transformed_data = list(generate_gundi_observations(telemetry_data, action_config.historical_limit_days))
        all_batches = list(batches_from_generator(iter(transformed_data), action_config.batch_size))
        total_batches = len(all_batches)
        observations_sent = 0
        sensors_client = await _get_sensors_api_client(integration_id=str(integration.id))

        for i, batch in enumerate(all_batches):
            await send_observations_to_gundi(observations=batch, sensors_api_client=sensors_client, integration_id=integration.id)
            observations_sent += len(batch)

        # Move to archive only after all observations are sent
        archive_path = f"archive/{action_config.file_name}"
        await file_storage.move_file(integration_id, in_progress_path, archive_path)
        logger.info(f"{tag} Archived successfully")

        if action_config.source_file:
            await _trigger_next_chunk(file_storage, integration_id, action_config, integration)

        message = f"{tag} Processed: extracted {len(telemetry_data)} records, sent {observations_sent} observations"
        logger.info(message)
        await log_action_activity(
            integration_id=integration_id,
            action_id="process_ornitela_file",
            title=message,
            level=LogLevel.INFO
        )

        return {
            "status": "success",
            "file_name": action_config.file_name,
            "telemetry_records": len(telemetry_data),
            "observations_sent": observations_sent,
        }

    except asyncio.CancelledError:
        logger.error(f"{tag} Timed out — moving to dead_letter/")
        await _move_to_dead_letter(file_storage, integration_id, in_progress_path, action_config.file_name)
        await log_action_activity(
            integration_id=integration_id,
            action_id="process_ornitela_file",
            title=f"{tag} Processing exceeded the {settings.MAX_ACTION_EXECUTION_TIME // 60}-minute limit and was sent to the dead letter queue.",
            level=LogLevel.ERROR
        )
        raise
    except Exception as e:
        if getattr(e, "status", None) == 404:
            logger.info(f"{tag} Not found in in_progress/ — already processed by a previous delivery, skipping")
            return {"status": "skipped", "file_name": action_config.file_name, "reason": "already_processed"}
        error_detail = str(e) or type(e).__name__
        logger.exception(f"{tag} Error: {error_detail}")
        await _move_to_dead_letter(file_storage, integration_id, in_progress_path, action_config.file_name)
        message = f"{tag} Error: {error_detail}"
        await log_action_activity(
            integration_id=str(integration.id),
            action_id="process_ornitela_file",
            title=message,
            level=LogLevel.ERROR
        )
        return {
            "status": "error",
            "file_name": action_config.file_name,
            "error": error_detail
        }
    finally:
        if file_storage:
            await file_storage.close()


async def _create_chunk(file_storage, integration_id: str, file_name: str, chunk_size: int) -> Optional[str]:
    """
    Download file from root, carve off the first chunk_size data rows, upload the chunk
    to in_progress/, and write the remaining rows back to root (or delete if empty).
    Returns the chunk filename, or None if the file had no data rows.
    """
    raw_bytes = await file_storage.download_bytes(integration_id, file_name)
    encoding = _detect_encoding(raw_bytes)
    content = raw_bytes.decode('utf-8', errors='replace') if encoding == 'utf-8' else raw_bytes.decode(encoding)

    reader = csv.reader(io.StringIO(content))
    rows = list(reader)

    if len(rows) <= 1:
        await file_storage.delete_file(integration_id, file_name)
        logger.info(f"File {file_name} is empty, deleted from root")
        return None

    header = rows[0]
    data_rows = rows[1:]
    chunk_rows = data_rows[:chunk_size]
    remaining_rows = data_rows[chunk_size:]

    # Build chunk bytes
    chunk_buffer = io.StringIO()
    csv.writer(chunk_buffer).writerows([header] + chunk_rows)
    chunk_bytes = chunk_buffer.getvalue().encode('utf-8')

    # Unique chunk filename
    timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    base_name = file_name.rsplit('.', 1)[0]
    chunk_name = f"{base_name}_chunk_{timestamp}.csv"

    await file_storage.upload_bytes(integration_id, f"in_progress/{chunk_name}", chunk_bytes)

    if remaining_rows:
        remaining_buffer = io.StringIO()
        csv.writer(remaining_buffer).writerows([header] + remaining_rows)
        remaining_bytes = remaining_buffer.getvalue().encode('utf-8')
        await file_storage.upload_bytes(integration_id, file_name, remaining_bytes)
        logger.info(f"Chunk created: {chunk_name} ({len(chunk_rows)} rows), {len(remaining_rows)} rows remaining in {file_name}")
    else:
        await file_storage.delete_file(integration_id, file_name)
        logger.info(f"Chunk created: {chunk_name} ({len(chunk_rows)} rows), root file {file_name} deleted")

    return chunk_name


async def _move_to_dead_letter(file_storage, integration_id: str, in_progress_path: str, file_name: str):
    """Move a file from in_progress/ to dead_letter/ after a processing failure."""
    if file_storage is None:
        return
    logger.error(f"Attempting to move {file_name} to dead_letter/")
    try:
        dead_letter_path = f"dead_letter/{file_name}"
        await file_storage.move_file(integration_id, in_progress_path, dead_letter_path)
        logger.error(f"Successfully moved {file_name} to dead_letter/")
    except Exception:
        logger.exception(f"Could not move {file_name} to dead_letter/ — file remains in in_progress/")



async def _trigger_next_chunk(file_storage, integration_id: str, action_config: ProcessOrnitelaFileActionConfiguration, integration) -> None:
    """
    After successfully archiving a chunk, immediately carve and trigger the next chunk
    from the same source file, bypassing the 5-minute cron wait.
    If the source file is locked (cron beat us to it) or exhausted, exits silently.
    """
    source_file = action_config.source_file
    lock_manager = FileProcessingLockManager()

    acquired = await lock_manager.acquire_lock(integration_id, source_file)
    if not acquired:
        logger.info(f"Source file {source_file} is locked — cron will handle the next chunk")
        return

    try:
        chunk_name = await _create_chunk(
            file_storage, integration_id, source_file, action_config.chunk_size
        )
        if chunk_name is None:
            logger.info(f"Source file {source_file} exhausted, chain complete")
            return

        config = ProcessOrnitelaFileActionConfiguration(
            bucket_path=action_config.bucket_path,
            file_name=chunk_name,
            source_file=source_file,
            chunk_size=action_config.chunk_size,
            historical_limit_days=action_config.historical_limit_days,
            delete_after_archive_days=action_config.delete_after_archive_days,
            batch_size=action_config.batch_size,
            include_sensor_data=action_config.include_sensor_data,
        )
        await trigger_action(
            integration_id=integration.id,
            action_id="process_ornitela_file",
            config=config,
        )
        logger.info(f"Next chunk triggered for {source_file}: {chunk_name}")
    except Exception as e:
        if getattr(e, "status", None) == 404:
            logger.info(f"Source file {source_file} no longer exists — chain complete")
        else:
            logger.exception(f"Error triggering next chunk for {source_file}: {str(e)}")
    finally:
        await lock_manager.release_lock(integration_id, source_file)


@crontab_schedule("*/5 * * * *")  # Regular schedule.
async def action_process_new_files(integration, action_config: ProcessTelemetryDataActionConfiguration):
    """
    Action handler that processes new telemetry data files from Google Cloud Storage.
    
    This handler:
    1. Lists files in the GCS bucket
    2. Identifies new files that haven't been processed
    3. Triggers individual file processing actions
    4. Archives and deletion are handled by the individual file processing actions
    """
    
    state_manager = IntegrationStateManager()
    integration_id = str(integration.id)
    action_id = "process_new_files"
    file_storage = None

    try:
        # Initialize CloudFileStorage service
        file_storage = CloudFileStorage(
            bucket_name=settings.INFILE_STORAGE_BUCKET,
            root_prefix=action_config.bucket_path
        )
        
        # List all files in the bucket path
        file_list = await file_storage.list_files(integration_id)

        new_files = []
        archived_files_to_delete = []
        current_time = datetime.now(timezone.utc)

        for file_name in file_list:
            # Skip directories
            if file_name.endswith("/"):
                continue

            # Skip files already in progress, archived, or in dead_letter — only process root files
            if file_name.startswith("in_progress/") or file_name.startswith("archive/") or file_name.startswith("dead_letter/"):
                # Check archived files for deletion
                if file_name.startswith("archive/"):
                    try:
                        metadata = await file_storage.get_file_metadata(integration_id, file_name)
                        file_created = metadata.timeCreated or current_time
                        if file_created.tzinfo is None:
                            file_created = file_created.replace(tzinfo=timezone.utc)
                        days_since_created = (current_time - file_created).days
                        if days_since_created >= action_config.delete_after_archive_days:
                            archived_files_to_delete.append(file_name)
                    except Exception as e:
                        logger.warning(f"Could not get metadata for archived file {file_name}: {str(e)}")
                continue

            # Get file metadata
            try:
                metadata = await file_storage.get_file_metadata(integration_id, file_name)
                file_modified = metadata.updated or current_time
                file_size = metadata.size or 0
                content_type = metadata.contentType or "application/octet-stream"
            except Exception as e:
                logger.warning(f"Could not get metadata for file {file_name}: {str(e)}")
                file_modified = current_time
                file_size = 0
                content_type = "application/octet-stream"

            new_files.append({
                "name": file_name,
                "size": file_size,
                "created": file_modified.isoformat(),
                "content_type": content_type
            })
        
        # Move each file to in_progress and trigger processing
        if action_config.process_most_recent_first:
            new_files.sort(key=lambda f: f["created"], reverse=True)
        max_files = action_config.max_files_per_run
        logger.info(f"Found {len(new_files)} new files, processing {min(len(new_files), max_files)}")
        subactions_triggered = 0
        lock_manager = FileProcessingLockManager()
        for file_info in new_files[:max_files]:
            file_name = file_info["name"]
            acquired = await lock_manager.acquire_lock(integration_id, file_name)
            if not acquired:
                logger.info(f"File {file_name} is locked by another process, skipping")
                continue
            try:
                chunk_name = await _create_chunk(
                    file_storage, integration_id, file_name, action_config.chunk_size
                )
                if chunk_name is None:
                    continue

                config = ProcessOrnitelaFileActionConfiguration(
                    bucket_path=action_config.bucket_path,
                    file_name=chunk_name,
                    source_file=file_name,
                    chunk_size=action_config.chunk_size,
                    historical_limit_days=action_config.historical_limit_days,
                    delete_after_archive_days=action_config.delete_after_archive_days,
                    batch_size=action_config.batch_size,
                    include_sensor_data=action_config.include_sensor_data,
                )

                await trigger_action(
                    integration_id=integration.id,
                    action_id="process_ornitela_file",
                    config=config
                )
                logger.info(f"Sub-action triggered for chunk: {chunk_name}")
                subactions_triggered += 1
            except Exception as e:
                logger.exception(f"Error triggering action for file {file_name}: {str(e)}")
            finally:
                await lock_manager.release_lock(integration_id, file_name)

        # Delete old archived files
        deleted_count = 0
        for file_name in archived_files_to_delete:
            try:
                await file_storage.delete_file(integration_id, file_name)
                deleted_count += 1
                logger.info(f"Deleted old archived file: {file_name}")
            except Exception as e:
                logger.error(f"Error deleting archived file {file_name}: {str(e)}")

        # Update state
        await state_manager.set_state(
            integration_id,
            action_id,
            {
                "last_run": current_time.isoformat(),
                "last_subactions_triggered": subactions_triggered,
                "last_deleted_count": deleted_count,
            }
        )

        return {
            "status": "success",
            "new_files_found": len(new_files),
            "subactions_triggered": subactions_triggered,
            "files_deleted": deleted_count,
        }
        
    except Exception as e:
        logger.exception(f"Error in action_process_new_files: {str(e)}")
        return {
            "status": "error",
            "error": str(e)
        }
    finally:
        if file_storage:
            await file_storage.close()


async def _process_csv_file(file_storage, integration_id: str, file_name: str, include_sensor_data: bool = True) -> List[Dict[str, Any]]:
    """
    Process CSV telemetry data. Downloads the full file into memory, then parses it.
    Each GPS row becomes one observation with its real location.
    Each SEN_ row becomes one observation with location (0, 0).
    """
    telemetry_data = []

    try:
        raw_bytes = await file_storage.download_bytes(integration_id, file_name)
        encoding = _detect_encoding(raw_bytes)
        logger.debug(f"Detected encoding '{encoding}' for file {file_name}")

        if encoding == 'utf-8':
            content = raw_bytes.decode('utf-8', errors='replace')
        else:
            content = raw_bytes.decode(encoding)

        reader = csv.DictReader(io.StringIO(content))
        rows = list(reader)
        row_count = len(rows)
        logger.info(f"Processing {file_name}: {row_count} rows")
        await log_action_activity(
            integration_id=integration_id,
            action_id="process_ornitela_file",
            title=f"File {file_name}: {row_count} rows",
            level=LogLevel.INFO
        )

        for row_data in rows:
            if row_data.get("device_id") == "device_id":
                continue

            datatype = row_data.get("datatype", "")

            try:
                if datatype in ["GPS", "GPSS"]:
                    telemetry_data.append(_create_observation(_parse_gps_row(row_data, file_name), [], file_name))
                elif include_sensor_data and datatype.startswith("SEN_"):
                    telemetry_data.append(_parse_sensor_row_as_observation(row_data, file_name))
            except (ValueError, KeyError) as e:
                logger.exception(f"Error parsing CSV row in {file_name}: {str(e)}")
                continue

        return telemetry_data

    except Exception as e:
        if getattr(e, "status", None) == 404:
            raise
        logger.exception(f"Error processing CSV file {file_name}: {str(e)}")
        raise OrnitelaFileProcessingError(f"Error processing CSV file {file_name}: {str(e)}")
        


def _parse_gps_row(row_data: Dict[str, Any], file_name: str) -> Dict[str, Any]:
    """Parse a GPS row (datatype = GPSS) into a location object."""
    return {
        "file": file_name,
        "timestamp": row_data.get("UTC_datetime", ""),
        "device_id": row_data.get("device_id", ""),
        "device_name": row_data.get("device_name", ""),
        "location": {
            "lat": _safe_float(row_data.get("Latitude")),
            "lon": _safe_float(row_data.get("Longitude")),
            "altitude": _safe_float(row_data.get("MSL_altitude_m"))
        },
        "movement": {
            "speed": _safe_float(row_data.get("speed_km/h")),
            "direction": _safe_float(row_data.get("direction_deg"))
        },
        "device_status": {
            "battery_voltage": _safe_float(row_data.get("U_bat_mV")),
            "battery_soc": _safe_float(row_data.get("bat_soc_pct")),
            "solar_current": _safe_float(row_data.get("solar_I_mA")),
            "satellite_count": _safe_int(row_data.get("satcount")),
            "hdop": _safe_float(row_data.get("hdop"))
        },
        "additional": {
            "datatype": row_data.get("datatype", ""),
            "utc_date": row_data.get("UTC_date", ""),
            "utc_time": row_data.get("UTC_time", ""),
            "utc_timestamp": row_data.get("UTC_timestamp", ""),
            "milliseconds": _safe_int(row_data.get("milliseconds"))
        },
        "sensors": {
            "magnetometer": {
                "x": _safe_float(row_data.get("mag_x")),
                "y": _safe_float(row_data.get("mag_y")),
                "z": _safe_float(row_data.get("mag_z"))
            },
            "accelerometer": {
                "x": _safe_float(row_data.get("acc_x")),
                "y": _safe_float(row_data.get("acc_y")),
                "z": _safe_float(row_data.get("acc_z"))
            }
        },
    }


def _parse_sensor_row(row_data: Dict[str, Any]) -> Dict[str, Any]:
    """Parse a sensor row into a sensor reading object."""
    return {
        "timestamp": row_data.get("UTC_datetime", ""),
        "datatype": row_data.get("datatype", ""),
        "environmental": {
            "temperature": _safe_float(row_data.get("int_temperature_C")),
            "external_temperature": _safe_float(row_data.get("ext_temperature_C")),
            "light": _safe_float(row_data.get("light")),
            "altimeter": _safe_float(row_data.get("altimeter_m")),
            "depth": _safe_float(row_data.get("depth_m")),
            "conductivity": _safe_float(row_data.get("conductivity_mS/cm"))
        },
        "sensors": {
            "magnetometer": {
                "x": _safe_float(row_data.get("mag_x")),
                "y": _safe_float(row_data.get("mag_y")),
                "z": _safe_float(row_data.get("mag_z"))
            },
            "accelerometer": {
                "x": _safe_float(row_data.get("acc_x")),
                "y": _safe_float(row_data.get("acc_y")),
                "z": _safe_float(row_data.get("acc_z"))
            }
        },
        "additional": {
            "utc_date": row_data.get("UTC_date", ""),
            "utc_time": row_data.get("UTC_time", ""),
            "utc_timestamp": row_data.get("UTC_timestamp", ""),
            "milliseconds": _safe_int(row_data.get("milliseconds"))
        }
    }


def _make_observation_id(row_data: Dict[str, Any]) -> str:
    base = (
        f"{row_data.get('device_id', '')}_"
        f"{(row_data.get('UTC_datetime') or '').replace(' ', '_').replace(':', '-')}"
    )
    ms = row_data.get("milliseconds")
    try:
        if ms not in (None, ""):
            return f"{base}-{int(ms):03d}"
    except (ValueError, TypeError):
        pass
    return base


def _parse_sensor_row_as_observation(row_data: Dict[str, Any], file_name: str) -> Dict[str, Any]:
    """Parse a sensor row into a standalone observation with location (0, 0)."""
    sensor = _parse_sensor_row(row_data)
    return {
        "file": file_name,
        "observation_id": _make_observation_id(row_data),
        "timestamp": row_data.get("UTC_datetime", ""),
        "device_id": row_data.get("device_id", ""),
        "device_name": row_data.get("device_name", ""),
        "location": {"lat": 0, "lon": 0, "altitude": None},
        "movement": {},
        "device_status": {},
        "sensor_readings": [],
        "sensor_count": 0,
        "sensors": sensor["sensors"],
        "environmental": sensor["environmental"],
        "additional": {
            "datatype": row_data.get("datatype", ""),
            "utc_date": row_data.get("UTC_date", ""),
            "utc_time": row_data.get("UTC_time", ""),
            "utc_timestamp": row_data.get("UTC_timestamp", ""),
            "milliseconds": _safe_int(row_data.get("milliseconds")),
        },
    }


def _create_observation(gps_location: Dict[str, Any], sensor_readings: List[Dict[str, Any]], file_name: str) -> Dict[str, Any]:
    """Create a single observation combining GPS location with sensor readings."""
    return {
        "file": file_name,
        "observation_id": f"{gps_location['device_id']}_{gps_location['timestamp'].replace(' ', '_').replace(':', '-')}",
        "timestamp": gps_location["timestamp"],
        "device_id": gps_location["device_id"],
        "device_name": gps_location["device_name"],
        "location": gps_location["location"],
        "movement": gps_location["movement"],
        "device_status": gps_location["device_status"],
        "sensor_readings": sensor_readings,
        "sensor_count": len(sensor_readings),
        "sensors": gps_location["sensors"],
        "additional": gps_location["additional"]
    }


def generate_gundi_observations(telemetry_data: List[Dict[str, Any]], historical_limit_days: int = 30) -> Generator[Dict[str, Any], None, None]:
    """
    Filters and transforms parsed telemetry rows into Gundi observation dicts.
    GPS rows carry their real location; sensor rows carry location (0, 0).
    Rows older than historical_limit_days are skipped.
    Millisecond offsets from the additional field are applied to recorded_at.

    Args:
        telemetry_data: List of observations produced by _process_csv_file
        historical_limit_days: Maximum age of observations to include (in days)

    Yields:
        One Gundi observation dict per input row
    """
    current_time = datetime.now(timezone.utc)
    cutoff_time = current_time - timedelta(days=historical_limit_days)
    for observation in telemetry_data:

        recorded_at = datetime.strptime(observation["timestamp"], "%Y-%m-%d %H:%M:%S")
        milliseconds = observation.get("additional", {}).get("milliseconds") or 0
        recorded_at = recorded_at + timedelta(milliseconds=milliseconds)
        recorded_at = recorded_at.replace(tzinfo=timezone.utc)

        if recorded_at < cutoff_time:
            continue

        additional = {
            "datatype": observation["additional"].get("datatype", ""),
            "movement": observation.get("movement", {}),
            "device_status": observation.get("device_status", {}),
            "sensors": observation.get("sensors", {}),
            "environmental": observation.get("environmental", {}),
        }
        yield {
            "file": observation["file"],
            "recorded_at": recorded_at.isoformat(),
            "source": observation["device_id"],
            "source_name": observation["device_name"],
            "subject_type": "unassigned",
            "type": "tracking-device",
            "location": observation["location"],
            "additional": additional,
        }


def _process_telemetry_file(content: str, file_name: str) -> List[Dict[str, Any]]:
    """
    Process telemetry data from a file.
    This is a placeholder implementation - customize based on your telemetry data format.
    """
    try:
        # Assuming JSON format for telemetry data
        # Adjust this based on your actual data format (CSV, JSON, etc.)
        if file_name.endswith('.json'):
            data = json.loads(content)
            if isinstance(data, list):
                return data
            else:
                return [data]
        else:
            # For other formats, you might need to parse CSV, XML, etc.
            # This is a placeholder - implement based on your data format
            return [{"raw_data": content, "file": file_name}]
            
    except json.JSONDecodeError:
        logger.warning(f"Could not parse JSON from file {file_name}")
        return [{"raw_data": content, "file": file_name, "parse_error": "invalid_json"}]
    except Exception as e:
        logger.error(f"Error processing telemetry file {file_name}: {str(e)}")
        return [{"raw_data": content, "file": file_name, "error": str(e)}]
