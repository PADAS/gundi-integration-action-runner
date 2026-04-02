import asyncio
import json
import logging
import os
import csv
import io
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, AsyncGenerator, Generator
from app.services.gundi import send_observations_to_gundi
from app.services.utils import batches_from_generator, find_config_for_action
from app.services.errors import ConfigurationNotFound
from app.services.activity_logger import activity_logger, log_action_activity
from app.services.action_scheduler import crontab_schedule, trigger_action
from gundi_core.schemas.v2.gundi import LogLevel
from app.actions.configurations import ProcessTelemetryDataActionConfiguration, ProcessOrnitelaFileActionConfiguration
from app.services.state import IntegrationStateManager
from app.services.file_storage import CloudFileStorage
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

    try:
        file_storage = CloudFileStorage(
            bucket_name=settings.INFILE_STORAGE_BUCKET,
            root_prefix=action_config.bucket_path
        )

        logger.info(f"Processing file {action_config.file_name} for integration {integration_id}")

        telemetry_data = await _process_csv_file_streaming(file_storage, integration_id, in_progress_path)

        # Transform and send observations
        transformed_data = generate_gundi_observations(telemetry_data, action_config.historical_limit_days)
        observations_sent = 0

        for i, batch in enumerate(batches_from_generator(transformed_data, 200)):
            logger.info(f'Sending observations batch #{i}: {len(batch)} observations.')
            await send_observations_to_gundi(observations=batch, integration_id=integration.id)
            observations_sent += len(batch)

        # Move to archive only after all observations are sent
        archive_path = f"archive/{action_config.file_name}"
        await file_storage.move_file(integration_id, in_progress_path, archive_path)
        logger.info(f"Archived file: {action_config.file_name}")

        message = f"Processed file {action_config.file_name}: extracted {len(telemetry_data)} records, sent {observations_sent} observations"
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
        logger.error(f"Task cancelled while processing file {action_config.file_name} — moving to dead_letter/")
        await _move_to_dead_letter(file_storage, integration_id, in_progress_path, action_config.file_name)
        raise
    except Exception as e:
        logger.exception(f"Error processing file {action_config.file_name}: {str(e)}")
        await _move_to_dead_letter(file_storage, integration_id, in_progress_path, action_config.file_name)
        message = f"Error processing file {action_config.file_name}: {str(e)}"
        await log_action_activity(
            integration_id=str(integration.id),
            action_id="process_ornitela_file",
            title=message,
            level=LogLevel.ERROR
        )
        return {
            "status": "error",
            "file_name": action_config.file_name,
            "error": str(e)
        }


async def _move_to_dead_letter(file_storage, integration_id: str, in_progress_path: str, file_name: str):
    """Move a file from in_progress/ to dead_letter/ after a processing failure."""
    if file_storage is None:
        return
    try:
        dead_letter_path = f"dead_letter/{file_name}"
        await file_storage.move_file(integration_id, in_progress_path, dead_letter_path)
        logger.error(f"Moved to dead_letter/: {file_name}")
    except Exception:
        logger.exception(f"Could not move {file_name} to dead_letter/ — file remains in in_progress/")



@crontab_schedule("*/10 * * * *")  # Regular schedule.
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
        subactions_triggered = 0
        for file_info in new_files:
            try:
                in_progress_path = f"in_progress/{file_info['name']}"
                await file_storage.move_file(integration_id, file_info["name"], in_progress_path)
                logger.info(f"Moved file to in_progress: {file_info['name']}")

                config = ProcessOrnitelaFileActionConfiguration(
                    bucket_path=action_config.bucket_path,
                    file_name=file_info["name"],
                    historical_limit_days=action_config.historical_limit_days,
                    delete_after_archive_days=action_config.delete_after_archive_days,
                )

                await trigger_action(
                    integration_id=integration.id,
                    action_id="process_ornitela_file",
                    config=config
                )

                subactions_triggered += 1

            except Exception as e:
                logger.exception(f"Error triggering action for file {file_info['name']}: {str(e)}")
                continue

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


async def _process_csv_file_streaming(file_storage, integration_id: str, file_name: str) -> List[Dict[str, Any]]:
    """
    Process CSV telemetry data using streaming for memory efficiency.
    This handles both SMS and GPRS files, grouping sensor data with GPS locations.
    """
    telemetry_data = []
    buffer = ""
    csv_reader = None
    detected_encoding = None
    
    # Track current GPS location and sensor readings
    current_gps_location = None
    sensor_readings = []
    in_sensor_sequence = False
    
    try:
        # Stream the file content
        async for chunk in file_storage.stream_file(integration_id, file_name):
            # Detect encoding on first chunk if not already detected
            if detected_encoding is None:
                detected_encoding = _detect_encoding(chunk)
                logger.debug(f"Detected encoding '{detected_encoding}' for file {file_name}")

            try:
                if detected_encoding == 'utf-8':
                    buffer += chunk.decode('utf-8', errors='replace')
                else:
                    buffer += chunk.decode(detected_encoding)
            except Exception as e:
                logger.exception(f"Error decoding chunk in {file_name}: {str(e)}")
                continue
            
            # Process complete lines
            while '\n' in buffer:
                line, buffer = buffer.split('\n', 1)
                
                if csv_reader is None:
                    # Initialize CSV reader with the first line (header)
                    csv_reader = csv.DictReader(io.StringIO(line + '\n'))
                    continue
                
                # Process data row
                if line.strip():  # Skip empty lines
                    try:

                        if line.startswith("device_id"):
                            # Skip rows that look like headers (contain field names as values)
                            continue

                        # Parse CSV row using the same fieldnames as the header
                        row_data = dict(zip(csv_reader.fieldnames, next(csv.reader(io.StringIO(line)))))
                            
                        datatype = row_data.get("datatype", "")
                        
                        # Handle different data types
                        if datatype in ["GPS", "GPSS"]:
                            # GPS location data - create new observation
                            if current_gps_location:
                                # Create observation with GPS location and any sensor data
                                observation = _create_observation(current_gps_location, sensor_readings, file_name)
                                telemetry_data.append(observation)
                            
                            # Start new GPS location
                            current_gps_location = _parse_gps_row(row_data, file_name)
                            sensor_readings = []
                            in_sensor_sequence = False
                            
                        elif datatype.startswith("SEN_"):
                            # Sensor data - add to current readings
                            if datatype.endswith("_START"):
                                in_sensor_sequence = True
                                sensor_readings = []
                            elif datatype.endswith("_END"):
                                in_sensor_sequence = False
                            
                            if in_sensor_sequence and current_gps_location:
                                sensor_reading = _parse_sensor_row(row_data)
                                sensor_readings.append(sensor_reading)
                        
                        # Optional: Process in batches to avoid memory issues
                        if len(telemetry_data) % 1000 == 0:
                            logger.debug(f"Processed {len(telemetry_data)} observations from {file_name}")
                            
                    except (ValueError, KeyError) as e:
                        logger.exception(f"Error parsing CSV row in {file_name}: {str(e)}")
                        continue
        
        # Process any remaining data in buffer
        if buffer.strip() and csv_reader is not None:
            try:
                row_data = dict(zip(csv_reader.fieldnames, next(csv.reader(io.StringIO(buffer)))))
                
                datatype = row_data.get("datatype", "")
                
                if datatype in ["GPS", "GPSS"]:
                    if current_gps_location:
                        observation = _create_observation(current_gps_location, sensor_readings, file_name)
                        telemetry_data.append(observation)
                    current_gps_location = _parse_gps_row(row_data, file_name)
                elif datatype.startswith("SEN_"):
                    if in_sensor_sequence and current_gps_location:
                        sensor_reading = _parse_sensor_row(row_data)
                        sensor_readings.append(sensor_reading)
                elif datatype.startswith("datatype"):
                    logger.warning(f"Skipping a header row. Is this a programming error?")

            except (ValueError, KeyError) as e:
                logger.exception(f"Error parsing final CSV row in {file_name}: {str(e)}")
        
        # Create final observation if we have GPS location (with or without sensor data)
        if current_gps_location:
            observation = _create_observation(current_gps_location, sensor_readings, file_name)
            telemetry_data.append(observation)
        
        logger.info(f"Streamed and processed {len(telemetry_data)} observations from CSV file {file_name}")
        return telemetry_data
        
    except Exception as e:
        logger.exception(f"Error streaming CSV file {file_name}: {str(e)}")
        raise OrnitelaFileProcessingError(f"Error streaming CSV file {file_name}: {str(e)}")
        


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
    Transform grouped observations into individual observations for each sensor record.
    
    This generator function takes observations that have GPS location + sensor readings grouped together
    and yields individual observations for each sensor record, with the GPS location applied
    to each sensor observation. This saves memory by yielding observations one at a time.
    
    Args:
        telemetry_data: List of grouped observations with GPS location and sensor readings
        historical_limit_days: Maximum age of observations to include (in days)
        
    Yields:
        Individual observations - one GPS observation + one per sensor reading
    """
    current_time = datetime.now(timezone.utc)
    cutoff_time = current_time - timedelta(days=historical_limit_days)
    for observation in telemetry_data:

        recorded_at = datetime.strptime(observation["timestamp"], "%Y-%m-%d %H:%M:%S")
        recorded_at = recorded_at.replace(tzinfo=timezone.utc)

        # Skip observations older than historical_limit_days
        if recorded_at < cutoff_time:
            continue

        additional = {
            "datatype": observation["additional"].get("datatype", ""),
            "movement": observation.get("movement", {}),
            "device_status": observation.get("device_status", {}),
            "sensors": observation.get("sensors", {}),
            "environmental": observation.get("environmental", {}),
        }
        # Always create a GPS-only observation first
        gundi_observation = {
            "file": observation["file"],
            # "observation_id": f"{observation['device_id']}_{observation['timestamp'].replace(' ', '_').replace(':', '-')}",
            "recorded_at": recorded_at.isoformat(),
            "source": observation["device_id"],
            "source_name": observation["device_name"],
            'subject_type': 'unassigned',
            "type": "tracking-device",
            "additional": additional,
            "location": observation["location"],
        }
        yield gundi_observation
        
        # Create one observation per sensor reading record
        for sensor_reading in observation.get("sensor_readings", []):
            
            # Calculate recorded_at by adding milliseconds to timestamp
            sensor_timestamp = datetime.strptime(sensor_reading["timestamp"], "%Y-%m-%d %H:%M:%S")
            milliseconds = sensor_reading.get("additional", {}).get("milliseconds", 0)
            recorded_at = sensor_timestamp + timedelta(milliseconds=milliseconds)
            recorded_at = recorded_at.replace(tzinfo=timezone.utc)

            # Skip sensor observations older than historical_limit_days
            if recorded_at < cutoff_time:
                continue
            
            additional = {
                "datatype": sensor_reading["additional"].get("datatype", ""),
                "movement": sensor_reading.get("movement", {}),
                "device_status": sensor_reading.get("device_status", {}),
                "sensors": sensor_reading.get("sensors", {}),
                "environmental": sensor_reading.get("environmental", {}),
            }
            
            sensor_observation = {
                "file": observation["file"],
                # "observation_id": f"{observation['device_id']}_{sensor_reading['timestamp'].replace(' ', '_').replace(':', '-')}_{milliseconds}",
                "recorded_at": recorded_at.isoformat(),  # Precise timestamp with milliseconds
                "source": observation["device_id"],
                "source_name": observation["device_name"],
                'subject_type': 'unassigned',
                "type": "tracking-device",
                "location": observation["location"],  # Apply GPS location 
                "additional": additional,    
            }
            yield sensor_observation


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
