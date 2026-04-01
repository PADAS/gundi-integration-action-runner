import pytest
from unittest.mock import Mock, patch, AsyncMock, call
from datetime import datetime, timedelta, timezone

from app.actions.configurations import ProcessTelemetryDataActionConfiguration, ProcessOrnitelaFileActionConfiguration
from app.actions.handlers import (
    action_process_new_files,
    action_process_ornitela_file,
    _process_telemetry_file,
    _process_csv_file_streaming,
    generate_gundi_observations,
)
from app.services.file_storage import FileMetadata


@pytest.fixture(autouse=True)
def mock_publish_event():
    """Prevent all tests from making real GCP PubSub calls."""
    with patch("app.services.activity_logger.publish_event", new_callable=AsyncMock):
        yield


HEADER = "device_id,device_name,UTC_datetime,UTC_date,UTC_time,datatype,satcount,U_bat_mV,bat_soc_pct,solar_I_mA,hdop,Latitude,Longitude,MSL_altitude_m,Reserved,speed_km/h,direction_deg,int_temperature_C,mag_x,mag_y,mag_z,acc_x,acc_y,acc_z,UTC_timestamp,milliseconds,light,altimeter_m,depth_m,conductivity_mS/cm,ext_temperature_C\n"
GPS_ROW = "226976,GF_BAR_2022_ADU_W_IMA_Gauele,2025-01-18 09:10:11,2025-01-18,09:10:11,GPSS,3,3702,8,,,44.394531250000000,5.370184421539307,,,,,,,,,,,247,2025-01-18 09:10:11.0,0,,,,,\n"
GPS_ROW_2 = "226976,GF_BAR_2022_ADU_W_IMA_Gauele,2025-01-18 10:10:11,2025-01-18,10:10:11,GPSS,3,3702,8,,,44.395652770996094,5.367559432983398,,,,,,,,,,,247,2025-01-18 10:10:11.0,0,,,,,\n"
SEN_START = "226976,GF_BAR_2022_ADU_W_IMA_Gauele,2025-01-18 09:10:12,2025-01-18,09:10:12,SEN_ALL_20Hz_START,3,3702,8,,,44.394531250000000,5.370184421539307,,,,,,,,,,,247,2025-01-18 09:10:12.0,0,,,,,\n"
SEN_ROW = "226976,GF_BAR_2022_ADU_W_IMA_Gauele,2025-01-18 09:10:13,2025-01-18,09:10:13,SEN_ALL_20Hz,3,3702,8,,,44.394531250000000,5.370184421539307,,,,,,,,,,,247,2025-01-18 09:10:13.0,0,,,,,\n"
SEN_END = "226976,GF_BAR_2022_ADU_W_IMA_Gauele,2025-01-18 09:10:14,2025-01-18,09:10:14,SEN_ALL_20Hz_END,3,3702,8,,,44.394531250000000,5.370184421539307,,,,,,,,,,,247,2025-01-18 09:10:14.0,0,,,,,\n"


@pytest.fixture
def mock_integration():
    integration = Mock()
    integration.id = "test-integration-123"
    return integration


@pytest.fixture
def action_config():
    return ProcessTelemetryDataActionConfiguration(
        bucket_path="telemetry-data",
        delete_after_archive_days=90,
    )


@pytest.fixture
def file_action_config():
    return ProcessOrnitelaFileActionConfiguration(
        bucket_path="telemetry-data",
        file_name="bird001_20240101.csv",
        delete_after_archive_days=90,
    )


def make_file_storage_mock(files=None, metadata=None, move_file=None):
    """Helper to build a CloudFileStorage mock with sensible defaults."""
    mock = Mock()

    async def default_list(*a, **kw):
        return files or []

    async def default_metadata(*a, **kw):
        return metadata or FileMetadata(
            timeCreated=datetime.now(timezone.utc) - timedelta(hours=1),
            updated=datetime.now(timezone.utc) - timedelta(hours=1),
            size=1024,
            contentType="text/csv",
        )

    async def default_move(*a, **kw):
        return None

    async def default_delete(*a, **kw):
        return None

    async def default_stream(*a, **kw):
        now = datetime.now(timezone.utc)
        recent_row = (
            f"226976,TestBird,{now.strftime('%Y-%m-%d %H:%M:%S')},"
            f"{now.strftime('%Y-%m-%d')},{now.strftime('%H:%M:%S')},"
            f"GPSS,3,3702,8,,,44.394531250000000,5.370184421539307,"
            f",,,,,,,,,,247,{now.strftime('%Y-%m-%d %H:%M:%S')}.0,0,,,,,\n"
        )
        for chunk in [HEADER, recent_row]:
            yield chunk.encode("utf-8")

    mock.list_files = Mock(side_effect=default_list)
    mock.get_file_metadata = Mock(side_effect=default_metadata)
    mock.move_file = Mock(side_effect=move_file or default_move)
    mock.delete_file = Mock(side_effect=default_delete)
    mock.stream_file = default_stream
    return mock


# ---------------------------------------------------------------------------
# action_process_new_files
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@patch("app.actions.handlers.trigger_action")
@patch("app.actions.handlers.CloudFileStorage")
@patch("app.actions.handlers.IntegrationStateManager")
async def test_process_new_files_moves_to_archive_before_trigger(
    mock_state_manager, mock_file_storage_cls, mock_trigger_action, mock_integration, action_config
):
    """Files must be moved to archive BEFORE the sub-action is triggered."""
    call_order = []

    storage = make_file_storage_mock(files=["bird001.csv"])

    async def record_move(*a, **kw):
        call_order.append("move")

    async def record_trigger(*a, **kw):
        call_order.append("trigger")

    storage.move_file = Mock(side_effect=record_move)
    mock_file_storage_cls.return_value = storage
    mock_trigger_action.side_effect = record_trigger

    mock_state_manager.return_value.get_state = AsyncMock(return_value={})
    mock_state_manager.return_value.set_state = AsyncMock()

    result = await action_process_new_files(mock_integration, action_config)

    assert result["status"] == "success"
    assert result["new_files_found"] == 1
    assert result["subactions_triggered"] == 1
    assert call_order == ["move", "trigger"], "move must happen before trigger"


@pytest.mark.asyncio
@patch("app.actions.handlers.trigger_action")
@patch("app.actions.handlers.CloudFileStorage")
@patch("app.actions.handlers.IntegrationStateManager")
async def test_process_new_files_skips_archive_folder(
    mock_state_manager, mock_file_storage_cls, mock_trigger_action, mock_integration, action_config
):
    """Files already in archive/ must not be triggered for processing."""
    storage = make_file_storage_mock(files=["archive/bird001.csv", "bird002.csv"])
    mock_file_storage_cls.return_value = storage
    mock_trigger_action.side_effect = AsyncMock()
    mock_state_manager.return_value.get_state = AsyncMock(return_value={})
    mock_state_manager.return_value.set_state = AsyncMock()

    result = await action_process_new_files(mock_integration, action_config)

    assert result["new_files_found"] == 1
    assert result["subactions_triggered"] == 1
    # move_file should only be called for bird002.csv, not the archived one
    storage.move_file.assert_called_once()
    assert "bird002.csv" in storage.move_file.call_args[0]


@pytest.mark.asyncio
@patch("app.actions.handlers.trigger_action")
@patch("app.actions.handlers.CloudFileStorage")
@patch("app.actions.handlers.IntegrationStateManager")
async def test_process_new_files_deletes_old_archived_files(
    mock_state_manager, mock_file_storage_cls, mock_trigger_action, mock_integration, action_config
):
    """Archived files older than delete_after_archive_days must be deleted."""
    very_old = datetime.now(timezone.utc) - timedelta(days=95)
    recent = datetime.now(timezone.utc) - timedelta(days=10)

    async def metadata_by_name(integration_id, file_name):
        if "old" in file_name:
            return FileMetadata(timeCreated=very_old, updated=very_old, size=1024)
        return FileMetadata(timeCreated=recent, updated=recent, size=1024)

    storage = make_file_storage_mock(
        files=["archive/old_file.csv", "archive/recent_file.csv"]
    )
    storage.get_file_metadata = Mock(side_effect=metadata_by_name)
    mock_file_storage_cls.return_value = storage
    mock_trigger_action.side_effect = AsyncMock()
    mock_state_manager.return_value.get_state = AsyncMock(return_value={})
    mock_state_manager.return_value.set_state = AsyncMock()

    result = await action_process_new_files(mock_integration, action_config)

    assert result["files_deleted"] == 1
    storage.delete_file.assert_called_once()
    assert "old_file.csv" in storage.delete_file.call_args[0][1]


@pytest.mark.asyncio
@patch("app.actions.handlers.CloudFileStorage")
@patch("app.actions.handlers.IntegrationStateManager")
async def test_process_new_files_storage_error(
    mock_state_manager, mock_file_storage_cls, mock_integration, action_config
):
    """Storage errors must be returned gracefully."""
    mock_file_storage_cls.side_effect = Exception("GCS connection failed")

    result = await action_process_new_files(mock_integration, action_config)

    assert result["status"] == "error"
    assert "GCS connection failed" in result["error"]


# ---------------------------------------------------------------------------
# action_process_ornitela_file
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@patch("app.actions.handlers.send_observations_to_gundi")
@patch("app.actions.handlers.CloudFileStorage")
async def test_process_ornitela_file_reads_from_archive(
    mock_file_storage_cls, mock_send, mock_integration, file_action_config
):
    """action_process_ornitela_file must stream from archive/<file_name>."""
    storage = make_file_storage_mock()
    mock_file_storage_cls.return_value = storage
    mock_send.return_value = None

    await action_process_ornitela_file(mock_integration, file_action_config)

    assert mock_file_storage_cls.called


@pytest.mark.asyncio
@patch("app.actions.handlers.send_observations_to_gundi")
@patch("app.actions.handlers.CloudFileStorage")
async def test_process_ornitela_file_sends_observations(
    mock_file_storage_cls, mock_send, mock_integration, file_action_config
):
    """Observations parsed from the CSV must be sent to Gundi."""
    storage = make_file_storage_mock()
    mock_file_storage_cls.return_value = storage
    mock_send.return_value = None

    result = await action_process_ornitela_file(mock_integration, file_action_config)

    assert result["status"] == "success"
    assert result["observations_sent"] > 0
    mock_send.assert_called()


@pytest.mark.asyncio
@patch("app.actions.handlers.send_observations_to_gundi")
@patch("app.actions.handlers.CloudFileStorage")
async def test_process_ornitela_file_error_handling(
    mock_file_storage_cls, mock_send, mock_integration, file_action_config
):
    """Errors during processing must be caught and returned."""
    mock_file_storage_cls.side_effect = Exception("storage unavailable")

    result = await action_process_ornitela_file(mock_integration, file_action_config)

    assert result["status"] == "error"
    assert "storage unavailable" in result["error"]


# ---------------------------------------------------------------------------
# _process_csv_file_streaming
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_process_csv_file_streaming_gps_and_sensors():
    """GPS row with a sensor burst should produce 1 grouped observation."""
    mock_storage = Mock()

    async def mock_stream(*a, **kw):
        for chunk in [HEADER, GPS_ROW, SEN_START, SEN_ROW, SEN_END]:
            yield chunk.encode("utf-8")

    mock_storage.stream_file = mock_stream

    result = await _process_csv_file_streaming(mock_storage, "test-integration", "test.csv")

    assert len(result) == 1
    obs = result[0]
    assert obs["device_id"] == "226976"
    assert obs["timestamp"] == "2025-01-18 09:10:11"
    assert obs["location"]["lat"] == 44.394531250000000
    assert obs["location"]["lon"] == 5.370184421539307
    assert obs["device_status"]["battery_voltage"] == 3702.0
    assert obs["sensor_count"] == 2  # SEN_START and SEN_ROW
    assert obs["sensor_readings"][0]["datatype"] == "SEN_ALL_20Hz_START"
    assert obs["sensor_readings"][1]["datatype"] == "SEN_ALL_20Hz"


@pytest.mark.asyncio
async def test_process_csv_file_streaming_gps_only():
    """Multiple GPS rows with no sensors should each produce one observation."""
    mock_storage = Mock()

    async def mock_stream(*a, **kw):
        for chunk in [HEADER, GPS_ROW, GPS_ROW_2]:
            yield chunk.encode("utf-8")

    mock_storage.stream_file = mock_stream

    result = await _process_csv_file_streaming(mock_storage, "test-integration", "test.csv")

    assert len(result) == 2
    assert result[0]["timestamp"] == "2025-01-18 09:10:11"
    assert result[1]["timestamp"] == "2025-01-18 10:10:11"
    assert result[0]["sensor_count"] == 0
    assert result[1]["sensor_count"] == 0


# ---------------------------------------------------------------------------
# generate_gundi_observations
# ---------------------------------------------------------------------------

def test_generate_gundi_observations_yields_gps_observation():
    """GPS observation must include location and additional fields."""
    now = datetime.now(timezone.utc)
    telemetry = [
        {
            "file": "test.csv",
            "observation_id": "226976_2025-01-18_09:10:11",
            "timestamp": now.strftime("%Y-%m-%d %H:%M:%S"),
            "device_id": "226976",
            "device_name": "TestBird",
            "location": {"lat": 44.39, "lon": 5.37, "altitude": None},
            "movement": {"speed": 10.0, "direction": 90.0},
            "device_status": {"battery_voltage": 3700.0, "battery_soc": 80.0, "solar_current": None, "satellite_count": 6, "hdop": 1.2},
            "sensor_readings": [],
            "sensor_count": 0,
            "sensors": {"magnetometer": {"x": None, "y": None, "z": None}, "accelerometer": {"x": None, "y": None, "z": None}},
            "additional": {"datatype": "GPSS", "utc_date": "", "utc_time": "", "utc_timestamp": "", "milliseconds": 0},
        }
    ]

    results = list(generate_gundi_observations(telemetry, historical_limit_days=30))

    assert len(results) == 1
    obs = results[0]
    assert obs["source"] == "226976"
    assert obs["source_name"] == "TestBird"
    assert obs["location"]["lat"] == 44.39
    assert obs["type"] == "tracking-device"


def test_generate_gundi_observations_yields_sensor_observations():
    """Each sensor reading must produce a separate observation."""
    now = datetime.now(timezone.utc)
    telemetry = [
        {
            "file": "test.csv",
            "observation_id": "226976_ts",
            "timestamp": now.strftime("%Y-%m-%d %H:%M:%S"),
            "device_id": "226976",
            "device_name": "TestBird",
            "location": {"lat": 44.39, "lon": 5.37, "altitude": None},
            "movement": {},
            "device_status": {},
            "sensor_readings": [
                {
                    "timestamp": now.strftime("%Y-%m-%d %H:%M:%S"),
                    "datatype": "SEN_ALL_20Hz",
                    "environmental": {},
                    "sensors": {},
                    "additional": {"datatype": "SEN_ALL_20Hz", "utc_date": "", "utc_time": "", "utc_timestamp": "", "milliseconds": 50},
                },
                {
                    "timestamp": now.strftime("%Y-%m-%d %H:%M:%S"),
                    "datatype": "SEN_ALL_20Hz",
                    "environmental": {},
                    "sensors": {},
                    "additional": {"datatype": "SEN_ALL_20Hz", "utc_date": "", "utc_time": "", "utc_timestamp": "", "milliseconds": 100},
                },
            ],
            "sensor_count": 2,
            "sensors": {},
            "additional": {"datatype": "GPSS", "utc_date": "", "utc_time": "", "utc_timestamp": "", "milliseconds": 0},
        }
    ]

    results = list(generate_gundi_observations(telemetry, historical_limit_days=30))

    assert len(results) == 3  # 1 GPS + 2 sensor


def test_generate_gundi_observations_filters_old_records():
    """Observations older than historical_limit_days must be excluded."""
    old_ts = (datetime.now(timezone.utc) - timedelta(days=35)).strftime("%Y-%m-%d %H:%M:%S")
    telemetry = [
        {
            "file": "test.csv",
            "observation_id": "226976_old",
            "timestamp": old_ts,
            "device_id": "226976",
            "device_name": "TestBird",
            "location": {"lat": 44.39, "lon": 5.37, "altitude": None},
            "movement": {},
            "device_status": {},
            "sensor_readings": [],
            "sensor_count": 0,
            "sensors": {},
            "additional": {"datatype": "GPSS", "utc_date": "", "utc_time": "", "utc_timestamp": "", "milliseconds": 0},
        }
    ]

    results = list(generate_gundi_observations(telemetry, historical_limit_days=30))

    assert len(results) == 0


# ---------------------------------------------------------------------------
# _process_telemetry_file (legacy JSON handler)
# ---------------------------------------------------------------------------

def test_process_telemetry_file_json():
    json_content = '[{"device_id": "bird001", "timestamp": "2024-01-01T10:00:00Z", "location": {"lat": 40.7128, "lon": -74.0060}}]'
    result = _process_telemetry_file(json_content, "test_data.json")
    assert len(result) == 1
    assert result[0]["device_id"] == "bird001"


def test_process_telemetry_file_invalid_json():
    result = _process_telemetry_file("invalid json", "test_data.json")
    assert len(result) == 1
    assert "parse_error" in result[0]
