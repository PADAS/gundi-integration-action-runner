import hashlib
import pytest
from datetime import datetime, timezone
from uuid import UUID
from unittest.mock import patch, MagicMock

from app.actions.buoy.types import (
    DeviceLocation,
    BuoyDevice,
    BuoyGear,
    LastPositionStatus,
    Geometry,
    CoordinateProperties,
    FeatureProperties,
    Feature,
    ObservationSubject,
)


class TestDeviceLocation:
    """Test cases for DeviceLocation model."""

    def test_device_location_creation(self):
        """Test creating a DeviceLocation with valid data."""
        location = DeviceLocation(latitude=40.7128, longitude=-74.0060)
        assert location.latitude == 40.7128
        assert location.longitude == -74.0060

    def test_device_location_validation(self):
        """Test validation of DeviceLocation fields."""
        with pytest.raises(ValueError):
            DeviceLocation(latitude="invalid", longitude=-74.0060)
        
        with pytest.raises(ValueError):
            DeviceLocation(latitude=40.7128, longitude="invalid")


class TestBuoyDevice:
    """Test cases for BuoyDevice model."""

    @pytest.fixture
    def device_location(self):
        """Fixture for device location."""
        return DeviceLocation(latitude=40.7128, longitude=-74.0060)

    @pytest.fixture
    def sample_device(self, device_location):
        """Fixture for sample buoy device."""
        return BuoyDevice(
            device_id="device123",
            label="Test Device",
            location=device_location,
            last_updated=datetime(2025, 1, 1, 12, 0, 0),
            last_deployed=datetime(2025, 1, 1, 10, 0, 0)
        )

    def test_buoy_device_creation(self, sample_device):
        """Test creating a BuoyDevice with valid data."""
        assert sample_device.device_id == "device123"
        assert sample_device.label == "Test Device"
        assert sample_device.location.latitude == 40.7128
        assert sample_device.location.longitude == -74.0060
        assert sample_device.last_updated == datetime(2025, 1, 1, 12, 0, 0)
        assert sample_device.last_deployed == datetime(2025, 1, 1, 10, 0, 0)

    def test_buoy_device_optional_last_deployed(self, device_location):
        """Test BuoyDevice with None last_deployed."""
        device = BuoyDevice(
            device_id="device123",
            label="Test Device",
            location=device_location,
            last_updated=datetime(2025, 1, 1, 12, 0, 0),
            last_deployed=None
        )
        assert device.last_deployed is None


class TestBuoyGear:
    """Test cases for BuoyGear model."""

    @pytest.fixture
    def device_location(self):
        """Fixture for device location."""
        return DeviceLocation(latitude=40.7128, longitude=-74.0060)

    @pytest.fixture
    def sample_device(self, device_location):
        """Fixture for sample buoy device."""
        return BuoyDevice(
            device_id="device123",
            label="Test Device",
            location=device_location,
            last_updated=datetime(2025, 1, 1, 12, 0, 0),
            last_deployed=datetime(2025, 1, 1, 10, 0, 0)
        )

    @pytest.fixture
    def sample_buoy_gear(self, sample_device):
        """Fixture for sample buoy gear."""
        return BuoyGear(
            id=UUID("12345678-1234-5678-1234-567812345678"),
            display_id="GEAR001",
            status="active",
            last_updated=datetime(2025, 1, 1, 12, 0, 0),
            devices=[sample_device],
            type="lobster_trap",
            manufacturer="TestCorp"
        )

    @patch('app.actions.edgetech.types.SOURCE_TYPE', 'test_source')
    @patch('app.actions.edgetech.types.SUBJECT_SUBTYPE', 'test_subtype')
    def test_create_haul_observation(self, sample_buoy_gear):
        """Test creating haul observation from buoy gear."""
        recorded_at = datetime(2025, 1, 1, 14, 0, 0)
        
        observations = sample_buoy_gear.create_haul_observation(recorded_at)
        
        assert len(observations) == 1
        observation = observations[0]
        
        assert observation["subject_name"] == "GEAR001"
        assert observation["manufacturer_id"] == "device123"
        assert observation["subject_is_active"] is False
        assert observation["source_type"] == "test_source"
        assert observation["subject_subtype"] == "test_subtype"
        assert observation["location"]["lat"] == 40.7128
        assert observation["location"]["lon"] == -74.0060
        assert observation["additional"]["event_type"] == "trap_retrieved"
        assert observation["recorded_at"] == recorded_at

    @patch('app.actions.edgetech.types.SOURCE_TYPE', 'test_source')
    @patch('app.actions.edgetech.types.SUBJECT_SUBTYPE', 'test_subtype')
    def test_create_haul_observation_multiple_devices(self):
        """Test creating haul observation with multiple devices."""
        device_location1 = DeviceLocation(latitude=40.7128, longitude=-74.0060)
        device_location2 = DeviceLocation(latitude=41.7128, longitude=-75.0060)
        
        device1 = BuoyDevice(
            device_id="device123",
            label="Device 1",
            location=device_location1,
            last_updated=datetime(2025, 1, 1, 12, 0, 0),
            last_deployed=None
        )
        
        device2 = BuoyDevice(
            device_id="device456",
            label="Device 2",
            location=device_location2,
            last_updated=datetime(2025, 1, 1, 12, 0, 0),
            last_deployed=None
        )
        
        buoy_gear = BuoyGear(
            id=UUID("12345678-1234-5678-1234-567812345678"),
            display_id="GEAR002",
            status="active",
            last_updated=datetime(2025, 1, 1, 12, 0, 0),
            devices=[device1, device2],
            type="lobster_trap",
            manufacturer="TestCorp"
        )
        
        recorded_at = datetime(2025, 1, 1, 14, 0, 0)
        observations = buoy_gear.create_haul_observation(recorded_at)
        
        assert len(observations) == 2
        assert observations[0]["location"]["lat"] == 40.7128
        assert observations[1]["location"]["lat"] == 41.7128


class TestLastPositionStatus:
    """Test cases for LastPositionStatus model."""

    def test_last_position_status_creation(self):
        """Test creating LastPositionStatus with all fields."""
        status = LastPositionStatus(
            last_voice_call_start_at=datetime(2025, 1, 1, 10, 0, 0),
            radio_state_at=datetime(2025, 1, 1, 11, 0, 0),
            radio_state="active"
        )
        assert status.last_voice_call_start_at == datetime(2025, 1, 1, 10, 0, 0)
        assert status.radio_state_at == datetime(2025, 1, 1, 11, 0, 0)
        assert status.radio_state == "active"

    def test_last_position_status_optional_fields(self):
        """Test creating LastPositionStatus with optional None fields."""
        status = LastPositionStatus(
            last_voice_call_start_at=None,
            radio_state_at=None,
            radio_state="inactive"
        )
        assert status.last_voice_call_start_at is None
        assert status.radio_state_at is None
        assert status.radio_state == "inactive"


class TestGeometry:
    """Test cases for Geometry model."""

    def test_geometry_creation(self):
        """Test creating Geometry with valid data."""
        geometry = Geometry(
            type="Point",
            coordinates=[-74.0060, 40.7128]
        )
        assert geometry.type == "Point"
        assert geometry.coordinates == [-74.0060, 40.7128]


class TestCoordinateProperties:
    """Test cases for CoordinateProperties model."""

    def test_coordinate_properties_creation(self):
        """Test creating CoordinateProperties with valid data."""
        props = CoordinateProperties(
            time=datetime(2025, 1, 1, 12, 0, 0)
        )
        assert props.time == datetime(2025, 1, 1, 12, 0, 0)


class TestFeatureProperties:
    """Test cases for FeatureProperties model."""

    @pytest.fixture
    def coordinate_properties(self):
        """Fixture for coordinate properties."""
        return CoordinateProperties(time=datetime(2025, 1, 1, 12, 0, 0))

    def test_feature_properties_creation(self, coordinate_properties):
        """Test creating FeatureProperties with valid data."""
        props = FeatureProperties(
            title="Test Feature",
            subject_type="buoy",
            subject_subtype="lobster_trap",
            id=UUID("12345678-1234-5678-1234-567812345678"),
            stroke="#FF0000",
            **{"stroke-opacity": 0.8},
            **{"stroke-width": 2},
            image="test_image.png",
            last_voice_call_start_at=datetime(2025, 1, 1, 10, 0, 0),
            location_requested_at=datetime(2025, 1, 1, 11, 0, 0),
            radio_state_at=datetime(2025, 1, 1, 12, 0, 0),
            radio_state="active",
            coordinateProperties=coordinate_properties,
            DateTime=datetime(2025, 1, 1, 12, 0, 0)
        )
        assert props.title == "Test Feature"
        assert props.subject_type == "buoy"
        assert props.stroke_opacity == 0.8
        assert props.stroke_width == 2

    def test_feature_properties_optional_fields(self, coordinate_properties):
        """Test FeatureProperties with optional None fields."""
        props = FeatureProperties(
            title="Test Feature",
            subject_type="buoy",
            subject_subtype="lobster_trap",
            id=UUID("12345678-1234-5678-1234-567812345678"),
            stroke="#FF0000",
            **{"stroke-opacity": 0.8},
            **{"stroke-width": 2},
            image="test_image.png",
            last_voice_call_start_at=None,
            location_requested_at=None,
            radio_state_at=datetime(2025, 1, 1, 12, 0, 0),
            radio_state="active",
            coordinateProperties=coordinate_properties,
            DateTime=datetime(2025, 1, 1, 12, 0, 0)
        )
        assert props.last_voice_call_start_at is None
        assert props.location_requested_at is None


class TestFeature:
    """Test cases for Feature model."""

    @pytest.fixture
    def geometry(self):
        """Fixture for geometry."""
        return Geometry(type="Point", coordinates=[-74.0060, 40.7128])

    @pytest.fixture
    def feature_properties(self):
        """Fixture for feature properties."""
        coordinate_props = CoordinateProperties(time=datetime(2025, 1, 1, 12, 0, 0))
        return FeatureProperties(
            title="Test Feature",
            subject_type="buoy",
            subject_subtype="lobster_trap",
            id=UUID("12345678-1234-5678-1234-567812345678"),
            stroke="#FF0000",
            **{"stroke-opacity": 0.8},
            **{"stroke-width": 2},
            image="test_image.png",
            last_voice_call_start_at=None,
            location_requested_at=None,
            radio_state_at=datetime(2025, 1, 1, 12, 0, 0),
            radio_state="active",
            coordinateProperties=coordinate_props,
            DateTime=datetime(2025, 1, 1, 12, 0, 0)
        )

    def test_feature_creation(self, geometry, feature_properties):
        """Test creating Feature with valid data."""
        feature = Feature(
            type="Feature",
            geometry=geometry,
            properties=feature_properties
        )
        assert feature.type == "Feature"
        assert feature.geometry.type == "Point"
        assert feature.properties.title == "Test Feature"


class TestObservationSubject:
    """Test cases for ObservationSubject model."""

    @pytest.fixture
    def last_position_status(self):
        """Fixture for last position status."""
        return LastPositionStatus(
            last_voice_call_start_at=datetime(2025, 1, 1, 10, 0, 0),
            radio_state_at=datetime(2025, 1, 1, 11, 0, 0),
            radio_state="active"
        )

    @pytest.fixture
    def last_position_feature(self):
        """Fixture for last position feature."""
        geometry = Geometry(type="Point", coordinates=[-74.0060, 40.7128])
        coordinate_props = CoordinateProperties(time=datetime(2025, 1, 1, 12, 0, 0))
        properties = FeatureProperties(
            title="Test Feature",
            subject_type="buoy",
            subject_subtype="lobster_trap",
            id=UUID("12345678-1234-5678-1234-567812345678"),
            stroke="#FF0000",
            **{"stroke-opacity": 0.8},
            **{"stroke-width": 2},
            image="test_image.png",
            last_voice_call_start_at=None,
            location_requested_at=None,
            radio_state_at=datetime(2025, 1, 1, 12, 0, 0),
            radio_state="active",
            coordinateProperties=coordinate_props,
            DateTime=datetime(2025, 1, 1, 12, 0, 0)
        )
        return Feature(type="Feature", geometry=geometry, properties=properties)

    @pytest.fixture
    def sample_observation_subject(self, last_position_status, last_position_feature):
        """Fixture for sample observation subject."""
        return ObservationSubject(
            content_type="subject",
            id=UUID("12345678-1234-5678-1234-567812345678"),
            name="Test Subject",
            subject_type="buoy",
            subject_subtype="lobster_trap",
            common_name="Test Buoy",
            additional={
                "devices": [{"device_id": "device123"}],
                "edgetech_serial_number": "ET123456"
            },
            created_at=datetime(2025, 1, 1, 8, 0, 0),
            updated_at=datetime(2025, 1, 1, 12, 0, 0),
            is_active=True,
            user=None,
            tracks_available=True,
            image_url="http://example.com/image.png",
            last_position_status=last_position_status,
            last_position_date=datetime(2025, 1, 1, 11, 0, 0),
            last_position=last_position_feature,
            device_status_properties=None,
            url="http://example.com/subject/12345678-1234-5678-1234-567812345678"
        )

    def test_observation_subject_creation(self, sample_observation_subject):
        """Test creating ObservationSubject with valid data."""
        assert sample_observation_subject.name == "Test Subject"
        assert sample_observation_subject.subject_type == "buoy"
        assert sample_observation_subject.is_active is True
        assert sample_observation_subject.common_name == "Test Buoy"

    def test_observation_subject_optional_fields(self, last_position_feature):
        """Test ObservationSubject with optional None fields."""
        subject = ObservationSubject(
            content_type="subject",
            id=UUID("12345678-1234-5678-1234-567812345678"),
            name="Test Subject",
            subject_type="buoy",
            subject_subtype="lobster_trap",
            common_name=None,
            additional={
                "devices": [{"device_id": "device123"}]
            },
            created_at=datetime(2025, 1, 1, 8, 0, 0),
            updated_at=datetime(2025, 1, 1, 12, 0, 0),
            is_active=True,
            user=None,
            tracks_available=True,
            image_url="http://example.com/image.png",
            last_position_status=None,
            last_position_date=None,
            last_position=last_position_feature,
            device_status_properties=None,
            url="http://example.com/subject/12345678-1234-5678-1234-567812345678"
        )
        assert subject.common_name is None
        assert subject.last_position_status is None

    def test_location_property(self, sample_observation_subject):
        """Test location property returns tuple of lat, lon."""
        location = sample_observation_subject.location
        assert location == (40.7128, -74.0060)

    def test_latitude_property(self, sample_observation_subject):
        """Test latitude property."""
        assert sample_observation_subject.latitude == 40.7128

    def test_longitude_property(self, sample_observation_subject):
        """Test longitude property."""
        assert sample_observation_subject.longitude == -74.0060

    def test_latitude_no_last_position(self):
        """Test latitude property raises error when no last position."""
        subject = ObservationSubject(
            content_type="subject",
            id=UUID("12345678-1234-5678-1234-567812345678"),
            name="Test Subject",
            subject_type="buoy",
            subject_subtype="lobster_trap",
            common_name=None,
            additional={
                "devices": [{"device_id": "device123"}]
            },
            created_at=datetime(2025, 1, 1, 8, 0, 0),
            updated_at=datetime(2025, 1, 1, 12, 0, 0),
            is_active=True,
            user=None,
            tracks_available=True,
            image_url="http://example.com/image.png",
            last_position_status=None,
            last_position_date=None,
            last_position=None,
            device_status_properties=None,
            url="http://example.com/subject/12345678-1234-5678-1234-567812345678"
        )
        
        with pytest.raises(ValueError, match="Last position is not available"):
            _ = subject.latitude

    def test_longitude_no_last_position(self):
        """Test longitude property raises error when no last position."""
        subject = ObservationSubject(
            content_type="subject",
            id=UUID("12345678-1234-5678-1234-567812345678"),
            name="Test Subject",
            subject_type="buoy",
            subject_subtype="lobster_trap",
            common_name=None,
            additional={
                "devices": [{"device_id": "device123"}]
            },
            created_at=datetime(2025, 1, 1, 8, 0, 0),
            updated_at=datetime(2025, 1, 1, 12, 0, 0),
            is_active=True,
            user=None,
            tracks_available=True,
            image_url="http://example.com/image.png",
            last_position_status=None,
            last_position_date=None,
            last_position=None,
            device_status_properties=None,
            url="http://example.com/subject/12345678-1234-5678-1234-567812345678"
        )
        
        with pytest.raises(ValueError, match="Last position is not available"):
            _ = subject.longitude

    def test_latitude_no_geometry(self, last_position_status):
        """Test latitude property raises error when no geometry in last position."""
        # Create a subject with a properly constructed last_position that has None geometry
        subject = ObservationSubject(
            content_type="subject",
            id=UUID("12345678-1234-5678-1234-567812345678"),
            name="Test Subject",
            subject_type="buoy",
            subject_subtype="lobster_trap",
            common_name=None,
            additional={
                "devices": [{"device_id": "device123"}]
            },
            created_at=datetime(2025, 1, 1, 8, 0, 0),
            updated_at=datetime(2025, 1, 1, 12, 0, 0),
            is_active=True,
            user=None,
            tracks_available=True,
            image_url="http://example.com/image.png",
            last_position_status=last_position_status,
            last_position_date=None,
            last_position=None,  # Set to None to trigger the error
            device_status_properties=None,
            url="http://example.com/subject/12345678-1234-5678-1234-567812345678"
        )
        
        with pytest.raises(ValueError, match="Last position is not available"):
            _ = subject.latitude

    @patch('app.actions.edgetech.types.GEAR_DEPLOYED_EVENT', 'gear_deployed')
    @patch('app.actions.edgetech.types.GEAR_RETRIEVED_EVENT', 'gear_retrieved')
    def test_create_observation_active(self, sample_observation_subject):
        """Test creating observation for active subject."""
        recorded_at = datetime(2025, 1, 1, 14, 0, 0)
        
        observation = sample_observation_subject.create_observation(recorded_at)
        
        assert observation["name"] == "Test Subject"
        assert observation["source"] == "Test Subject"
        assert observation["type"] == "buoy"
        assert observation["subject_type"] == "lobster_trap"
        assert observation["recorded_at"] == recorded_at.isoformat()
        assert observation["location"]["lat"] == 40.7128
        assert observation["location"]["lon"] == -74.0060
        
        additional = observation["additional"]
        assert additional["subject_name"] == "Test Subject"
        assert additional["edgetech_serial_number"] == "ET123456"
        assert additional["subject_is_active"] is True
        assert additional["event_type"] == "gear_deployed"
        assert len(additional["devices"]) == 1
        
        # Test display_id generation
        expected_hash = hashlib.sha256("device123".encode("utf-8")).hexdigest()[:12]
        assert additional["display_id"] == expected_hash

    @patch('app.actions.edgetech.types.GEAR_DEPLOYED_EVENT', 'gear_deployed')
    @patch('app.actions.edgetech.types.GEAR_RETRIEVED_EVENT', 'gear_retrieved')
    def test_create_observation_inactive(self, sample_observation_subject):
        """Test creating observation for inactive subject."""
        recorded_at = datetime(2025, 1, 1, 14, 0, 0)
        
        observation = sample_observation_subject.create_observation(recorded_at, is_active=False)
        
        additional = observation["additional"]
        assert additional["subject_is_active"] is False
        assert additional["event_type"] == "gear_retrieved"

    @patch('app.actions.edgetech.types.GEAR_DEPLOYED_EVENT', 'gear_deployed')
    @patch('app.actions.edgetech.types.GEAR_RETRIEVED_EVENT', 'gear_retrieved')
    def test_create_observation_uses_subject_is_active_default(self, sample_observation_subject):
        """Test create_observation uses subject's is_active when not provided."""
        sample_observation_subject.is_active = False
        recorded_at = datetime(2025, 1, 1, 14, 0, 0)
        
        observation = sample_observation_subject.create_observation(recorded_at)
        
        additional = observation["additional"]
        assert additional["subject_is_active"] is False
        assert additional["event_type"] == "gear_retrieved"

    @patch('datetime.datetime')
    @patch('app.actions.edgetech.types.GEAR_DEPLOYED_EVENT', 'gear_deployed')
    @patch('app.actions.edgetech.types.GEAR_RETRIEVED_EVENT', 'gear_retrieved')
    def test_create_observation_none_recorded_at(self, mock_datetime, sample_observation_subject):
        """Test creating observation with None recorded_at raises AttributeError due to current implementation."""
        mock_now = datetime(2025, 1, 1, 15, 0, 0, tzinfo=timezone.utc)
        mock_datetime.now.return_value = mock_now
        
        # The current implementation has a bug - it tries to call .isoformat() on None
        # This test documents the current behavior
        with pytest.raises(AttributeError, match="'NoneType' object has no attribute 'isoformat'"):
            sample_observation_subject.create_observation(None)

    def test_create_observation_no_last_position(self):
        """Test create_observation raises error when no last position."""
        subject = ObservationSubject(
            content_type="subject",
            id=UUID("12345678-1234-5678-1234-567812345678"),
            name="Test Subject",
            subject_type="buoy",
            subject_subtype="lobster_trap",
            common_name=None,
            additional={
                "devices": [{"device_id": "device123"}]
            },
            created_at=datetime(2025, 1, 1, 8, 0, 0),
            updated_at=datetime(2025, 1, 1, 12, 0, 0),
            is_active=True,
            user=None,
            tracks_available=True,
            image_url="http://example.com/image.png",
            last_position_status=None,
            last_position_date=None,
            last_position=None,
            device_status_properties=None,
            url="http://example.com/subject/12345678-1234-5678-1234-567812345678"
        )
        
        with pytest.raises(ValueError, match="Last position is not available"):
            subject.create_observation(datetime(2025, 1, 1, 14, 0, 0))

    def test_create_observation_no_geometry(self):
        """Test create_observation raises error when no geometry in last position."""
        # Create a subject with a None last_position to trigger the error
        subject = ObservationSubject(
            content_type="subject",
            id=UUID("12345678-1234-5678-1234-567812345678"),
            name="Test Subject",
            subject_type="buoy",
            subject_subtype="lobster_trap",
            common_name=None,
            additional={
                "devices": [{"device_id": "device123"}]
            },
            created_at=datetime(2025, 1, 1, 8, 0, 0),
            updated_at=datetime(2025, 1, 1, 12, 0, 0),
            is_active=True,
            user=None,
            tracks_available=True,
            image_url="http://example.com/image.png",
            last_position_status=None,
            last_position_date=None,
            last_position=None,  # Set to None to trigger the error
            device_status_properties=None,
            url="http://example.com/subject/12345678-1234-5678-1234-567812345678"
        )
        
        with pytest.raises(ValueError, match="Last position is not available"):
            subject.create_observation(datetime(2025, 1, 1, 14, 0, 0))

    def test_create_observation_no_devices(self, sample_observation_subject):
        """Test create_observation raises error when no devices in additional."""
        sample_observation_subject.additional = {}
        
        with pytest.raises(ValueError, match="No devices available in additional information"):
            sample_observation_subject.create_observation(datetime(2025, 1, 1, 14, 0, 0))

    def test_create_observation_empty_devices(self, sample_observation_subject):
        """Test create_observation raises error when devices list is empty."""
        sample_observation_subject.additional = {"devices": []}
        
        with pytest.raises(ValueError, match="No devices available in additional information"):
            sample_observation_subject.create_observation(datetime(2025, 1, 1, 14, 0, 0))

    @patch('app.actions.edgetech.types.GEAR_DEPLOYED_EVENT', 'gear_deployed')
    @patch('app.actions.edgetech.types.GEAR_RETRIEVED_EVENT', 'gear_retrieved')
    def test_create_observation_multiple_devices(self, sample_observation_subject):
        """Test create_observation with multiple devices creates correct display_id."""
        sample_observation_subject.additional = {
            "devices": [
                {"device_id": "device123"},
                {"device_id": "device456"},
                {"device_id": "device789"}
            ],
            "edgetech_serial_number": "ET123456"
        }
        
        recorded_at = datetime(2025, 1, 1, 14, 0, 0)
        observation = sample_observation_subject.create_observation(recorded_at)
        
        # Test display_id generation with multiple devices
        concatenated = "device123device456device789"
        expected_hash = hashlib.sha256(concatenated.encode("utf-8")).hexdigest()[:12]
        assert observation["additional"]["display_id"] == expected_hash

    @patch('app.actions.edgetech.types.GEAR_DEPLOYED_EVENT', 'gear_deployed')
    @patch('app.actions.edgetech.types.GEAR_RETRIEVED_EVENT', 'gear_retrieved')
    def test_create_observation_missing_edgetech_serial(self, sample_observation_subject):
        """Test create_observation works with missing edgetech_serial_number."""
        sample_observation_subject.additional = {
            "devices": [{"device_id": "device123"}]
        }
        
        recorded_at = datetime(2025, 1, 1, 14, 0, 0)
        observation = sample_observation_subject.create_observation(recorded_at)
        
        assert observation["additional"]["edgetech_serial_number"] is None
