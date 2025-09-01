from datetime import datetime, timezone
from uuid import uuid4

import pytest

from app.actions.buoy.types import (
    BuoyDevice,
    BuoyGear,
    DeviceLocation,
    ObservationSubject,
)


class TestBuoyGear:
    """Test cases for BuoyGear class and its methods."""

    @pytest.fixture
    def sample_device_location(self):
        """Create a sample device location."""
        return DeviceLocation(latitude=37.7749, longitude=-122.4194)

    @pytest.fixture
    def sample_buoy_device(self, sample_device_location):
        """Create a sample buoy device."""
        return BuoyDevice(
            device_id="device_123",
            label="Test Device",
            location=sample_device_location,
            last_updated=datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            last_deployed=datetime(2025, 1, 1, 10, 0, 0, tzinfo=timezone.utc),
        )

    @pytest.fixture
    def sample_buoy_gear(self, sample_buoy_device):
        """Create a sample buoy gear with devices."""
        return BuoyGear(
            id=uuid4(),
            display_id="GEAR-001",
            status="deployed",
            last_updated=datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            devices=[sample_buoy_device],
            type="fishing_gear",
            manufacturer="EdgeTech",
        )

    @pytest.fixture
    def sample_buoy_gear_multiple_devices(self, sample_device_location):
        """Create a sample buoy gear with multiple devices."""
        device1 = BuoyDevice(
            device_id="device_123",
            label="Test Device 1",
            location=sample_device_location,
            last_updated=datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            last_deployed=None,
        )
        device2 = BuoyDevice(
            device_id="device_456",
            label="Test Device 2",
            location=DeviceLocation(latitude=37.8049, longitude=-122.4694),
            last_updated=datetime(2025, 1, 1, 12, 30, 0, tzinfo=timezone.utc),
            last_deployed=datetime(2025, 1, 1, 11, 0, 0, tzinfo=timezone.utc),
        )
        return BuoyGear(
            id=uuid4(),
            display_id="GEAR-002",
            status="retrieved",
            last_updated=datetime(2025, 1, 1, 13, 0, 0, tzinfo=timezone.utc),
            devices=[device1, device2],
            type="buoy_line",
            manufacturer="EdgeTech",
        )

    def test_create_haul_observation_single_device(self, sample_buoy_gear):
        """Test creating haul observation for gear with single device."""
        recorded_at = datetime(2025, 1, 1, 14, 0, 0, tzinfo=timezone.utc)

        observations = sample_buoy_gear.create_haul_observation(recorded_at)

        assert len(observations) == 1
        obs = observations[0]

        assert obs["subject_name"] == sample_buoy_gear.display_id
        assert obs["manufacturer_id"] == "device_123"
        assert obs["subject_is_active"] is False
        assert "source_type" in obs
        assert "subject_subtype" in obs
        assert obs["location"]["lat"] == 37.7749
        assert obs["location"]["lon"] == -122.4194
        assert obs["recorded_at"] == recorded_at

    def test_create_haul_observation_multiple_devices(
        self, sample_buoy_gear_multiple_devices
    ):
        """Test creating haul observation for gear with multiple devices."""
        recorded_at = datetime(2025, 1, 1, 15, 0, 0, tzinfo=timezone.utc)

        observations = sample_buoy_gear_multiple_devices.create_haul_observation(
            recorded_at
        )

        assert len(observations) == 2

        # Check first device observation
        obs1 = observations[0]
        assert obs1["subject_name"] == sample_buoy_gear_multiple_devices.display_id
        assert obs1["manufacturer_id"] == "device_123"
        assert obs1["subject_is_active"] is False
        assert obs1["location"]["lat"] == 37.7749
        assert obs1["location"]["lon"] == -122.4194
        assert obs1["recorded_at"] == recorded_at

        # Check second device observation
        obs2 = observations[1]
        assert obs2["subject_name"] == sample_buoy_gear_multiple_devices.display_id
        assert obs2["manufacturer_id"] == "device_456"
        assert obs2["subject_is_active"] is False
        assert obs2["location"]["lat"] == 37.8049
        assert obs2["location"]["lon"] == -122.4694
        assert obs2["recorded_at"] == recorded_at


class DummyGeometry:
    """Simple geometry mock for testing."""

    def __init__(self, coords):
        self.coordinates = coords


class DummyPosition:
    """Simple position mock for testing."""

    def __init__(self, coords):
        self.geometry = DummyGeometry(coords)


class TestObservationSubject:
    """Test cases for ObservationSubject properties."""

    @pytest.fixture
    def base_subject(self):
        """Create a base ObservationSubject using construct to avoid validation."""
        subj = ObservationSubject.construct()
        subj.name = "TestSubject"
        subj.subject_type = "buoy"
        subj.subject_subtype = "gps"
        subj.is_active = True
        return subj

    def test_location_property(self, base_subject):
        """Test the location property returns correct tuple."""
        lon, lat = -122.4194, 37.7749
        base_subject.last_position = DummyPosition([lon, lat])

        location = base_subject.location
        assert location == (lat, lon)  # (latitude, longitude)

    def test_latitude_property(self, base_subject):
        """Test the latitude property returns correct value."""
        lon, lat = -122.4194, 37.7749
        base_subject.last_position = DummyPosition([lon, lat])

        latitude = base_subject.latitude
        assert latitude == lat

    def test_longitude_property(self, base_subject):
        """Test the longitude property returns correct value."""
        lon, lat = -122.4194, 37.7749
        base_subject.last_position = DummyPosition([lon, lat])

        longitude = base_subject.longitude
        assert longitude == lon

    def test_latitude_no_last_position_raises(self, base_subject):
        """Test latitude property raises ValueError when no last position."""
        base_subject.last_position = None

        with pytest.raises(ValueError, match="Last position is not available."):
            _ = base_subject.latitude

    def test_longitude_no_last_position_raises(self, base_subject):
        """Test longitude property raises ValueError when no last position."""
        base_subject.last_position = None

        with pytest.raises(ValueError, match="Last position is not available."):
            _ = base_subject.longitude

    def test_latitude_no_geometry_raises(self, base_subject):
        """Test latitude property raises ValueError when no geometry."""

        class NoGeom:
            geometry = None

        base_subject.last_position = NoGeom()

        with pytest.raises(ValueError, match="Last position is not available."):
            _ = base_subject.latitude

    def test_longitude_no_geometry_raises(self, base_subject):
        """Test longitude property raises ValueError when no geometry."""

        class NoGeom:
            geometry = None

        base_subject.last_position = NoGeom()

        with pytest.raises(ValueError, match="Last position is not available."):
            _ = base_subject.longitude

    def test_location_no_last_position_raises(self, base_subject):
        """Test location property raises ValueError when no last position."""
        base_subject.last_position = None

        with pytest.raises(ValueError, match="Last position is not available."):
            _ = base_subject.location

    def test_location_no_geometry_raises(self, base_subject):
        """Test location property raises ValueError when no geometry."""

        class NoGeom:
            geometry = None

        base_subject.last_position = NoGeom()

        with pytest.raises(ValueError, match="Last position is not available."):
            _ = base_subject.location
