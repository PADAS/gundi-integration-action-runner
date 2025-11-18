import hashlib
import pytest
from datetime import datetime
from uuid import UUID

from app.actions.buoy.types import (
    DeviceLocation,
    BuoyDevice,
    BuoyGear,
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
            mfr_device_id="mfr-device123",
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
            mfr_device_id="mfr-device123",
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
            mfr_device_id="mfr-device123",
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
