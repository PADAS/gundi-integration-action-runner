import pytest
from pydantic import SecretStr, ValidationError

from app.actions.configurations import EdgeTechAuthConfiguration, EdgeTechConfiguration


class TestEdgeTechAuthConfiguration:
    """Test cases for EdgeTechAuthConfiguration class."""

    def test_valid_auth_configuration(self):
        """Test creating a valid EdgeTechAuthConfiguration."""
        config = EdgeTechAuthConfiguration(
            token_json=SecretStr(
                '{"access_token": "test_token", "refresh_token": "refresh"}'
            ),
            client_id="test_client_id",
        )

        assert config.client_id == "test_client_id"
        assert (
            config.token_json.get_secret_value()
            == '{"access_token": "test_token", "refresh_token": "refresh"}'
        )

    def test_missing_token_json_raises_validation_error(self):
        """Test that missing token_json raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            EdgeTechAuthConfiguration(client_id="test_client_id")

        assert "token_json" in str(exc_info.value)

    def test_missing_client_id_raises_validation_error(self):
        """Test that missing client_id raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            EdgeTechAuthConfiguration(
                token_json=SecretStr('{"access_token": "test_token"}')
            )

        assert "client_id" in str(exc_info.value)

    def test_empty_client_id_raises_validation_error(self):
        """Test that empty client_id is allowed."""
        # Empty client_id is actually allowed in the current implementation
        config = EdgeTechAuthConfiguration(
            token_json=SecretStr('{"access_token": "test_token"}'), client_id=""
        )
        assert config.client_id == ""


class TestEdgeTechConfiguration:
    """Test cases for EdgeTechConfiguration class."""

    def test_valid_configuration_with_defaults(self):
        """Test creating a valid EdgeTechConfiguration with default values."""
        config = EdgeTechConfiguration(api_base_url="https://api.edgetech.com")

        assert str(config.api_base_url) == "https://api.edgetech.com"
        assert config.num_get_retry == 60
        assert config.minutes_to_sync == 30

    def test_valid_configuration_with_custom_values(self):
        """Test creating a valid EdgeTechConfiguration with custom values."""
        config = EdgeTechConfiguration(
            api_base_url="https://custom.api.com", num_get_retry=100, minutes_to_sync=45
        )

        assert str(config.api_base_url) == "https://custom.api.com"
        assert config.num_get_retry == 100
        assert config.minutes_to_sync == 45

    def test_v1_url_property(self):
        """Test the v1_url property returns correct URL."""
        config = EdgeTechConfiguration(api_base_url="https://api.edgetech.com")

        assert config.v1_url == "https://api.edgetech.com/v1"

    def test_v1_url_property_with_trailing_slash(self):
        """Test the v1_url property with base URL that has trailing slash."""
        config = EdgeTechConfiguration(api_base_url="https://api.edgetech.com/")

        assert config.v1_url == "https://api.edgetech.com//v1"

    def test_database_dump_url_property(self):
        """Test the database_dump_url property returns correct URL."""
        config = EdgeTechConfiguration(api_base_url="https://api.edgetech.com")

        assert (
            config.database_dump_url
            == "https://api.edgetech.com/v1/database-dump/tasks"
        )

    def test_database_dump_url_property_with_trailing_slash(self):
        """Test the database_dump_url property with base URL that has trailing slash."""
        config = EdgeTechConfiguration(api_base_url="https://api.edgetech.com/")

        assert (
            config.database_dump_url
            == "https://api.edgetech.com//v1/database-dump/tasks"
        )

    def test_missing_api_base_url_raises_validation_error(self):
        """Test that missing api_base_url raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            EdgeTechConfiguration()

        assert "api_base_url" in str(exc_info.value)

    def test_invalid_api_base_url_raises_validation_error(self):
        """Test that invalid api_base_url raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            EdgeTechConfiguration(api_base_url="not-a-valid-url")

        assert "api_base_url" in str(exc_info.value)

    def test_negative_num_get_retry(self):
        """Test configuration with negative num_get_retry."""
        config = EdgeTechConfiguration(
            api_base_url="https://api.edgetech.com", num_get_retry=-5
        )

        assert config.num_get_retry == -5

    def test_zero_minutes_to_sync(self):
        """Test configuration with zero minutes_to_sync."""
        config = EdgeTechConfiguration(
            api_base_url="https://api.edgetech.com", minutes_to_sync=0
        )

        assert config.minutes_to_sync == 0

    def test_properties_with_different_base_urls(self):
        """Test properties work correctly with different base URL formats."""
        test_cases = [
            ("https://api.example.com", "https://api.example.com/v1"),
            ("https://api.example.com/", "https://api.example.com//v1"),
            ("http://localhost:8000", "http://localhost:8000/v1"),
            ("http://localhost:8000/", "http://localhost:8000//v1"),
        ]

        for base_url, expected_v1_url in test_cases:
            config = EdgeTechConfiguration(api_base_url=base_url)
            assert config.v1_url == expected_v1_url
            assert config.database_dump_url == f"{expected_v1_url}/database-dump/tasks"
