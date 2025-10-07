from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
from gundi_core.schemas.v2 import ConnectionIntegration, Integration
from pydantic import SecretStr

from app.actions.configurations import EdgeTechAuthConfiguration, EdgeTechConfiguration
from app.actions.edgetech.exceptions import InvalidCredentials
from app.actions.handlers import (
    action_auth,
    action_pull_edgetech_observations,
    generate_batches,
    get_destination_credentials,
    process_destination,
)


@pytest.fixture
def sample_integration():
    """Create a sample integration for testing."""
    return Integration(
        id=uuid4(),
        name="Test EdgeTech Integration",
        base_url="https://test.com",
        enabled=True,
        type={
            "id": uuid4(),
            "name": "EdgeTech",
            "value": "edgetech",
            "description": "EdgeTech Integration",
            "actions": [],
        },
        owner={
            "id": uuid4(),
            "name": "Test Owner",
            "description": "Test Owner Description",
        },
        configurations=[
            {
                "id": uuid4(),
                "integration": uuid4(),
                "action": {
                    "id": "auth",
                    "name": "Authentication",
                },
                "data": {
                    "token_json": '{"access_token": "test_token", "refresh_token": "refresh_token", "expires_in": 3600, "expires_at": 9999999999}',
                    "client_id": "test_client_id",
                },
            }
        ],
    )


@pytest.fixture
def sample_destination():
    """Create a sample destination for testing."""
    return ConnectionIntegration(
        id=uuid4(), name="Test ER Destination", enabled=True, type_slug="earth_ranger"
    )


@pytest.fixture
def auth_config():
    """Create a test auth configuration."""
    return EdgeTechAuthConfiguration(
        token_json=SecretStr(
            '{"access_token": "test_token", "refresh_token": "refresh_token", "expires_in": 3600, "expires_at": 9999999999}'
        ),
        client_id="test_client_id",
    )


@pytest.fixture
def pull_config():
    """Create a test pull configuration."""
    return EdgeTechConfiguration(
        api_base_url="https://api.test.com", num_get_retry=3, minutes_to_sync=30
    )


@pytest.fixture
def sample_data():
    """Create sample EdgeTech data for testing."""
    return [
        {
            "serialNumber": "TEST123",
            "userId": "user123",
            "currentState": {
                "etag": "test_etag",
                "isDeleted": False,
                "serialNumber": "TEST123",
                "releaseCommand": "release123",
                "statusCommand": "status123",
                "idCommand": "id123",
                "isNfcTag": False,
                "latDeg": 40.7128,
                "lonDeg": -74.0060,
                "modelNumber": "Model123",
                "isDeployed": True,
                "dateDeployed": "2023-01-01T00:00:00.000Z",
                "lastUpdated": "2023-01-01T12:00:00.000Z",
            },
            "changeRecords": [],
        }
    ]


class TestGenerateBatches:
    """Test cases for generate_batches function."""

    def test_generate_batches_default_size(self):
        """Test generate_batches with default batch size."""
        data = list(range(250))  # 250 items
        batches = list(generate_batches(data))

        assert len(batches) == 3  # 100, 100, 50
        assert len(batches[0]) == 100
        assert len(batches[1]) == 100
        assert len(batches[2]) == 50

    def test_generate_batches_custom_size(self):
        """Test generate_batches with custom batch size."""
        data = list(range(25))
        batches = list(generate_batches(data, n=10))

        assert len(batches) == 3  # 10, 10, 5
        assert len(batches[0]) == 10
        assert len(batches[1]) == 10
        assert len(batches[2]) == 5

    def test_generate_batches_empty_list(self):
        """Test generate_batches with empty list."""
        data = []
        batches = list(generate_batches(data))

        assert len(batches) == 0

    def test_generate_batches_single_item(self):
        """Test generate_batches with single item."""
        data = [1]
        batches = list(generate_batches(data))

        assert len(batches) == 1
        assert len(batches[0]) == 1


class TestGetDestinationCredentials:
    """Test cases for get_destination_credentials function."""

    @pytest.mark.asyncio
    async def test_get_destination_credentials_success(self, sample_destination):
        """Test successful retrieval of destination credentials."""
        # Mock GundiClient
        mock_gundi_client = AsyncMock()

        # Mock integration details response
        mock_integration_details = MagicMock()
        mock_integration_details.base_url = "https://er.test.com"
        mock_integration_details.configurations = [
            MagicMock(action=MagicMock(id="auth"), data={"token": "er_test_token"})
        ]
        mock_gundi_client.get_integration_details.return_value = (
            mock_integration_details
        )

        with (
            patch("app.actions.handlers.find_config_for_action") as mock_find_config,
            patch(
                "app.actions.handlers.schemas.v2.ERAuthActionConfig"
            ) as mock_auth_config,
        ):
            # Mock find_config_for_action
            mock_config = MagicMock()
            mock_config.data = {"token": "er_test_token"}
            mock_find_config.return_value = mock_config

            # Mock ERAuthActionConfig
            mock_parsed_config = MagicMock()
            mock_parsed_config.token = "er_test_token"
            mock_auth_config.parse_obj.return_value = mock_parsed_config

            token, base_url = await get_destination_credentials(
                mock_gundi_client, sample_destination
            )

            assert token == "er_test_token"
            assert base_url == "https://er.test.com"

    @pytest.mark.asyncio
    async def test_get_destination_credentials_missing_auth_config(
        self, sample_destination
    ):
        """Test get_destination_credentials with missing auth configuration."""
        mock_gundi_client = AsyncMock()

        mock_integration_details = MagicMock()
        mock_integration_details.base_url = "https://er.test.com"
        mock_integration_details.configurations = []
        mock_gundi_client.get_integration_details.return_value = (
            mock_integration_details
        )

        with (
            patch("app.actions.handlers.find_config_for_action") as mock_find_config,
            patch(
                "app.actions.handlers.schemas.v2.ERAuthActionConfig"
            ) as mock_auth_config,
        ):
            mock_find_config.return_value = MagicMock(data={"token": "test"})
            mock_auth_config.parse_obj.return_value = None

            with pytest.raises(ValueError, match="Missing auth configuration"):
                await get_destination_credentials(mock_gundi_client, sample_destination)


class TestProcessDestination:
    """Test cases for process_destination function."""

    @pytest.mark.asyncio
    async def test_process_destination_success(
        self, sample_integration, sample_destination, sample_data
    ):
        """Test successful processing of destination."""
        mock_gundi_client = AsyncMock()

        # Mock observations
        mock_observations = [{"observation": "test1"}, {"observation": "test2"}]

        with (
            patch("app.actions.handlers.get_destination_credentials") as mock_get_creds,
            patch("app.actions.handlers.EdgeTechProcessor") as mock_processor_class,
            patch("app.actions.handlers.send_observations_to_gundi") as mock_send_obs,
            patch("app.actions.handlers.log_action_activity") as mock_log_activity,
        ):
            # Mock get_destination_credentials
            mock_get_creds.return_value = ("test_token", "https://er.test.com")

            # Mock EdgeTechProcessor
            mock_processor = AsyncMock()
            mock_processor.process.return_value = mock_observations
            mock_processor_class.return_value = mock_processor

            # Mock send_observations_to_gundi
            mock_send_obs.return_value = {"success": True}

            # Mock log_action_activity
            mock_log_activity.return_value = None

            start_datetime = datetime.now(timezone.utc)
            result = await process_destination(
                mock_gundi_client,
                sample_integration,
                sample_data,
                sample_destination,
                start_datetime,
            )

            assert result == 2  # Length of observations
            mock_processor_class.assert_called_once()
            mock_send_obs.assert_called_once()
            mock_log_activity.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_destination_without_start_datetime(
        self, sample_integration, sample_destination, sample_data
    ):
        """Test processing destination without start_datetime filter."""
        mock_gundi_client = AsyncMock()

        mock_observations = [{"observation": "test"}]

        with (
            patch("app.actions.handlers.get_destination_credentials") as mock_get_creds,
            patch("app.actions.handlers.EdgeTechProcessor") as mock_processor_class,
            patch("app.actions.handlers.send_observations_to_gundi") as mock_send_obs,
            patch("app.actions.handlers.log_action_activity") as mock_log_activity,
        ):
            mock_get_creds.return_value = ("test_token", "https://er.test.com")

            mock_processor = AsyncMock()
            mock_processor.process.return_value = mock_observations
            mock_processor_class.return_value = mock_processor

            mock_send_obs.return_value = {"success": True}
            mock_log_activity.return_value = None

            result = await process_destination(
                mock_gundi_client, sample_integration, sample_data, sample_destination
            )

            # Check that processor was called with filters=None
            mock_processor_class.assert_called_once_with(
                sample_data, "test_token", "https://er.test.com", filters=None
            )
            assert result == 1


class TestActionAuth:
    """Test cases for action_auth function."""

    @pytest.mark.asyncio
    async def test_action_auth_valid_credentials(self, sample_integration, auth_config):
        """Test action_auth with valid credentials."""
        with patch("app.actions.handlers.EdgeTechClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.get_token.return_value = {"access_token": "test_token"}
            mock_client_class.return_value = mock_client

            result = await action_auth(sample_integration, auth_config)

            assert result == {"valid_credentials": True}
            mock_client_class.assert_called_once_with(
                auth_config=auth_config, pull_config=None
            )
            mock_client.get_token.assert_called_once()

    @pytest.mark.asyncio
    async def test_action_auth_invalid_credentials(
        self, sample_integration, auth_config
    ):
        """Test action_auth with invalid credentials."""
        with patch("app.actions.handlers.EdgeTechClient") as mock_client_class:
            mock_client = AsyncMock()
            error_response = {"error": "invalid_grant"}
            mock_client.get_token.side_effect = InvalidCredentials(error_response)
            mock_client_class.return_value = mock_client

            result = await action_auth(sample_integration, auth_config)

            assert result["valid_credentials"] is False
            assert "error" in result

    @pytest.mark.asyncio
    async def test_action_auth_general_exception(self, sample_integration, auth_config):
        """Test action_auth with general exception."""
        with patch("app.actions.handlers.EdgeTechClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.get_token.side_effect = Exception("Network error")
            mock_client_class.return_value = mock_client

            result = await action_auth(sample_integration, auth_config)

            assert result["valid_credentials"] is None
            assert "Network error" in result["error"]


class TestActionPullEdgetechObservations:
    """Test cases for action_pull_edgetech_observations function."""

    @pytest.mark.asyncio
    async def test_action_pull_edgetech_observations_success(
        self, sample_integration, pull_config, sample_data
    ):
        """Test successful execution of action_pull_edgetech_observations."""
        # Create mock connection details
        mock_connection_details = MagicMock()
        mock_destination1 = MagicMock()
        mock_destination1.id = uuid4()
        mock_destination1.name = "Destination 1"
        mock_destination2 = MagicMock()
        mock_destination2.id = uuid4()
        mock_destination2.name = "Destination 2"
        mock_connection_details.destinations = [mock_destination1, mock_destination2]

        with (
            patch("app.actions.handlers.GundiClient") as mock_gundi_client_class,
            patch("app.actions.handlers.find_config_for_action") as mock_find_config,
            patch("app.actions.handlers.EdgeTechClient") as mock_edgetech_client_class,
            patch("app.actions.handlers.process_destination") as mock_process_dest,
            patch("app.services.activity_logger.publish_event") as mock_publish,
        ):
            # Mock publish_event to avoid actual Pub/Sub calls
            mock_publish.return_value = None

            # Mock GundiClient
            mock_gundi_client = AsyncMock()
            mock_gundi_client.get_connection_details.return_value = (
                mock_connection_details
            )
            mock_gundi_client_class.return_value = mock_gundi_client

            # Mock find_config_for_action
            mock_config = MagicMock()
            mock_config.data = {
                "token_json": '{"access_token": "test_token", "refresh_token": "refresh_token", "expires_in": 3600, "expires_at": 9999999999}',
                "client_id": "test_client_id",
            }
            mock_find_config.return_value = mock_config

            # Mock EdgeTechClient
            mock_edgetech_client = AsyncMock()
            mock_edgetech_client.download_data.return_value = sample_data
            mock_edgetech_client_class.return_value = mock_edgetech_client

            # Mock process_destination
            mock_process_dest.side_effect = [
                5,
                3,
            ]  # Return different observation counts

            result = await action_pull_edgetech_observations(
                sample_integration, pull_config
            )

            # Verify the result structure
            assert len(result) == 2  # Two destinations

            # Check that all mocks were called appropriately
            mock_gundi_client.get_connection_details.assert_called_once_with(
                sample_integration.id
            )
            mock_edgetech_client.download_data.assert_called_once()
            assert mock_process_dest.call_count == 2

    @pytest.mark.asyncio
    async def test_action_pull_edgetech_observations_with_datetime_filter(
        self, sample_integration, pull_config, sample_data
    ):
        """Test action_pull_edgetech_observations with datetime filtering."""
        mock_connection_details = MagicMock()
        mock_destination = MagicMock()
        mock_destination.id = uuid4()
        mock_destination.name = "Test Destination"
        mock_connection_details.destinations = [mock_destination]

        with (
            patch("app.actions.handlers.GundiClient") as mock_gundi_client_class,
            patch("app.actions.handlers.find_config_for_action") as mock_find_config,
            patch("app.actions.handlers.EdgeTechClient") as mock_edgetech_client_class,
            patch("app.actions.handlers.process_destination") as mock_process_dest,
            patch("app.actions.handlers.datetime") as mock_datetime,
            patch("app.services.activity_logger.publish_event") as mock_publish,
        ):
            # Mock publish_event to avoid actual Pub/Sub calls
            mock_publish.return_value = None

            # Mock datetime.now to return a fixed time
            fixed_time = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
            mock_datetime.now.return_value = fixed_time
            mock_datetime.side_effect = lambda *args, **kwargs: datetime(
                *args, **kwargs
            )

            # Mock GundiClient
            mock_gundi_client = AsyncMock()
            mock_gundi_client.get_connection_details.return_value = (
                mock_connection_details
            )
            mock_gundi_client_class.return_value = mock_gundi_client

            # Mock find_config_for_action
            mock_config = MagicMock()
            mock_config.data = {
                "token_json": '{"access_token": "test", "refresh_token": "refresh", "expires_in": 3600, "expires_at": 9999999999}',
                "client_id": "test_id",
            }
            mock_find_config.return_value = mock_config

            # Mock EdgeTechClient
            mock_edgetech_client = AsyncMock()
            mock_edgetech_client.download_data.return_value = sample_data
            mock_edgetech_client_class.return_value = mock_edgetech_client

            # Mock process_destination
            mock_process_dest.return_value = 1

            await action_pull_edgetech_observations(sample_integration, pull_config)

            # Verify that download_data was called with start_datetime
            mock_edgetech_client.download_data.assert_called_once()
            call_args = mock_edgetech_client.download_data.call_args
            assert "start_datetime" in call_args.kwargs

            # Verify that process_destination was called with start_datetime
            mock_process_dest.assert_called_once()
            call_args = mock_process_dest.call_args
            # start_datetime is passed as 5th positional argument
            assert (
                len(call_args[0]) == 5
            )  # gundi_client, integration, data, destination, start_datetime
            assert call_args[0][4] is not None  # start_datetime should not be None
