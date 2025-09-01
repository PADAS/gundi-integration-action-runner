import gzip
import json
import time
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest
from pydantic import SecretStr

from app.actions.configurations import EdgeTechAuthConfiguration, EdgeTechConfiguration
from app.actions.edgetech.client import EdgeTechClient
from app.actions.edgetech.exceptions import InvalidCredentials
from app.actions.edgetech.types import Buoy


class MockGetContext:
    """Mock async context manager for session.get()"""

    def __init__(self, response):
        self.response = response

    async def __aenter__(self):
        return self.response

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return None


class MockPostContext:
    """Mock async context manager for session.post()"""

    def __init__(self, response):
        self.response = response

    async def __aenter__(self):
        return self.response

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return None


class MockSessionContext:
    """Mock async context manager for ClientSession"""

    def __init__(self, session):
        self.session = session

    async def __aenter__(self):
        return self.session

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return None


class MockSession:
    """Mock session with proper get and post methods"""

    def __init__(self, post_responses=None, get_responses=None):
        self.post_responses = (
            post_responses
            if isinstance(post_responses, list)
            else [post_responses]
            if post_responses
            else []
        )
        self.get_responses = (
            get_responses
            if isinstance(get_responses, list)
            else [get_responses]
            if get_responses
            else []
        )
        self.post_calls = []
        self.get_calls = []
        self.post_call_count = 0
        self.get_call_count = 0

    def post(self, *args, **kwargs):
        self.post_calls.append((args, kwargs))
        response = self.post_responses[
            min(self.post_call_count, len(self.post_responses) - 1)
        ]
        self.post_call_count += 1
        return MockPostContext(response)

    def get(self, *args, **kwargs):
        self.get_calls.append((args, kwargs))
        response = self.get_responses[
            min(self.get_call_count, len(self.get_responses) - 1)
        ]
        self.get_call_count += 1
        return MockGetContext(response)


@pytest.fixture
def auth_config():
    """Create a test auth configuration."""
    token_data = {
        "access_token": "test_access_token",
        "refresh_token": "test_refresh_token",
        "expires_in": 3600,
        "expires_at": time.time() + 3600,
        "token_type": "Bearer",
    }
    return EdgeTechAuthConfiguration(
        token_json=SecretStr(json.dumps(token_data)), client_id="test_client_id"
    )


@pytest.fixture
def pull_config():
    """Create a test pull configuration."""
    return EdgeTechConfiguration(
        api_base_url="https://api.test.com", num_get_retry=3, minutes_to_sync=30
    )


@pytest.fixture
def edgetech_client(auth_config, pull_config):
    """Create a test EdgeTechClient instance."""
    return EdgeTechClient(auth_config, pull_config)


@pytest.fixture
def sample_buoy_data():
    """Create sample buoy data for testing."""
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


class TestEdgeTechClient:
    """Test cases for EdgeTechClient."""

    def test_init(self, auth_config, pull_config):
        """Test EdgeTechClient initialization."""
        client = EdgeTechClient(auth_config, pull_config)

        assert client._auth_config == auth_config
        assert client._pull_config == pull_config
        assert isinstance(client._token_json, dict)
        assert "access_token" in client._token_json

    def test_set_token(self, edgetech_client):
        """Test _set_token method."""
        token_response = {"access_token": "new_token", "expires_in": 7200}
        refresh_token = "new_refresh_token"

        edgetech_client._set_token(token_response, refresh_token)

        assert edgetech_client._token_json["access_token"] == "new_token"
        assert edgetech_client._token_json["refresh_token"] == "new_refresh_token"
        assert "expires_at" in edgetech_client._token_json
        assert edgetech_client._token_json["expires_at"] > time.time()

    def test_set_token_without_refresh_token(self, edgetech_client):
        """Test _set_token method without providing refresh token."""
        token_response = {
            "access_token": "new_token",
            "expires_in": 7200,
            "refresh_token": "existing_refresh",
        }

        edgetech_client._set_token(token_response)

        assert edgetech_client._token_json["access_token"] == "new_token"
        assert edgetech_client._token_json["refresh_token"] == "existing_refresh"

    @pytest.mark.asyncio
    async def test_update_token_success(self, edgetech_client):
        """Test successful token update."""
        mock_response_data = {
            "access_token": "updated_token",
            "expires_in": 3600,
            "token_type": "Bearer",
        }

        # Create mock response
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=mock_response_data)

        # Create mock session
        mock_session = MockSession(post_responses=mock_response)

        with patch("aiohttp.ClientSession") as mock_client_session:
            mock_client_session.return_value = MockSessionContext(mock_session)

            result = await edgetech_client._update_token()

            assert result["access_token"] == "updated_token"
            assert "expires_at" in result

    @pytest.mark.asyncio
    async def test_update_token_invalid_credentials(self, edgetech_client):
        """Test token update with invalid credentials."""
        error_response = {"error": "invalid_grant"}

        # Create mock response
        mock_response = AsyncMock()
        mock_response.status = 400
        mock_response.json = AsyncMock(return_value=error_response)

        # Create mock session
        mock_session = MockSession(post_responses=mock_response)

        with patch("aiohttp.ClientSession") as mock_client_session:
            mock_client_session.return_value = MockSessionContext(mock_session)

            with pytest.raises(InvalidCredentials):
                await edgetech_client._update_token()

    @pytest.mark.asyncio
    async def test_get_token_valid(self, edgetech_client):
        """Test get_token with valid token."""
        # Set token to be valid for another hour
        edgetech_client._token_json["expires_at"] = time.time() + 3600

        token = await edgetech_client.get_token()

        assert token == edgetech_client._token_json
        assert "access_token" in token

    @pytest.mark.asyncio
    async def test_get_token_expired(self, edgetech_client):
        """Test get_token with expired token."""
        # Set token to be expired
        edgetech_client._token_json["expires_at"] = time.time() - 100

        mock_response_data = {
            "access_token": "refreshed_token",
            "expires_in": 3600,
            "token_type": "Bearer",
        }

        # Create mock response
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=mock_response_data)

        # Create mock session
        mock_session = MockSession(post_responses=mock_response)

        with patch("aiohttp.ClientSession") as mock_client_session:
            mock_client_session.return_value = MockSessionContext(mock_session)

            token = await edgetech_client.get_token()

            assert token["access_token"] == "refreshed_token"

    @pytest.mark.asyncio
    async def test_download_data_success(self, edgetech_client, sample_buoy_data):
        """Test successful data download."""
        # Mock compressed data
        json_data = json.dumps(sample_buoy_data).encode("utf-8")
        compressed_data = gzip.compress(json_data)

        # Mock initial POST response (start dump)
        mock_post_response = AsyncMock()
        mock_post_response.status = 303
        mock_post_response.headers = {"Location": "/dump/123"}

        # Mock GET response (check dump status)
        mock_get_response = AsyncMock()
        mock_get_response.status = 303
        mock_get_response.headers = {"Location": "https://download.url/file.gz"}

        # Mock download response
        mock_download_response = AsyncMock()
        mock_download_response.headers = {
            "Content-Disposition": 'attachment; filename="data.gz"'
        }
        mock_download_response.read = AsyncMock(return_value=compressed_data)

        # Create mock session with custom get method to handle different URLs
        mock_session = MockSession(post_responses=mock_post_response)

        def custom_get(url, **kwargs):
            if "dump" in url:
                return MockGetContext(mock_get_response)
            else:
                return MockGetContext(mock_download_response)

        mock_session.get = custom_get

        with patch("aiohttp.ClientSession") as mock_client_session:
            mock_client_session.return_value = MockSessionContext(mock_session)

            buoys = await edgetech_client.download_data()

            assert len(buoys) == 1
            assert isinstance(buoys[0], Buoy)
            assert buoys[0].serialNumber == "TEST123"

    @pytest.mark.asyncio
    async def test_download_data_with_start_datetime_filter(
        self, edgetech_client, sample_buoy_data
    ):
        """Test data download with start_datetime filter."""
        # Modify sample data to have different timestamps
        old_data = sample_buoy_data[0].copy()
        old_data["currentState"] = old_data["currentState"].copy()
        old_data["currentState"]["lastUpdated"] = "2022-01-01T12:00:00.000Z"
        old_data["serialNumber"] = "TEST123"
        old_data["currentState"]["serialNumber"] = "TEST123"

        new_data = sample_buoy_data[0].copy()
        new_data["currentState"] = new_data["currentState"].copy()
        new_data["serialNumber"] = "TEST456"
        new_data["currentState"]["serialNumber"] = "TEST456"
        new_data["currentState"]["lastUpdated"] = "2023-06-01T12:00:00.000Z"

        all_data = [old_data, new_data]

        # Mock compressed data
        json_data = json.dumps(all_data).encode("utf-8")
        compressed_data = gzip.compress(json_data)

        start_datetime = datetime(2023, 1, 1, tzinfo=timezone.utc)

        # Mock responses
        mock_post_response = AsyncMock()
        mock_post_response.status = 303
        mock_post_response.headers = {"Location": "/dump/123"}

        mock_get_response = AsyncMock()
        mock_get_response.status = 303
        mock_get_response.headers = {"Location": "https://download.url/file.gz"}

        mock_download_response = AsyncMock()
        mock_download_response.headers = {
            "Content-Disposition": 'attachment; filename="data.gz"'
        }
        mock_download_response.read = AsyncMock(return_value=compressed_data)

        # Create mock session
        mock_session = MockSession(post_responses=mock_post_response)

        def custom_get(url, **kwargs):
            if "dump" in url:
                return MockGetContext(mock_get_response)
            else:
                return MockGetContext(mock_download_response)

        mock_session.get = custom_get

        with patch("aiohttp.ClientSession") as mock_client_session:
            mock_client_session.return_value = MockSessionContext(mock_session)

            buoys = await edgetech_client.download_data(start_datetime=start_datetime)

            # Should only return the new data (after start_datetime)
            assert len(buoys) == 1
            assert buoys[0].serialNumber == "TEST456"

    @pytest.mark.asyncio
    async def test_download_data_invalid_start_response(self, edgetech_client):
        """Test download_data with invalid start response."""
        mock_post_response = AsyncMock()
        mock_post_response.status = 400

        mock_session = MockSession(post_responses=mock_post_response)

        with patch("aiohttp.ClientSession") as mock_client_session:
            mock_client_session.return_value = MockSessionContext(mock_session)

            with pytest.raises(ValueError, match="Invalid response: 400"):
                await edgetech_client.download_data()

    @pytest.mark.asyncio
    async def test_download_data_missing_location_header(self, edgetech_client):
        """Test download_data with missing location header."""
        mock_post_response = AsyncMock()
        mock_post_response.status = 303
        mock_post_response.headers = {}  # Missing Location header

        mock_session = MockSession(post_responses=mock_post_response)

        with patch("aiohttp.ClientSession") as mock_client_session:
            mock_client_session.return_value = MockSessionContext(mock_session)

            with pytest.raises(ValueError, match="Missing Location header in response"):
                await edgetech_client.download_data()

    @pytest.mark.asyncio
    async def test_download_data_missing_download_location(self, edgetech_client):
        """Test download_data with missing download location header."""
        # Mock initial POST
        mock_post_response = AsyncMock()
        mock_post_response.status = 303
        mock_post_response.headers = {"Location": "/dump/123"}

        # Mock GET without Location header
        mock_get_response = AsyncMock()
        mock_get_response.status = 303
        mock_get_response.headers = {}  # Missing Location header

        mock_session = MockSession(
            post_responses=mock_post_response, get_responses=mock_get_response
        )

        with patch("aiohttp.ClientSession") as mock_client_session:
            mock_client_session.return_value = MockSessionContext(mock_session)

            with pytest.raises(
                ValueError, match="Missing Location header in download response"
            ):
                await edgetech_client.download_data()

    @pytest.mark.asyncio
    async def test_download_data_missing_filename(self, edgetech_client):
        """Test download_data with missing filename in Content-Disposition."""
        json_data = json.dumps([]).encode("utf-8")
        compressed_data = gzip.compress(json_data)

        # Mock initial POST
        mock_post_response = AsyncMock()
        mock_post_response.status = 303
        mock_post_response.headers = {"Location": "/dump/123"}

        # Mock GET
        mock_get_response = AsyncMock()
        mock_get_response.status = 303
        mock_get_response.headers = {"Location": "https://download.url/file.gz"}

        # Mock download without proper Content-Disposition
        mock_download_response = AsyncMock()
        mock_download_response.headers = {
            "Content-Disposition": "attachment"
        }  # No filename
        mock_download_response.read = AsyncMock(return_value=compressed_data)

        mock_session = MockSession(post_responses=mock_post_response)

        def custom_get(url, **kwargs):
            if "dump" in url:
                return MockGetContext(mock_get_response)
            else:
                return MockGetContext(mock_download_response)

        mock_session.get = custom_get

        with patch("aiohttp.ClientSession") as mock_client_session:
            mock_client_session.return_value = MockSessionContext(mock_session)

            with pytest.raises(
                ValueError, match="Filename not found in Content-Disposition header"
            ):
                await edgetech_client.download_data()

    @pytest.mark.asyncio
    async def test_download_data_retry_mechanism(
        self, edgetech_client, sample_buoy_data
    ):
        """Test the retry mechanism in download_data."""
        json_data = json.dumps(sample_buoy_data).encode("utf-8")
        compressed_data = gzip.compress(json_data)

        # Mock initial POST
        mock_post_response = AsyncMock()
        mock_post_response.status = 303
        mock_post_response.headers = {"Location": "/dump/123"}

        # Mock GET requests - first few return 200 (still processing), last returns 303 (ready)
        mock_get_processing = AsyncMock()
        mock_get_processing.status = 200

        mock_get_ready = AsyncMock()
        mock_get_ready.status = 303
        mock_get_ready.headers = {"Location": "https://download.url/file.gz"}

        mock_download_response = AsyncMock()
        mock_download_response.headers = {
            "Content-Disposition": 'attachment; filename="data.gz"'
        }
        mock_download_response.read = AsyncMock(return_value=compressed_data)

        mock_session = MockSession(post_responses=mock_post_response)

        call_count = 0

        def custom_get(url, **kwargs):
            nonlocal call_count
            if "dump" in url:
                call_count += 1
                if call_count <= 2:  # First 2 calls return 200
                    return MockGetContext(mock_get_processing)
                else:  # Third call returns 303
                    return MockGetContext(mock_get_ready)
            else:
                return MockGetContext(mock_download_response)

        mock_session.get = custom_get

        with (
            patch("aiohttp.ClientSession") as mock_client_session,
            patch("asyncio.sleep", new_callable=AsyncMock),
        ):
            mock_client_session.return_value = MockSessionContext(mock_session)

            buoys = await edgetech_client.download_data()

        assert len(buoys) == 1
        assert call_count == 3  # Should have made 3 attempts
