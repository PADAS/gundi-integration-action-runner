import gzip
import json
import re
import time

import pytest

from app.actions.edgetech.client import EdgeTechClient


@pytest.mark.asyncio
async def test_download_data_success(
    mocker, a_good_pull_configuration, a_good_auth_configuration, get_mock_edgetech_data
):
    """
    Verify that EdgeTechClient.download_data returns a list of Buoy objects
    when the HTTP interactions complete successfully.
    """
    # Ensure the token is valid by updating its expiry.
    token_data = json.loads(a_good_auth_configuration.token_json.get_secret_value())
    token_data["expires_at"] = time.time() + 3600  # valid for one hour
    a_good_auth_configuration.token_json = type(a_good_auth_configuration.token_json)(
        json.dumps(token_data)
    )

    # FakeResponse simulates an aiohttp response.
    class FakeResponse:
        def __init__(self, status, headers, body=None):
            self.status = status
            self.headers = headers
            self._body = body

        async def read(self):
            return self._body

        async def json(self):
            return {}  # For token refresh, if needed.

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            pass

    # FakeSession simulates aiohttp.ClientSession.
    class FakeSession:
        def __init__(self, api_base_url, edgetech_data):
            # Normalize the base URL (remove trailing slash).
            self.api_base_url = api_base_url.rstrip("/")
            self.edgetech_data = edgetech_data
            self.get_call_count = 0

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            pass

        def post(self, url, **kwargs):
            # The client uses the database_dump_url property.
            if url == a_good_pull_configuration.database_dump_url:
                # Simulate a 303 redirect with a dump location.
                return FakeResponse(303, {"Location": "/dump/location"})
            return FakeResponse(200, {})

        def get(self, url, **kwargs):
            # Use endswith to catch both "/dump/location" and "//dump/location"
            if url.endswith("/dump/location"):
                if self.get_call_count == 0:
                    self.get_call_count += 1
                    # First attempt returns 200.
                    return FakeResponse(200, {})
                else:
                    # Subsequent attempt returns 303 with a download URL.
                    return FakeResponse(303, {"Location": "/download/url"})
            elif url == "/download/url":
                # Simulate downloading a gzip-compressed JSON file.
                compressed_body = gzip.compress(
                    json.dumps(self.edgetech_data).encode("utf-8")
                )
                return FakeResponse(
                    200,
                    {"Content-Disposition": 'attachment; filename="data.gz"'},
                    body=compressed_body,
                )
            return FakeResponse(200, {})

    # Patch ClientSession and asyncio.sleep.
    fake_session = FakeSession(
        a_good_pull_configuration.api_base_url, get_mock_edgetech_data
    )
    mocker.patch("aiohttp.ClientSession", return_value=fake_session)
    mocker.patch("asyncio.sleep", return_value=None)

    client = EdgeTechClient(
        pull_config=a_good_pull_configuration, auth_config=a_good_auth_configuration
    )
    buoys = await client.download_data()

    # Validate that we received Buoy objects and that one key property matches.
    assert isinstance(buoys, list)
    assert len(buoys) == len(get_mock_edgetech_data)
    assert buoys[0].serialNumber == get_mock_edgetech_data[0]["serialNumber"]


@pytest.mark.asyncio
async def test_download_data_invalid_initial_response(
    mocker, a_good_pull_configuration, a_good_auth_configuration
):
    """
    Verify that EdgeTechClient.download_data raises a ValueError if the initial POST
    does not return a 303 status.
    """
    a_good_pull_configuration.num_get_retry = 1
    token_data = json.loads(a_good_auth_configuration.token_json.get_secret_value())
    token_data["expires_at"] = time.time() + 3600  # token valid
    a_good_auth_configuration.token_json = type(a_good_auth_configuration.token_json)(
        json.dumps(token_data)
    )

    class FakeResponse:
        def __init__(self, status, headers):
            self.status = status
            self.headers = headers

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            pass

    class FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            pass

        def post(self, url, **kwargs):
            # Simulate an invalid response (e.g. 400 instead of 303)
            return FakeResponse(400, {})

        def get(self, url, **kwargs):
            return FakeResponse(200, {})

    mocker.patch("aiohttp.ClientSession", return_value=FakeSession())
    client = EdgeTechClient(
        pull_config=a_good_pull_configuration, auth_config=a_good_auth_configuration
    )
    with pytest.raises(ValueError, match="Invalid response: 400"):
        await client.download_data()


@pytest.mark.asyncio
async def test_buoy_create_observation(mock_edgetech_items):
    """
    Verify that a Buoy instance correctly builds an observation dictionary using
    the create_observation method.
    """
    buoy = mock_edgetech_items[0]
    prefix = "TEST-"
    observation = buoy.create_observation(prefix)

    expected_name = f"{prefix}{buoy.serialNumber}"
    assert observation["name"] == expected_name
    assert observation["source"] == expected_name
    assert observation["type"] == "ropeless_buoy"
    assert observation["subject_type"] == "ropeless_buoy_device"
    assert re.match(
        r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?$",
        observation["recorded_at"],
    )
    assert observation["location"]["lat"] == buoy.currentState.latDeg
    assert observation["location"]["lon"] == buoy.currentState.lonDeg
