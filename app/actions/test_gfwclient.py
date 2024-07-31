import pytest
import httpx
import respx

from app.actions.gfwclient import DataAPI, DataAPIKeysResponse

@pytest.fixture
def f_api_keys_response():
    return {
        "data": [
            {
                "created_on": "2021-09-14T08:00:00.000Z",
                "updated_on": "2021-09-14T08:00:00.000Z",
                "user_id": "er_user",
                "expires_on": "2025-09-14T08:00:00.000Z",
                "api_key": "1234567890",
                "alias": "test_key",
                "email": "test@example.com",
                "organization": "EarthRanger",
                "domains": []
            }
        ]
    }

@pytest.fixture
def f_auth_token_response():
    return {
        "data": 
            {
                "access_token": "a fancy access token",
                "token_type": "bearer",
                "expires_in": 3600
            }
    }

@pytest.fixture
def f_create_api_key_response():
    return {
        "data": {
            "created_on": "2021-09-14T08:00:00.000Z",
            "updated_on": "2021-09-14T08:00:00.000Z",
            "user_id": "er_user",
            "expires_on": "2025-09-14T08:00:00.000Z",
            "api_key": "1234567890",
            "alias": "test_key",
            "email": "test@example.com",
            "organization": "EarthRanger",
            "domains": []
        }
    }

@pytest.mark.asyncio
@respx.mock
async def test_get_api_keys(f_api_keys_response, f_auth_token_response, f_create_api_key_response):
    # Mock the response from the API

    access_token_route = respx.post("https://data-api.globalforestwatch.org/auth/token").respond(status_code=200, json=f_auth_token_response)

    # Mock lookup for apikeys, before and after creation.
    respx.get("https://data-api.globalforestwatch.org/auth/apikeys").mock(
        side_effect=[httpx.Response(status_code=404), httpx.Response(status_code=200, json=f_api_keys_response)]
    )
    
    create_api_key_route = respx.post("https://data-api.globalforestwatch.org/auth/apikey").mock(
        side_effect=[httpx.Response(status_code=201, json=f_create_api_key_response),]
    )

    client = DataAPI(username="test@example.com", password="test_password")
    api_keys = await client.get_api_keys()
    assert api_keys == DataAPIKeysResponse.parse_obj(f_api_keys_response).data
    assert create_api_key_route.called
    assert create_api_key_route.call_count == 1
    assert access_token_route.called