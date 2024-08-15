from datetime import datetime, timedelta, timezone
import asyncio

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
                "expires_on": (datetime.now(tz=timezone.utc) + timedelta(days=10)).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
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
            "expires_on": (datetime.now(tz=timezone.utc) + timedelta(days=10)).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "api_key": "1234567890",
            "alias": "test_key",
            "email": "test@example.com",
            "organization": "EarthRanger",
            "domains": []
        }
    }

@pytest.fixture
def f_api_keys_with_one_expired_response():
    return {
        "data": [
            {
                "created_on": "2021-09-14T08:00:00.000Z",
                "updated_on": "2021-09-14T08:00:00.000Z",
                "user_id": "er_user",
                "expires_on": (datetime.now(tz=timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "api_key": "1234567890",
                "alias": "test_key",
                "email": "test@example.com",
                "organization": "EarthRanger",
                "domains": []
            }
        ]
    }


@pytest.mark.asyncio
@respx.mock
async def test_get_api_keys(f_api_keys_response, f_auth_token_response, f_create_api_key_response):
    '''
    Test the code to authenticate and fetch (and conditionally create) API Keys.
    '''
    access_token_route = respx.post(f"{DataAPI.DATA_API_URL}/auth/token").respond(status_code=200, json=f_auth_token_response)

    # Mock lookup for apikeys, before and after creation.
    respx.get(f"{DataAPI.DATA_API_URL}/auth/apikeys").mock(
        side_effect=[httpx.Response(status_code=404), httpx.Response(status_code=200, json=f_api_keys_response)]
    )
    
    create_api_key_route = respx.post(f"{DataAPI.DATA_API_URL}/auth/apikey").mock(
        side_effect=[httpx.Response(status_code=201, json=f_create_api_key_response),]
    )

    client = DataAPI(username="test@example.com", password="test_password")
    api_keys = await client.get_api_keys()
    assert api_keys == DataAPIKeysResponse.parse_obj(f_api_keys_response).data
    assert create_api_key_route.called
    assert create_api_key_route.call_count == 1
    assert access_token_route.called


@pytest.mark.asyncio
@respx.mock
async def test_get_api_keys_when_one_is_expired(f_api_keys_with_one_expired_response, f_auth_token_response, f_create_api_key_response, f_api_keys_response):
    '''
    Test the code to authenticate and fetch (and conditionally create) API Keys.
    '''
    access_token_route = respx.post(f"{DataAPI.DATA_API_URL}/auth/token").respond(status_code=200, json=f_auth_token_response)

    # Mock lookup for apikeys, before and after creation.
    get_api_keys = respx.get(f"{DataAPI.DATA_API_URL}/auth/apikeys").mock(
        side_effect=[httpx.Response(status_code=200, json=f_api_keys_with_one_expired_response), httpx.Response(status_code=200, json=f_api_keys_response)]
    )
    
    create_api_key_route = respx.post(f"{DataAPI.DATA_API_URL}/auth/apikey").mock(
        side_effect=[httpx.Response(status_code=201, json=f_create_api_key_response),]
    )

    client = DataAPI(username="test@example.com", password="test_password")
    api_keys = await client.get_api_keys()
    assert api_keys == DataAPIKeysResponse.parse_obj(f_api_keys_response).data
    assert get_api_keys.call_count == 2
    assert create_api_key_route.called
    assert create_api_key_route.call_count == 1
    assert access_token_route.called


@pytest.mark.asyncio
@respx.mock
async def test_fetch_integrated_alerts(f_api_keys_response, f_auth_token_response, f_create_api_key_response, f_get_alerts_response):

    respx.post(f"{DataAPI.DATA_API_URL}/auth/token").respond(status_code=200, json=f_auth_token_response)

    # Mock lookup for apikeys, before and after creation.
    respx.get(f"{DataAPI.DATA_API_URL}/auth/apikeys").mock(
        side_effect=[httpx.Response(status_code=404), httpx.Response(status_code=200, json=f_api_keys_response)]
    )
    
    respx.post(f"{DataAPI.DATA_API_URL}/auth/apikey").mock(
        side_effect=[httpx.Response(status_code=201, json=f_create_api_key_response),]
    )
    '''
    Test the code to fetch integrated alerts.
    '''
    dataset = 'gfw_integrated_alerts'
    respx.get(f"{DataAPI.DATA_API_URL}/dataset/{dataset}/latest/query/json").mock(
        side_effect=[httpx.Response(status_code=200, json=f_get_alerts_response)]
    )

    client = DataAPI(username="test@example.com", password="test_password")
    end_date = datetime(2024,7,30, tzinfo=timezone.utc)
    start_date = end_date - timedelta(days=7)
    sema = asyncio.Semaphore(5)
    alerts = await client.get_gfw_integrated_alerts(geostore_id="668c84df810f3b001fe61acf", date_range=(start_date, end_date), semaphore=sema)

    assert len(alerts) == len(f_get_alerts_response['data'])


@pytest.mark.asyncio
@respx.mock
async def test_fetch_integrated_alerts_backs_off_3_times_then_gives_up(
        caplog,
        f_api_keys_response,
        f_auth_token_response,
        f_create_api_key_response
):
    respx.post(f"{DataAPI.DATA_API_URL}/auth/token").respond(status_code=200, json=f_auth_token_response)

    # Mock lookup for apikeys, before and after creation.
    respx.get(f"{DataAPI.DATA_API_URL}/auth/apikeys").mock(
        side_effect=[httpx.Response(status_code=404), httpx.Response(status_code=200, json=f_api_keys_response)]
    )

    respx.post(f"{DataAPI.DATA_API_URL}/auth/apikey").mock(
        side_effect=[httpx.Response(status_code=201, json=f_create_api_key_response), ]
    )
    '''
    Test the code to fetch integrated alerts.
    '''
    dataset = 'gfw_integrated_alerts'
    # Calling GFW DataApi 3 times with 504 response
    respx.get(f"{DataAPI.DATA_API_URL}/dataset/{dataset}/latest/query/json").mock(
        side_effect=[httpx.Response(504), httpx.Response(504), httpx.Response(504)]
    )

    client = DataAPI(username="test@example.com", password="test_password")
    end_date = datetime(2024, 7, 30, tzinfo=timezone.utc)
    start_date = end_date - timedelta(days=7)
    sema = asyncio.Semaphore(5)

    alerts = await client.get_gfw_integrated_alerts(
        geostore_id="668c84df810f3b001fe61acf",
        date_range=(start_date, end_date), semaphore=sema
    )

    assert len([i.response.status_code for i in respx.calls if i.response.status_code == 504]) == 3
    assert len([log for log in caplog.messages if "Backing off" in log]) == 4
    assert len([log for log in caplog.messages if "Giving up" in log]) == 1
    assert alerts == []


@pytest.mark.asyncio
@respx.mock
async def test_fetch_integrated_alerts_backs_off_2_times_then_succeed(
        caplog,
        f_api_keys_response,
        f_auth_token_response,
        f_create_api_key_response,
        f_get_alerts_response
):
    respx.post(f"{DataAPI.DATA_API_URL}/auth/token").respond(status_code=200, json=f_auth_token_response)

    # Mock lookup for apikeys, before and after creation.
    respx.get(f"{DataAPI.DATA_API_URL}/auth/apikeys").mock(
        side_effect=[httpx.Response(status_code=404), httpx.Response(status_code=200, json=f_api_keys_response)]
    )

    respx.post(f"{DataAPI.DATA_API_URL}/auth/apikey").mock(
        side_effect=[httpx.Response(status_code=201, json=f_create_api_key_response), ]
    )
    '''
    Test the code to fetch integrated alerts.
    '''
    dataset = 'gfw_integrated_alerts'
    # Calling GFW DataApi 2 times with 504 response, then 200
    respx.get(f"{DataAPI.DATA_API_URL}/dataset/{dataset}/latest/query/json").mock(
        side_effect=[
            httpx.Response(504),
            httpx.Response(504),
            httpx.Response(status_code=200, json=f_get_alerts_response)
        ]
    )

    client = DataAPI(username="test@example.com", password="test_password")
    end_date = datetime(2024, 7, 30, tzinfo=timezone.utc)
    start_date = end_date - timedelta(days=7)
    sema = asyncio.Semaphore(5)
    alerts = await client.get_gfw_integrated_alerts(
        geostore_id="668c84df810f3b001fe61acf",
        date_range=(start_date, end_date), semaphore=sema
    )
    assert len([i.response.status_code for i in respx.calls if i.response.status_code == 504]) == 2
    assert len([log for log in caplog.messages if "Backing off" in log]) == 4
    assert len(alerts) == len(f_get_alerts_response['data'])

