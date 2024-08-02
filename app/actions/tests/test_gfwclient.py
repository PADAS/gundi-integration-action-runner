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


@pytest.fixture
def f_get_alerts_response():
    return {
        "data": [{'latitude': -7.91765, 'longitude': -74.09495, 'gfw_integrated_alerts__confidence': 'highest', 'gfw_integrated_alerts__date': '2024-07-25'}, {'latitude': -7.94555, 'longitude': -74.06825, 'gfw_integrated_alerts__confidence': 'highest', 'gfw_integrated_alerts__date': '2024-07-25'}, {'latitude': -7.94575, 'longitude': -74.08285, 'gfw_integrated_alerts__confidence': 'highest', 'gfw_integrated_alerts__date': '2024-07-25'}, {'latitude': -7.94895, 'longitude': -74.11125, 'gfw_integrated_alerts__confidence': 'highest', 'gfw_integrated_alerts__date': '2024-07-25'}, {'latitude': -7.94975, 'longitude': -74.10715, 'gfw_integrated_alerts__confidence': 'highest', 'gfw_integrated_alerts__date': '2024-07-25'}, {'latitude': -7.95025, 'longitude': -74.09195, 'gfw_integrated_alerts__confidence': 'highest', 'gfw_integrated_alerts__date': '2024-07-25'}, {'latitude': -7.95025, 'longitude': -74.09185, 'gfw_integrated_alerts__confidence': 'highest', 'gfw_integrated_alerts__date': '2024-07-25'}, {'latitude': -7.95045, 'longitude': -74.09185, 'gfw_integrated_alerts__confidence': 'highest', 'gfw_integrated_alerts__date': '2024-07-25'}, {'latitude': -7.98705, 'longitude': -74.17565, 'gfw_integrated_alerts__confidence': 'highest', 'gfw_integrated_alerts__date': '2024-07-25'}, {'latitude': -7.98785, 'longitude': -74.16675, 'gfw_integrated_alerts__confidence': 'highest', 'gfw_integrated_alerts__date': '2024-07-25'}]
    }

@pytest.mark.asyncio
@respx.mock
async def test_fetch_integrated_alerts(f_api_keys_response, f_auth_token_response, f_create_api_key_response, f_get_alerts_response):

    access_token_route = respx.post(f"{DataAPI.DATA_API_URL}/auth/token").respond(status_code=200, json=f_auth_token_response)

    # Mock lookup for apikeys, before and after creation.
    respx.get(f"{DataAPI.DATA_API_URL}/auth/apikeys").mock(
        side_effect=[httpx.Response(status_code=404), httpx.Response(status_code=200, json=f_api_keys_response)]
    )
    
    create_api_key_route = respx.post(f"{DataAPI.DATA_API_URL}/auth/apikey").mock(
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
