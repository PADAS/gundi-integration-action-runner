from datetime import datetime, timedelta, timezone
import asyncio

import pytest
import httpx
import respx

from app.actions.gfwclient import DataAPI, DataAPIKeysResponse, CartoDBClient


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
    assert len([log for log in caplog.messages if "Backing off" in log]) == 2
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
    assert len([log for log in caplog.messages if "Backing off" in log]) == 2
    assert len(alerts) == len(f_get_alerts_response['data'])


@pytest.mark.asyncio
@respx.mock
async def test_get_fire_alerts(f_get_fire_alerts_response):
    client = CartoDBClient()
    
    respx.post("https://wri-01.carto.com/api/v2/sql").respond(status_code=200, json=f_get_fire_alerts_response)
    fire_alerts = await client.get_fire_alerts(geojson={"type":"FeatureCollection","features":[{"type":"Feature","properties":{},"geometry":{"type":"Polygon","coordinates":[[[-74.17565,-7.98705],[-74.16675,-7.98785],[-74.09185,-7.95045],[-74.09185,-7.95025],[-74.09195,-7.95025],[-74.10715,-7.94975],[-74.11125,-7.94895],[-74.08285,-7.94575],[-74.06825,-7.94555],[-74.09495,-7.91765],[-74.17565,-7.98705]]]}}]},
                                 carto_url="https://wri-01.carto.com/api/v2/sql",
                                 fire_lookback_days=7)  
    assert len(fire_alerts) == len([r for r in f_get_fire_alerts_response['rows'] if r['confidence'] in ('high', 'highest')]) 


@pytest.mark.asyncio
@respx.mock
async def test_get_fire_alerts_backs_off_3_times_then_gives_up(caplog):
    client = CartoDBClient()

    respx.post("https://wri-01.carto.com/api/v2/sql").mock(
        side_effect=[
            httpx.Response(status_code=504, json={}),
            httpx.Response(status_code=504, json={}),
            httpx.Response(status_code=504, json={})
        ]
    )
    fire_alerts = await client.get_fire_alerts(geojson={"type": "FeatureCollection", "features": [
        {"type": "Feature", "properties": {}, "geometry": {"type": "Polygon", "coordinates": [
            [[-74.17565, -7.98705], [-74.16675, -7.98785], [-74.09185, -7.95045], [-74.09185, -7.95025],
             [-74.09195, -7.95025], [-74.10715, -7.94975], [-74.11125, -7.94895], [-74.08285, -7.94575],
             [-74.06825, -7.94555], [-74.09495, -7.91765], [-74.17565, -7.98705]]]}}]},
                                               carto_url="https://wri-01.carto.com/api/v2/sql",
                                               fire_lookback_days=7)
    assert len([i.response.status_code for i in respx.calls if i.response.status_code == 504]) == 3
    assert len([log for log in caplog.messages if "Backing off" in log]) == 2
    assert len([log for log in caplog.messages if "Giving up" in log]) == 1
    assert fire_alerts == []


@pytest.mark.asyncio
@respx.mock
async def test_get_fire_alerts_backs_off_2_times_then_succeed(caplog, f_get_fire_alerts_response):
    client = CartoDBClient()

    respx.post("https://wri-01.carto.com/api/v2/sql").mock(
        side_effect=[
            httpx.Response(504),
            httpx.Response(504),
            httpx.Response(status_code=200, json=f_get_fire_alerts_response)
        ]
    )
    fire_alerts = await client.get_fire_alerts(geojson={"type": "FeatureCollection", "features": [
        {"type": "Feature", "properties": {}, "geometry": {"type": "Polygon", "coordinates": [
            [[-74.17565, -7.98705], [-74.16675, -7.98785], [-74.09185, -7.95045], [-74.09185, -7.95025],
             [-74.09195, -7.95025], [-74.10715, -7.94975], [-74.11125, -7.94895], [-74.08285, -7.94575],
             [-74.06825, -7.94555], [-74.09495, -7.91765], [-74.17565, -7.98705]]]}}]},
                                               carto_url="https://wri-01.carto.com/api/v2/sql",
                                               fire_lookback_days=7)
    assert len([i.response.status_code for i in respx.calls if i.response.status_code == 504]) == 2
    assert len([log for log in caplog.messages if "Backing off" in log]) == 2
    assert len(fire_alerts) == len([r for r in f_get_fire_alerts_response['rows'] if r['confidence'] in ('high', 'highest')])
