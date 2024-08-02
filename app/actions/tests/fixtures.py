import pytest


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


@pytest.fixture
def f_get_alerts_response():
    return {
        "data": [{'latitude': -7.91765, 'longitude': -74.09495, 'gfw_integrated_alerts__confidence': 'highest', 'gfw_integrated_alerts__date': '2024-07-25'}, {'latitude': -7.94555, 'longitude': -74.06825, 'gfw_integrated_alerts__confidence': 'highest', 'gfw_integrated_alerts__date': '2024-07-25'}, {'latitude': -7.94575, 'longitude': -74.08285, 'gfw_integrated_alerts__confidence': 'highest', 'gfw_integrated_alerts__date': '2024-07-25'}, {'latitude': -7.94895, 'longitude': -74.11125, 'gfw_integrated_alerts__confidence': 'highest', 'gfw_integrated_alerts__date': '2024-07-25'}, {'latitude': -7.94975, 'longitude': -74.10715, 'gfw_integrated_alerts__confidence': 'highest', 'gfw_integrated_alerts__date': '2024-07-25'}, {'latitude': -7.95025, 'longitude': -74.09195, 'gfw_integrated_alerts__confidence': 'highest', 'gfw_integrated_alerts__date': '2024-07-25'}, {'latitude': -7.95025, 'longitude': -74.09185, 'gfw_integrated_alerts__confidence': 'highest', 'gfw_integrated_alerts__date': '2024-07-25'}, {'latitude': -7.95045, 'longitude': -74.09185, 'gfw_integrated_alerts__confidence': 'highest', 'gfw_integrated_alerts__date': '2024-07-25'}, {'latitude': -7.98705, 'longitude': -74.17565, 'gfw_integrated_alerts__confidence': 'highest', 'gfw_integrated_alerts__date': '2024-07-25'}, {'latitude': -7.98785, 'longitude': -74.16675, 'gfw_integrated_alerts__confidence': 'highest', 'gfw_integrated_alerts__date': '2024-07-25'}]
    }


@pytest.fixture
def f_get_fire_alerts_response():
    return {
        "rows": [
            {
                "cartodb_id": 1,
                "the_geom": "fake geom",
                "the_geom_webmercator": "fake geom",
                "latitude": -7.91765,
                "longitude": -74.09495,
                "bright_ti4": 328.1,
                "scan": 1,
                "track": 1,
                "acq_date": "2024-07-25 00:00:00",
                "acq_time": "00:00:00",
                "satellite": "Terra",
                "confidence": "high",
                "version": "1.0",
                "bright_ti5": 328.1,
                "frp": 328.1,
                "daynight": "D"
            },            {
                "cartodb_id": 1,
                "the_geom": "fake geom",
                "the_geom_webmercator": "fake geom",
                "latitude": -7.1,
                "longitude": -74.1,
                "bright_ti4": 328.1,
                "scan": 1,
                "track": 1,
                "acq_date": "2024-07-25 00:00:00",
                "acq_time": "00:00:00",
                "satellite": "Terra",
                "confidence": "nominal",
                "version": "1.0",
                "bright_ti5": 328.1,
                "frp": 328.1,
                "daynight": "D"
            },            {
                "cartodb_id": 1,
                "the_geom": "fake geom",
                "the_geom_webmercator": "fake geom",
                "latitude": -7.91765,
                "longitude": -74.09495,
                "bright_ti4": 328.1,
                "scan": 1,
                "track": 1,
                "acq_date": "2024-07-25 00:00:00",
                "acq_time": "00:00:00",
                "satellite": "Terra",
                "confidence": "highest",
                "version": "1.0",
                "bright_ti5": 328.1,
                "frp": 328.1,
                "daynight": "D"
            },            {
                "cartodb_id": 1,
                "the_geom": "fake geom",
                "the_geom_webmercator": "fake geom",
                "latitude": -7.91765,
                "longitude": -74.09495,
                "bright_ti4": 328.1,
                "scan": 1,
                "track": 1,
                "acq_date": "2024-07-25 00:00:00",
                "acq_time": "00:00:00",
                "satellite": "Terra",
                "confidence": "nominal",
                "version": "1.0",
                "bright_ti5": 328.1,
                "frp": 328.1,
                "daynight": "D"
            },            {
                "cartodb_id": 1,
                "the_geom": "fake geom",
                "the_geom_webmercator": "fake geom",
                "latitude": -7.91765,
                "longitude": -74.09495,
                "bright_ti4": 328.1,
                "scan": 1,
                "track": 1,
                "acq_date": "2024-07-25 00:00:00",
                "acq_time": "00:00:00",
                "satellite": "Terra",
                "confidence": "high",
                "version": "1.0",
                "bright_ti5": 328.1,
                "frp": 328.1,
                "daynight": "D"
            },
        ]
    }
