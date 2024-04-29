import pytest
from unittest.mock import patch
from fastapi.testclient import TestClient
from app.main import app
from app.services.self_registration import register_integration_in_gundi


api_client = TestClient(app)


@pytest.mark.asyncio
async def test_register_integration_with_slug_setting(mocker, mock_gundi_client_v2):
    mocker.patch("app.services.self_registration.INTEGRATION_TYPE_SLUG", "x_tracker")
    await register_integration_in_gundi(gundi_client=mock_gundi_client_v2)
    assert mock_gundi_client_v2.register_integration_type.called


@pytest.mark.asyncio
async def test_register_integration_with_slug_arg(mock_gundi_client_v2):
    await register_integration_in_gundi(gundi_client=mock_gundi_client_v2, type_slug="x_tracker")
    assert mock_gundi_client_v2.register_integration_type.called


@pytest.mark.asyncio
async def test_register_integration_with_slug_service_url(mocker, mock_gundi_client_v2):
    mocker.patch("app.services.self_registration.INTEGRATION_TYPE_SLUG", "x_tracker")
    await register_integration_in_gundi(
        gundi_client=mock_gundi_client_v2,
        service_url="https://xtracker-actions-runner-jabcutl8yb-uc.a.run.app"
    )
    assert mock_gundi_client_v2.register_integration_type.called

