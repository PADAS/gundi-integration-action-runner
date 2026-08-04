"""Contract tests for password-grant authentication.

The action runner does not implement authentication itself — it relies on
gundi-client-v2 selecting the OAuth2 password grant when user credentials are
present, and on every runner code path using a bare env-driven GundiClient.
These tests pin both halves of that contract against future upgrades.
"""
from unittest.mock import AsyncMock

import pytest

from gundi_client_v2 import GundiClient
from gundi_client_v2.auth import OAuthToken


def _token():
    return OAuthToken(
        access_token="test-access",
        refresh_token="test-refresh",
        token_type="Bearer",
        expires_in=300,
        refresh_expires_in=1800,
    )


@pytest.mark.asyncio
async def test_password_grant_selected_when_user_credentials_present(mocker):
    password_grant = mocker.patch(
        "gundi_client_v2.auth.get_access_token_password_grant",
        new_callable=AsyncMock,
        return_value=_token(),
    )
    client_credentials = mocker.patch(
        "gundi_client_v2.auth.get_access_token_client_credentials",
        new_callable=AsyncMock,
    )
    client = GundiClient(
        username="dev@example.com",
        password="not-a-real-password",
        oauth_client_id="cdip-oauth2",
        oauth_token_url="https://auth.example.com/token",
    )
    token = await client.get_access_token()
    assert token.access_token == "test-access"
    password_grant.assert_awaited_once()
    client_credentials.assert_not_called()


def test_runner_portal_is_bare_env_driven_client():
    """The runner must not override credentials on its portal client —
    bareness is what lets GUNDI_USERNAME/GUNDI_PASSWORD select the grant."""
    from gundi_client_v2 import settings as client_settings

    from gundi_action_runner.services.action_runner import _portal

    assert isinstance(_portal, GundiClient)
    assert _portal.username == client_settings.GUNDI_USERNAME
    assert _portal.password == client_settings.GUNDI_PASSWORD
    assert _portal.client_id == client_settings.OAUTH_CLIENT_ID
    assert _portal.client_secret == client_settings.OAUTH_CLIENT_SECRET
