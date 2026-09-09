"""The Gundi API retry policies must recognise gundi-client-v2 3.x's errors.

The client reports a non-2xx Gundi response as GundiAPIError and a token
endpoint failure as AuthenticationError. Neither subclasses httpx.HTTPError,
which is all the policies retried on before the 3.7 upgrade, so a transient
503 or a Keycloak outage failed on the first attempt while the suite, which
simulated failures with httpx errors, stayed green.
"""
import asyncio
from unittest.mock import AsyncMock

import httpx
import pytest
import stamina
from gundi_client_v2.errors import AuthenticationError, GundiAPIError

from app.services.retry_policies import is_transient_gundi_error


@pytest.fixture
def no_backoff():
    """stamina's testing mode: no sleeps between attempts, attempts capped."""
    stamina.set_testing(True, attempts=3)
    yield
    stamina.set_testing(False)


def _status_error(status_code):
    request = httpx.Request("POST", "https://sensors.example.org/v2/observations/")
    return httpx.HTTPStatusError(
        f"HTTP {status_code}", request=request, response=httpx.Response(status_code, request=request)
    )


def _discovery_failure(outcome):
    """An OIDC discovery failure exactly as gundi_client_v2.auth wraps it (no
    status, no transport flag, httpx's error as the cause), produced by the
    library's own discovery call against an IdP that answers ``outcome``: an
    HTTP status, or an exception the transport raises."""
    from gundi_client_v2 import auth

    def idp(request):
        if isinstance(outcome, BaseException):
            raise outcome
        return httpx.Response(outcome)

    async def discover():
        async with httpx.AsyncClient(transport=httpx.MockTransport(idp)) as session:
            await auth.discover_token_endpoint(session, "https://auth.example.org/realms/x")

    try:
        asyncio.run(discover())
    except AuthenticationError as e:
        return e
    raise AssertionError("discover_token_endpoint did not raise")


@pytest.mark.parametrize("exc", [
    GundiAPIError(status_code=503, detail="upstream unavailable"),
    GundiAPIError(status_code=502),
    GundiAPIError(status_code=429, detail="slow down"),
    AuthenticationError("keycloak unreachable", transport=True),
    AuthenticationError("keycloak 503", status_code=503),
    AuthenticationError("keycloak rate limit", status_code=429),
    _discovery_failure(httpx.ConnectError("keycloak unreachable")),  # OIDC discovery, network
    _discovery_failure(503),  # OIDC discovery endpoint down during a Keycloak restart
    _discovery_failure(429),
    httpx.ConnectError("portal unreachable"),
    httpx.ReadTimeout("timed out"),
    _status_error(503),
])
def test_transient_failures_are_retried(exc):
    assert is_transient_gundi_error(exc) is True


@pytest.mark.parametrize("exc", [
    GundiAPIError(status_code=404, detail="integration not found"),
    GundiAPIError(status_code=400, detail="bad payload"),
    GundiAPIError(status_code=403),
    AuthenticationError("invalid_client", status_code=401, error="invalid_client"),
    AuthenticationError("invalid_grant", status_code=400, error="invalid_grant", refresh_token_rejected=True),
    # Status-less and not a transport failure: permanent misconfiguration.
    AuthenticationError("No token URL configured"),
    AuthenticationError("No credentials configured"),
    AuthenticationError("malformed token response"),
    _discovery_failure(404),  # OIDC discovery document missing
    _status_error(404),
    ValueError("not an HTTP problem at all"),
])
def test_definite_failures_are_not_retried(exc):
    assert is_transient_gundi_error(exc) is False


@pytest.mark.asyncio
async def test_send_observations_retries_a_transient_gundi_api_error(
        no_backoff, mocker, mock_gundi_sensors_client_class, mock_get_gundi_api_key,
):
    """End to end through the real GUNDI_API_RETRY: one 503 from the Sensors
    API, then success."""
    from app.services.gundi import send_observations_to_gundi

    mocker.patch("app.services.gundi.GundiDataSenderClient", mock_gundi_sensors_client_class)
    mocker.patch("app.services.gundi._get_gundi_api_key", mock_get_gundi_api_key)
    sensors = mock_gundi_sensors_client_class.return_value
    sensors.post_observations = AsyncMock(side_effect=[GundiAPIError(status_code=503), {"status": "ok"}])

    result = await send_observations_to_gundi(observations=[{"source": "x"}], integration_id="abc-123")

    assert result == {"status": "ok"}
    assert sensors.post_observations.await_count == 2


@pytest.mark.asyncio
async def test_send_observations_does_not_retry_a_client_error(
        no_backoff, mocker, mock_gundi_sensors_client_class, mock_get_gundi_api_key,
):
    from app.services.gundi import send_observations_to_gundi

    mocker.patch("app.services.gundi.GundiDataSenderClient", mock_gundi_sensors_client_class)
    mocker.patch("app.services.gundi._get_gundi_api_key", mock_get_gundi_api_key)
    sensors = mock_gundi_sensors_client_class.return_value
    sensors.post_observations = AsyncMock(side_effect=GundiAPIError(status_code=400, detail="bad payload"))

    with pytest.raises(GundiAPIError):
        await send_observations_to_gundi(observations=[{"source": "x"}], integration_id="abc-123")

    assert sensors.post_observations.await_count == 1


@pytest.mark.asyncio
async def test_registration_retries_a_portal_outage(no_backoff, mocker):
    """Self-registration keeps its own three-attempt policy; it must use the
    same predicate, or REGISTER_ON_START fails the boot on one portal 502."""
    from app.services import self_registration

    mocker.patch.object(self_registration, "action_handlers", {})
    client = mocker.MagicMock()
    client.register_integration_type = AsyncMock(side_effect=[GundiAPIError(status_code=502), {"id": "1"}])

    response = await self_registration.register_integration_in_gundi(gundi_client=client, type_slug="acme_tracker")

    assert response == {"id": "1"}
    assert client.register_integration_type.await_count == 2


@pytest.mark.asyncio
async def test_registration_does_not_retry_a_rejected_registration(no_backoff, mocker):
    from app.services import self_registration

    mocker.patch.object(self_registration, "action_handlers", {})
    client = mocker.MagicMock()
    client.register_integration_type = AsyncMock(side_effect=GundiAPIError(status_code=403, detail="forbidden"))

    with pytest.raises(GundiAPIError):
        await self_registration.register_integration_in_gundi(gundi_client=client, type_slug="acme_tracker")

    assert client.register_integration_type.await_count == 1
