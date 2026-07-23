import asyncio

import aiohttp
import httpx
import pytest

from app.services.errors import (
    IntegrationError,
    IntegrationAuthError,
    IntegrationConnectionError,
    IntegrationRateLimitError,
    IntegrationBadResponseError,
    classify_error,
    format_error_message,
)


@pytest.mark.parametrize(
    "exception_class,expected_type,expected_title",
    [
        (IntegrationAuthError, "auth", "Authentication failed"),
        (IntegrationConnectionError, "connectivity", "Could not reach the provider"),
        (IntegrationRateLimitError, "rate_limit", "Rate limited by the provider"),
        (IntegrationBadResponseError, "bad_response", "Unexpected response from the provider"),
    ],
)
def test_integration_error_subclasses_define_category(exception_class, expected_type, expected_title):
    exc = exception_class()

    assert isinstance(exc, IntegrationError)
    assert exc.error_type == expected_type
    assert exc.default_title == expected_title
    assert exc.message == expected_title  # defaults to the title when no message given
    assert exc.status_code is None


def test_integration_error_carries_message_and_status_code():
    exc = IntegrationAuthError("TrackIt rejected the credentials", status_code=401)

    assert exc.message == "TrackIt rejected the credentials"
    assert exc.status_code == 401
    assert str(exc) == "TrackIt rejected the credentials"


def test_integration_error_preserves_status_code_set_by_another_base():
    # Client exception hierarchies (e.g. TrackitBaseException) set status_code
    # BEFORE calling super().__init__(message) with no status_code argument.
    # IntegrationError must not clobber it with None.
    class ClientBase(Exception):
        def __init__(self, message, status_code=None):
            self.status_code = status_code
            self.message = message
            super().__init__(message)

    class ClientAuthError(ClientBase, IntegrationAuthError):
        pass

    exc = ClientAuthError("Unauthorized access", status_code=401)

    assert exc.status_code == 401
    assert exc.message == "Unauthorized access"


def _http_status_error(status_code: int) -> httpx.HTTPStatusError:
    request = httpx.Request("GET", "https://api.example.com/data")
    response = httpx.Response(status_code, request=request)
    return httpx.HTTPStatusError(f"HTTP {status_code}", request=request, response=response)


def test_classify_explicit_integration_error_wins_over_heuristics():
    exc = IntegrationBadResponseError("Provider returned XML instead of JSON", status_code=401)

    classified = classify_error(exc)

    # 401 would heuristically be auth, but the explicit type wins
    assert classified.error_type == "bad_response"
    assert classified.title == "Unexpected response from the provider"
    assert classified.message == "Provider returned XML instead of JSON"
    assert classified.status_code == 401


@pytest.mark.parametrize(
    "status_code,expected_type,expected_title",
    [
        (401, "auth", "Authentication failed"),
        (403, "auth", "Authentication failed"),
        (429, "rate_limit", "Rate limited by the provider"),
        (500, "bad_response", "Unexpected response from the provider"),
        (503, "bad_response", "Unexpected response from the provider"),
    ],
)
def test_classify_by_response_status_code(status_code, expected_type, expected_title):
    classified = classify_error(_http_status_error(status_code))

    assert classified.error_type == expected_type
    assert classified.title == expected_title
    assert classified.status_code == status_code


def test_classify_uses_only_first_line_of_multiline_exception_message():
    # Real httpx.HTTPStatusError from raise_for_status() stringifies to
    # multi-line text (URL plus a "For more information check: ..." line);
    # only the first line should end up in the classified message.
    request = httpx.Request("GET", "https://x")
    response = httpx.Response(500, request=request)
    exc = httpx.HTTPStatusError(
        "Server error '500' for url 'https://x'\nFor more information check: https://mozilla.org",
        request=request, response=response,
    )

    classified = classify_error(exc)

    assert classified.message == "Server error '500' for url 'https://x'"


@pytest.mark.parametrize(
    "exc",
    [
        asyncio.TimeoutError("timed out"),
        ConnectionRefusedError("connection refused"),
        httpx.ConnectError("connection failed"),
        httpx.ReadTimeout("read timed out"),
        aiohttp.ClientConnectionError("cannot connect"),
    ],
)
def test_classify_connectivity_errors(exc):
    classified = classify_error(exc)

    assert classified.error_type == "connectivity"
    assert classified.title == "Could not reach the provider"
    assert classified.status_code is None


@pytest.mark.parametrize(
    "exc",
    [
        ValueError("boom"),
        KeyError("missing"),
        _http_status_error(400),  # 4xx other than 401/403/429 stays unclassified
    ],
)
def test_classify_returns_none_for_unclassified_errors(exc):
    assert classify_error(exc) is None
    assert format_error_message(exc) is None


def test_format_full_message_with_status():
    exc = IntegrationAuthError("TrackIt rejected the credentials", status_code=401)

    assert format_error_message(exc) == "Authentication failed — TrackIt rejected the credentials (HTTP 401)"


def test_format_omits_message_segment_when_it_equals_the_title():
    exc = IntegrationAuthError(status_code=401)

    assert format_error_message(exc) == "Authentication failed (HTTP 401)"


def test_format_omits_http_suffix_without_status_code():
    exc = IntegrationConnectionError("DNS lookup failed")

    assert format_error_message(exc) == "Could not reach the provider — DNS lookup failed"


def test_classify_builtin_timeout_as_connectivity():
    # On Python 3.10, builtin TimeoutError (== socket.timeout) is distinct
    # from asyncio.TimeoutError; both must classify as connectivity.
    import socket

    for exc in (TimeoutError("timed out"), socket.timeout("timed out")):
        classified = classify_error(exc)
        assert classified is not None
        assert classified.error_type == "connectivity"
