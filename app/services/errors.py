import asyncio
from typing import NamedTuple, Optional

import aiohttp
import httpx


class ActionNotFound(Exception):
    pass


class ConfigurationNotFound(Exception):
    pass


class ConfigurationValidationError(Exception):
    pass


class ActionExecutionError(Exception):
    pass


class IntegrationError(Exception):
    """Base for classified third-party failures.

    Subclasses set `error_type` (machine-readable category) and
    `default_title` (human-first phrase shown in the portal activity log).

    Cooperates with client exception hierarchies (e.g. a connector's own
    base exception) that set `message`/`status_code` before calling
    super().__init__(message): a status_code already set by an earlier
    __init__ in the MRO is never clobbered with None.
    """
    error_type = "unknown"
    default_title = "Error"

    def __init__(self, message: str = "", status_code: Optional[int] = None):
        super().__init__(message or self.default_title)
        self.message = message or self.default_title
        if status_code is not None:
            self.status_code = status_code
        elif not hasattr(self, "status_code"):
            self.status_code = None


class IntegrationAuthError(IntegrationError):
    error_type = "auth"
    default_title = "Authentication failed"


class IntegrationConnectionError(IntegrationError):
    error_type = "connectivity"
    default_title = "Could not reach the provider"


class IntegrationRateLimitError(IntegrationError):
    error_type = "rate_limit"
    default_title = "Rate limited by the provider"


class IntegrationBadResponseError(IntegrationError):
    error_type = "bad_response"
    default_title = "Unexpected response from the provider"


class ClassifiedError(NamedTuple):
    error_type: str
    title: str
    message: str
    status_code: Optional[int]


# Exceptions that mean the provider could not be reached at all.
CONNECTIVITY_EXCEPTIONS = (
    asyncio.TimeoutError,  # distinct from builtin TimeoutError until Python 3.11
    TimeoutError,  # builtin: also covers socket.timeout (alias since 3.10)
    ConnectionError,  # builtin: covers ConnectionRefusedError, ConnectionResetError, etc.
    httpx.TransportError,  # covers ConnectError, ReadTimeout, and all transport failures
    aiohttp.ClientConnectionError,
)


def classify_error(exc: Exception) -> Optional[ClassifiedError]:
    """Classify a third-party failure for consistent activity-log reporting.

    Explicitly raised `IntegrationError` subclasses always win. Otherwise fall
    back to heuristics based on signals the action runner already reads
    (`exc.response.status_code`, exception type). Returns None when the error
    can't be classified — callers keep the generic format.
    """
    if isinstance(exc, IntegrationError):
        return ClassifiedError(
            error_type=exc.error_type,
            title=exc.default_title,
            message=getattr(exc, "message", None) or "",
            status_code=getattr(exc, "status_code", None),
        )

    # getattr chain: non-HTTP exceptions have no .response attribute.
    status_code = getattr(getattr(exc, "response", None), "status_code", None)
    # httpx.HTTPStatusError from raise_for_status() stringifies to multi-line
    # text (URL plus a "For more information check: ..." line) — only the
    # first line is useful as a short, human-first message.
    first_line = (str(exc).splitlines() or [""])[0]
    if status_code in (401, 403):
        return ClassifiedError("auth", IntegrationAuthError.default_title, first_line, status_code)
    if status_code == 429:
        return ClassifiedError("rate_limit", IntegrationRateLimitError.default_title, first_line, status_code)
    if status_code is not None and status_code >= 500:
        return ClassifiedError("bad_response", IntegrationBadResponseError.default_title, first_line, status_code)
    if isinstance(exc, CONNECTIVITY_EXCEPTIONS):
        return ClassifiedError("connectivity", IntegrationConnectionError.default_title, first_line, None)
    return None


def format_classified_error(classified: ClassifiedError) -> str:
    """Build the clean text: "<title> — <message> (HTTP <status>)".

    The portal prepends "Error running action '<id>': " to this string, so it
    must be short and lead with what an operator needs to see. The message
    segment is skipped when redundant; the HTTP suffix when unknown.
    """
    text = classified.title
    if classified.message and classified.message != classified.title:
        text = f"{text} — {classified.message}"
    if classified.status_code:
        text = f"{text} (HTTP {classified.status_code})"
    return text


def format_error_message(exc: Exception) -> Optional[str]:
    """Return clean, human-first error text for classified errors, or None."""
    classified = classify_error(exc)
    return format_classified_error(classified) if classified else None
