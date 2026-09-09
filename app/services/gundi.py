"""Public helpers for talking to Gundi from an action handler.

Every write helper here, and `action_scheduler.trigger_action`, short-circuits
with `_block_if_ephemeral` on the ephemeral path (defense in depth on top of
the config-model whitelist in `action_runner.execute_action`). Guards only
cover code that routes through these helpers — handlers that construct
`GundiDataSenderClient`, an `httpx.AsyncClient`, or a PubSub publisher
directly are out of scope.
"""
import datetime
import logging
import time
from typing import Awaitable, Callable, List, TypeVar
import stamina
# app.settings before gundi_client_v2 (see app/services/errors.py).
from app import settings  # noqa: F401
from gundi_client_v2.client import GundiClient, GundiDataSenderClient
from gundi_client_v2.errors import GundiAPIError

from .activity_logger import ephemeral_run
from .retry_policies import is_transient_gundi_error

T = TypeVar("T")

logger = logging.getLogger(__name__)

# A portal that answers 401 to everything (the runner's OAuth client lost a
# role, the API broke) would otherwise cost one IdP token request per portal
# call: every call builds its own client, adopts the shared token, is
# rejected, evicts it and mints a replacement that is rejected in turn. That
# is a large amplification aimed at the IdP during an incident, where the
# shared cache had made it about one request per token lifetime. One forced
# refresh per process per cooldown bounds it. Per process, so replicas heal
# independently, and a genuine early invalidation still heals on the first
# 401: a healthy process has not forced a refresh recently.
FORCE_REFRESH_COOLDOWN_SECONDS = 60.0
_last_forced_refresh = None
_cooldown_warned = False


def reset_force_refresh_cooldown() -> None:
    """Forget the last forced refresh. For tests; the cooldown is process state."""
    global _last_forced_refresh, _cooldown_warned
    _last_forced_refresh = None
    _cooldown_warned = False


def _claim_forced_refresh() -> bool:
    """Take the process's one forced refresh for this cooldown, if it is free.

    Synchronous between the read and the write, so concurrent coroutines
    cannot both claim it.
    """
    global _last_forced_refresh, _cooldown_warned
    now = time.monotonic()
    if _last_forced_refresh is not None and now - _last_forced_refresh < FORCE_REFRESH_COOLDOWN_SECONDS:
        return False
    _last_forced_refresh = now
    _cooldown_warned = False
    return True


class EphemeralWriteBlocked(RuntimeError):
    """Blocked write from a reference/auth handler on the ephemeral path."""


def _block_if_ephemeral(op: str) -> None:
    if ephemeral_run.get():
        raise EphemeralWriteBlocked(
            f"{op} is not allowed on the ephemeral (draft-integration) path"
        )


async def with_fresh_token_on_401(client: GundiClient, call: Callable[[], Awaitable[T]]) -> T:
    """Run one portal call; if Gundi answers 401, replace the token and retry once.

    Since gundi-client-v2 3.7 every client shares one OAuth token per set of
    credentials, judged live by its expiry alone, and the client only fetches
    a new one on the API's login redirect, never on a 401. A token Keycloak
    invalidated early (a restart, a session revocation) would otherwise be
    served to every replica until it expires, failing every portal call with
    a 401 that the retry policy rightly treats as final. Before the shared
    cache each call minted its own token, so this healed itself.

    ``force_refresh_token`` evicts the shared entry (or adopts a replacement a
    sibling already fetched) and gets a fresh token, so the first replica to
    see the 401 heals the fleet; the others adopt without an IdP call. One
    replacement per call: a second 401 is the answer. Only ``GundiAPIError``
    qualifies: an ``AuthenticationError`` 401 is the token endpoint rejecting
    the credentials themselves, and no new token will change that.

    Replacing is rate-limited per process (FORCE_REFRESH_COOLDOWN_SECONDS): a
    portal that rejects every token must not turn each portal call into an IdP
    request. Past the limit the 401 is simply reported, which is what it means.
    """
    try:
        return await call()
    except GundiAPIError as e:
        if e.status_code != 401:
            raise
        if not _claim_forced_refresh():
            # A replacement token was fetched moments ago and this 401 is on
            # that one: minting another would not help the caller and would
            # aim the retry at the IdP. Warn once per cooldown, not once per
            # call: a scheduled pull would otherwise warn on every tick.
            global _cooldown_warned
            if not _cooldown_warned:
                _cooldown_warned = True
                logger.warning(
                    "Gundi answered 401 again within %ss of replacing the OAuth token; "
                    "reporting these instead of fetching another.", FORCE_REFRESH_COOLDOWN_SECONDS,
                )
            raise
    await client.get_auth_header(force_refresh_token=True)
    return await call()


# One retry policy for every request-time Gundi API call (the send helpers
# below and the config manager's reloads), defined once so the wait curve and
# the stop condition can't drift apart, and applied exactly once per call
# path: nesting it (a decorated helper calling another decorated helper)
# multiplies the attempts. Self-registration is the deliberate exception: a
# one-shot startup/CLI path that keeps its own three-attempt policy in
# self_registration.py, so a slow portal fails the boot fast instead of
# holding the process for two minutes.
#
# stamina combines `attempts` and `timeout` with stop_any(), so the tighter
# one wins, and its defaults (attempts=10 / timeout=45 s) silently truncate a
# long curve: the hand-copied 10-20-40 s decorators this replaced ran three
# attempts, not ten. Both stops are spelled out here and sized so every
# declared attempt is reachable. The waits are min(2 * 2**n + jitter, 30) for
# n = 0..4, i.e. 2-7, 4-9, 8-13, 16-21 and 30 s: 60-80 s of waiting in total
# for six attempts, inside the 120 s budget whenever the calls themselves are
# quick. tenacity checks the stop after a failed attempt and then sleeps the
# full wait, so the loop's own overhead for one failing call is bounded by
# timeout + wait_max = 150 s. The requests themselves come on top of that:
# GundiDataSenderClient posts with an httpx timeout of 120 s, so a Sensors
# API that hangs rather than fails can hold one send for roughly four and a
# half minutes. Gundi sends run inline in the PubSub push request by default
# (PROCESS_PUBSUB_MESSAGES_IN_BACKGROUND=False), so deployments that expect
# hangs should turn background processing on or shorten this policy; the
# tests in test_gundi_api.py pin the loop-overhead bound.
GUNDI_API_RETRY = dict(
    on=is_transient_gundi_error,
    attempts=6,
    timeout=120.0,
    wait_initial=2.0,
    wait_jitter=5.0,
    wait_max=30.0,
)


async def _get_gundi_api_key(integration_id):
    # No retry decorator of its own: every caller is one of the retry-decorated
    # send helpers below, and a second policy nested inside the first restarts
    # the inner six attempts on each outer attempt (36 portal calls, many
    # minutes of sleep) for a portal that keeps failing.
    # An ephemeral run's synthetic integration has no portal row: letting
    # this reach the portal would 404 for an integration that does not exist.
    _block_if_ephemeral("_get_gundi_api_key")
    async with GundiClient() as gundi_client:
        return await with_fresh_token_on_401(
            gundi_client, lambda: gundi_client.get_integration_api_key(integration_id=integration_id)
        )


async def _get_sensors_api_client(integration_id):
    gundi_api_key = await _get_gundi_api_key(integration_id=integration_id)
    assert gundi_api_key, f"Cannot get a valid API Key for integration {integration_id}"
    sensors_api_client = GundiDataSenderClient(
        integration_api_key=gundi_api_key
    )
    return sensors_api_client


@stamina.retry(**GUNDI_API_RETRY)
async def send_events_to_gundi(events: List[dict], **kwargs) -> dict:
    """
    Send Events to Gundi using the REST API v2
    :param events: A list of events in the following format:
    [
        {
        "title": "Animal Sighting",
        "event_type": "wildlife_sighting_rep",
        "recorded_at":"2024-01-08 21:51:10-03:00",
        "location":{
            "lat":-51.688645,
            "lon":-72.704421
        },
        "event_details":{
            "site_name":"MM Spot",
            "species":"lion"
        },
        ...
    ]
    :param kwargs: integration_id: The UUID of the related integration
    :return: A dict with the response from the API
    """
    _block_if_ephemeral("send_events_to_gundi")
    integration_id = kwargs.get("integration_id")
    assert integration_id, "integration_id is required"
    sensors_api_client = await _get_sensors_api_client(integration_id=str(integration_id))
    return await sensors_api_client.post_events(data=events)


@stamina.retry(**GUNDI_API_RETRY)
async def send_event_attachments_to_gundi(event_id: str, attachments: List[tuple], **kwargs) -> dict:
    """
    Send Event Attachments to Gundi using the REST API v2
    :param event_id: Created event in which the attachments are going to be linked
    :param attachments: A list of attachments (tuples with filename, file in bytes). Example:
    filename = 'example.png'
    file_in_bytes = open(filename, 'rb')
    attachments = [(filename, file_in_bytes)]
    :param kwargs: integration_id: The UUID of the related integration
    :return: A dict with the response from the API
    """
    _block_if_ephemeral("send_event_attachments_to_gundi")
    integration_id = kwargs.get("integration_id")
    assert integration_id, "integration_id is required"
    sensors_api_client = await _get_sensors_api_client(integration_id=str(integration_id))
    return await sensors_api_client.post_event_attachments(event_id=event_id, attachments=attachments)


@stamina.retry(**GUNDI_API_RETRY)
async def send_observations_to_gundi(observations: List[dict], **kwargs) -> dict:
    """
    Send Observations to Gundi using the REST API v2
    :param observations: A list of observations in the following format:
    [
        {
            "source": "collar-xy123",
            "type": "tracking-device",
            "subject_type": "puma",
            "recorded_at": "2024-01-24 09:03:00-0300",
            "location": {
                "lat": -51.748,
                "lon": -72.720
            },
            "additional": {
                "speed_kmph": 10
            }
        },
        ...
    ]
    :param kwargs: integration_id: The UUID of the related integration
    :return: A dict with the response from the API
    """
    _block_if_ephemeral("send_observations_to_gundi")
    integration_id = kwargs.get("integration_id")
    assert integration_id, "integration_id is required"
    sensors_api_client = await _get_sensors_api_client(integration_id=str(integration_id))
    return await sensors_api_client.post_observations(data=observations)


@stamina.retry(**GUNDI_API_RETRY)
async def send_messages_to_gundi(messages: List[dict], **kwargs) -> dict:
    """
    Send Messages to Gundi using the REST API v2
    :param messages: A list of messages in the following format:
    [
        {
            "sender": "2075752244",
            "recipients": ["admin@sitex.pamdas.org"],
            "text": "Help! I need assistance.",
            "recorded_at": "2025-08-09 09:54:10-0300",
            "location": {
                "latitude": -51.689,
                "longitude": -72.705
            },
            "additional": {
                "gpsFix": 2,
                "course": 45,
                "speed": 50,
                "status": {
                    "autonomous": 0,
                    "lowBattery": 1,
                    "intervalChange": 0,
                    "resetDetected": 0
                }
            }
        },
        ...
    ]
    :param kwargs: integration_id: The UUID of the related integration
    :return: A dict with the response from the API
    """
    _block_if_ephemeral("send_messages_to_gundi")
    integration_id = kwargs.get("integration_id")
    assert integration_id, "integration_id is required"
    sensors_api_client = await _get_sensors_api_client(integration_id=str(integration_id))
    return await sensors_api_client.post_messages(data=messages)
