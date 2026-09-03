import pytest
from app.services.gundi import (
    send_events_to_gundi,
    send_observations_to_gundi,
    send_event_attachments_to_gundi,
    send_messages_to_gundi,
    _get_gundi_api_key,
    EphemeralWriteBlocked,
)
from app.services.activity_logger import ephemeral_run


@pytest.mark.asyncio
async def test_send_events_to_gundi(
        mocker, mock_gundi_client_v2_class, mock_gundi_sensors_client_class,
        mock_get_gundi_api_key, integration_v2
):
    mocker.patch("app.services.gundi.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("app.services.gundi.GundiDataSenderClient", mock_gundi_sensors_client_class)
    mocker.patch("app.services.gundi._get_gundi_api_key", mock_get_gundi_api_key)
    events = [
        {
            "title": "Animal Sighting",
            "event_type": "wildlife_sighting_rep",
            "recorded_at": "2024-01-08 21:51:10-03:00",
            "location": {
                "lat": -51.688645,
                "lon": -72.704421
            },
            "event_details": {
                "site_name": "MM Spot",
                "species": "lion"
            }
        },
        {
            "title": "Animal Sighting",
            "event_type": "wildlife_sighting_rep",
            "recorded_at": "2024-01-08 21:51:10-03:00",
            "location": {
                "lat": -51.688645,
                "lon": -72.704421
            },
            "event_details": {
                "site_name": "MM Spot",
                "species": "lion"
            }
        }
    ]
    response = await send_events_to_gundi(
        events=events,
        integration_id=integration_v2.id
    )

    # Data is sent to gundi using the REST API for now
    assert len(response) == 2
    assert mock_gundi_sensors_client_class.called
    mock_gundi_sensors_client_class.return_value.post_events.assert_called_once_with(data=events)


@pytest.mark.asyncio
async def test_send_event_attachments_to_gundi(
        mocker, mock_gundi_client_v2_class, mock_gundi_sensors_client_class,
        mock_get_gundi_api_key, integration_v2
):
    mocker.patch("app.services.gundi.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("app.services.gundi.GundiDataSenderClient", mock_gundi_sensors_client_class)
    mocker.patch("app.services.gundi._get_gundi_api_key", mock_get_gundi_api_key)
    attachments = [
        ("file1.png", b'\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x01\x00x\x00x\x00\x00\xff\xdb\x00C\x00\x02\x01\x01\x02'),
        ("file2.png", b'\xff\xd8\xff\xe0\x00\x10JFIF\x00\x06\x01\x01\x00x\x00x\x01\x00\xff\xd5\x00C\x00\x98\x01\x01\x56')
    ]
    response = await send_event_attachments_to_gundi(
        event_id="dummy-1234",
        attachments=attachments,
        integration_id=integration_v2.id
    )

    # Data is sent to gundi using the REST API for now
    assert len(response) == 2
    assert mock_gundi_sensors_client_class.called
    mock_gundi_sensors_client_class.return_value.post_event_attachments.assert_called_once_with(
        event_id="dummy-1234",
        attachments=attachments
    )


@pytest.mark.asyncio
async def test_send_observations_to_gundi(
        mocker, mock_gundi_client_v2_class, mock_gundi_sensors_client_class,
        mock_get_gundi_api_key, integration_v2
):
    mocker.patch("app.services.gundi.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("app.services.gundi.GundiDataSenderClient", mock_gundi_sensors_client_class)
    mocker.patch("app.services.gundi._get_gundi_api_key", mock_get_gundi_api_key)
    observations = [
        {
            "source": "device-xy123",
            "type": "tracking-device",
            "subject_type": "puma",
            "recorded_at": "2024-01-24 09:03:00-0300",
            "location": {
                "lat": -51.748,
                "lon": -72.720
            },
            "additional": {
                "speed_kmph": 5
            }
        },
        {
            "source": "test-device-mariano",
            "type": "tracking-device",
            "subject_type": "puma",
            "recorded_at": "2024-01-24 09:05:00-0300",
            "location": {
                "lat": -51.755,
                "lon": -72.755
            },
            "additional": {
                "speed_kmph": 5
            }
        }
    ]
    response = await send_observations_to_gundi(
        observations=observations,
        integration_id=integration_v2.id
    )

    # Data is sent to gundi using the REST API for now
    assert len(response) == 2
    assert mock_gundi_sensors_client_class.called
    mock_gundi_sensors_client_class.return_value.post_observations.assert_called_once_with(data=observations)


@pytest.mark.parametrize("fn_and_args", [
    (send_events_to_gundi, {"events": [], "integration_id": "id"}),
    (send_observations_to_gundi, {"observations": [], "integration_id": "id"}),
    (send_event_attachments_to_gundi, {"event_id": "e", "attachments": [], "integration_id": "id"}),
    (send_messages_to_gundi, {"messages": [], "integration_id": "id"}),
    (_get_gundi_api_key, {"integration_id": "id"}),
])
@pytest.mark.asyncio
async def test_gundi_helpers_block_on_ephemeral_run(fn_and_args):
    # A reference handler running against a draft integration must not be
    # able to move data through Gundi (design invariant: reference actions
    # are read-only). Each entry point checks the contextvar and raises
    # before doing any I/O.
    fn, args = fn_and_args
    token = ephemeral_run.set(True)
    try:
        with pytest.raises(EphemeralWriteBlocked):
            await fn(**args)
    finally:
        ephemeral_run.reset(token)


def test_gundi_api_retry_policy_is_explicit_and_reachable():
    """stamina combines `attempts` and `timeout` with stop_any(), so the tighter
    one wins. Its defaults (attempts=10, timeout=45s) silently truncate a long
    backoff: with wait_initial=10 the 45s budget is spent after ~3 attempts and
    wait_max is never reached. Both bounds must be declared explicitly, and the
    timeout must be wide enough for the declared wait curve to actually play out."""
    from app.services.gundi import GUNDI_API_RETRY

    attempts = GUNDI_API_RETRY["attempts"]
    timeout = GUNDI_API_RETRY["timeout"]
    wait_initial = GUNDI_API_RETRY["wait_initial"]
    wait_max = GUNDI_API_RETRY["wait_max"]
    assert attempts is not None and timeout is not None

    # Worst case (no jitter contribution needed -- jitter only adds delay), how
    # many attempts fit inside the timeout?
    elapsed, fits = 0.0, 1
    while fits < attempts:
        elapsed += min(wait_initial * (2 ** (fits - 1)), wait_max)
        if elapsed >= timeout:
            break
        fits += 1
    assert fits >= 5, (
        f"retry budget allows only {fits} attempts before the {timeout}s timeout; "
        "the declared backoff is mostly unreachable"
    )


def test_all_gundi_api_calls_share_one_retry_policy():
    """Six call sites previously repeated the same decorator by hand, which is
    how the wait curve and the stop condition drifted apart in the first place.
    Five are the helpers in gundi.py; the sixth is the webhook path's
    integration lookup, which retries the same Gundi API inline."""
    import re
    from pathlib import Path

    source = Path("app/services/gundi.py").read_text()
    decorators = re.findall(r"@stamina\.retry\((.*?)\)\n", source)
    assert decorators, "expected retry-decorated functions in app/services/gundi.py"
    assert all(d.strip() == "**GUNDI_API_RETRY" for d in decorators), decorators

    webhooks_source = Path("app/services/webhooks.py").read_text()
    contexts = re.findall(r"stamina\.retry_context\((.*?)\)", webhooks_source)
    assert contexts, "expected a retry_context in app/services/webhooks.py"
    assert all(c.strip() == "**GUNDI_API_RETRY" for c in contexts), contexts
