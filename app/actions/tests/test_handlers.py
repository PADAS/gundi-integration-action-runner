"""Unit tests for app.actions.handlers."""

from datetime import date, datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.actions.handlers import (
    STATE_DATETIME_FMT,
    STATE_LAST_RUN_KEY,
    _build_pull_events_state,
    _get_load_since,
    _normalize_recorded_at,
    _transform_inat_to_gundi_event,
    action_pull_events,
    chunk_list,
    handle_transformed_data,
)
from app.actions.configurations import PullEventsConfig


# --- _get_load_since ---


def test_get_load_since_empty_state_uses_fallback_days():
    state = {}
    result = _get_load_since(state, 5)
    now = datetime.now(tz=timezone.utc)
    expected_floor = now - timedelta(days=6)
    expected_ceil = now - timedelta(days=4)
    assert expected_floor <= result <= expected_ceil
    assert result.tzinfo is not None


def test_get_load_since_with_last_run_parses_and_returns():
    state = {STATE_LAST_RUN_KEY: "2024-01-15 12:00:00+0000"}
    result = _get_load_since(state, 5)
    assert result == datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)


def test_get_load_since_prefers_last_run_over_updated_to():
    state = {
        STATE_LAST_RUN_KEY: "2024-02-01 00:00:00+0000",
        "updated_to": "2024-01-01 00:00:00+0000",
    }
    result = _get_load_since(state, 5)
    assert result == datetime(2024, 2, 1, 0, 0, 0, tzinfo=timezone.utc)


def test_get_load_since_uses_updated_to_when_last_run_missing():
    state = {"updated_to": "2024-03-10 08:30:00+0000"}
    result = _get_load_since(state, 3)
    assert result == datetime(2024, 3, 10, 8, 30, 0, tzinfo=timezone.utc)


# --- _build_pull_events_state ---


def test_build_pull_events_state():
    dt = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    result = _build_pull_events_state(dt)
    assert result == {STATE_LAST_RUN_KEY: "2024-06-01 12:00:00+0000"}
    # Round-trip
    parsed = datetime.strptime(result[STATE_LAST_RUN_KEY], STATE_DATETIME_FMT)
    assert parsed == dt


# --- chunk_list ---


def test_chunk_list_exact_multiple():
    assert list(chunk_list([1, 2, 3, 4], 2)) == [[1, 2], [3, 4]]


def test_chunk_list_partial_final():
    assert list(chunk_list([1, 2, 3, 4, 5], 2)) == [[1, 2], [3, 4], [5]]


def test_chunk_list_empty():
    assert list(chunk_list([], 10)) == []


def test_chunk_list_single_chunk():
    assert list(chunk_list([1, 2, 3], 10)) == [[1, 2, 3]]


# --- _normalize_recorded_at ---


def test_normalize_recorded_at_none_returns_created_at():
    created = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    assert _normalize_recorded_at(None, created) is created


def test_normalize_recorded_at_date_only():
    d = date(2024, 5, 10)
    created = datetime(2024, 1, 1, tzinfo=timezone.utc)
    result = _normalize_recorded_at(d, created)
    assert result == datetime(2024, 5, 10, 0, 0, 0, tzinfo=timezone.utc)


def test_normalize_recorded_at_naive_datetime():
    naive = datetime(2024, 5, 10, 14, 30, 0)
    created = datetime(2024, 1, 1, tzinfo=timezone.utc)
    result = _normalize_recorded_at(naive, created)
    assert result.tzinfo == timezone.utc
    assert result.replace(tzinfo=None) == naive


def test_normalize_recorded_at_aware_datetime_unchanged():
    aware = datetime(2024, 5, 10, 14, 30, 0, tzinfo=timezone.utc)
    created = datetime(2024, 1, 1, tzinfo=timezone.utc)
    assert _normalize_recorded_at(aware, created) is aware


# --- _transform_inat_to_gundi_event ---


def _make_observation(
    id_=12345,
    observed_on=None,
    created_at=None,
    user=None,
    location=None,
    place_ids=None,
    taxon=None,
    **kwargs,
):
    """Minimal Observation-like object for transformation tests."""
    from pyinaturalist import Observation

    created_at = created_at or datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    data = {
        "id": id_,
        "observed_on": observed_on or "2024-06-15",
        "created_at": created_at,
        "captive": kwargs.get("captive", False),
        "obscured": kwargs.get("obscured", False),
        "place_guess": kwargs.get("place_guess"),
        "quality_grade": kwargs.get("quality_grade", "research"),
        "species_guess": kwargs.get("species_guess", "Some species"),
        "updated_at": kwargs.get("updated_at", created_at),
        "uri": kwargs.get("uri", "https://www.inaturalist.org/observations/12345"),
        "photos": kwargs.get("photos", []),
        "user": user,
        "location": location,
        "place_ids": place_ids or [],
        "taxon": taxon,
        "annotations": kwargs.get("annotations", []),
    }
    return Observation.from_json(data)


def test_transform_inat_to_gundi_event_minimal():
    ob = _make_observation()
    config = PullEventsConfig(
        days_to_load=3,
        event_type="inat_observation",
        event_prefix="iNat: ",
    )
    event = _transform_inat_to_gundi_event(ob, config)
    assert event["event_type"] == "inat_observation"
    assert event["event_details"]["inat_id"] == "12345"
    assert event["title"] == "iNat: Some species"
    assert "recorded_at" in event
    assert event["event_details"]["place_guess"] is None


def test_transform_inat_to_gundi_event_with_user_location_taxon():
    ob = _make_observation(
        user={"id": 99, "name": "Jane", "login": "jane"},
        location=[-27.7, 16.7],
        place_ids=[1, 2, 3],
        taxon={
            "id": 120255,
            "rank": "species",
            "name": "Crassula muscosa",
            "preferred_common_name": "lizard's-tail",
            "wikipedia_url": "http://en.wikipedia.org/wiki/Crassula_muscosa",
            "ancestor_ids": [1, 2, 3],
        },
    )
    config = PullEventsConfig(days_to_load=3, event_prefix="iNat: ")
    event = _transform_inat_to_gundi_event(ob, config)
    assert event["event_details"]["user_id"] == 99
    assert event["event_details"]["user_name"] == "Jane"
    assert event["location"] == {"lat": -27.7, "lon": 16.7}
    assert event["event_details"]["place_ids"] == "1,2,3"
    assert event["event_details"]["taxon_id"] == 120255
    assert event["event_details"]["taxon_name"] == "Crassula muscosa"
    assert event["title"] == "iNat: lizard's-tail"
    assert event["event_details"]["taxon_ancestors"] == "1,2,3"


def test_transform_inat_to_gundi_event_title_fallback_to_species_guess():
    ob = _make_observation(
        species_guess="Unknown Bird",
        taxon={"id": 1, "rank": "species", "name": "Spp", "preferred_common_name": None},
    )
    config = PullEventsConfig(days_to_load=3, event_prefix="")
    event = _transform_inat_to_gundi_event(ob, config)
    assert event["title"] == "Unknown Bird"


# --- action_pull_events: no observations path (state still updated) ---


@pytest.mark.asyncio
async def test_action_pull_events_no_observations_updates_state(mocker):
    from uuid import UUID

    mock_state = AsyncMock()
    mock_state.get_state.return_value = {}
    mocker.patch("app.actions.handlers.state_manager", mock_state)
    mocker.patch("app.actions.handlers.get_observations", return_value={})
    mocker.patch("app.actions.handlers.log_action_activity", AsyncMock())
    mocker.patch("app.services.activity_logger.publish_event", AsyncMock())

    integration = MagicMock()
    integration.id = UUID("f03ec73e-f3fe-41b6-8597-3eb89dde5ae1")
    config = PullEventsConfig(days_to_load=3, event_prefix="iNat: ")

    result = await action_pull_events(integration, config)

    assert result["result"]["events_extracted"] == 0
    assert result["result"]["events_updated"] == 0
    mock_state.set_state.assert_called_once()
    call_args = mock_state.set_state.call_args[0]
    assert call_args[0] == str(integration.id)
    assert call_args[1] == "pull_events"
    state = call_args[2]
    assert STATE_LAST_RUN_KEY in state
    # Should be a recent timestamp
    from datetime import datetime
    parsed = datetime.strptime(state[STATE_LAST_RUN_KEY], STATE_DATETIME_FMT)
    assert parsed.tzinfo is not None


# --- handle_transformed_data ---


@pytest.mark.asyncio
async def test_handle_transformed_data_success(mocker):
    mocker.patch("app.actions.handlers.send_events_to_gundi", AsyncMock(return_value=["id-1"]))
    result = await handle_transformed_data(
        transformed_data=[{"title": "Test"}],
        integration_id="int-123",
        action_id="pull_events",
    )
    assert result == ["id-1"]


@pytest.mark.asyncio
async def test_handle_transformed_data_http_error_returns_message(mocker):
    import httpx
    mocker.patch(
        "app.actions.handlers.send_events_to_gundi",
        AsyncMock(side_effect=httpx.HTTPError("Server error")),
    )
    result = await handle_transformed_data(
        transformed_data=[],
        integration_id="int-456",
        action_id="pull_events",
    )
    assert len(result) == 1
    assert "int-456" in result[0]
    assert "Server error" in result[0]
