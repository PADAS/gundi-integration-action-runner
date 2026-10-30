"""Contracts against Studio develop and recoverable LCC monitor workflows."""

import copy
import datetime as dt
import json
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
import pytest_asyncio
from pydantic import ValidationError

from app.actions import client, handlers
from app.actions.change_events import geohash, parse_change_date
from app.actions.configurations import (
    Cadence,
    PullEventsConfig,
    SubmitPredictionsConfig,
)
from app.actions import monitor as monitor_module
from app.actions.monitor import (
    Monitor,
    build_feature_query,
    schedule_slot,
)
from app.actions.delivery import GundiEventSender
from app.actions.monitor_state import STATE_ACTION, Job
from app.services.errors import (
    IntegrationAuthError,
    IntegrationBadResponseError,
    IntegrationConfigurationError,
    IntegrationConnectionError,
    IntegrationError,
)
from app.services.state import IntegrationStateManager

UTC = dt.timezone.utc
NOW = dt.datetime(2026, 10, 23, tzinfo=UTC)
PROJECT = "00000000-0000-0000-0000-000000000001"
MODEL = "00000000-0000-0000-0000-000000000002"
ORG = "00000000-0000-0000-0000-000000000003"
AREA = {
    "type": "Polygon",
    "coordinates": [[[10, 10], [11, 10], [11, 11], [10, 11], [10, 10]]],
}


@pytest.fixture
def config():
    return PullEventsConfig(
        project_id=PROJECT,
        model_id=MODEL,
        area_of_interest=AREA,
        schedule_start="2026-10-01",
        cadence="weekly",
        select_all_types=False,
        post_change_types=["mining"],
    )


@pytest.fixture(autouse=True)
def activity_log(mocker):
    """The template's activity log publishes to PubSub; record calls instead."""
    return mocker.patch.object(monitor_module, "log_action_activity", AsyncMock())


@pytest.fixture(autouse=True)
def small_pages(monkeypatch):
    monkeypatch.setattr(monitor_module, "FEATURE_PAGE_SIZE", 2)
    monkeypatch.setattr(monitor_module, "PAGES_PER_POLL", 1)


class MemoryState:
    """Stands in for IntegrationStateManager.get_state/set_state."""

    def __init__(self):
        self.value = {}

    async def get_state(self, *args):
        return copy.deepcopy(self.value)

    async def set_state(self, *args):
        self.value = copy.deepcopy(args[-1])


@pytest.fixture
def store():
    return MemoryState()


@pytest.fixture
def api():
    api = AsyncMock(spec=client.OlmoEarthClient)
    api.base_url = "https://example.test"
    api.verify_token.return_value = 0
    api.create_prediction.return_value = prediction("p1", status="pending")
    api.get_prediction.return_value = prediction("p1")
    api.search_predictions.return_value = client.PredictionSearchResponse(records=[])
    api.search_features.return_value = page([])
    return api


def prediction(pid="p1", status="completed", result_id="r1", formats=None):
    return client.Prediction(
        id=pid,
        status=status,
        result=client.PredictionResult(
            id=result_id,
            file_formats=formats if formats is not None else ["mvt", "png"],
        ),
    )


def feature(fid="f1", changed="2026-09-10", change_type="mining", geometry=None):
    return client.Feature.parse_obj(
        {
            "id": fid,
            "type": "Feature",
            "geometry": geometry or AREA,
            "properties": {
                "post_change": change_type,
                "ts_start": changed,
                "oe_start_time": "2026-10-01T00:00:00Z",
                "oe_prediction_result_id": "run-result-id",
            },
        }
    )


def page(features, total=None):
    return client.FeatureSearchResponse(
        records=features, meta=client.SearchMeta(total=total)
    )


def make_monitor(config, api, store, now=NOW):
    sender = SimpleNamespace(send=AsyncMock())
    return Monitor("integration-1", config, api, store, sender, now)


def pending_job(config, prediction_id="p1", name=None, reference_time="2026-10-01T00:00:00Z"):
    return Job(
        name=name or prediction_id,
        prediction_id=prediction_id,
        reference_time=reference_time,
        selected_types=config.selected_types,
        submitted_at=NOW,
    )


async def seed(config, api, store, jobs=None):
    monitor = make_monitor(config, api, store)
    await monitor.load()
    monitor.state.jobs = jobs or [
        pending_job(config, name="job")
    ]
    await monitor.save()
    return monitor


def test_monitor_form_requires_selection_and_valid_polygon(config):
    assert SubmitPredictionsConfig().run_on_schedule is False
    assert config.selected_types == ["mining"]
    assert "none" not in config.copy(update={"select_all_types": True}).selected_types
    for changes in (
        {"post_change_types": []},
        {"project_id": "bad"},
        {"area_of_interest": {"type": "Point", "coordinates": [1, 2]}},
        {"area_of_interest": "not json"},
    ):
        with pytest.raises(ValidationError):
            PullEventsConfig.parse_obj({**config.dict(), **changes})


def test_area_is_pasted_as_text(config):
    assert PullEventsConfig.schema()["properties"]["area_of_interest"]["type"] == "string"
    pasted = {**config.dict(), "area_of_interest": json.dumps(config.area_of_interest)}
    assert PullEventsConfig.parse_obj(pasted).area_of_interest == config.area_of_interest


def test_calendar_schedule_keeps_anchor_after_short_month(config):
    config.cadence = Cadence.MONTHLY
    config.schedule_start = dt.date(2024, 1, 31)
    assert schedule_slot(config, 1) == dt.date(2024, 2, 29)
    assert schedule_slot(config, 2) == dt.date(2024, 3, 31)
    config.cadence = Cadence.ANNUAL
    config.schedule_start = dt.date(2024, 2, 29)
    assert schedule_slot(config, 1) == dt.date(2025, 2, 28)


def test_feature_filter_uses_post_change_and_not_a_moving_date_cursor(config):
    body = build_feature_query(config, pending_job(config), 6).as_body()
    assert body["intersects_geometry"] == AREA
    assert body["offset"] == 6
    assert body["property_filters"] == [
        {"property_name": "post_change", "keyword_filter": {"inc": ["mining"]}}
    ]
    assert "oe_created_at" not in body and "oe_start_time" not in body


@pytest.mark.parametrize(
    "value", [None, 145, "21", "2026-99-99", "2026-01-01T12:00:00"]
)
def test_encoded_or_invalid_dates_are_not_silently_used(value):
    with pytest.raises(IntegrationBadResponseError):
        parse_change_date(value)


@pytest.mark.asyncio
async def test_due_submission_is_saved_with_reference_date_and_no_vectorize_flag(
    config, api, store
):
    result = await make_monitor(config, api, store).submit()
    body = api.create_prediction.call_args.args[0]
    assert body["project_id"] == PROJECT
    assert body["start_time"] == body["end_time"] == "2026-09-17T00:00:00+00:00"
    assert body["geojson"]["features"][0]["geometry"] == AREA
    assert "vectorize" not in body
    assert result["submitted_prediction_id"] == "p1"
    assert store.value["jobs"][0]["prediction_id"] == "p1"
    assert store.value["next_slot"] == 1
    await make_monitor(config, api, store).submit()
    api.create_prediction.assert_awaited_once()


@pytest.mark.asyncio
async def test_future_slot_does_not_submit(config, api, store):
    config.schedule_start = dt.date(2027, 1, 1)
    assert (await make_monitor(config, api, store).submit())["status"] == "not_due"
    api.create_prediction.assert_not_called()


@pytest.mark.asyncio
async def test_lost_submission_is_recovered_by_name_without_a_second_post(
    config, api, store
):
    api.create_prediction.side_effect = IntegrationConnectionError("timeout")
    with pytest.raises(IntegrationConnectionError):
        await make_monitor(config, api, store).submit()
    assert store.value == {}
    api.search_predictions.return_value = client.PredictionSearchResponse(
        records=[prediction("recovered")]
    )
    result = await make_monitor(config, api, store).submit()
    assert result["recovered_prediction_id"] == "recovered"
    assert api.search_predictions.call_args.args[0].name.eq == store.value["jobs"][0]["name"]
    assert store.value["next_slot"] == 1
    api.create_prediction.assert_awaited_once()


@pytest.mark.asyncio
async def test_submission_with_no_match_is_posted_again(config, api, store):
    api.create_prediction.side_effect = IntegrationConnectionError("timeout")
    with pytest.raises(IntegrationConnectionError):
        await make_monitor(config, api, store).submit()
    api.create_prediction.side_effect = None
    assert (await make_monitor(config, api, store).submit())["submitted_prediction_id"] == "p1"
    assert api.create_prediction.await_count == 2


@pytest.mark.asyncio
async def test_duplicate_predictions_block_submission(config, api, store):
    api.search_predictions.return_value = client.PredictionSearchResponse(
        records=[prediction("a"), prediction("b")], meta=client.SearchMeta(total=2)
    )
    with pytest.raises(IntegrationConfigurationError, match="Delete all but one"):
        await make_monitor(config, api, store).submit()
    api.create_prediction.assert_not_called()


@pytest.mark.asyncio
async def test_late_discovery_uses_change_date_and_keeps_both_result_ids(
    config, api, store
):
    monitor = await seed(config, api, store)
    api.search_features.return_value = page([feature()])
    result = await monitor.collect()
    event = monitor.sender.send.call_args.args[0][0]
    assert event["recorded_at"].startswith("2026-09-10")
    assert event["geometry"] == AREA
    assert event["event_details"]["first_discovered_at"].startswith("2026-10-23")
    assert event["event_details"]["prediction_result_id"] == "r1"
    assert event["event_details"]["oerun_prediction_result_id"] == "run-result-id"
    assert result["job"]["status"] == "complete"
    assert api.search_features.call_args.args[0] == "r1"


@pytest.mark.asyncio
async def test_page_budget_resumes_without_marking_complete(config, api, store):
    monitor = await seed(config, api, store)
    api.search_features.side_effect = [
        page([feature("1"), feature("2")]),
        page([feature("3")]),
    ]
    await monitor.collect()
    assert store.value["jobs"][0]["offset"] == 2
    assert store.value["jobs"][0]["status"] == "collecting"
    await monitor.collect()
    assert api.search_features.call_args.args[1].offset == 2
    assert store.value["jobs"][0]["status"] == "complete"


@pytest.mark.asyncio
async def test_failed_delivery_resends_the_same_batch_next_run(config, api, store):
    monitor = await seed(config, api, store)
    api.search_features.return_value = page([feature()])
    monitor.sender.send.side_effect = RuntimeError("delivery acknowledgement lost")
    with pytest.raises(RuntimeError):
        await monitor.collect()
    first = copy.deepcopy(monitor.sender.send.call_args.args[0])
    assert store.value["jobs"][0]["offset"] == 0
    assert store.value["outbox"] is not None
    monitor.sender.send.side_effect = None
    result = await monitor.collect()
    assert monitor.sender.send.call_args.args[0] == first
    assert result["events_sent"] == 1
    api.search_features.assert_awaited_once()
    assert store.value["jobs"][0]["status"] == "complete"
    assert store.value["outbox"] is None


async def run_second_prediction(monitor, config, api, features):
    monitor.state.jobs.append(
        pending_job(config, "p2", name="second", reference_time="2026-10-08T00:00:00Z")
    )
    await monitor.save()
    api.get_prediction.return_value = prediction("p2", result_id="r2")
    api.search_features.return_value = page(features)
    return await monitor.collect()


@pytest.mark.asyncio
async def test_repeat_detection_is_sent_and_marked_seen_before(config, api, store):
    monitor = await seed(config, api, store)
    api.search_features.return_value = page([feature()])
    first = await monitor.collect()
    assert first["detections_by_kind"] == {"new_site": 1}
    result = await run_second_prediction(
        monitor, config, api, [feature("different-feature-id", changed="2026-09-12")]
    )
    assert result["events_sent"] == 1
    assert result["detections_by_kind"] == {"seen_before": 1}
    event = monitor.sender.send.call_args.args[0][0]
    assert event["title"] == "OlmoEarth: mining (seen before, 2 predictions)"
    assert event["event_details"]["detection_kind"] == "seen_before"
    assert event["event_details"]["site_id"].startswith("gh7:")
    (site,) = store.value["sites"].values()
    assert site["predictions"] == 2
    assert site["first_seen_at"] == NOW


@pytest.mark.asyncio
async def test_new_change_type_at_known_site_is_marked(config, api, store):
    config = PullEventsConfig.parse_obj(
        {**config.dict(), "post_change_types": ["mining", "new_road"]}
    )
    monitor = await seed(config, api, store)
    api.search_features.return_value = page([feature()])
    await monitor.collect()
    await run_second_prediction(monitor, config, api, [feature("road", change_type="new_road")])
    event = monitor.sender.send.call_args.args[0][0]
    assert event["title"] == "OlmoEarth: new road (new change at known site)"
    (site,) = store.value["sites"].values()
    assert sorted(site["change_types"]) == ["mining", "new_road"]


@pytest.mark.asyncio
async def test_detections_in_one_prediction_never_count_as_repeats(config, api, store):
    monitor = await seed(config, api, store)
    api.search_features.return_value = page([feature("part-a"), feature("part-b")])
    result = await monitor.collect()
    assert result["detections_by_kind"] == {"new_site": 2}
    (site,) = store.value["sites"].values()
    assert site["predictions"] == 1


@pytest.mark.asyncio
async def test_distant_detection_is_a_different_site(config, api, store):
    monitor = await seed(config, api, store)
    elsewhere = {
        "type": "Polygon",
        "coordinates": [[[20, 20], [20.01, 20], [20.01, 20.01], [20, 20.01], [20, 20]]],
    }
    api.search_features.return_value = page([feature(), feature("far", geometry=elsewhere)])
    result = await monitor.collect()
    assert result["detections_by_kind"] == {"new_site": 2}
    assert result["known_sites"] == 2


def test_site_id_is_a_standard_geohash():
    assert geohash(57.64911, 10.40744, 11) == "u4pruydqqvj"


@pytest.mark.asyncio
async def test_failed_prediction_is_recorded_and_other_jobs_can_continue(
    config, api, store
):
    monitor = await seed(config, api, store)
    api.get_prediction.return_value = prediction(status="failed")
    result = await monitor.collect()
    assert result["job"]["status"] == "failed"
    api.search_features.assert_not_called()


@pytest.mark.asyncio
async def test_raster_only_result_is_not_treated_as_no_detections(config, api, store):
    monitor = await seed(config, api, store)
    api.get_prediction.return_value = prediction(formats=["png"])
    with pytest.raises(IntegrationConfigurationError, match="vector"):
        await monitor.collect()
    api.search_features.assert_not_called()


@pytest.mark.asyncio
async def test_invalid_event_does_not_advance_or_drop_a_page(config, api, store):
    monitor = await seed(config, api, store)
    api.search_features.return_value = page([feature(changed=145)])
    with pytest.raises(IntegrationBadResponseError):
        await monitor.collect()
    assert store.value["jobs"][0]["offset"] == 0
    assert store.value["jobs"][0]["status"] == "failed"
    assert "offset 0" in store.value["jobs"][0]["error"]
    monitor.sender.send.assert_not_called()


@pytest.mark.asyncio
async def test_search_window_is_explicitly_incomplete(config, api, store):
    monitor = await seed(config, api, store)
    monitor.state.jobs[0].offset = 10000
    await monitor.save()
    result = await monitor.collect()
    assert result["job"]["status"] == "needs_export"
    api.search_features.assert_not_called()


@pytest.mark.asyncio
async def test_config_changes_do_not_mix_monitor_identity(config, api, store):
    await seed(config, api, store)
    config.model_id = ORG
    with pytest.raises(IntegrationConfigurationError, match="new monitor"):
        await make_monitor(config, api, store).collect()


@pytest.mark.asyncio
async def test_auth_failure_prevents_optional_auth_feature_search(config, api, store):
    monitor = await seed(config, api, store)
    api.get_prediction.side_effect = IntegrationAuthError("expired")
    with pytest.raises(IntegrationAuthError):
        await monitor.collect()
    api.search_features.assert_not_called()


@pytest.mark.asyncio
async def test_actual_http_contracts_use_nested_result_and_per_result_search(config):
    seen = []

    def route(request):
        seen.append(request)
        if request.url.path.endswith("/features/search"):
            return httpx.Response(200, json={"records": [], "meta": {"total": 0}})
        return httpx.Response(
            200,
            json={
                "records": [
                    {
                        "id": "p1",
                        "status": "completed",
                        "result": {"id": "studio-result", "file_formats": ["mvt"]},
                    }
                ]
            },
        )

    async with client.OlmoEarthClient(
        "https://example.test", "token", transport=httpx.MockTransport(route)
    ) as http:
        pred = await http.get_prediction("p1")
        await http.search_features(
            pred.result.id, build_feature_query(config, pending_job(config), 0)
        )
    assert seen[0].method == "GET"
    assert (
        seen[1].url.path == "/api/v1/prediction-results/studio-result/features/search"
    )
    assert "prediction_results" not in json.loads(seen[1].content)
    assert seen[1].headers["Authorization"] == "Bearer token"


@pytest.mark.asyncio
async def test_http_creation_timeout_is_not_retried():
    requests = []

    def route(request):
        requests.append(request)
        raise httpx.ReadTimeout("response lost", request=request)

    async with client.OlmoEarthClient(
        "https://example.test", "token", transport=httpx.MockTransport(route)
    ) as http:
        with pytest.raises(IntegrationConnectionError):
            await http.create_prediction({"name": "unique-job"})
    assert len(requests) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        {"records": [], "errors": [{"message": "partial"}]},
        {"records": [{"id": "p1"}], "errors": [{"message": "partial"}]},
        {"records": "invalid"},
    ],
)
async def test_incomplete_api_responses_are_not_checkpointed(payload):
    async with client.OlmoEarthClient(
        "https://example.test",
        "token",
        transport=httpx.MockTransport(lambda r: httpx.Response(200, json=payload)),
    ) as http:
        with pytest.raises(IntegrationBadResponseError):
            await http.search_predictions(client.PredictionSearchRequest())


@pytest.mark.asyncio
async def test_lock_prevents_overlapping_actions(config, mocker):
    mocker.patch.object(
        handlers.state_manager, "set_if_absent", AsyncMock(return_value=False)
    )
    release = mocker.patch.object(handlers.state_manager, "delete_state", AsyncMock())
    api_factory = mocker.patch.object(handlers, "client_for")
    assert await handlers.run_monitor(
        SimpleNamespace(id="integration-1"), config, Monitor.submit
    ) == {"status": "already_running"}
    api_factory.assert_not_called()
    release.assert_not_awaited()


@pytest.mark.asyncio
async def test_lock_released_after_failure(config, mocker):
    mocker.patch.object(
        handlers.state_manager, "set_if_absent", AsyncMock(return_value=True)
    )
    release = mocker.patch.object(handlers.state_manager, "delete_state", AsyncMock())
    mocker.patch.object(
        handlers, "client_for", side_effect=RuntimeError("test failure")
    )
    with pytest.raises(RuntimeError):
        await handlers.run_monitor(
            SimpleNamespace(id="integration-1"), config, Monitor.submit
        )
    release.assert_awaited_once()


@pytest.mark.asyncio
async def test_result_creation_failure_can_recover_later(config, api, store):
    monitor = await seed(config, api, store)
    api.get_prediction.return_value = prediction(status="result_creation_failed")
    await monitor.collect()
    assert store.value["jobs"][0]["status"] == "pending"
    api.get_prediction.return_value = prediction()
    await monitor.collect()
    assert store.value["jobs"][0]["status"] == "complete"


@pytest.mark.asyncio
async def test_changed_selection_only_applies_to_future_jobs(config, api, store):
    await seed(config, api, store)
    config.post_change_types = ["new_road"]
    api.search_features.return_value = page([feature()])
    result = await make_monitor(config, api, store).collect()
    assert result["events_sent"] == 1
    assert api.search_features.call_args.args[1].property_filters[
        0
    ].keyword_filter.inc == ["mining"]


@pytest.mark.asyncio
async def test_event_and_monitor_id_are_separate(config, api, store):
    monitor = await seed(config, api, store)
    api.search_features.return_value = page([feature()])
    await monitor.collect()
    event = monitor.sender.send.call_args.args[0][0]
    assert event["source"] == "olmoearth-monitor:integration-1"
    assert event["event_details"]["external_event_id"] != event["source"]


def test_portal_schema_and_example_keep_environment_and_ids_configurable():
    example = json.loads(
        (
            Path(__file__).resolve().parents[3] / "examples" / "lcc-monitor.json"
        ).read_text()
    )
    parsed = PullEventsConfig.parse_obj(example)
    schema = PullEventsConfig.schema()
    assert "project_id" in schema["required"] and "model_id" in schema["required"]
    assert schema["properties"]["post_change_types"]["type"] == "array"
    assert schema["properties"]["post_change_types"]["uniqueItems"] is True
    assert PullEventsConfig.ui_schema()["post_change_types"]["ui:widget"] == "select"
    assert parsed.selected_types == ["mining"]


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [400, 401, 403, 404, 422, 429])
async def test_rejected_submission_preserves_slot_for_retry(config, api, store, status):
    api.create_prediction.side_effect = IntegrationAuthError(
        "Rejected", status_code=status
    )
    with pytest.raises(IntegrationAuthError):
        await make_monitor(config, api, store).submit()
    assert store.value == {}
    api.create_prediction.side_effect = None
    await make_monitor(config, api, store).submit()
    assert store.value["next_slot"] == 1
    assert store.value["jobs"][0]["prediction_id"] == "p1"


@pytest.mark.asyncio
async def test_environment_change_cannot_reuse_saved_jobs(config, api, store):
    await seed(config, api, store)
    api.base_url = "https://another-environment.test"
    with pytest.raises(IntegrationConfigurationError, match="Environment"):
        await make_monitor(config, api, store).collect()
    api.get_prediction.assert_not_awaited()


@pytest.mark.asyncio
async def test_unacknowledged_batch_stays_in_outbox(config, api, store):
    monitor = await seed(config, api, store)
    api.search_features.return_value = page([feature("f1"), feature("f2")])
    monitor.sender.send.side_effect = IntegrationBadResponseError("not acknowledged")
    with pytest.raises(IntegrationBadResponseError):
        await monitor.collect()
    assert len(store.value["outbox"]["events"]) == 2
    assert store.value["jobs"][0]["offset"] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [401, 422, 503])
async def test_failed_submission_saves_nothing_and_posts_once(config, store, status):
    posts = []

    def route(request):
        if request.url.path.endswith("/search"):
            return httpx.Response(200, json={"records": [], "meta": {"total": 0}})
        posts.append(request)
        return httpx.Response(status, json={"detail": "failure"})

    async with client.OlmoEarthClient(
        "https://example.test", "token", transport=httpx.MockTransport(route)
    ) as api:
        with pytest.raises(IntegrationError):
            await make_monitor(config, api, store).submit()
    assert len(posts) == 1
    assert store.value == {}


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["needs_export", "failed"])
async def test_stopped_job_does_not_block_next_submission(config, api, store, status):
    monitor = await seed(config, api, store)
    monitor.state.jobs[0].status = status
    monitor.state.next_slot = 1
    await monitor.save()
    result = await make_monitor(config, api, store).submit()
    assert result["submitted_prediction_id"] == "p1"


@pytest.mark.asyncio
async def test_undelivered_page_is_kept_with_its_sites_for_resend(config, api, store):
    monitor = await seed(config, api, store)
    api.search_features.return_value = page([feature()])
    monitor.sender.send.side_effect = IntegrationConnectionError("portal down")
    with pytest.raises(IntegrationConnectionError):
        await monitor.collect()
    assert store.value["outbox"] is not None
    assert store.value["jobs"][0]["offset"] == 0
    assert len(store.value["sites"]) == 1
    monitor.sender.send.side_effect = None
    result = await monitor.collect()
    assert result["events_sent"] == 1
    assert result["detections_by_kind"] == {"new_site": 1}
    assert monitor.sender.send.call_args.args[0][0]["event_details"]["detection_kind"] == "new_site"


@pytest.mark.asyncio
async def test_event_type_can_change(config, api, store):
    await seed(config, api, store)
    config.event_type = "olmoearth_mining"
    await make_monitor(config, api, store).collect()


@pytest.mark.asyncio
async def test_accepted_page_commits_its_events(config, api, store):
    monitor = await seed(config, api, store)
    api.search_features.return_value = page([feature()])
    result = await monitor.collect()
    assert len(store.value["sites"]) == 1
    assert result["known_sites"] == 1
    assert store.value["outbox"] is None


@pytest_asyncio.fixture
async def redis_state_manager():
    """A state manager on a Redis the developer opted in with OLMOEARTH_TEST_REDIS_URL.

    Never the app's own REDIS_HOST: these tests flush the database they use.
    """
    import redis.asyncio as redis

    url = os.environ.get("OLMOEARTH_TEST_REDIS_URL")
    if not url:
        pytest.skip("Set OLMOEARTH_TEST_REDIS_URL to a disposable Redis to run this test")
    manager = IntegrationStateManager()
    manager.db_client = redis.Redis.from_url(url)
    await manager.db_client.flushdb()
    yield manager
    await manager.db_client.flushdb()
    await manager.db_client.aclose()


@pytest.mark.asyncio
async def test_monitor_site_tracking_on_redis(config, api, redis_state_manager):
    monitor = await seed(config, api, redis_state_manager)
    api.search_features.return_value = page([feature()])
    assert (await monitor.collect())["events_sent"] == 1
    result = await run_second_prediction(
        monitor, config, api, [feature("again", changed="2026-09-12")]
    )
    assert result["detections_by_kind"] == {"seen_before": 1}
    ledger = await redis_state_manager.get_state("integration-1", STATE_ACTION)
    (site,) = ledger["sites"].values()
    assert site["predictions"] == 2
    assert ledger["outbox"] is None


@pytest.mark.asyncio
async def test_event_sender_posts_once_per_call_and_reuses_its_client(mocker):
    client_ = SimpleNamespace(
        post_events=AsyncMock(side_effect=httpx.ReadTimeout("lost acknowledgement"))
    )
    lookup = mocker.patch(
        "app.actions.delivery.get_gundi_client", AsyncMock(return_value=client_)
    )
    sender = GundiEventSender("integration-1")
    for _ in range(2):
        with pytest.raises(httpx.ReadTimeout):
            await sender.send([{"title": "test"}])
    lookup.assert_awaited_once()
    assert client_.post_events.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response", [{"object_id": "1"}, [], ["id"], [{"errors": ["rejected"]}]]
)
async def test_event_sender_rejects_unclean_acknowledgement(mocker, response):
    client_ = SimpleNamespace(post_events=AsyncMock(return_value=response))
    mocker.patch("app.actions.delivery.get_gundi_client", AsyncMock(return_value=client_))
    with pytest.raises(IntegrationBadResponseError):
        await GundiEventSender("integration-1").send([{"title": "test"}])


@pytest.mark.asyncio
async def test_deleted_prediction_fails_its_job_instead_of_wedging(config, api, store):
    monitor = await seed(config, api, store)
    api.get_prediction.side_effect = IntegrationBadResponseError("gone", status_code=404)
    with pytest.raises(IntegrationBadResponseError, match="no longer exists"):
        await monitor.collect()
    assert store.value["jobs"][0]["status"] == "failed"
    api.get_prediction.reset_mock()
    await make_monitor(config, api, store).collect()
    api.get_prediction.assert_not_awaited()
    assert (await make_monitor(config, api, store).submit())["submitted_prediction_id"] == "p1"


@pytest.mark.asyncio
async def test_identity_change_names_the_changed_settings(config, api, store):
    await seed(config, api, store)
    config.cadence = Cadence.MONTHLY
    with pytest.raises(IntegrationConfigurationError, match=r"\(cadence\)"):
        await make_monitor(config, api, store).collect()


@pytest.mark.asyncio
async def test_stalled_prediction_fails_after_max_wait(config, api, store, activity_log):
    await seed(config, api, store)
    api.get_prediction.return_value = prediction(status="queued")
    soon = NOW + monitor_module.MAX_PREDICTION_WAIT - dt.timedelta(hours=1)
    assert (await make_monitor(config, api, store, now=soon).collect())["job"]["status"] == "pending"
    activity_log.assert_not_awaited()
    late = NOW + monitor_module.MAX_PREDICTION_WAIT + dt.timedelta(hours=1)
    result = await make_monitor(config, api, store, now=late).collect()
    assert result["job"]["status"] == "failed"
    assert "did not finish within 3 days (last status: queued)" in result["job"]["error"]
    assert activity_log.call_args.kwargs["level"] == monitor_module.LogLevel.WARNING
    assert "submitted_prediction_id" in await make_monitor(config, api, store, now=late).submit()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "setup,status",
    [
        (lambda api: setattr(api.get_prediction, "return_value", prediction(status="cancelled")), "failed"),
        (lambda api: setattr(api.search_features, "return_value", page([], total=20000)), "needs_export"),
    ],
)
async def test_quietly_stopped_job_logs_a_warning(config, api, store, activity_log, setup, status):
    monitor = await seed(config, api, store)
    setup(api)
    result = await monitor.collect()
    assert result["job"]["status"] == status
    (integration_id, action_id, title), kwargs = activity_log.call_args
    assert (integration_id, action_id) == ("integration-1", "pull_events")
    assert f"({status})" in title
    assert kwargs["level"] == monitor_module.LogLevel.WARNING


@pytest.mark.asyncio
async def test_result_reports_only_the_current_job(config, api, store):
    monitor = await seed(config, api, store)
    monitor.state.jobs.insert(0, pending_job(config, "old", name="old").copy(update={"status": "complete"}))
    await monitor.save()
    result = await make_monitor(config, api, store).collect()
    assert result["job"]["prediction_id"] == "p1"
    assert "jobs" not in result
    assert (await make_monitor(config, api, store).collect())["job"] is None


def test_form_dropdowns_name_registered_reference_actions():
    from app.actions.core import ReferenceActionConfiguration, discover_actions

    actions = discover_actions("app.actions.handlers", "action_")
    ui = PullEventsConfig.ui_schema()
    for field in ("project_id", "model_id"):
        reference = ui[field]["gundi:reference"]
        handler, config_model, _ = actions[reference["action"]]
        assert issubclass(config_model, ReferenceActionConfiguration)
        assert set(reference["params"]) <= set(config_model.__fields__)
        assert "ui:widget" not in ui[field]
    assert ui["model_id"]["gundi:reference"]["params"] == {"project_id": {"$data": "project_id"}}
    schema = PullEventsConfig.schema()["properties"]
    assert "UUID" in schema["project_id"]["description"]
    assert "UUID" in schema["model_id"]["description"]


@pytest.mark.asyncio
async def test_reference_actions_list_projects_and_finished_models(mocker):
    seen = []

    def route(request):
        seen.append((request.url.path, json.loads(request.content)))
        return httpx.Response(
            200,
            json={
                "records": [{"id": PROJECT, "name": "Mining pilot"}, {"id": MODEL}],
                "meta": {"total": 3},
            },
        )

    mocker.patch.object(
        handlers,
        "client_for",
        lambda integration: client.OlmoEarthClient(
            "https://example.test", "token", transport=httpx.MockTransport(route)
        ),
    )
    from app.actions.configurations import ListModelsConfig, ListProjectsConfig

    projects = await handlers.action_list_projects(SimpleNamespace(), ListProjectsConfig())
    assert projects["options"][0] == {
        "value": PROJECT, "label": "Mining pilot", "description": None, "group": None
    }
    assert projects["options"][1]["label"] == MODEL
    assert projects["truncated"] is True
    await handlers.action_list_models(SimpleNamespace(), ListModelsConfig(project_id=PROJECT))
    path, body = seen[-1]
    assert path == "/api/v1/models/search"
    assert body["project_id"] == {"eq": PROJECT}
    assert body["model_type"] == {"eq": "fine_tuned"}
    assert body["status"] == {"eq": "completed"}
