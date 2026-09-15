"""Tests for the OlmoEarth connector.

The contracts worth pinning down here are the ones the API sample does not
enforce for us: that an unset filter operator is *absent* from the request
rather than sent empty, that offset paging cannot skip a record, that a
polygon detection still gets a point location, and that a failed run leaves a
watermark the next run can resume from.
"""
import datetime
import json
from typing import List, Optional

import httpx
import pytest
from gundi_core.schemas.v2 import Integration

import app.actions.client as client
from app.actions import handlers
from app.actions.configurations import (
    AuthenticateConfig,
    ListPredictionsConfig,
    PredictionSelection,
    PullEventsConfig,
)
from app.services.errors import (
    IntegrationAuthError,
    IntegrationBadResponseError,
    IntegrationConfigurationError,
    IntegrationRateLimitError,
)

BASE_URL = "https://olmoearth.example.org"
PREDICTIONS_PATH = "/api/v1/predictions/search"


def features_path(result_id: str) -> str:
    return f"/api/v1/prediction-results/{result_id}/features/search"


# --------------------------------------------------------------------------
# Fixtures
# --------------------------------------------------------------------------
@pytest.fixture
def integration():
    return Integration.parse_obj(
        {
            "id": "bf1e4b30-6d1a-4c58-9a55-f2c1b0a9e111",
            "name": "OlmoEarth Test",
            "base_url": BASE_URL,
            "enabled": True,
            "type": {
                "id": "9a0c6f2a-2a55-4b1c-8f1e-0c2a4e5d6f70",
                "name": "OlmoEarth",
                "value": "olmoearth",
                "description": "OlmoEarth predictions",
                "actions": [],
            },
            "owner": {
                "id": "45018398-7a2a-4f48-8971-39a2710d5dbd",
                "name": "Gundi Engineering",
                "description": "Test organization",
            },
            "configurations": [
                {
                    "id": "0a6b4f3e-8b2c-4a55-9d21-7f5e4c3b2a19",
                    "integration": "bf1e4b30-6d1a-4c58-9a55-f2c1b0a9e111",
                    "action": {
                        "id": "3f0a1b2c-4d5e-6f70-8192-a3b4c5d6e7f8",
                        "type": "auth",
                        "name": "Authenticate",
                        "value": "auth",
                    },
                    "data": {"api_token": "s3cr3t-token"},
                }
            ],
            "additional": {},
            "default_route": None,
            "status": "healthy",
        }
    )


@pytest.fixture
def pull_config():
    return PullEventsConfig(model_id="model-abc")


def make_transport(routes):
    """A MockTransport dispatching by request path, recording every request.

    `routes` maps a path to either an httpx.Response or a list of responses
    served in order (for paging).
    """
    recorded = []

    def handler(request: httpx.Request) -> httpx.Response:
        recorded.append(request)
        route = routes[request.url.path]
        if isinstance(route, list):
            return route.pop(0) if route else httpx.Response(200, json={"records": [], "meta": {"total": 0}})
        return route

    return httpx.MockTransport(handler), recorded


def json_response(records, total=None, errors=None, status=200):
    body = {"records": records, "meta": {"total": total if total is not None else len(records)}}
    if errors is not None:
        body["errors"] = errors
    return httpx.Response(status, json=body)


def feature(
    feature_id=1,
    geometry=None,
    bbox=None,
    start_time="2026-09-01T00:00:00Z",
    created_at="2026-09-02T00:00:00Z",
    extra_properties=None,
):
    properties = {
        "oe_start_time": start_time,
        "oe_created_at": created_at,
        "oe_prediction_result_id": "result-1",
        "oe_prediction_result_file_id": "file-1",
    }
    properties.update(extra_properties or {})
    record = {
        "type": "Feature",
        "id": feature_id,
        "geometry": geometry or {"type": "Point", "coordinates": [-72.7, -51.7]},
        "properties": properties,
    }
    if bbox:
        record["bbox"] = bbox
    return record


def prediction(pid="pred-1", creation_time="2026-09-02T00:00:00Z", **extra):
    record = {
        "id": pid,
        "name": f"Run {pid}",
        "status": "completed",
        "model_id": "model-abc",
        "creation_time": creation_time,
    }
    record.update(extra)
    return record


# --------------------------------------------------------------------------
# Request serialization
# --------------------------------------------------------------------------
def test_an_unset_operator_is_absent_from_the_body_not_sent_empty():
    """The documented sample shows every operator with an empty value. Sent
    literally, `{"eq": ""}` asks for records equal to the empty string — which
    matches nothing — so only the operators we set may appear."""
    body = client.PredictionSearchRequest(
        model_id=client.KeywordFilter(eq="model-abc")
    ).as_body()

    assert body["model_id"] == {"eq": "model-abc"}
    assert "neq" not in body["model_id"]
    assert "exists" not in body["model_id"]
    # Filters we never touched are absent entirely.
    assert "project_id" not in body
    assert "status" not in body


def test_datetimes_are_serialized_as_iso_strings():
    """`as_body` has to produce something json-serializable: httpx would choke
    on a datetime, and the failure would only show up against a live API."""
    when = datetime.datetime(2026, 9, 1, 12, 30, tzinfo=datetime.timezone.utc)
    body = client.PredictionSearchRequest(creation_time=client.DatetimeFilter(gte=when)).as_body()

    assert body["creation_time"] == {"gte": "2026-09-01T12:30:00+00:00"}
    json.dumps(body)  # would raise if anything were left un-encoded


def test_explicit_paging_values_survive_serialization():
    """limit/offset have defaults, so `exclude_defaults` would drop offset=0 —
    and an omitted offset is a different request from offset 0."""
    body = client.FeatureSearchRequest(limit=50, offset=0).as_body()

    assert body["limit"] == 50
    assert body["offset"] == 0


# --------------------------------------------------------------------------
# Resolving result ids
# --------------------------------------------------------------------------
@pytest.mark.parametrize(
    "extra",
    [
        {"prediction_result_ids": ["result-9"]},
        {"result_ids": ["result-9"]},
        {"results": [{"id": "result-9"}]},
        {"prediction_results": ["result-9"]},
    ],
)
def test_result_ids_are_read_from_whichever_field_names_them(extra):
    """The predictions response schema was not documented, so the connector
    probes the plausible field names rather than assuming one."""
    assert client.result_ids_for(client.Prediction.parse_obj(prediction(**extra))) == ["result-9"]


def test_a_prediction_naming_no_results_falls_back_to_its_own_id():
    """The documented flow gives us no link between the two identifiers. Until
    that is confirmed, assume they are the same value — and log loudly."""
    assert client.result_ids_for(client.Prediction.parse_obj(prediction("pred-7"))) == ["pred-7"]


def test_several_results_are_all_returned():
    parsed = client.Prediction.parse_obj(prediction(result_ids=["a", "b", "c"]))

    assert client.result_ids_for(parsed) == ["a", "b", "c"]


# --------------------------------------------------------------------------
# Geometry
# --------------------------------------------------------------------------
def test_a_point_feature_keeps_its_own_coordinates():
    location = handlers.centroid_of(client.Feature.parse_obj(feature()))

    assert location == {"lat": -51.7, "lon": -72.7}


def test_a_polygon_is_reduced_to_a_point_so_gundi_can_place_it():
    polygon = {
        "type": "Polygon",
        "coordinates": [[[0.0, 0.0], [0.0, 2.0], [2.0, 2.0], [2.0, 0.0], [0.0, 0.0]]],
    }
    location = handlers.centroid_of(client.Feature.parse_obj(feature(geometry=polygon)))

    assert location["lat"] == pytest.approx(0.8)
    assert location["lon"] == pytest.approx(0.8)


def test_the_providers_bbox_wins_over_averaging_the_vertices():
    """The provider computed the bbox over the real geometry; averaging the
    vertices of a ring double-counts the repeated closing point."""
    polygon = {
        "type": "Polygon",
        "coordinates": [[[0.0, 0.0], [0.0, 2.0], [2.0, 2.0], [2.0, 0.0], [0.0, 0.0]]],
    }
    location = handlers.centroid_of(
        client.Feature.parse_obj(feature(geometry=polygon, bbox=[0.0, 0.0, 2.0, 2.0]))
    )

    assert location == {"lat": 1.0, "lon": 1.0}


def test_a_feature_with_no_geometry_has_no_location():
    record = feature()
    record["geometry"] = None
    assert handlers.centroid_of(client.Feature.parse_obj(record)) is None


# --------------------------------------------------------------------------
# Transformation
# --------------------------------------------------------------------------
def test_an_event_is_timestamped_when_the_model_saw_it_not_when_it_was_written(pull_config):
    """`oe_start_time` is the observation window; `oe_created_at` is when the
    record was written. They differ by however long processing took, which for
    a detection feed is the difference between a useful timestamp and a useless
    one."""
    event = handlers.transform_feature(
        client.Feature.parse_obj(feature(start_time="2026-08-01T00:00:00Z", created_at="2026-09-02T00:00:00Z")),
        client.Prediction.parse_obj(prediction()),
        "result-1",
        pull_config,
    )

    assert event["recorded_at"].startswith("2026-08-01T00:00:00")
    assert event["event_details"]["detected_at"].startswith("2026-09-02T00:00:00")


def test_a_feature_without_a_start_time_falls_back_to_when_it_was_created(pull_config):
    record = feature(start_time=None)
    record["properties"].pop("oe_start_time")
    event = handlers.transform_feature(
        client.Feature.parse_obj(record), client.Prediction.parse_obj(prediction()), "result-1", pull_config
    )

    assert event["recorded_at"].startswith("2026-09-02T00:00:00")


def test_the_models_own_properties_are_kept_apart_from_the_oe_provenance(pull_config):
    """`oe_`-prefixed fields are bookkeeping. Burying the model's actual output
    among them is what makes a detection feed unreadable downstream."""
    event = handlers.transform_feature(
        client.Feature.parse_obj(feature(extra_properties={"confidence": 0.91, "species": "elephant"})),
        client.Prediction.parse_obj(prediction()),
        "result-1",
        pull_config,
    )

    details = event["event_details"]
    assert details["confidence"] == 0.91
    assert details["species"] == "elephant"
    assert not [key for key in details if key.startswith("oe_")]
    # Provenance is still present, under names that say what they are.
    assert details["prediction_id"] == "pred-1"
    assert details["prediction_result_id"] == "result-1"
    assert details["model_id"] == "model-abc"


def test_a_feature_id_is_qualified_by_its_result_so_it_is_globally_unique(pull_config):
    """Feature ids restart per prediction result — the sample response returns
    `1`. Unqualified, feature 1 of every result would collide."""
    event = handlers.transform_feature(
        client.Feature.parse_obj(feature(feature_id=1)),
        client.Prediction.parse_obj(prediction()),
        "result-42",
        pull_config,
    )

    assert event["external_source_id"] == "result-42:1"


def test_the_full_geometry_travels_with_the_event_by_default(pull_config):
    polygon = {"type": "Polygon", "coordinates": [[[0.0, 0.0], [0.0, 1.0], [1.0, 1.0], [0.0, 0.0]]]}
    event = handlers.transform_feature(
        client.Feature.parse_obj(feature(geometry=polygon)),
        client.Prediction.parse_obj(prediction()),
        "result-1",
        pull_config,
    )

    assert event["geometry"]["type"] == "Polygon"
    assert event["geometry"]["coordinates"] == polygon["coordinates"]


def test_geometry_can_be_left_off(pull_config):
    pull_config.include_geometry = False
    event = handlers.transform_feature(
        client.Feature.parse_obj(feature()), client.Prediction.parse_obj(prediction()), "result-1", pull_config
    )

    assert "geometry" not in event


def test_an_unplaceable_feature_is_skipped_rather_than_sent_without_a_location(pull_config):
    record = feature()
    record["geometry"] = None
    assert handlers.transform_feature(
        client.Feature.parse_obj(record), client.Prediction.parse_obj(prediction()), "result-1", pull_config
    ) is None


# --------------------------------------------------------------------------
# Query construction
# --------------------------------------------------------------------------
def test_the_prediction_window_is_inclusive_of_the_watermark(pull_config):
    """`gt` would drop a prediction created in the same second as the
    watermark. The processed-id list is what stops the boundary prediction from
    being ingested twice."""
    since = datetime.datetime(2026, 9, 1, tzinfo=datetime.timezone.utc)
    request = handlers.build_prediction_query(pull_config, since)

    assert request.creation_time.gte == since
    assert request.creation_time.gt is None
    assert request.model_id.eq == "model-abc"
    assert request.sort_direction == "desc"


def test_an_empty_status_means_every_status(pull_config):
    pull_config.prediction_status = None
    assert handlers.build_prediction_query(pull_config, datetime.datetime.now(datetime.timezone.utc)).status is None


def test_a_minimum_confidence_becomes_a_provider_side_property_filter(pull_config):
    """Filtering here rather than after the fact is the difference between
    paging through every detection and paging through the ones that matter."""
    pull_config.min_confidence = 0.8
    request = handlers.build_feature_query(pull_config)

    assert request.property_filters[0].property_name == "confidence"
    assert request.property_filters[0].numeric_filter.gte == 0.8


def test_the_confidence_property_name_is_configurable(pull_config):
    pull_config.min_confidence = 0.5
    pull_config.confidence_property = "score"

    assert handlers.build_feature_query(pull_config).property_filters[0].property_name == "score"


def test_an_area_of_interest_becomes_the_intersects_geometry_filter(pull_config):
    pull_config.area_of_interest = {"type": "Polygon", "coordinates": [[[0, 0], [0, 1], [1, 1], [0, 0]]]}
    request = handlers.build_feature_query(pull_config)

    assert request.intersects_geometry.type == "Polygon"


def test_a_malformed_area_of_interest_is_reported_as_a_configuration_problem(pull_config):
    """Classified as configuration, not as a provider failure: nothing was
    wrong upstream, and the operator needs to know it is their field."""
    pull_config.area_of_interest = {"coordinates": [[0, 0]]}  # no `type`

    with pytest.raises(IntegrationConfigurationError):
        handlers.build_feature_query(pull_config)


# --------------------------------------------------------------------------
# Client transport
# --------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_a_rejected_token_is_reported_as_an_auth_failure():
    transport, _ = make_transport({PREDICTIONS_PATH: httpx.Response(401, text="bad token")})
    async with client.OlmoEarthClient(BASE_URL, "t", transport=transport) as api:
        with pytest.raises(IntegrationAuthError):
            await api.search_predictions(client.PredictionSearchRequest())


@pytest.mark.asyncio
async def test_rate_limiting_is_classified_as_such_after_its_retries():
    transport, recorded = make_transport({PREDICTIONS_PATH: httpx.Response(429, text="slow down")})
    async with client.OlmoEarthClient(BASE_URL, "t", transport=transport) as api:
        with pytest.raises(IntegrationRateLimitError):
            await api.search_predictions(client.PredictionSearchRequest())
    assert len(recorded) > 1, "a 429 is worth retrying"


@pytest.mark.asyncio
async def test_a_bad_request_is_not_retried():
    """A 4xx is a definite answer. Retrying it just delays the error."""
    transport, recorded = make_transport({PREDICTIONS_PATH: httpx.Response(422, text="bad filter")})
    async with client.OlmoEarthClient(BASE_URL, "t", transport=transport) as api:
        with pytest.raises(IntegrationBadResponseError):
            await api.search_predictions(client.PredictionSearchRequest())
    assert len(recorded) == 1


@pytest.mark.asyncio
async def test_errors_returned_alongside_a_200_with_no_records_are_a_failure():
    """The documented response carries `errors` next to `records`, so a 200 is
    not on its own proof that the search ran."""
    transport, _ = make_transport(
        {PREDICTIONS_PATH: json_response([], errors=[{"code": "not_found_error", "message": "no such model"}])}
    )
    async with client.OlmoEarthClient(BASE_URL, "t", transport=transport) as api:
        with pytest.raises(IntegrationBadResponseError, match="not_found_error"):
            await api.search_predictions(client.PredictionSearchRequest())


@pytest.mark.asyncio
async def test_partial_results_are_kept_when_errors_accompany_records():
    transport, _ = make_transport(
        {PREDICTIONS_PATH: json_response([prediction()], errors=[{"code": "partial", "message": "one shard down"}])}
    )
    async with client.OlmoEarthClient(BASE_URL, "t", transport=transport) as api:
        response = await api.search_predictions(client.PredictionSearchRequest())

    assert len(response.records) == 1


@pytest.mark.asyncio
async def test_the_token_is_sent_as_a_bearer_header():
    transport, recorded = make_transport({PREDICTIONS_PATH: json_response([])})
    async with client.OlmoEarthClient(BASE_URL, "s3cr3t", transport=transport) as api:
        await api.search_predictions(client.PredictionSearchRequest())

    assert recorded[0].headers["authorization"] == "Bearer s3cr3t"


def test_a_client_without_a_base_url_says_so_rather_than_building_a_broken_url():
    with pytest.raises(IntegrationConfigurationError, match="base URL"):
        client.OlmoEarthClient("", "token")


def test_a_client_without_a_token_points_at_the_auth_action():
    with pytest.raises(IntegrationConfigurationError, match="Authenticate"):
        client.OlmoEarthClient(BASE_URL, "")


# --------------------------------------------------------------------------
# Paging
# --------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_paging_walks_every_page_until_a_short_one():
    path = features_path("result-1")
    transport, recorded = make_transport(
        {path: [json_response([feature(i) for i in range(3)], total=5), json_response([feature(3), feature(4)], total=5)]}
    )
    async with client.OlmoEarthClient(BASE_URL, "t", transport=transport) as api:
        collected = [f async for f in api.iter_features("result-1", client.FeatureSearchRequest(limit=3))]

    # Ids come back as strings: the response sends integers, the filters send
    # keywords, and the model settles on one so identity is stable.
    assert [f.id for f in collected] == ["0", "1", "2", "3", "4"]
    assert [json.loads(r.content)["offset"] for r in recorded] == [0, 3]


@pytest.mark.asyncio
async def test_paging_sorts_ascending_so_a_new_record_cannot_shift_the_window():
    """Offset paging over a `desc` sort skips a record whenever one is inserted
    mid-walk: everything after it shifts one slot forward. Ascending order only
    ever appends past the window already read."""
    path = features_path("result-1")
    transport, recorded = make_transport({path: [json_response([feature(1)], total=1)]})
    async with client.OlmoEarthClient(BASE_URL, "t", transport=transport) as api:
        request = client.FeatureSearchRequest(limit=50, sort_direction="desc")
        [f async for f in api.iter_features("result-1", request)]

    body = json.loads(recorded[0].content)
    assert body["sort_by"] == "oe_created_at"
    assert body["sort_direction"] == "asc"
    # The caller's own request object is left as they built it.
    assert request.sort_direction == "desc"


@pytest.mark.asyncio
async def test_paging_stops_at_the_feature_cap():
    path = features_path("result-1")
    transport, _ = make_transport({path: [json_response([feature(i) for i in range(50)], total=1000)] * 5})
    async with client.OlmoEarthClient(BASE_URL, "t", transport=transport) as api:
        collected = [
            f async for f in api.iter_features("result-1", client.FeatureSearchRequest(limit=50), max_features=75)
        ]

    assert len(collected) == 75


@pytest.mark.asyncio
async def test_paging_stops_once_the_reported_total_is_reached():
    """A provider that pads the last page to full size would otherwise be
    paged forever."""
    path = features_path("result-1")
    transport, recorded = make_transport({path: [json_response([feature(i) for i in range(2)], total=2)] * 3})
    async with client.OlmoEarthClient(BASE_URL, "t", transport=transport) as api:
        collected = [f async for f in api.iter_features("result-1", client.FeatureSearchRequest(limit=2))]

    assert len(collected) == 2
    assert len(recorded) == 1


# --------------------------------------------------------------------------
# Actions
# --------------------------------------------------------------------------
@pytest.fixture
def captured_gundi(mocker):
    """Capture what the connector sends to Gundi instead of sending it."""
    sent = []

    async def capture(events, **kwargs):
        sent.append({"events": events, "integration_id": kwargs.get("integration_id")})
        return {"created": len(events)}

    mocker.patch.object(handlers.gundi_tools, "send_events_to_gundi", side_effect=capture)
    return sent


@pytest.fixture
def captured_state(mocker):
    """An in-memory stand-in for the Redis-backed state manager."""
    store = {}

    async def get_state(integration_id, action_id, source_id="no-source"):
        return store.get((integration_id, action_id, source_id), {})

    async def set_state(integration_id, action_id, state, source_id="no-source"):
        store[(integration_id, action_id, source_id)] = state

    mocker.patch.object(handlers.state_manager, "get_state", side_effect=get_state)
    mocker.patch.object(handlers.state_manager, "set_state", side_effect=set_state)
    return store


@pytest.fixture
def no_activity_logs(mocker):
    """The activity logger publishes to PubSub; the actions' own logic is what
    these tests are about."""
    async def noop(*args, **kwargs):
        return None

    mocker.patch.object(handlers, "log_action_activity", side_effect=noop)
    mocker.patch("app.services.activity_logger.publish_event", side_effect=noop)


def route_client(mocker, routes):
    """Point every client the handlers build at a MockTransport."""
    transport, recorded = make_transport(routes)
    real = handlers.client_for

    def build(integration, auth_config=None):
        api = real(integration, auth_config)
        api._client = httpx.AsyncClient(
            base_url=api.base_url,
            headers={"Authorization": f"Bearer {api._api_token}", "Content-Type": "application/json"},
            transport=transport,
        )
        return api

    mocker.patch.object(handlers, "client_for", side_effect=build)
    return recorded


@pytest.mark.asyncio
async def test_auth_checks_the_credentials_against_a_real_search(mocker, integration):
    """An empty result is still proof the host, path and token all work."""
    recorded = route_client(mocker, {PREDICTIONS_PATH: json_response([], total=0)})

    result = await handlers.action_auth(integration, AuthenticateConfig(api_token="s3cr3t-token"))

    assert result["valid_credentials"] is True
    assert json.loads(recorded[0].content)["limit"] == 1


@pytest.mark.asyncio
async def test_auth_surfaces_a_rejected_token(mocker, integration):
    route_client(mocker, {PREDICTIONS_PATH: httpx.Response(403, text="forbidden")})

    with pytest.raises(IntegrationAuthError):
        await handlers.action_auth(integration, AuthenticateConfig(api_token="wrong"))


@pytest.mark.asyncio
async def test_list_predictions_renders_options_the_portal_can_show(mocker, integration):
    route_client(
        mocker,
        {PREDICTIONS_PATH: json_response([prediction("pred-1"), prediction("pred-2")], total=2)},
    )

    response = await handlers.action_list_predictions(integration, ListPredictionsConfig())

    assert [option["value"] for option in response["options"]] == ["pred-1", "pred-2"]
    assert response["options"][0]["label"] == "Run pred-1"
    assert response["truncated"] is False


@pytest.mark.asyncio
async def test_list_predictions_says_when_the_list_is_only_a_prefix(mocker, integration):
    """The portal caches the list; it has to know it is not the whole set."""
    route_client(mocker, {PREDICTIONS_PATH: json_response([prediction("pred-1")], total=900)})

    response = await handlers.action_list_predictions(integration, ListPredictionsConfig(limit=1))

    assert response["truncated"] is True


@pytest.mark.asyncio
async def test_a_pull_turns_features_into_events(
    mocker, integration, pull_config, captured_gundi, captured_state, no_activity_logs
):
    route_client(
        mocker,
        {
            PREDICTIONS_PATH: json_response([prediction("pred-1", result_ids=["result-1"])], total=1),
            features_path("result-1"): [json_response([feature(1), feature(2)], total=2)],
        },
    )

    result = await handlers.action_pull_events(integration, pull_config)

    assert result["predictions_processed"] == 1
    assert result["features_extracted"] == 2
    assert result["events_sent"] == 2
    events = captured_gundi[0]["events"]
    assert [e["external_source_id"] for e in events] == ["result-1:1", "result-1:2"]
    assert events[0]["event_type"] == "olmoearth_detection"


@pytest.mark.asyncio
async def test_a_prediction_already_ingested_is_not_ingested_again(
    mocker, integration, pull_config, captured_gundi, captured_state, no_activity_logs
):
    """The search is bounded with `gte`, so the boundary prediction comes back
    every run. Recognising it is what keeps events from being duplicated."""
    routes = {
        PREDICTIONS_PATH: json_response([prediction("pred-1", result_ids=["result-1"])], total=1),
        features_path("result-1"): [json_response([feature(1)], total=1)],
    }
    route_client(mocker, routes)
    await handlers.action_pull_events(integration, pull_config)

    # The same prediction is still the newest thing the provider has.
    routes[PREDICTIONS_PATH] = json_response([prediction("pred-1", result_ids=["result-1"])], total=1)
    routes[features_path("result-1")] = [json_response([feature(1)], total=1)]
    route_client(mocker, routes)
    second = await handlers.action_pull_events(integration, pull_config)

    assert second["predictions_processed"] == 0
    assert len(captured_gundi) == 1, "nothing should have been re-sent"


@pytest.mark.asyncio
async def test_the_watermark_advances_to_the_newest_prediction(
    mocker, integration, pull_config, captured_gundi, captured_state, no_activity_logs
):
    route_client(
        mocker,
        {
            PREDICTIONS_PATH: json_response(
                [
                    prediction("pred-2", creation_time="2026-09-05T00:00:00Z", result_ids=["result-2"]),
                    prediction("pred-1", creation_time="2026-09-04T00:00:00Z", result_ids=["result-1"]),
                ],
                total=2,
            ),
            features_path("result-1"): [json_response([feature(1)], total=1)],
            features_path("result-2"): [json_response([feature(2)], total=1)],
        },
    )

    result = await handlers.action_pull_events(integration, pull_config)

    assert result["predictions_processed"] == 2
    assert result["watermark"].startswith("2026-09-05T00:00:00")
    state = captured_state[(str(integration.id), "pull_events", "no-source")]
    # Newest first, so the most recently ingested survives the bound.
    assert state["processed_prediction_ids"] == ["pred-2", "pred-1"]


@pytest.mark.asyncio
async def test_a_failure_partway_through_leaves_the_finished_predictions_behind(
    mocker, integration, pull_config, captured_gundi, captured_state, no_activity_logs
):
    """State is written per prediction, not once at the end. A run that dies on
    the second prediction must not re-send the first one's events next time."""
    route_client(
        mocker,
        {
            PREDICTIONS_PATH: json_response(
                [
                    prediction("pred-2", creation_time="2026-09-05T00:00:00Z", result_ids=["result-2"]),
                    prediction("pred-1", creation_time="2026-09-04T00:00:00Z", result_ids=["result-1"]),
                ],
                total=2,
            ),
            features_path("result-1"): [json_response([feature(1)], total=1)],
            features_path("result-2"): httpx.Response(500, text="boom"),
        },
    )

    with pytest.raises(IntegrationBadResponseError):
        await handlers.action_pull_events(integration, pull_config)

    state = captured_state[(str(integration.id), "pull_events", "no-source")]
    assert state["processed_prediction_ids"] == ["pred-1"]
    assert state["last_prediction_creation_time"].startswith("2026-09-04T00:00:00")


@pytest.mark.asyncio
async def test_latest_only_takes_the_newest_prediction_and_leaves_the_rest(
    mocker, integration, captured_gundi, captured_state, no_activity_logs
):
    route_client(
        mocker,
        {
            PREDICTIONS_PATH: json_response(
                [
                    prediction("pred-2", creation_time="2026-09-05T00:00:00Z", result_ids=["result-2"]),
                    prediction("pred-1", creation_time="2026-09-04T00:00:00Z", result_ids=["result-1"]),
                ],
                total=2,
            ),
            features_path("result-2"): [json_response([feature(1)], total=1)],
        },
    )
    config = PullEventsConfig(model_id="model-abc", prediction_selection=PredictionSelection.LATEST_ONLY)

    result = await handlers.action_pull_events(integration, config)

    assert result["predictions_processed"] == 1
    assert captured_gundi[0]["events"][0]["event_details"]["prediction_id"] == "pred-2"


@pytest.mark.asyncio
async def test_a_first_run_reaches_back_by_the_configured_lookback(
    mocker, integration, captured_gundi, captured_state, no_activity_logs
):
    recorded = route_client(mocker, {PREDICTIONS_PATH: json_response([], total=0)})
    config = PullEventsConfig(model_id="model-abc", lookback_days=3)

    await handlers.action_pull_events(integration, config)

    since = datetime.datetime.fromisoformat(json.loads(recorded[0].content)["creation_time"]["gte"])
    age = datetime.datetime.now(datetime.timezone.utc) - since
    assert 2.9 < age.total_seconds() / 86400 < 3.1


@pytest.mark.asyncio
async def test_events_are_sent_in_batches_of_the_configured_size(
    mocker, integration, captured_gundi, captured_state, no_activity_logs
):
    route_client(
        mocker,
        {
            PREDICTIONS_PATH: json_response([prediction("pred-1", result_ids=["result-1"])], total=1),
            features_path("result-1"): [json_response([feature(i) for i in range(5)], total=5)],
        },
    )
    config = PullEventsConfig(model_id="model-abc", events_per_request=2)

    result = await handlers.action_pull_events(integration, config)

    assert result["events_sent"] == 5
    assert [len(call["events"]) for call in captured_gundi] == [2, 2, 1]


@pytest.mark.asyncio
async def test_a_run_with_nothing_new_is_a_clean_no_op(
    mocker, integration, pull_config, captured_gundi, captured_state, no_activity_logs
):
    route_client(mocker, {PREDICTIONS_PATH: json_response([], total=0)})

    result = await handlers.action_pull_events(integration, pull_config)

    assert result == {
        "predictions_processed": 0,
        "features_extracted": 0,
        "events_sent": 0,
        "since": result["since"],
    }
    assert captured_gundi == []


@pytest.mark.asyncio
async def test_unplaceable_features_are_counted_as_read_but_not_sent(
    mocker, integration, pull_config, captured_gundi, captured_state, no_activity_logs
):
    """`features_extracted` counts what the provider returned; `events_sent`
    counts what reached Gundi. Collapsing them would hide the skips."""
    placeless = feature(2)
    placeless["geometry"] = None
    route_client(
        mocker,
        {
            PREDICTIONS_PATH: json_response([prediction("pred-1", result_ids=["result-1"])], total=1),
            features_path("result-1"): [json_response([feature(1), placeless], total=2)],
        },
    )

    result = await handlers.action_pull_events(integration, pull_config)

    assert result["features_extracted"] == 2
    assert result["events_sent"] == 1


@pytest.mark.asyncio
async def test_an_unparseable_stored_watermark_falls_back_to_the_lookback(
    mocker, integration, pull_config, captured_gundi, captured_state, no_activity_logs
):
    """A corrupt state row should degrade to "re-read the lookback window", not
    wedge the integration."""
    captured_state[(str(integration.id), "pull_events", "no-source")] = {
        "last_prediction_creation_time": "not-a-date",
        "processed_prediction_ids": [],
    }
    recorded = route_client(mocker, {PREDICTIONS_PATH: json_response([], total=0)})

    await handlers.action_pull_events(integration, pull_config)

    assert "creation_time" in json.loads(recorded[0].content)


@pytest.mark.asyncio
async def test_an_integration_with_no_auth_configured_says_so(integration, pull_config):
    integration.configurations = []

    with pytest.raises(IntegrationConfigurationError, match="authentication"):
        handlers.client_for(integration)
