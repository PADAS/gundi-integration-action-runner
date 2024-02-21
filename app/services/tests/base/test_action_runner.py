import pytest
from fastapi.testclient import TestClient
from app.main import app


api_client = TestClient(app)


def set_mocks(
    action_runner_story,
    gundi_api_story
):
    return {
        "given_action_handlers_mock": action_runner_story.given_action_handlers_mock(),
        "given_gundi_client_v2_mock": gundi_api_story.given_gundi_client_v2_mock(),
        "given_publish_event_mock": action_runner_story.given_publish_event_mock()
    }


@pytest.mark.asyncio
async def test_execute_action_from_pubsub(
        mocker,
        action_runner_story,
        gundi_api_story
):
    mocks = set_mocks(action_runner_story, gundi_api_story)
    mocker.patch("app.services.action_runner.action_handlers", mocks["given_action_handlers_mock"])
    mocker.patch("app.services.action_runner._portal", mocks["given_gundi_client_v2_mock"])
    mocker.patch("app.services.activity_logger.publish_event", mocks["given_publish_event_mock"])

    response = api_client.post(
        "/",
        headers=action_runner_story.given_event_v2_cloud_event_headers(),
        json=action_runner_story.given_event_v2_cloud_event_payload(),
    )

    assert response.status_code == 200
    assert mocks["given_gundi_client_v2_mock"].get_integration_details.called
    assert mocks["given_action_handlers_mock"]["pull_observations"].called


@pytest.mark.asyncio
async def test_execute_action_from_api(
        mocker,
        action_runner_story,
        gundi_api_story
):
    mocks = set_mocks(action_runner_story, gundi_api_story)
    mocker.patch("app.services.action_runner.action_handlers", mocks["given_action_handlers_mock"])
    mocker.patch("app.services.action_runner._portal", mocks["given_gundi_client_v2_mock"])
    mocker.patch("app.services.activity_logger.publish_event", mocks["given_publish_event_mock"])

    response = api_client.post(
        "/v1/actions/execute/",
        json={
            "integration_id": str(gundi_api_story.given_default_integration_v2_object().id),
            "action_id": "pull_observations"
        }
    )

    assert response.status_code == 200
    assert mocks["given_gundi_client_v2_mock"].get_integration_details.called
    assert mocks["given_action_handlers_mock"]["pull_observations"].called
