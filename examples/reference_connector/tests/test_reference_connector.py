import importlib

import pytest
from fastapi.testclient import TestClient

from gundi_action_runner import create_app
from gundi_action_runner.registry import registry


@pytest.fixture
def reference_registry():
    saved_actions = dict(registry.action_handlers)
    saved_webhook = registry.webhook_handler
    import reference_connector.handlers as handlers
    registry.reset()
    importlib.reload(handlers)  # re-fire decorators against the clean registry
    yield registry
    registry.reset()
    registry.action_handlers.update(saved_actions)
    registry.webhook_handler = saved_webhook


def test_actions_are_registered(reference_registry):
    assert set(reference_registry.action_handlers) == {"auth", "pull_observations"}
    pull_func, pull_config, _ = reference_registry.action_handlers["pull_observations"]
    assert pull_func.action_title == "Pull Reference Observations"


def test_webhook_is_registered(reference_registry):
    from reference_connector.configurations import ReferenceWebhookPayload
    func, payload_model, config_model = reference_registry.webhook_handler
    assert payload_model is ReferenceWebhookPayload


def test_actions_endpoint_lists_reference_actions(reference_registry):
    app = create_app(handlers_modules=[])  # registry populated by the fixture
    client = TestClient(app)
    response = client.get("/v1/actions/")
    assert response.status_code == 200
    assert set(response.json()) == {"auth", "pull_observations"}
