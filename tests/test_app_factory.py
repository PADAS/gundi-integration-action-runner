import sys
import types

import pytest
from fastapi.testclient import TestClient

from gundi_action_runner import create_app
from gundi_action_runner.registry import registry


@pytest.fixture(autouse=True)
def clean_registry():
    saved_actions = dict(registry.action_handlers)
    saved_webhook = registry.webhook_handler
    registry.reset()
    yield registry
    registry.reset()
    registry.action_handlers.update(saved_actions)
    registry.webhook_handler = saved_webhook


@pytest.fixture
def decorated_module():
    from gundi_action_runner.actions.core import PullActionConfiguration
    from gundi_action_runner.decorators import action

    module = types.ModuleType("fake_decorated_handlers")

    def _register():
        @action.pull(config=PullActionConfiguration, id="pull_stuff")
        async def pull_stuff(integration, action_config):
            return {}

    module.register = _register
    sys.modules["fake_decorated_handlers"] = module
    # Registration happens when create_app imports the module; emulate
    # decorator-at-import by registering on first import via module-level call.
    _register()
    yield "fake_decorated_handlers"
    del sys.modules["fake_decorated_handlers"]


def test_create_app_builds_routes():
    app = create_app(handlers_modules=[])
    paths = {route.path for route in app.routes}
    assert "/" in paths
    assert "/push-data" in paths
    assert any(path.startswith("/v1/actions") for path in paths)
    assert any(path.startswith("/webhooks") for path in paths)


def test_health_endpoint():
    app = create_app(handlers_modules=[])
    client = TestClient(app)
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}


def test_actions_endpoint_lists_registered_actions(decorated_module):
    app = create_app(handlers_modules=[])  # registry already populated by fixture
    client = TestClient(app)
    response = client.get("/v1/actions/")
    assert response.status_code == 200
    assert "pull_stuff" in response.json()


def test_create_app_scans_legacy_module_when_registry_empty(monkeypatch):
    from gundi_action_runner.actions.core import PullActionConfiguration

    module = types.ModuleType("fake_legacy_for_factory")

    async def action_pull_legacy(integration, action_config: PullActionConfiguration):
        return {}

    module.action_pull_legacy = action_pull_legacy
    sys.modules["fake_legacy_for_factory"] = module
    monkeypatch.setattr(
        "gundi_action_runner.settings.GUNDI_LEGACY_ACTIONS_MODULE", "fake_legacy_for_factory"
    )
    try:
        create_app()
        assert "pull_legacy" in registry.action_handlers
    finally:
        del sys.modules["fake_legacy_for_factory"]


def test_create_app_tolerates_entirely_missing_legacy_packages(monkeypatch):
    # Pure-library scenario: no `app` package at all. The parent package being
    # missing must not crash create_app; a broken import INSIDE an existing
    # module still must (covered by the e.name narrowing).
    monkeypatch.setattr(
        "gundi_action_runner.settings.GUNDI_LEGACY_ACTIONS_MODULE",
        "nonexistent_pkg.actions.handlers",
    )
    monkeypatch.setattr(
        "gundi_action_runner.settings.GUNDI_LEGACY_WEBHOOKS_MODULE",
        "nonexistent_pkg.webhooks.handlers",
    )
    app = create_app()  # must not raise
    assert app.title == "Gundi Integration Actions Execution Service"


@pytest.mark.asyncio
async def test_execute_action_populates_registry_lazily(
    monkeypatch, mock_gundi_client_v2_class, mock_publish_event, mock_config_manager,
):
    """Fork pattern: execute_action called directly without create_app().

    The registry is empty (clean_registry autouse fixture guarantees that).
    When the lookup for a known action_id is attempted, ensure_loaded() must
    fire, discover the handler from the legacy module, and the call must
    succeed (handler return value in the result) rather than KeyError-ing.
    """
    from gundi_action_runner.actions.core import PullActionConfiguration
    from gundi_action_runner.services.action_runner import execute_action
    from gundi_action_runner.testing.fixtures import async_return

    # Build a minimal fake "legacy" module with one action_pull_things handler
    module = types.ModuleType("fake_lazy_fork_handlers")

    async def action_pull_things(integration, action_config: PullActionConfiguration):
        return {"pulled": 42}

    module.action_pull_things = action_pull_things
    sys.modules["fake_lazy_fork_handlers"] = module

    monkeypatch.setattr(
        "gundi_action_runner.settings.GUNDI_LEGACY_ACTIONS_MODULE",
        "fake_lazy_fork_handlers",
    )
    # Disable the webhooks legacy scan (no webhook_handler in our fake module)
    monkeypatch.setattr(
        "gundi_action_runner.settings.GUNDI_LEGACY_WEBHOOKS_MODULE",
        "nonexistent_pkg.webhooks.handlers",
    )

    # Patch the service-layer dependencies exactly as test_action_runner.py does
    monkeypatch.setattr(
        "gundi_action_runner.services.action_runner.config_manager", mock_config_manager
    )
    monkeypatch.setattr(
        "gundi_action_runner.services.activity_logger.publish_event", mock_publish_event
    )
    monkeypatch.setattr(
        "gundi_action_runner.services.action_runner.publish_event", mock_publish_event
    )

    # Confirm the registry is truly empty before the call (clean_registry autouse)
    from gundi_action_runner.services import action_runner as ar
    assert not ar.action_handlers, "Registry must be empty at test start"

    try:
        result = await execute_action(
            integration_id=str(mock_config_manager.get_integration_details.return_value.result().id),
            action_id="pull_things",
        )
        # The lazy-load path must have found and executed the handler
        assert result == {"pulled": 42}, f"Expected handler result, got: {result}"
    finally:
        del sys.modules["fake_lazy_fork_handlers"]
