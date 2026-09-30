import sys
import types

import pytest

from gundi_action_runner.actions.core import (
    AuthActionConfiguration,
    PullActionConfiguration,
    PushActionConfiguration,
)
from gundi_action_runner.decorators import action, webhook
from gundi_action_runner.registry import RegistryError, registry


class DummyAuthConfig(AuthActionConfiguration):
    pass


class DummyPullConfig(PullActionConfiguration):
    pass


class DummyPushConfig(PushActionConfiguration):
    pass


class DummyData:
    pass


@pytest.fixture(autouse=True)
def clean_registry():
    saved_actions = dict(registry.action_handlers)
    saved_webhook = registry.webhook_handler
    registry.reset()
    yield registry
    registry.reset()
    registry.action_handlers.update(saved_actions)
    registry.webhook_handler = saved_webhook


def test_pull_decorator_registers_handler_under_function_name():
    @action.pull(config=DummyPullConfig)
    async def pull_observations(integration, action_config):
        return {}

    func, config_model, data_model = registry.action_handlers["pull_observations"]
    assert func is pull_observations
    assert config_model is DummyPullConfig
    assert data_model is None


def test_decorator_returns_the_function_unwrapped():
    @action.auth(config=DummyAuthConfig)
    async def auth(integration, action_config):
        return {}

    assert auth.__name__ == "auth"


def test_explicit_id_overrides_function_name():
    @action.pull(config=DummyPullConfig, id="pull_things")
    async def some_function(integration, action_config):
        return {}

    assert "pull_things" in registry.action_handlers
    assert "some_function" not in registry.action_handlers


def test_title_sets_action_title_attribute():
    @action.pull(config=DummyPullConfig, title="Fetch Collar Positions")
    async def pull_observations(integration, action_config):
        return {}

    assert pull_observations.action_title == "Fetch Collar Positions"


def test_duplicate_action_id_raises():
    @action.pull(config=DummyPullConfig)
    async def pull_observations(integration, action_config):
        return {}

    with pytest.raises(RegistryError, match="pull_observations"):
        @action.pull(config=DummyPullConfig, id="pull_observations")
        async def another(integration, action_config):
            return {}


def test_config_must_subclass_expected_base():
    with pytest.raises(RegistryError, match="AuthActionConfiguration"):
        @action.auth(config=DummyPullConfig)
        async def auth(integration, action_config):
            return {}


def test_handler_must_be_async():
    with pytest.raises(RegistryError, match="async"):
        @action.pull(config=DummyPullConfig)
        def not_async(integration, action_config):
            return {}


def test_handler_must_accept_integration_and_action_config():
    with pytest.raises(RegistryError, match="integration"):
        @action.pull(config=DummyPullConfig)
        async def bad(action_config):
            return {}


def test_push_requires_annotated_data_param():
    with pytest.raises(RegistryError, match="data"):
        @action.push(config=DummyPushConfig)
        async def push(integration, action_config, data, metadata):
            return {}


def test_push_requires_metadata_param():
    with pytest.raises(RegistryError, match="metadata"):
        @action.push(config=DummyPushConfig)
        async def push(integration, action_config, data: DummyData):
            return {}


def test_push_captures_data_model():
    @action.push(config=DummyPushConfig)
    async def push(integration, action_config, data: DummyData, metadata):
        return {}

    _, _, data_model = registry.action_handlers["push"]
    assert data_model is DummyData


def test_webhook_bare_decorator_introspects_annotations():
    from gundi_action_runner.webhooks.core import GenericJsonPayload, GenericJsonTransformConfig

    @webhook
    async def webhook_handler(payload: GenericJsonPayload, integration,
                              webhook_config: GenericJsonTransformConfig):
        return {}

    func, payload_model, config_model = registry.webhook_handler
    assert func is webhook_handler
    assert payload_model is GenericJsonPayload
    assert config_model is GenericJsonTransformConfig


def test_second_webhook_raises():
    @webhook
    async def webhook_handler(payload, webhook_config):
        return {}

    with pytest.raises(RegistryError, match="already registered"):
        @webhook
        async def another(payload, webhook_config):
            return {}


def test_webhook_requires_payload_and_config_params():
    with pytest.raises(RegistryError, match="webhook_config"):
        @webhook
        async def bad(payload):
            return {}


@pytest.fixture
def legacy_module():
    module = types.ModuleType("fake_legacy_handlers")

    async def action_pull_things(integration, action_config: DummyPullConfig):
        return {}

    module.action_pull_things = action_pull_things
    sys.modules["fake_legacy_handlers"] = module
    yield "fake_legacy_handlers"
    del sys.modules["fake_legacy_handlers"]


def test_load_legacy_actions_registers_prefixed_functions(legacy_module):
    registry.load_legacy_actions(legacy_module)
    func, config_model, data_model = registry.action_handlers["pull_things"]
    assert config_model is DummyPullConfig


def test_load_legacy_actions_never_overrides_decorator_registrations(legacy_module):
    @action.pull(config=DummyPullConfig, id="pull_things")
    async def decorated(integration, action_config):
        return {}

    registry.load_legacy_actions(legacy_module)
    assert registry.action_handlers["pull_things"][0] is decorated


def test_load_legacy_webhook_adopts_module_handler(legacy_module):
    async def webhook_handler(payload, webhook_config):
        return {}

    sys.modules[legacy_module].webhook_handler = webhook_handler
    registry.load_legacy_webhook(legacy_module)
    assert registry.webhook_handler[0] is webhook_handler


def test_sync_webhook_handler_raises_registry_error():
    """@webhook must reject synchronous functions with an async RegistryError."""
    from gundi_action_runner.decorators import webhook

    with pytest.raises(RegistryError, match="async"):
        @webhook
        def sync_webhook(payload, webhook_config):  # not async
            return {}


def test_load_legacy_webhook_does_not_override_decorator_registration(legacy_module):
    """A decorator-registered webhook handler must survive load_legacy_webhook."""
    from gundi_action_runner.decorators import webhook

    @webhook
    async def decorator_webhook(payload, webhook_config):
        return {}

    # Add a different webhook_handler to the legacy module
    async def legacy_webhook(payload, webhook_config):
        return {}

    sys.modules[legacy_module].webhook_handler = legacy_webhook
    registry.load_legacy_webhook(legacy_module)
    # The decorator-registered handler must NOT have been replaced
    assert registry.webhook_handler[0] is decorator_webhook
