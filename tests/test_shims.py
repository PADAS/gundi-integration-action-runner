"""Fork-compatibility contract: every legacy app.* path must keep importing
and resolve to its gundi_action_runner counterpart."""
import importlib
import sys

import pytest

ALIASED = {
    **{f"app.services.{m}": f"gundi_action_runner.services.{m}"
       for m in ["action_runner", "action_scheduler", "activity_logger",
                 "config_events_consumer", "config_manager", "core", "errors",
                 "gundi", "self_registration", "state", "utils", "webhooks"]},
    "app.routers.actions": "gundi_action_runner.routers.actions",
    "app.routers.webhooks": "gundi_action_runner.routers.webhooks",
    "app.routers.config_events": "gundi_action_runner.routers.config_events",
    "app.actions.core": "gundi_action_runner.actions.core",
    "app.webhooks.core": "gundi_action_runner.webhooks.core",
    "app.api_schemas": "gundi_action_runner.api_schemas",
    "app.settings.base": "gundi_action_runner.settings",
}


@pytest.mark.parametrize("legacy, target", sorted(ALIASED.items()))
def test_leaf_shim_is_the_library_module(legacy, target):
    sys.modules.pop(legacy, None)
    with pytest.warns(DeprecationWarning):
        module = importlib.import_module(legacy)
    assert module is importlib.import_module(target)


def test_app_actions_package_reexports():
    import app.actions
    import gundi_action_runner.actions as lib
    from gundi_action_runner.actions.core import PullActionConfiguration, action_title
    assert app.actions.PullActionConfiguration is PullActionConfiguration
    assert app.actions.action_title is action_title
    # __getattr__-resolved registry names
    assert app.actions.action_handlers is lib.action_handlers


def test_app_settings_package_reexports():
    import app.settings
    import gundi_action_runner.settings as lib
    assert app.settings.INTEGRATION_TYPE_SLUG == lib.INTEGRATION_TYPE_SLUG
    assert hasattr(app.settings, "GUNDI_API_BASE_URL")
    assert hasattr(app.settings, "INTEGRATION_TYPE_NAME")


def test_app_webhooks_package_reexports():
    import app.webhooks
    from gundi_action_runner.webhooks.core import GenericJsonTransformConfig
    assert app.webhooks.GenericJsonTransformConfig is GenericJsonTransformConfig


def test_app_main_exposes_fastapi_app():
    from app.main import app as fastapi_app
    assert fastapi_app.title == "Gundi Integration Actions Execution Service"
