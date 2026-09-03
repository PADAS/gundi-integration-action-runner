import sys
import types

import pytest
from fastapi.testclient import TestClient
from app.actions import action_title, discover_actions, PullActionConfiguration
from app.main import app
from app.services.self_registration import register_integration_in_gundi
from app.services.action_scheduler import crontab_schedule, CrontabSchedule

api_client = TestClient(app)


@pytest.mark.asyncio
async def test_register_integration_with_slug_setting(
    mocker,
    mock_gundi_client_v2,
    mock_action_handlers,
    mock_get_webhook_handler_for_fixed_json_payload,
):
    mocker.patch("app.services.self_registration.INTEGRATION_TYPE_SLUG", "x_tracker")
    mocker.patch("app.services.self_registration.action_handlers", mock_action_handlers)
    mocker.patch(
        "app.services.self_registration.get_webhook_handler",
        mock_get_webhook_handler_for_fixed_json_payload,
    )
    await register_integration_in_gundi(gundi_client=mock_gundi_client_v2)
    assert mock_gundi_client_v2.register_integration_type.called
    mock_gundi_client_v2.register_integration_type.assert_called_with(
        {
            "name": "X Tracker",
            "value": "x_tracker",
            "description": f"Default type for integrations with X Tracker",
            "actions": [
                {
                    "type": "pull",
                    "name": "Pull Observations",
                    "value": "pull_observations",
                    "description": "X Tracker Pull Observations action",
                    "schema": {
                        "title": "MockPullActionConfiguration",
                        "type": "object",
                        "properties": {
                            "run_on_schedule": {
                                "title": "Run On Schedule",
                                "description": (
                                    "When enabled, this action runs automatically on its configured "
                                    "schedule. Turn it off to pause scheduled execution for this "
                                    "integration without deleting the configuration."
                                ),
                                "default": True,
                                "type": "boolean",
                            },
                            "lookback_days": {
                                "title": "Data lookback days",
                                "description": "Number of days to look back for data.",
                                "default": 30,
                                "minimum": 1,
                                "maximum": 30,
                                "type": "integer",
                            },
                            "force_fetch": {
                                "title": "Force fetch",
                                "description": "Force fetch even if in a quiet period.",
                                "default": False,
                                "type": "boolean",
                            },
                            "region_code": {
                                "title": "Region Code", 
                                "type": ["string", "null"]
                            }
                        },
                        "definitions": {},
                    },
                    "ui_schema": {
                        "lookback_days": {"ui:widget": "range"},
                        "force_fetch": {"ui:widget": "select"},
                        "ui:order": ["region_code", "lookback_days", "force_fetch"],
                    },
                    "is_periodic_action": True,
                    "crontab_schedule": {
                        "day_of_month": "*",
                        "day_of_week": "*",
                        "hour": "*",
                        "minute": "*/10",
                        "month_of_year": "*",
                        "tz_offset": -5
                    },
                },
                {
                    "type": "push",
                    "name": "Push Observations",
                    "value": "push_observations",
                    "description": "X Tracker Push Observations action",
                    "schema": {
                        "title": "MockPushActionConfiguration",
                        "type": "object",
                        "properties": {},
                        "definitions": {}
                    },
                    "ui_schema": {},
                    "is_periodic_action": False
                }

            ],
            "webhook": {
                "name": "X Tracker Webhook",
                "value": "x_tracker_webhook",
                "description": "Webhook Integration with X Tracker",
                "schema": {
                    "title": "MockWebhookConfigModel",
                    "type": "object",
                    "properties": {
                        "diagnostic_destination_url": {
                            "title": "Diagnostic Destination URL",
                            "description": "Optional URL to forward the raw incoming payload to for diagnostic purposes. When set, the original JSON payload is POST'd to this URL before any transformation.",
                            "type": ["string", "null"],
                        },
                        "allowed_devices_list": {
                            "title": "Allowed Devices List",
                            "type": "array",
                            "items": {},
                        },
                        "deduplication_enabled": {
                            "title": "Deduplication Enabled",
                            "type": "boolean",
                        },
                    },
                    "required": ["allowed_devices_list", "deduplication_enabled"],
                    "definitions": {},
                },
                "ui_schema": {
                    "diagnostic_destination_url": {"ui:placeholder": "https://your-diagnostic-app.example.com/webhook-dump", "ui:widget": "text"},
                    "allowed_devices_list": {"ui:widget": "list"},
                    "deduplication_enabled": {"ui:widget": "radio"},
                },
            },
        }
    )


@pytest.mark.asyncio
async def test_register_integration_with_slug_arg(
    mocker,
    mock_gundi_client_v2,
    mock_action_handlers,
    mock_get_webhook_handler_for_fixed_json_payload,
):
    mocker.patch("app.services.action_runner.action_handlers", mock_action_handlers)
    mocker.patch("app.services.self_registration.action_handlers", mock_action_handlers)
    mocker.patch(
        "app.services.self_registration.get_webhook_handler",
        mock_get_webhook_handler_for_fixed_json_payload,
    )
    await register_integration_in_gundi(
        gundi_client=mock_gundi_client_v2, type_slug="x_tracker"
    )
    assert mock_gundi_client_v2.register_integration_type.called
    mock_gundi_client_v2.register_integration_type.assert_called_with(
        {
            "name": "X Tracker",
            "value": "x_tracker",
            "description": f"Default type for integrations with X Tracker",
            "actions": [
                {
                    "type": "pull",
                    "name": "Pull Observations",
                    "value": "pull_observations",
                    "description": "X Tracker Pull Observations action",
                    "schema": {
                        "title": "MockPullActionConfiguration",
                        "type": "object",
                        "properties": {
                            "run_on_schedule": {
                                "title": "Run On Schedule",
                                "description": (
                                    "When enabled, this action runs automatically on its configured "
                                    "schedule. Turn it off to pause scheduled execution for this "
                                    "integration without deleting the configuration."
                                ),
                                "default": True,
                                "type": "boolean",
                            },
                            "lookback_days": {
                                "title": "Data lookback days",
                                "description": "Number of days to look back for data.",
                                "default": 30,
                                "minimum": 1,
                                "maximum": 30,
                                "type": "integer",
                            },
                            "force_fetch": {
                                "title": "Force fetch",
                                "description": "Force fetch even if in a quiet period.",
                                "default": False,
                                "type": "boolean",
                            },
                            "region_code": {
                                "title": "Region Code",
                                "type": ["string", "null"]
                            },
                        },
                        "definitions": {},
                    },
                    "ui_schema": {
                        "lookback_days": {"ui:widget": "range"},
                        "force_fetch": {"ui:widget": "select"},
                        "ui:order": ["region_code", "lookback_days", "force_fetch"],
                    },
                    "is_periodic_action": True,
                    "crontab_schedule": {
                        "day_of_month": "*",
                        "day_of_week": "*",
                        "hour": "*",
                        "minute": "*/10",
                        "month_of_year": "*",
                        "tz_offset": -5
                    },
                },
                {
                    "type": "push",
                    "name": "Push Observations",
                    "value": "push_observations",
                    "description": "X Tracker Push Observations action",
                    "schema": {
                        "title": "MockPushActionConfiguration",
                        "type": "object",
                        "properties": {},
                        "definitions": {}
                    },
                    "ui_schema": {},
                    "is_periodic_action": False
                }
            ],
            "webhook": {
                "name": "X Tracker Webhook",
                "value": "x_tracker_webhook",
                "description": "Webhook Integration with X Tracker",
                "schema": {
                    "title": "MockWebhookConfigModel",
                    "type": "object",
                    "properties": {
                        "diagnostic_destination_url": {
                            "title": "Diagnostic Destination URL",
                            "description": "Optional URL to forward the raw incoming payload to for diagnostic purposes. When set, the original JSON payload is POST'd to this URL before any transformation.",
                            "type": ["string", "null"],
                        },
                        "allowed_devices_list": {
                            "title": "Allowed Devices List",
                            "type": "array",
                            "items": {},
                        },
                        "deduplication_enabled": {
                            "title": "Deduplication Enabled",
                            "type": "boolean",
                        },
                    },
                    "required": ["allowed_devices_list", "deduplication_enabled"],
                    "definitions": {},
                },
                "ui_schema": {
                    "diagnostic_destination_url": {"ui:placeholder": "https://your-diagnostic-app.example.com/webhook-dump", "ui:widget": "text"},
                    "allowed_devices_list": {"ui:widget": "list"},
                    "deduplication_enabled": {"ui:widget": "radio"},
                },
            },
        }
    )


@pytest.mark.asyncio
async def test_register_integration_with_service_url_arg(
    mocker,
    mock_gundi_client_v2,
    mock_action_handlers,
    mock_get_webhook_handler_for_fixed_json_payload,
):
    mocker.patch("app.services.self_registration.INTEGRATION_TYPE_SLUG", "x_tracker")
    mocker.patch("app.services.self_registration.action_handlers", mock_action_handlers)
    mocker.patch(
        "app.services.self_registration.get_webhook_handler",
        mock_get_webhook_handler_for_fixed_json_payload,
    )
    service_url = "https://xtracker-actions-runner-jabcutl8yb-uc.a.run.app"
    await register_integration_in_gundi(
        gundi_client=mock_gundi_client_v2, service_url=service_url
    )
    assert mock_gundi_client_v2.register_integration_type.called
    mock_gundi_client_v2.register_integration_type.assert_called_with(
        {
            "name": "X Tracker",
            "value": "x_tracker",
            "description": f"Default type for integrations with X Tracker",
            "service_url": service_url,
            "actions": [
                {
                    "type": "pull",
                    "name": "Pull Observations",
                    "value": "pull_observations",
                    "description": "X Tracker Pull Observations action",
                    "schema": {
                        "title": "MockPullActionConfiguration",
                        "type": "object",
                        "properties": {
                            "run_on_schedule": {
                                "title": "Run On Schedule",
                                "description": (
                                    "When enabled, this action runs automatically on its configured "
                                    "schedule. Turn it off to pause scheduled execution for this "
                                    "integration without deleting the configuration."
                                ),
                                "default": True,
                                "type": "boolean",
                            },
                            "lookback_days": {
                                "title": "Data lookback days",
                                "description": "Number of days to look back for data.",
                                "default": 30,
                                "minimum": 1,
                                "maximum": 30,
                                "type": "integer",
                            },
                            "force_fetch": {
                                "title": "Force fetch",
                                "description": "Force fetch even if in a quiet period.",
                                "default": False,
                                "type": "boolean",
                            },
                            "region_code": {
                                "title": "Region Code",
                                "type": ["string", "null"]
                            },
                        },
                        "definitions": {},
                    },
                    "ui_schema": {
                        "lookback_days": {"ui:widget": "range"},
                        "force_fetch": {"ui:widget": "select"},
                        "ui:order": ["region_code", "lookback_days", "force_fetch"],
                    },
                    "is_periodic_action": True,
                    "crontab_schedule": {
                        "day_of_month": "*",
                        "day_of_week": "*",
                        "hour": "*",
                        "minute": "*/10",
                        "month_of_year": "*",
                        "tz_offset": -5
                    },
                },
                {
                    "type": "push",
                    "name": "Push Observations",
                    "value": "push_observations",
                    "description": "X Tracker Push Observations action",
                    "schema": {
                        "title": "MockPushActionConfiguration",
                        "type": "object",
                        "properties": {},
                        "definitions": {}
                    },
                    "ui_schema": {},
                    "is_periodic_action": False
                }
            ],
            "webhook": {
                "name": "X Tracker Webhook",
                "value": "x_tracker_webhook",
                "description": "Webhook Integration with X Tracker",
                "schema": {
                    "title": "MockWebhookConfigModel",
                    "type": "object",
                    "properties": {
                        "diagnostic_destination_url": {
                            "title": "Diagnostic Destination URL",
                            "description": "Optional URL to forward the raw incoming payload to for diagnostic purposes. When set, the original JSON payload is POST'd to this URL before any transformation.",
                            "type": ["string", "null"],
                        },
                        "allowed_devices_list": {
                            "title": "Allowed Devices List",
                            "type": "array",
                            "items": {},
                        },
                        "deduplication_enabled": {
                            "title": "Deduplication Enabled",
                            "type": "boolean",
                        },
                    },
                    "required": ["allowed_devices_list", "deduplication_enabled"],
                    "definitions": {},
                },
                "ui_schema": {
                    "diagnostic_destination_url": {"ui:placeholder": "https://your-diagnostic-app.example.com/webhook-dump", "ui:widget": "text"},
                    "allowed_devices_list": {"ui:widget": "list"},
                    "deduplication_enabled": {"ui:widget": "radio"},
                },
            },
        }
    )


@pytest.mark.asyncio
async def test_register_integration_with_service_url_setting(
    mocker,
    mock_gundi_client_v2,
    mock_action_handlers,
    mock_get_webhook_handler_for_fixed_json_payload,
):
    service_url = "https://xtracker-actions-runner-jabcutl8yb-uc.a.run.app"
    mocker.patch("app.services.self_registration.INTEGRATION_TYPE_SLUG", "x_tracker")
    mocker.patch("app.services.self_registration.INTEGRATION_SERVICE_URL", service_url)
    mocker.patch("app.services.self_registration.action_handlers", mock_action_handlers)
    mocker.patch(
        "app.services.self_registration.get_webhook_handler",
        mock_get_webhook_handler_for_fixed_json_payload,
    )

    await register_integration_in_gundi(
        gundi_client=mock_gundi_client_v2,
    )

    assert mock_gundi_client_v2.register_integration_type.called
    mock_gundi_client_v2.register_integration_type.assert_called_with(
        {
            "name": "X Tracker",
            "value": "x_tracker",
            "description": f"Default type for integrations with X Tracker",
            "service_url": service_url,
            "actions": [
                {
                    "type": "pull",
                    "name": "Pull Observations",
                    "value": "pull_observations",
                    "description": "X Tracker Pull Observations action",
                    "schema": {
                        "title": "MockPullActionConfiguration",
                        "type": "object",
                        "properties": {
                            "run_on_schedule": {
                                "title": "Run On Schedule",
                                "description": (
                                    "When enabled, this action runs automatically on its configured "
                                    "schedule. Turn it off to pause scheduled execution for this "
                                    "integration without deleting the configuration."
                                ),
                                "default": True,
                                "type": "boolean",
                            },
                            "lookback_days": {
                                "title": "Data lookback days",
                                "description": "Number of days to look back for data.",
                                "default": 30,
                                "minimum": 1,
                                "maximum": 30,
                                "type": "integer",
                            },
                            "force_fetch": {
                                "title": "Force fetch",
                                "description": "Force fetch even if in a quiet period.",
                                "default": False,
                                "type": "boolean",
                            },
                            "region_code": {
                                "title": "Region Code",
                                "type": ["string", "null"]
                            },
                        },
                        "definitions": {},
                    },
                    "ui_schema": {
                        "lookback_days": {"ui:widget": "range"},
                        "force_fetch": {"ui:widget": "select"},
                        "ui:order": ["region_code", "lookback_days", "force_fetch"],
                    },
                    "is_periodic_action": True,
                    "crontab_schedule": {
                        "day_of_month": "*",
                        "day_of_week": "*",
                        "hour": "*",
                        "minute": "*/10",
                        "month_of_year": "*",
                        "tz_offset": -5
                    },
                },
                {
                    "type": "push",
                    "name": "Push Observations",
                    "value": "push_observations",
                    "description": "X Tracker Push Observations action",
                    "schema": {
                        "title": "MockPushActionConfiguration",
                        "type": "object",
                        "properties": {},
                        "definitions": {}
                    },
                    "ui_schema": {},
                    "is_periodic_action": False
                }
            ],
            "webhook": {
                "name": "X Tracker Webhook",
                "value": "x_tracker_webhook",
                "description": "Webhook Integration with X Tracker",
                "schema": {
                    "title": "MockWebhookConfigModel",
                    "type": "object",
                    "properties": {
                        "diagnostic_destination_url": {
                            "title": "Diagnostic Destination URL",
                            "description": "Optional URL to forward the raw incoming payload to for diagnostic purposes. When set, the original JSON payload is POST'd to this URL before any transformation.",
                            "type": ["string", "null"],
                        },
                        "allowed_devices_list": {
                            "title": "Allowed Devices List",
                            "type": "array",
                            "items": {},
                        },
                        "deduplication_enabled": {
                            "title": "Deduplication Enabled",
                            "type": "boolean",
                        },
                    },
                    "required": ["allowed_devices_list", "deduplication_enabled"],
                    "definitions": {},
                },
                "ui_schema": {
                    "diagnostic_destination_url": {"ui:placeholder": "https://your-diagnostic-app.example.com/webhook-dump", "ui:widget": "text"},
                    "allowed_devices_list": {"ui:widget": "list"},
                    "deduplication_enabled": {"ui:widget": "radio"},
                },
            },
        }
    )


@pytest.mark.asyncio
async def test_register_integration_with_executable_action(
    mocker,
    mock_gundi_client_v2,
    mock_auth_action_handlers,
    mock_get_webhook_handler_for_fixed_json_payload,
):
    mocker.patch("app.services.self_registration.INTEGRATION_TYPE_SLUG", "x_tracker")
    mocker.patch(
        "app.services.self_registration.action_handlers", mock_auth_action_handlers
    )
    mocker.patch(
        "app.services.self_registration.get_webhook_handler",
        mock_get_webhook_handler_for_fixed_json_payload,
    )
    await register_integration_in_gundi(gundi_client=mock_gundi_client_v2)
    assert mock_gundi_client_v2.register_integration_type.called
    mock_gundi_client_v2.register_integration_type.assert_called_with(
        {
            "name": "X Tracker",
            "value": "x_tracker",
            "description": "Default type for integrations with X Tracker",
            "actions": [
                {
                    "type": "auth",
                    "name": "Auth",
                    "value": "auth",
                    "description": "X Tracker Auth action",
                    "schema": {
                        "title": "MockAuthenticateActionConfiguration",
                        "type": "object",
                        "properties": {
                            "username": {"title": "Username", "type": "string"},
                            "password": {
                                "title": "Password",
                                "type": "string",
                                "writeOnly": True,
                                "format": "password",
                            },
                        },
                        "required": ["username", "password"],
                        "definitions": {},
                        "is_executable": True,
                    },
                    "ui_schema": {
                        "username": {"ui:widget": "text"},
                        "password": {"ui:widget": "password"},
                    },
                    "is_periodic_action": False,
                }
            ],
            "webhook": {
                "name": "X Tracker Webhook",
                "value": "x_tracker_webhook",
                "description": "Webhook Integration with X Tracker",
                "schema": {
                    "title": "MockWebhookConfigModel",
                    "type": "object",
                    "properties": {
                        "diagnostic_destination_url": {
                            "title": "Diagnostic Destination URL",
                            "description": "Optional URL to forward the raw incoming payload to for diagnostic purposes. When set, the original JSON payload is POST'd to this URL before any transformation.",
                            "type": ["string", "null"],
                        },
                        "allowed_devices_list": {
                            "title": "Allowed Devices List",
                            "type": "array",
                            "items": {},
                        },
                        "deduplication_enabled": {
                            "title": "Deduplication Enabled",
                            "type": "boolean",
                        },
                    },
                    "required": ["allowed_devices_list", "deduplication_enabled"],
                    "definitions": {},
                },
                "ui_schema": {
                    "diagnostic_destination_url": {"ui:placeholder": "https://your-diagnostic-app.example.com/webhook-dump", "ui:widget": "text"},
                    "allowed_devices_list": {"ui:widget": "list"},
                    "deduplication_enabled": {"ui:widget": "radio"},
                },
            },
        }
    )


@pytest.mark.asyncio
async def test_register_integration_with_type_name_arg(
    mocker,
    mock_gundi_client_v2,
    mock_action_handlers,
    mock_get_webhook_handler_for_fixed_json_payload,
):
    mocker.patch("app.services.self_registration.action_handlers", mock_action_handlers)
    mocker.patch(
        "app.services.self_registration.get_webhook_handler",
        mock_get_webhook_handler_for_fixed_json_payload,
    )
    await register_integration_in_gundi(
        gundi_client=mock_gundi_client_v2,
        type_slug="savannahtracking",
        type_name="Savannah Tracking",
    )
    assert mock_gundi_client_v2.register_integration_type.called
    data = mock_gundi_client_v2.register_integration_type.call_args.args[0]
    assert data["name"] == "Savannah Tracking"
    assert data["value"] == "savannahtracking"
    assert data["description"] == "Default type for integrations with Savannah Tracking"
    pull_action = next(a for a in data["actions"] if a["value"] == "pull_observations")
    assert pull_action["description"] == "Savannah Tracking Pull Observations action"
    assert data["webhook"]["name"] == "Savannah Tracking Webhook"
    assert data["webhook"]["description"] == "Webhook Integration with Savannah Tracking"


@pytest.mark.asyncio
async def test_register_integration_with_type_name_setting(
    mocker,
    mock_gundi_client_v2,
    mock_action_handlers,
    mock_get_webhook_handler_for_fixed_json_payload,
):
    mocker.patch("app.services.self_registration.INTEGRATION_TYPE_SLUG", "savannahtracking")
    mocker.patch("app.services.self_registration.INTEGRATION_TYPE_NAME", "Savannah Tracking")
    mocker.patch("app.services.self_registration.action_handlers", mock_action_handlers)
    mocker.patch(
        "app.services.self_registration.get_webhook_handler",
        mock_get_webhook_handler_for_fixed_json_payload,
    )
    await register_integration_in_gundi(gundi_client=mock_gundi_client_v2)
    assert mock_gundi_client_v2.register_integration_type.called
    data = mock_gundi_client_v2.register_integration_type.call_args.args[0]
    assert data["name"] == "Savannah Tracking"
    assert data["value"] == "savannahtracking"


@pytest.mark.asyncio
async def test_register_integration_with_action_title(
    mocker,
    mock_gundi_client_v2,
    mock_action_handlers,
    mock_get_webhook_handler_for_fixed_json_payload,
):
    mocker.patch("app.services.self_registration.INTEGRATION_TYPE_SLUG", "x_tracker")
    mock_action_handlers["pull_observations"][0].action_title = "Fetch Collar Positions"
    mocker.patch("app.services.self_registration.action_handlers", mock_action_handlers)
    mocker.patch(
        "app.services.self_registration.get_webhook_handler",
        mock_get_webhook_handler_for_fixed_json_payload,
    )
    await register_integration_in_gundi(gundi_client=mock_gundi_client_v2)
    assert mock_gundi_client_v2.register_integration_type.called
    data = mock_gundi_client_v2.register_integration_type.call_args.args[0]
    pull_action = next(a for a in data["actions"] if a["value"] == "pull_observations")
    assert pull_action["name"] == "Fetch Collar Positions"
    assert pull_action["description"] == "X Tracker Fetch Collar Positions action"
    # Actions without a custom title keep the name derived from the action id
    push_action = next(a for a in data["actions"] if a["value"] == "push_observations")
    assert push_action["name"] == "Push Observations"


def test_action_title_decorator():

    @action_title("Fetch Collar Positions")
    async def action_pull_observations(integration, action_config):
        return {"observations_extracted": 10}

    assert action_pull_observations.action_title == "Fetch Collar Positions"


def test_action_title_decorator_stacks_with_crontab_schedule():

    @action_title("Fetch Collar Positions")
    @crontab_schedule("*/10 * * * *")
    async def action_pull_observations(integration, action_config):
        return {"observations_extracted": 10}

    assert action_pull_observations.action_title == "Fetch Collar Positions"
    assert action_pull_observations.crontab_schedule == CrontabSchedule.parse_obj_from_crontab("*/10 * * * *")


def _discover_actions_in_fake_module(module):
    module_name = "fake_handlers_module"
    previous = sys.modules.get(module_name)
    sys.modules[module_name] = module
    try:
        return discover_actions(module_name=module_name, prefix="action_")
    finally:
        if previous is not None:
            sys.modules[module_name] = previous
        else:
            del sys.modules[module_name]


def test_discover_actions_ignores_imported_action_title_decorator():
    # Simulates handlers.py doing `from app.actions import action_title`
    module = types.ModuleType("fake_handlers_module")
    module.action_title = action_title

    @action_title("Fetch Collar Positions")
    async def action_pull_observations(integration, action_config: PullActionConfiguration):
        return {"observations_extracted": 10}

    module.action_pull_observations = action_pull_observations

    handlers = _discover_actions_in_fake_module(module)

    assert "title" not in handlers
    assert list(handlers.keys()) == ["pull_observations"]
    assert handlers["pull_observations"][0] is action_pull_observations


def test_discover_actions_ignores_functions_without_action_config():
    module = types.ModuleType("fake_handlers_module")

    def action_helper(value):
        return value

    async def action_pull_observations(integration, action_config: PullActionConfiguration):
        return {"observations_extracted": 10}

    module.action_helper = action_helper
    module.action_pull_observations = action_pull_observations

    handlers = _discover_actions_in_fake_module(module)

    assert "helper" not in handlers
    assert list(handlers.keys()) == ["pull_observations"]


@pytest.mark.asyncio
async def test_crontab_schedule_decorator(
        mocker, mock_publish_event, integration_v2, pull_observations_config
):

    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)

    @crontab_schedule("5-55/10 * * * *")
    async def action_pull_observations(integration, action_config):
        return {"observations_extracted": 10}

    assert hasattr(action_pull_observations, "crontab_schedule")
    expected_schedule = CrontabSchedule(
        minute='5-55/10',
        hour='*',
        day_of_week='*',
        day_of_month='*',
        month_of_year='*',
        tz_offset=0
    )
    assert action_pull_observations.crontab_schedule == expected_schedule


def test_action_type_enum_has_reference():
    from app.services.core import ActionTypeEnum

    assert ActionTypeEnum.REFERENCE.value == "reference"


def _dummy_reference_handlers():
    from app.actions.core import ReferenceActionConfiguration

    class DummyQuery(ReferenceActionConfiguration):
        pass

    async def action_list_dummy(integration, action_config: DummyQuery):
        return {"options": []}

    return {"list_dummy": (action_list_dummy, DummyQuery, None)}


@pytest.mark.asyncio
async def test_reference_actions_are_registered_with_the_reference_type(mocker):
    # The platform accepts the "reference" action type, so reference actions
    # always register, and with their own type rather than "generic" (which
    # the runner's ephemeral whitelist would otherwise be the only guard for).
    from unittest.mock import AsyncMock, MagicMock
    from app.services import self_registration

    mocker.patch.object(self_registration, "action_handlers", _dummy_reference_handlers())
    gundi_client = MagicMock()
    gundi_client.register_integration_type = AsyncMock(return_value={})

    await self_registration.register_integration_in_gundi(gundi_client, type_slug="my_tracker")

    data = gundi_client.register_integration_type.call_args.args[0]
    assert [a["value"] for a in data["actions"]] == ["list_dummy"]
    assert data["actions"][0]["type"] == "reference"
    assert data["actions"][0]["is_periodic_action"] is False
