import pytest
from fastapi.testclient import TestClient
from app.main import app
from app.services.self_registration import register_integration_in_gundi

api_client = TestClient(app)


@pytest.mark.asyncio
async def test_register_integration_with_slug_setting(
        mocker, mock_gundi_client_v2, mock_action_handlers, mock_get_webhook_handler_for_fixed_json_payload
):
    mocker.patch("app.services.self_registration.INTEGRATION_TYPE_SLUG", "x_tracker")
    mocker.patch("app.services.self_registration.action_handlers", mock_action_handlers)
    mocker.patch("app.services.self_registration.get_webhook_handler", mock_get_webhook_handler_for_fixed_json_payload)
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
                            "lookback_days": {
                                "title": "Data lookback days",
                                "description": "Number of days to look back for data.",
                                "default": 30,
                                "minimum": 1, "maximum": 30,
                                "type": "integer"
                            },
                            "force_fetch": {
                                "title": "Force fetch",
                                "description": "Force fetch even if in a quiet period.",
                                "default": False,
                                "type": "boolean"
                            }
                        },
                        "definitions": {}
                    },
                    "ui_schema": {
                        "lookback_days": {
                            "ui:widget": "range"
                        },
                        "force_fetch": {
                            "ui:widget": "select"
                        },
                        "ui:order": ["lookback_days", "force_fetch"],
                    },
                    "is_periodic_action": True
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
                        "allowed_devices_list": {
                            "title": "Allowed Devices List",
                            "type": "array",
                            "items": {}
                        },
                        "deduplication_enabled": {
                            "title": "Deduplication Enabled",
                            "type": "boolean"
                        }
                    },
                    "definitions": {},
                    "required": [
                        "allowed_devices_list",
                        "deduplication_enabled"
                    ]
                },
                "ui_schema": {
                    "allowed_devices_list": {"ui:widget": "list"},
                    "deduplication_enabled": {"ui:widget": "radio"}
                },
            }
        }
    )


@pytest.mark.asyncio
async def test_register_integration_with_slug_arg(
        mocker, mock_gundi_client_v2, mock_action_handlers, mock_get_webhook_handler_for_fixed_json_payload
):
    mocker.patch("app.services.action_runner.action_handlers", mock_action_handlers)
    mocker.patch("app.services.self_registration.action_handlers", mock_action_handlers)
    mocker.patch("app.services.self_registration.get_webhook_handler", mock_get_webhook_handler_for_fixed_json_payload)
    await register_integration_in_gundi(gundi_client=mock_gundi_client_v2, type_slug="x_tracker")
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
                            "lookback_days": {
                                "title": "Data lookback days",
                                "description": "Number of days to look back for data.",
                                "default": 30,
                                "minimum": 1, "maximum": 30,
                                "type": "integer"
                            },
                            "force_fetch": {
                                "title": "Force fetch",
                                "description": "Force fetch even if in a quiet period.",
                                "default": False,
                                "type": "boolean"
                            }
                        },
                        "definitions": {}
                    },
                    "ui_schema": {
                        "lookback_days": {
                            "ui:widget": "range"
                        },
                        "force_fetch": {

                            "ui:widget": "select"
                        },
                        "ui:order": ["lookback_days", "force_fetch"],
                    },
                    "is_periodic_action": True
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
                        "allowed_devices_list": {
                            "title": "Allowed Devices List",
                            "type": "array",
                            "items": {}
                        },
                        "deduplication_enabled": {
                            "title": "Deduplication Enabled",
                            "type": "boolean"
                        }
                    },
                    "definitions": {},
                    "required": [
                        "allowed_devices_list",
                        "deduplication_enabled"
                    ]
                },
                "ui_schema": {
                    "allowed_devices_list": {"ui:widget": "list"},
                    "deduplication_enabled": {"ui:widget": "radio"}
                },
            }
        }
    )


@pytest.mark.asyncio
async def test_register_integration_with_service_url_arg(
        mocker, mock_gundi_client_v2, mock_action_handlers, mock_get_webhook_handler_for_fixed_json_payload
):
    mocker.patch("app.services.self_registration.INTEGRATION_TYPE_SLUG", "x_tracker")
    mocker.patch("app.services.self_registration.action_handlers", mock_action_handlers)
    mocker.patch("app.services.self_registration.get_webhook_handler", mock_get_webhook_handler_for_fixed_json_payload)
    service_url = "https://xtracker-actions-runner-jabcutl8yb-uc.a.run.app"
    await register_integration_in_gundi(
        gundi_client=mock_gundi_client_v2,
        service_url=service_url
    )
    assert mock_gundi_client_v2.register_integration_type.called
    mock_gundi_client_v2.register_integration_type.assert_called_with(
        {
            "name": "X Tracker",
            "value": "x_tracker",
            "description": f"Default type for integrations with X Tracker",
            'service_url': service_url,
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
                            "lookback_days": {
                                "title": "Data lookback days",
                                "description": "Number of days to look back for data.",
                                "default": 30,
                                "minimum": 1, "maximum": 30,
                                "type": "integer"
                            },
                            "force_fetch": {
                                "title": "Force fetch",
                                "description": "Force fetch even if in a quiet period.",
                                "default": False,
                                "type": "boolean"
                            }
                        },
                        "definitions": {}
                    },
                    "ui_schema": {
                        "lookback_days": {
                            "ui:widget": "range"
                        },
                        "force_fetch": {
                            "ui:widget": "select"
                        },
                        "ui:order": ["lookback_days", "force_fetch"],
                    },
                    "is_periodic_action": True
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
                        "allowed_devices_list": {
                            "title": "Allowed Devices List",
                            "type": "array",
                            "items": {}
                        },
                        "deduplication_enabled": {
                            "title": "Deduplication Enabled",
                            "type": "boolean"
                        }
                    },
                    "definitions": {},
                    "required": [
                        "allowed_devices_list",
                        "deduplication_enabled"
                    ]
                },
                "ui_schema": {
                    "allowed_devices_list": {"ui:widget": "list"},
                    "deduplication_enabled": {"ui:widget": "radio"}
                },
            }
        }
    )


@pytest.mark.asyncio
async def test_register_integration_with_service_url_setting(
        mocker, mock_gundi_client_v2, mock_action_handlers, mock_get_webhook_handler_for_fixed_json_payload
):
    service_url = "https://xtracker-actions-runner-jabcutl8yb-uc.a.run.app"
    mocker.patch("app.services.self_registration.INTEGRATION_TYPE_SLUG", "x_tracker")
    mocker.patch("app.services.self_registration.INTEGRATION_SERVICE_URL", service_url)
    mocker.patch("app.services.self_registration.action_handlers", mock_action_handlers)
    mocker.patch("app.services.self_registration.get_webhook_handler", mock_get_webhook_handler_for_fixed_json_payload)

    await register_integration_in_gundi(gundi_client=mock_gundi_client_v2, )

    assert mock_gundi_client_v2.register_integration_type.called
    mock_gundi_client_v2.register_integration_type.assert_called_with(
        {
            "name": "X Tracker",
            "value": "x_tracker",
            "description": f"Default type for integrations with X Tracker",
            'service_url': service_url,
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
                            "lookback_days": {
                                "title": "Data lookback days",
                                "description": "Number of days to look back for data.",
                                "default": 30,
                                "minimum": 1, "maximum": 30,
                                "type": "integer"
                            },
                            "force_fetch": {
                                "title": "Force fetch",
                                "description": "Force fetch even if in a quiet period.",
                                "default": False,
                                "type": "boolean"
                            }
                        },
                        "definitions": {}
                    },
                    "ui_schema": {
                        "lookback_days": {
                            "ui:widget": "range"
                        },
                        "force_fetch": {
                            "ui:widget": "select"
                        },
                        "ui:order": ["lookback_days", "force_fetch"],
                    },
                    "is_periodic_action": True
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
                        "allowed_devices_list": {
                            "title": "Allowed Devices List",
                            "type": "array",
                            "items": {}
                        },
                        "deduplication_enabled": {
                            "title": "Deduplication Enabled",
                            "type": "boolean"
                        }
                    },
                    "definitions": {},
                    "required": [
                        "allowed_devices_list",
                        "deduplication_enabled"
                    ]
                },
                "ui_schema": {
                    "allowed_devices_list": {"ui:widget": "list"},
                    "deduplication_enabled": {"ui:widget": "radio"}
                },
            }
        }
    )
