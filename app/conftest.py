import asyncio
import datetime
import json
import pydantic
import pytest
from unittest.mock import MagicMock
from app import settings
from gcloud.aio import pubsub
from gundi_core.schemas.v2 import Integration
from gundi_core.events import (
    IntegrationActionCustomLog,
    CustomActivityLog,
    IntegrationActionStarted,
    ActionExecutionStarted,
    IntegrationActionFailed,
    ActionExecutionFailed,
    IntegrationActionComplete,
    ActionExecutionComplete,
    IntegrationWebhookStarted,
    WebhookExecutionStarted,
    IntegrationWebhookComplete,
    WebhookExecutionComplete,
    IntegrationWebhookFailed,
    WebhookExecutionFailed,
    IntegrationWebhookCustomLog,
    CustomWebhookLog,
    LogLevel
)
from app.actions import PullActionConfiguration
from app.services.utils import GlobalUISchemaOptions, FieldWithUIOptions, UIOptions
from app.webhooks import GenericJsonTransformConfig, GenericJsonPayload, WebhookPayload, WebhookConfiguration


class AsyncMock(MagicMock):
    async def __call__(self, *args, **kwargs):
        return super(AsyncMock, self).__call__(*args, **kwargs)


def async_return(result):
    f = asyncio.Future()
    f.set_result(result)
    return f


@pytest.fixture
def mock_integration_state():
    return {"last_execution": "2024-01-29T11:20:00+0200"}


@pytest.fixture
def mock_redis(mocker, mock_integration_state):
    redis = MagicMock()
    redis_client = mocker.MagicMock()
    redis_client.set.return_value = async_return(MagicMock())
    redis_client.get.return_value = async_return(json.dumps(mock_integration_state, default=str))
    redis_client.delete.return_value = async_return(MagicMock())
    redis_client.setex.return_value = async_return(None)
    redis_client.incr.return_value = redis_client
    redis_client.decr.return_value = async_return(None)
    redis_client.expire.return_value = redis_client
    redis_client.execute.return_value = async_return((1, True))
    redis_client.__aenter__.return_value = redis_client
    redis_client.__aexit__.return_value = None
    redis_client.pipeline.return_value = redis_client
    redis.Redis.return_value = redis_client
    return redis


@pytest.fixture
def integration_v2():
    return Integration.parse_obj(
        {'id': '779ff3ab-5589-4f4c-9e0a-ae8d6c9edff0', 'name': 'Gundi X', 'base_url': 'https://gundi-er.pamdas.org',
         'enabled': True,
         'type': {'id': '50229e21-a9fe-4caa-862c-8592dfb2479b', 'name': 'EarthRanger', 'value': 'earth_ranger',
                  'description': 'Integration type for Integration X Sites', 'actions': [
                 {'id': '80448d1c-4696-4b32-a59f-f3494fc949ac', 'type': 'auth', 'name': 'Authenticate', 'value': 'auth',
                  'description': 'Authenticate against Integration X',
                  'schema': {'type': 'object', 'required': ['token'], 'properties': {'token': {'type': 'string'}}}},
                 {'id': '4b721b37-f4ca-4f20-b07c-2caadb095ecb', 'type': 'pull', 'name': 'Pull Events',
                  'value': 'pull_events', 'description': 'Extract events from EarthRanger sites',
                  'schema': {'type': 'object', 'title': 'PullObservationsConfig', 'required': ['start_datetime'],
                             'properties': {'start_datetime': {'type': 'string', 'title': 'Start Datetime'}}}},
                 {'id': '75b3040f-ab1f-42e7-b39f-8965c088b154', 'type': 'pull', 'name': 'Pull Observations',
                  'value': 'pull_observations', 'description': 'Extract observations from an EarthRanger Site',
                  'schema': {'type': 'object', 'title': 'PullObservationsConfig', 'required': ['start_datetime'],
                             'properties': {'start_datetime': {'type': 'string', 'title': 'Start Datetime'}}}},
                 {'id': '425a2e2f-ae71-44fb-9314-bc0116638e4f', 'type': 'push', 'name': 'Push Event Attachments',
                  'value': 'push_event_attachments',
                  'description': 'EarthRanger sites support adding attachments to events', 'schema': {}},
                 {'id': '8e101f31-e693-404c-b6ee-20fde6019f16', 'type': 'push', 'name': 'Push Events',
                  'value': 'push_events', 'description': 'EarthRanger sites support sending Events (a.k.a Reports)',
                  'schema': {}}]},
         'owner': {'id': 'a91b400b-482a-4546-8fcb-ee42b01deeb6', 'name': 'Test Org', 'description': ''},
         'configurations': [
             {'id': '5577c323-b961-4277-9047-b1f27fd6a1b7', 'integration': '779ff3ab-5589-4f4c-9e0a-ae8d6c9edff0',
              'action': {'id': '75b3040f-ab1f-42e7-b39f-8965c088b154', 'type': 'pull', 'name': 'Pull Observations',
                         'value': 'pull_observations'},
              'data': {'end_datetime': '2023-11-10T06:00:00-00:00', 'start_datetime': '2023-11-10T05:30:00-00:00',
                       'force_run_since_start': False}},
             {'id': '431af42b-c431-40af-8b57-a349253e15df', 'integration': '779ff3ab-5589-4f4c-9e0a-ae8d6c9edff0',
              'action': {'id': '4b721b37-f4ca-4f20-b07c-2caadb095ecb', 'type': 'pull', 'name': 'Pull Events',
                         'value': 'pull_events'}, 'data': {'start_datetime': '2023-11-16T00:00:00-03:00'}},
             {'id': '30f8878c-4a98-4c95-88eb-79f73c40fb2f', 'integration': '779ff3ab-5589-4f4c-9e0a-ae8d6c9edff0',
              'action': {'id': '80448d1c-4696-4b32-a59f-f3494fc949ac', 'type': 'auth', 'name': 'Authenticate',
                         'value': 'auth'}, 'data': {'token': 'testtoken2a97022f21732461ee103a08fac8a35'}}],
         'additional': {},
         'default_route': {'id': '5abf3845-7c9f-478a-bc0f-b24d87038c4b', 'name': 'Gundi X Provider - Default Route'},
         'status': 'healthy',
         'status_details': '',
        }
    )


@pytest.fixture
def integration_v2_with_webhook():
    return Integration.parse_obj(
        {
            "id": "abced116-efb4-4fb1-9d68-0ecc4b0996b2",
            "name": "Integration Tech X",
            "base_url": "",
            "enabled": True,
            "type": {
                "id": "f9891512-a334-4b36-95aa-50089cef25d3",
                "name": "Tech X",
                "value": "techx",
                "description": "Default type for integrations with Onyesha Wh",
                "actions": [],
                "webhook": {
                    "id": "1242a1bb-6d26-4dde-9ecb-72cb208695c2",
                    "name": "Tech X Webhook",
                    "value": "techx_webhook",
                    "description": "Webhook Integration with Tech X",
                    "schema": {
                        "title": "MockWebhookConfigModel",
                        "type": "object",
                        "properties": {
                            "allowed_devices_list": {"title": "Allowed Devices List", "type": "array", "items": {}},
                            "deduplication_enabled": {"title": "Deduplication Enabled", "type": "boolean"}},
                        "required": ["allowed_devices_list", "deduplication_enabled"]
                    },
                    "ui_schema": {
                        "allowed_devices_list": {
                            "ui:widget": "select"
                        },
                        "deduplication_enabled": {
                            "ui:widget": "radio"
                        }
                    }
                }
            },
            "owner": {
                "id": "a91b400b-482a-4546-8fcb-ee42b01deeb6",
                "name": "Test Org",
                "description": ""
            },
            "configurations": [],
            "webhook_configuration": {
                "id": "66904406-938a-48db-bbfe-08a99951dcb0",
                "integration": "ed8ed116-efb4-4fb1-9d68-0ecc4b0996a1",
                "webhook": {
                    "id": "1242a1bb-6d26-4dde-9ecb-72cb208695c2",
                    "name": "Tech X Webhook",
                    "value": "techx_webhook",
                },
                "data": {
                    "allowed_devices_list": ["device1", "device2"],
                    "deduplication_enabled": True
                }
            },
            "additional": {},
            "default_route": None,
            "status": "healthy",
            "status_details": "",
        }
    )


@pytest.fixture
def integration_v2_with_webhook_generic():
    return Integration.parse_obj(
        {
            "id": "ed8ed116-efb4-4fb1-9d68-0ecc4b0996a1",
            "name": "Smart Parks LT10",
            "base_url": "",
            "enabled": True,
            "type": {
                "id": "f9891512-a334-4b36-95aa-50089cef25d3",
                "name": "Onyesha Wh",
                "value": "onyesha_wh",
                "description": "Default type for integrations with Onyesha Wh",
                "actions": [],
                "webhook": {
                    "id": "3a42a1bb-6d26-4dde-9ecb-72cb208695c2",
                    "name": "Onyesha Wh Webhook",
                    "value": "onyesha_wh_webhook",
                    "description": "Webhook Integration with Onyesha Wh",
                    "schema": {
                        "type": "object",
                        "title": "GenericJsonTransformConfig",
                        "required": [
                            "json_schema",
                            "output_type"
                        ],
                        "properties": {
                            "jq_filter": {
                                "type": "string",
                                "title": "Jq Filter",
                                "default": ".",
                                "example": ". | map(select(.isActive))",
                                "description": "JQ filter to transform JSON data."
                            },
                            "json_schema": {
                                "type": "object",
                                "title": "Json Schema"
                            },
                            "output_type": {
                                "type": "string",
                                "title": "Output Type",
                                "description": "Output type for the transformed data: 'obv' or 'event'"
                            }
                        }
                    },
                    "ui_schema": {
                        "jq_filter": {
                            "ui:widget": "textarea"
                        },
                        "json_schema": {
                            "ui:widget": "textarea"
                        },
                        "output_type": {
                            "ui:widget": "text"
                        }
                    }
                }
            },
            "owner": {
                "id": "a91b400b-482a-4546-8fcb-ee42b01deeb6",
                "name": "Test Org",
                "description": ""
            },
            "configurations": [],
            "webhook_configuration": {
                "id": "66904406-938a-48db-bbfe-08a99951dcb0",
                "integration": "ed8ed116-efb4-4fb1-9d68-0ecc4b0996a1",
                "webhook": {
                    "id": "3a42a1bb-6d26-4dde-9ecb-72cb208695c2",
                    "name": "Onyesha Wh Webhook",
                    "value": "onyesha_wh_webhook"
                },
                "data": {
                    "jq_filter": "{     \"source\": .end_device_ids.device_id,     \"source_name\": .end_device_ids.device_id,     \"type\": .uplink_message.locations.\"frm-payload\".source,     \"recorded_at\": .uplink_message.settings.time,     \"location\": {       \"lat\": .uplink_message.locations.\"frm-payload\".latitude,       \"lon\": .uplink_message.locations.\"frm-payload\".longitude     },     \"additional\": {       \"application_id\": .end_device_ids.application_ids.application_id,       \"dev_eui\": .end_device_ids.dev_eui,       \"dev_addr\": .end_device_ids.dev_addr,       \"batterypercent\": .uplink_message.decoded_payload.batterypercent,       \"gps\": .uplink_message.decoded_payload.gps     }   }",
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "received_at": {
                                "type": "string",
                                "format": "date-time"
                            },
                            "end_device_ids": {
                                "type": "object",
                                "properties": {
                                    "dev_eui": {
                                        "type": "string"
                                    },
                                    "dev_addr": {
                                        "type": "string"
                                    },
                                    "device_id": {
                                        "type": "string"
                                    },
                                    "application_ids": {
                                        "type": "object",
                                        "properties": {
                                            "application_id": {
                                                "type": "string"
                                            }
                                        },
                                        "additionalProperties": False
                                    }
                                },
                                "additionalProperties": False
                            },
                            "uplink_message": {
                                "type": "object",
                                "properties": {
                                    "f_cnt": {
                                        "type": "integer"
                                    },
                                    "f_port": {
                                        "type": "integer"
                                    },
                                    "settings": {
                                        "type": "object",
                                        "properties": {
                                            "time": {
                                                "type": "string",
                                                "format": "date-time"
                                            },
                                            "data_rate": {
                                                "type": "object",
                                                "properties": {
                                                    "lora": {
                                                        "type": "object",
                                                        "properties": {
                                                            "bandwidth": {
                                                                "type": "integer"
                                                            },
                                                            "coding_rate": {
                                                                "type": "string"
                                                            },
                                                            "spreading_factor": {
                                                                "type": "integer"
                                                            }
                                                        },
                                                        "additionalProperties": False
                                                    }
                                                },
                                                "additionalProperties": False
                                            },
                                            "frequency": {
                                                "type": "string"
                                            },
                                            "timestamp": {
                                                "type": "integer"
                                            }
                                        },
                                        "additionalProperties": False
                                    },
                                    "locations": {
                                        "type": "object",
                                        "properties": {
                                            "frm-payload": {
                                                "type": "object",
                                                "properties": {
                                                    "source": {
                                                        "type": "string"
                                                    },
                                                    "latitude": {
                                                        "type": "number"
                                                    },
                                                    "longitude": {
                                                        "type": "number"
                                                    }
                                                },
                                                "additionalProperties": False
                                            }
                                        },
                                        "additionalProperties": False
                                    },
                                    "frm_payload": {
                                        "type": "string"
                                    },
                                    "network_ids": {
                                        "type": "object",
                                        "properties": {
                                            "ns_id": {
                                                "type": "string"
                                            },
                                            "net_id": {
                                                "type": "string"
                                            },
                                            "tenant_id": {
                                                "type": "string"
                                            },
                                            "cluster_id": {
                                                "type": "string"
                                            },
                                            "tenant_address": {
                                                "type": "string"
                                            },
                                            "cluster_address": {
                                                "type": "string"
                                            }
                                        },
                                        "additionalProperties": False
                                    },
                                    "received_at": {
                                        "type": "string",
                                        "format": "date-time"
                                    },
                                    "rx_metadata": {
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "properties": {
                                                "snr": {
                                                    "type": "number"
                                                },
                                                "rssi": {
                                                    "type": "integer"
                                                },
                                                "time": {
                                                    "type": "string",
                                                    "format": "date-time"
                                                },
                                                "gps_time": {
                                                    "type": "string",
                                                    "format": "date-time"
                                                },
                                                "timestamp": {
                                                    "type": "integer"
                                                },
                                                "gateway_ids": {
                                                    "type": "object",
                                                    "properties": {
                                                        "eui": {
                                                            "type": "string"
                                                        },
                                                        "gateway_id": {
                                                            "type": "string"
                                                        }
                                                    },
                                                    "additionalProperties": False
                                                },
                                                "received_at": {
                                                    "type": "string",
                                                    "format": "date-time"
                                                },
                                                "channel_rssi": {
                                                    "type": "integer"
                                                },
                                                "uplink_token": {
                                                    "type": "string"
                                                },
                                                "channel_index": {
                                                    "type": "integer"
                                                }
                                            },
                                            "additionalProperties": False
                                        }
                                    },
                                    "decoded_payload": {
                                        "type": "object",
                                        "properties": {
                                            "gps": {
                                                "type": "string"
                                            },
                                            "latitude": {
                                                "type": "number"
                                            },
                                            "longitude": {
                                                "type": "number"
                                            },
                                            "batterypercent": {
                                                "type": "integer"
                                            }
                                        },
                                        "additionalProperties": False
                                    },
                                    "consumed_airtime": {
                                        "type": "string"
                                    }
                                },
                                "additionalProperties": False
                            },
                            "correlation_ids": {
                                "type": "array",
                                "items": {
                                    "type": "string"
                                }
                            }
                        },
                        "additionalProperties": False
                    },
                    "output_type": "obv"
                }
            },
            "additional": {},
            "default_route": None,
            "status": "healthy",
            "status_details": "",
        }
    )


@pytest.fixture
def mock_generic_webhook_config():
    return {
        "jq_filter": "{     \"source\": .end_device_ids.device_id,     \"source_name\": .end_device_ids.device_id,     \"type\": .uplink_message.locations.\"frm-payload\".source,     \"recorded_at\": .uplink_message.settings.time,     \"location\": {       \"lat\": .uplink_message.locations.\"frm-payload\".latitude,       \"lon\": .uplink_message.locations.\"frm-payload\".longitude     },     \"additional\": {       \"application_id\": .end_device_ids.application_ids.application_id,       \"dev_eui\": .end_device_ids.dev_eui,       \"dev_addr\": .end_device_ids.dev_addr,       \"batterypercent\": .uplink_message.decoded_payload.batterypercent,       \"gps\": .uplink_message.decoded_payload.gps     }   }",
        "json_schema": {
            "type": "object",
            "properties": {
                "received_at": {
                    "type": "string",
                    "format": "date-time"
                },
                "end_device_ids": {
                    "type": "object",
                    "properties": {
                        "dev_eui": {
                            "type": "string"
                        },
                        "dev_addr": {
                            "type": "string"
                        },
                        "device_id": {
                            "type": "string"
                        },
                        "application_ids": {
                            "type": "object",
                            "properties": {
                                "application_id": {
                                    "type": "string"
                                }
                            },
                            "additionalProperties": False
                        }
                    },
                    "additionalProperties": False
                },
                "uplink_message": {
                    "type": "object",
                    "properties": {
                        "f_cnt": {
                            "type": "integer"
                        },
                        "f_port": {
                            "type": "integer"
                        },
                        "settings": {
                            "type": "object",
                            "properties": {
                                "time": {
                                    "type": "string",
                                    "format": "date-time"
                                },
                                "data_rate": {
                                    "type": "object",
                                    "properties": {
                                        "lora": {
                                            "type": "object",
                                            "properties": {
                                                "bandwidth": {
                                                    "type": "integer"
                                                },
                                                "coding_rate": {
                                                    "type": "string"
                                                },
                                                "spreading_factor": {
                                                    "type": "integer"
                                                }
                                            },
                                            "additionalProperties": False
                                        }
                                    },
                                    "additionalProperties": False
                                },
                                "frequency": {
                                    "type": "string"
                                },
                                "timestamp": {
                                    "type": "integer"
                                }
                            },
                            "additionalProperties": False
                        },
                        "locations": {
                            "type": "object",
                            "properties": {
                                "frm-payload": {
                                    "type": "object",
                                    "properties": {
                                        "source": {
                                            "type": "string"
                                        },
                                        "latitude": {
                                            "type": "number"
                                        },
                                        "longitude": {
                                            "type": "number"
                                        }
                                    },
                                    "additionalProperties": False
                                }
                            },
                            "additionalProperties": False
                        },
                        "frm_payload": {
                            "type": "string"
                        },
                        "network_ids": {
                            "type": "object",
                            "properties": {
                                "ns_id": {
                                    "type": "string"
                                },
                                "net_id": {
                                    "type": "string"
                                },
                                "tenant_id": {
                                    "type": "string"
                                },
                                "cluster_id": {
                                    "type": "string"
                                },
                                "tenant_address": {
                                    "type": "string"
                                },
                                "cluster_address": {
                                    "type": "string"
                                }
                            },
                            "additionalProperties": False
                        },
                        "received_at": {
                            "type": "string",
                            "format": "date-time"
                        },
                        "rx_metadata": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "snr": {
                                        "type": "number"
                                    },
                                    "rssi": {
                                        "type": "integer"
                                    },
                                    "time": {
                                        "type": "string",
                                        "format": "date-time"
                                    },
                                    "gps_time": {
                                        "type": "string",
                                        "format": "date-time"
                                    },
                                    "timestamp": {
                                        "type": "integer"
                                    },
                                    "gateway_ids": {
                                        "type": "object",
                                        "properties": {
                                            "eui": {
                                                "type": "string"
                                            },
                                            "gateway_id": {
                                                "type": "string"
                                            }
                                        },
                                        "additionalProperties": False
                                    },
                                    "received_at": {
                                        "type": "string",
                                        "format": "date-time"
                                    },
                                    "channel_rssi": {
                                        "type": "integer"
                                    },
                                    "uplink_token": {
                                        "type": "string"
                                    },
                                    "channel_index": {
                                        "type": "integer"
                                    }
                                },
                                "additionalProperties": False
                            }
                        },
                        "decoded_payload": {
                            "type": "object",
                            "properties": {
                                "gps": {
                                    "type": "string"
                                },
                                "latitude": {
                                    "type": "number"
                                },
                                "longitude": {
                                    "type": "number"
                                },
                                "batterypercent": {
                                    "type": "integer"
                                }
                            },
                            "additionalProperties": False
                        },
                        "consumed_airtime": {
                            "type": "string"
                        }
                    },
                    "additionalProperties": False
                },
                "correlation_ids": {
                    "type": "array",
                    "items": {
                        "type": "string"
                    }
                }
            },
            "additionalProperties": False
        },
        "output_type": "obv"
    }


@pytest.fixture
def pull_observations_config():
    return MockPullActionConfiguration(lookback_days=30)


@pytest.fixture
def mock_gundi_client_v2(
        mocker,
        integration_v2,
        mock_get_gundi_api_key
):
    mock_client = mocker.MagicMock()
    mock_client.get_integration_api_key.return_value = async_return(mock_get_gundi_api_key),
    mock_client.get_integration_details.return_value = async_return(
        integration_v2
    )
    mock_client.register_integration_type = AsyncMock()
    mock_client.__aenter__.return_value = mock_client
    return mock_client


@pytest.fixture
def mock_gundi_client_v2_for_webhooks(
        mocker,
        integration_v2_with_webhook,
        mock_get_gundi_api_key
):
    mock_client = mocker.MagicMock()
    mock_client.get_integration_api_key.return_value = async_return(mock_get_gundi_api_key),
    mock_client.get_integration_details.return_value = async_return(
        integration_v2_with_webhook
    )
    mock_client.register_integration_type = AsyncMock()
    mock_client.__aenter__.return_value = mock_client
    return mock_client


@pytest.fixture
def mock_gundi_client_v2_for_webhooks_generic(
        mocker,
        integration_v2_with_webhook_generic,
        mock_get_gundi_api_key
):
    mock_client = mocker.MagicMock()
    mock_client.get_integration_api_key.return_value = async_return(mock_get_gundi_api_key),
    mock_client.get_integration_details.return_value = async_return(
        integration_v2_with_webhook_generic
    )
    mock_client.register_integration_type = AsyncMock()
    mock_client.__aenter__.return_value = mock_client
    return mock_client


@pytest.fixture
def mock_gundi_client_v2_class(mocker, mock_gundi_client_v2):
    mock_gundi_client_v2_class = mocker.MagicMock()
    mock_gundi_client_v2_class.return_value = mock_gundi_client_v2
    return mock_gundi_client_v2_class


@pytest.fixture
def mock_gundi_sensors_client_class(
        mocker,
        events_created_response,
        event_attachment_created_response,
        observations_created_response
):
    mock_gundi_sensors_client_class = mocker.MagicMock()
    mock_gundi_sensors_client = mocker.MagicMock()
    mock_gundi_sensors_client.post_events.return_value = async_return(
        events_created_response
    )
    mock_gundi_sensors_client.post_event_attachments.return_value = async_return(
        event_attachment_created_response
    )
    mock_gundi_sensors_client.post_observations.return_value = async_return(
        observations_created_response
    )
    mock_gundi_sensors_client_class.return_value = mock_gundi_sensors_client
    return mock_gundi_sensors_client_class


@pytest.fixture
def events_created_response():
    return [
        {
            "id": "e1e1e1e1-e1e1-e1e1-e1e1-e1e1e1e1e1e1",
            "title": "Animal Sighting",
            "event_type": "wildlife_sighting_rep",
            "recorded_at": "2024-01-29 20:51:10-03:00",
            "location": {
                "lat": -51.688645,
                "lon": -72.704421
            },
            "event_details": {
                "site_name": "MM Spot",
                "species": "lion"
            }
        },
        {
            "id": "e1e1e1e1-e1e1-e1e1-e1e1-e1e1e1e1e1e2",
            "title": "Animal Sighting",
            "event_type": "wildlife_sighting_rep",
            "recorded_at": "2024-01-29 20:51:25-03:00",
            "location": {
                "lat": -51.688646,
                "lon": -72.704421
            },
            "event_details": {
                "site_name": "MM Spot",
                "species": "lion"
            }
        }
    ]


@pytest.fixture
def event_attachment_created_response():
    return [
        {
            "object_id": "af8e2946-bad6-4d02-8a26-99dde34bd9fa",
            "created_at": "2024-07-04T13:15:26.559894Z",
            "updated_at": None
        },
        {
            "object_id": "gat51h73-dd71-dj88-91uh-jah7162hy6fs",
            "created_at": "2024-07-03T13:15:26.559894Z",
            "updated_at": None
        }
    ]


@pytest.fixture
def observations_created_response():
    return [
        {
            "id": "e1e1e1e1-e1e1-e1e1-e1e1-e1e1e1e1e1e1",
            "source": "device-xy123",
            "type": "tracking-device",
            "subject_type": "puma",
            "recorded_at": "2024-01-24 09:03:00-0300",
            "location": {
                "lat": -51.748,
                "lon": -72.720
            },
            "additional": {
                "speed_kmph": 5
            }
        },
        {
            "id": "e1e1e1e1-e1e1-e1e1-e1e1-e1e1e1e1e1e2",
            "source": "test-device-mariano",
            "type": "tracking-device",
            "subject_type": "puma",
            "recorded_at": "2024-01-24 09:05:00-0300",
            "location": {
                "lat": -51.755,
                "lon": -72.755
            },
            "additional": {
                "speed_kmph": 5
            }
        }
    ]


@pytest.fixture
def mock_state_manager(mocker):
    mock_state_manager = mocker.MagicMock()
    mock_state_manager.get_state.return_value = async_return(
        {'last_execution': '2023-11-17T11:20:00+0200'}
    )
    mock_state_manager.set_state.return_value = async_return(None)
    return mock_state_manager


@pytest.fixture
def mock_pubsub_client(
        mocker, integration_event_pubsub_message, gcp_pubsub_publish_response
):
    mock_client = mocker.MagicMock()
    mock_publisher = mocker.MagicMock()
    mock_publisher.publish.return_value = async_return(gcp_pubsub_publish_response)
    mock_publisher.topic_path.return_value = (
        f"projects/{settings.GCP_PROJECT_ID}/topics/{settings.INTEGRATION_EVENTS_TOPIC}"
    )
    mock_client.PublisherClient.return_value = mock_publisher
    mock_client.PubsubMessage.return_value = integration_event_pubsub_message
    return mock_client


@pytest.fixture
def integration_event_pubsub_message():
    return pubsub.PubsubMessage(
        b'{"event_id": "6214c049-f786-45eb-9877-2efb2c2cf8e9", "timestamp": "2024-01-26 14:03:46.199385+00:00", "schema_version": "v1", "payload": {"integration_id": "779ff3ab-5589-4f4c-9e0a-ae8d6c9edff0", "action_id": "pull_observations", "config_data": {"end_datetime": "2024-01-01T00:00:00-00:00", "start_datetime": "2024-01-10T23:59:59-00:00", "force_run_since_start": True}}, "event_type": "IntegrationActionStarted"}'
    )


@pytest.fixture
def gcp_pubsub_publish_response():
    return {"messageIds": ["7061707768812258"]}


@pytest.fixture
def mock_publish_event(gcp_pubsub_publish_response):
    mock_publish_event = AsyncMock()
    mock_publish_event.return_value = gcp_pubsub_publish_response
    return mock_publish_event


class MockPullActionConfiguration(PullActionConfiguration):
    lookback_days: int = FieldWithUIOptions(
        30,
        le=30,
        ge=1,
        title="Data lookback days",
        description="Number of days to look back for data.",
        ui_options=UIOptions(
            widget="range",
        )
    )
    force_fetch: bool = FieldWithUIOptions(
        False,
        title="Force fetch",
        description="Force fetch even if in a quiet period.",
        ui_options=UIOptions(
            widget="select",
        )
    )
    ui_global_options = GlobalUISchemaOptions(
        order=[
            "lookback_days",
            "force_fetch",
        ],
    )


@pytest.fixture
def mock_action_handlers(mocker):
    mock_action_handler = AsyncMock()
    mock_action_handler.return_value = {"observations_extracted": 10}
    mock_action_handlers = {"pull_observations": (mock_action_handler, MockPullActionConfiguration)}
    return mock_action_handlers


@pytest.fixture
def auth_headers_response():
    return {
        'Accept-Type': 'application/json',
        'Authorization': 'Bearer testtoken2a97022f21732461ee103a08fac8a35'
    }


@pytest.fixture
def mock_get_gundi_api_key(mocker, mock_api_key):
    mock = mocker.MagicMock()
    mock.return_value = async_return(mock_api_key)
    return mock


@pytest.fixture
def mock_api_key():
    return "MockAP1K3y"


@pytest.fixture
def event_v2_cloud_event_payload():
    timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return {
        "message": {
            "data": "eyJpbnRlZ3JhdGlvbl9pZCI6ICI4NDNlMDgwMS1lODFhLTQ3ZTUtOWNlMi1iMTc2ZTQ3MzZhODUiLCAiYWN0aW9uX2lkIjogInB1bGxfb2JzZXJ2YXRpb25zIn0=",
            "messageId": "10298788169291041", "message_id": "10298788169291041",
            "publishTime": timestamp,
            "publish_time": timestamp
        },
        "subscription": "projects/cdip-stage-78ca/subscriptions/integrationx-actions-sub"
    }


@pytest.fixture
def event_v2_cloud_event_payload_with_config_overrides():
    timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return {
        "message": {
            "data": "eyJpbnRlZ3JhdGlvbl9pZCI6ICI4NDNlMDgwMS1lODFhLTQ3ZTUtOWNlMi1iMTc2ZTQ3MzZhODUiLCAiYWN0aW9uX2lkIjogInB1bGxfb2JzZXJ2YXRpb25zIiwgImNvbmZpZ19vdmVycmlkZXMiOiB7Imxvb2tiYWNrX2RheXMiOiAzfX0=",
            "messageId": "10298788169291041", "message_id": "10298788169291041",
            "publishTime": timestamp,
            "publish_time": timestamp
        },
        "subscription": "projects/cdip-stage-78ca/subscriptions/integrationx-actions-sub"
    }


@pytest.fixture
def event_v2_cloud_event_headers():
    timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return {
        "host": "integrationx-actions-runner-jabcutl7za-uc.a.run.app",
        "content-type": "application/json",
        "authorization": "Bearer fake-token",
        "content-length": "2057",
        "accept": "application/json",
        "from": "noreply@google.com",
        "user-agent": "APIs-Google; (+https://developers.google.com/webmasters/APIs-Google.html)",
        "x-cloud-trace-context": "",
        "traceparent": "",
        "x-forwarded-for": "64.233.172.137",
        "x-forwarded-proto": "https",
        "forwarded": 'for="64.233.172.137";proto=https',
        "accept-encoding": "gzip, deflate, br",
        "ce-id": "20090163454824831",
        "ce-source": "//pubsub.googleapis.com/projects/cdip-stage-78ca/topics/integrationx-actions-topic-test",
        "ce-specversion": "1.0",
        "ce-type": "google.cloud.pubsub.topic.v1.messagePublished",
        "ce-time": timestamp,
    }


@pytest.fixture
def action_started_event():
    return IntegrationActionStarted(
        payload=ActionExecutionStarted(
            integration_id="779ff3ab-5589-4f4c-9e0a-ae8d6c9edff0",
            action_id="pull_observations",
            config_data={
                "end_datetime": "2024-01-10T00:00:00-00:00",
                "start_datetime": "2024-01-01T23:59:59-00:00",
                "force_run_since_start": True
            },
        )
    )


@pytest.fixture
def action_complete_event():
    return IntegrationActionComplete(
        payload=ActionExecutionComplete(
            integration_id="779ff3ab-5589-4f4c-9e0a-ae8d6c9edff0",
            action_id="pull_observations",
            config_data={
                "end_datetime": "2024-01-10T00:00:00-00:00",
                "start_datetime": "2024-01-01T23:59:59-00:00",
                "force_run_since_start": True
            },
            result={"observations_extracted": 10}
        )
    )


@pytest.fixture
def action_failed_event():
    return IntegrationActionFailed(
        payload=ActionExecutionFailed(
            integration_id="779ff3ab-5589-4f4c-9e0a-ae8d6c9edff0",
            action_id="pull_observations",
            config_data={
                "end_datetime": "2024-01-10T00:00:00-00:00",
                "start_datetime": "2024-01-01T23:59:59-00:00",
                "force_run_since_start": True
            },
            error="ConnectionError: Error connecting to X system"
        )
    )


@pytest.fixture
def custom_activity_log_event():
    return IntegrationActionCustomLog(
        payload=CustomActivityLog(
            integration_id="779ff3ab-5589-4f4c-9e0a-ae8d6c9edff0",
            action_id="pull_observations",
            config_data={
                "end_datetime": "2024-01-01T00:00:00-00:00",
                "start_datetime": "2024-01-10T23:59:59-00:00",
                "force_run_since_start": True
            },
            title="Invalid start_datetime for action pull_observations",
            level=LogLevel.ERROR,
            data={
                "details": "start_datetime cannot be grater than end_datetime. Please fix the configuration."
            }
        )
    )


@pytest.fixture
def webhook_started_event():
    return IntegrationWebhookStarted(
        payload=WebhookExecutionStarted(
            integration_id='ed8ed116-efb4-4fb1-9d68-0ecc4b0996a1',
            webhook_id='lionguards_webhook',
            config_data={
                'json_schema': {'type': 'object', 'properties': {'received_at': {'type': 'string', 'format': 'date-time'}, 'end_device_ids': {'type': 'object', 'properties': {'dev_eui': {'type': 'string'}, 'dev_addr': {'type': 'string'}, 'device_id': {'type': 'string'}, 'application_ids': {'type': 'object', 'properties': {'application_id': {'type': 'string'}}, 'additionalProperties': False}}, 'additionalProperties': False}, 'uplink_message': {'type': 'object', 'properties': {'f_cnt': {'type': 'integer'}, 'f_port': {'type': 'integer'}, 'settings': {'type': 'object', 'properties': {'time': {'type': 'string', 'format': 'date-time'}, 'data_rate': {'type': 'object', 'properties': {'lora': {'type': 'object', 'properties': {'bandwidth': {'type': 'integer'}, 'coding_rate': {'type': 'string'}, 'spreading_factor': {'type': 'integer'}}, 'additionalProperties': False}}, 'additionalProperties': False}, 'frequency': {'type': 'string'}, 'timestamp': {'type': 'integer'}}, 'additionalProperties': False}, 'locations': {'type': 'object', 'properties': {'frm-payload': {'type': 'object', 'properties': {'source': {'type': 'string'}, 'latitude': {'type': 'number'}, 'longitude': {'type': 'number'}}, 'additionalProperties': False}}, 'additionalProperties': False}, 'frm_payload': {'type': 'string'}, 'network_ids': {'type': 'object', 'properties': {'ns_id': {'type': 'string'}, 'net_id': {'type': 'string'}, 'tenant_id': {'type': 'string'}, 'cluster_id': {'type': 'string'}, 'tenant_address': {'type': 'string'}, 'cluster_address': {'type': 'string'}}, 'additionalProperties': False}, 'received_at': {'type': 'string', 'format': 'date-time'}, 'rx_metadata': {'type': 'array', 'items': {'type': 'object', 'properties': {'snr': {'type': 'number'}, 'rssi': {'type': 'integer'}, 'time': {'type': 'string', 'format': 'date-time'}, 'gps_time': {'type': 'string', 'format': 'date-time'}, 'timestamp': {'type': 'integer'}, 'gateway_ids': {'type': 'object', 'properties': {'eui': {'type': 'string'}, 'gateway_id': {'type': 'string'}}, 'additionalProperties': False}, 'received_at': {'type': 'string', 'format': 'date-time'}, 'channel_rssi': {'type': 'integer'}, 'uplink_token': {'type': 'string'}, 'channel_index': {'type': 'integer'}}, 'additionalProperties': False}}, 'decoded_payload': {'type': 'object', 'properties': {'gps': {'type': 'string'}, 'latitude': {'type': 'number'}, 'longitude': {'type': 'number'}, 'batterypercent': {'type': 'integer'}}, 'additionalProperties': False}, 'consumed_airtime': {'type': 'string'}}, 'additionalProperties': False}, 'correlation_ids': {'type': 'array', 'items': {'type': 'string'}}}, 'additionalProperties': False},
                'jq_filter': '{"source": .end_device_ids.device_id, "source_name": .end_device_ids.device_id, "type": .uplink_message.locations."frm-payload".source, "recorded_at": .uplink_message.settings.time, "location": { "lat": .uplink_message.locations."frm-payload".latitude, "lon": .uplink_message.locations."frm-payload".longitude}, "additional": {"application_id": .end_device_ids.application_ids.application_id, "dev_eui": .end_device_ids.dev_eui, "dev_addr": .end_device_ids.dev_addr, "batterypercent": .uplink_message.decoded_payload.batterypercent, "gps": .uplink_message.decoded_payload.gps}}',
                'output_type': 'obv'
            }
        )
    )


@pytest.fixture
def webhook_complete_event():
    return IntegrationWebhookComplete(
        payload=WebhookExecutionComplete(
            integration_id='ed8ed116-efb4-4fb1-9d68-0ecc4b0996a1',
            webhook_id='lionguards_webhook',
            config_data={
                'json_schema': {'type': 'object', 'properties': {'received_at': {'type': 'string', 'format': 'date-time'}, 'end_device_ids': {'type': 'object', 'properties': {'dev_eui': {'type': 'string'}, 'dev_addr': {'type': 'string'}, 'device_id': {'type': 'string'}, 'application_ids': {'type': 'object', 'properties': {'application_id': {'type': 'string'}}, 'additionalProperties': False}}, 'additionalProperties': False}, 'uplink_message': {'type': 'object', 'properties': {'f_cnt': {'type': 'integer'}, 'f_port': {'type': 'integer'}, 'settings': {'type': 'object', 'properties': {'time': {'type': 'string', 'format': 'date-time'}, 'data_rate': {'type': 'object', 'properties': {'lora': {'type': 'object', 'properties': {'bandwidth': {'type': 'integer'}, 'coding_rate': {'type': 'string'}, 'spreading_factor': {'type': 'integer'}}, 'additionalProperties': False}}, 'additionalProperties': False}, 'frequency': {'type': 'string'}, 'timestamp': {'type': 'integer'}}, 'additionalProperties': False}, 'locations': {'type': 'object', 'properties': {'frm-payload': {'type': 'object', 'properties': {'source': {'type': 'string'}, 'latitude': {'type': 'number'}, 'longitude': {'type': 'number'}}, 'additionalProperties': False}}, 'additionalProperties': False}, 'frm_payload': {'type': 'string'}, 'network_ids': {'type': 'object', 'properties': {'ns_id': {'type': 'string'}, 'net_id': {'type': 'string'}, 'tenant_id': {'type': 'string'}, 'cluster_id': {'type': 'string'}, 'tenant_address': {'type': 'string'}, 'cluster_address': {'type': 'string'}}, 'additionalProperties': False}, 'received_at': {'type': 'string', 'format': 'date-time'}, 'rx_metadata': {'type': 'array', 'items': {'type': 'object', 'properties': {'snr': {'type': 'number'}, 'rssi': {'type': 'integer'}, 'time': {'type': 'string', 'format': 'date-time'}, 'gps_time': {'type': 'string', 'format': 'date-time'}, 'timestamp': {'type': 'integer'}, 'gateway_ids': {'type': 'object', 'properties': {'eui': {'type': 'string'}, 'gateway_id': {'type': 'string'}}, 'additionalProperties': False}, 'received_at': {'type': 'string', 'format': 'date-time'}, 'channel_rssi': {'type': 'integer'}, 'uplink_token': {'type': 'string'}, 'channel_index': {'type': 'integer'}}, 'additionalProperties': False}}, 'decoded_payload': {'type': 'object', 'properties': {'gps': {'type': 'string'}, 'latitude': {'type': 'number'}, 'longitude': {'type': 'number'}, 'batterypercent': {'type': 'integer'}}, 'additionalProperties': False}, 'consumed_airtime': {'type': 'string'}}, 'additionalProperties': False}, 'correlation_ids': {'type': 'array', 'items': {'type': 'string'}}}, 'additionalProperties': False},
                'jq_filter': '{"source": .end_device_ids.device_id, "source_name": .end_device_ids.device_id, "type": .uplink_message.locations."frm-payload".source, "recorded_at": .uplink_message.settings.time, "location": { "lat": .uplink_message.locations."frm-payload".latitude, "lon": .uplink_message.locations."frm-payload".longitude}, "additional": {"application_id": .end_device_ids.application_ids.application_id, "dev_eui": .end_device_ids.dev_eui, "dev_addr": .end_device_ids.dev_addr, "batterypercent": .uplink_message.decoded_payload.batterypercent, "gps": .uplink_message.decoded_payload.gps}}',
                'output_type': 'obv'
            },
            result={'data_points_qty': 1}
        )
    )


@pytest.fixture
def webhook_failed_event():
    return IntegrationWebhookFailed(
        payload=WebhookExecutionFailed(
            integration_id='ed8ed116-efb4-4fb1-9d68-0ecc4b0996a1',
            webhook_id='lionguards_webhook',
            config_data={
                'json_schema': {'type': 'object', 'properties': {'received_at': {'type': 'string', 'format': 'date-time'}, 'end_device_ids': {'type': 'object', 'properties': {'dev_eui': {'type': 'string'}, 'dev_addr': {'type': 'string'}, 'device_id': {'type': 'string'}, 'application_ids': {'type': 'object', 'properties': {'application_id': {'type': 'string'}}, 'additionalProperties': False}}, 'additionalProperties': False}, 'uplink_message': {'type': 'object', 'properties': {'f_cnt': {'type': 'integer'}, 'f_port': {'type': 'integer'}, 'settings': {'type': 'object', 'properties': {'time': {'type': 'string', 'format': 'date-time'}, 'data_rate': {'type': 'object', 'properties': {'lora': {'type': 'object', 'properties': {'bandwidth': {'type': 'integer'}, 'coding_rate': {'type': 'string'}, 'spreading_factor': {'type': 'integer'}}, 'additionalProperties': False}}, 'additionalProperties': False}, 'frequency': {'type': 'string'}, 'timestamp': {'type': 'integer'}}, 'additionalProperties': False}, 'locations': {'type': 'object', 'properties': {'frm-payload': {'type': 'object', 'properties': {'source': {'type': 'string'}, 'latitude': {'type': 'number'}, 'longitude': {'type': 'number'}}, 'additionalProperties': False}}, 'additionalProperties': False}, 'frm_payload': {'type': 'string'}, 'network_ids': {'type': 'object', 'properties': {'ns_id': {'type': 'string'}, 'net_id': {'type': 'string'}, 'tenant_id': {'type': 'string'}, 'cluster_id': {'type': 'string'}, 'tenant_address': {'type': 'string'}, 'cluster_address': {'type': 'string'}}, 'additionalProperties': False}, 'received_at': {'type': 'string', 'format': 'date-time'}, 'rx_metadata': {'type': 'array', 'items': {'type': 'object', 'properties': {'snr': {'type': 'number'}, 'rssi': {'type': 'integer'}, 'time': {'type': 'string', 'format': 'date-time'}, 'gps_time': {'type': 'string', 'format': 'date-time'}, 'timestamp': {'type': 'integer'}, 'gateway_ids': {'type': 'object', 'properties': {'eui': {'type': 'string'}, 'gateway_id': {'type': 'string'}}, 'additionalProperties': False}, 'received_at': {'type': 'string', 'format': 'date-time'}, 'channel_rssi': {'type': 'integer'}, 'uplink_token': {'type': 'string'}, 'channel_index': {'type': 'integer'}}, 'additionalProperties': False}}, 'decoded_payload': {'type': 'object', 'properties': {'gps': {'type': 'string'}, 'latitude': {'type': 'number'}, 'longitude': {'type': 'number'}, 'batterypercent': {'type': 'integer'}}, 'additionalProperties': False}, 'consumed_airtime': {'type': 'string'}}, 'additionalProperties': False}, 'correlation_ids': {'type': 'array', 'items': {'type': 'string'}}}, 'additionalProperties': False},
                'jq_filter': '{"source": .end_device_ids.device_id, "source_name": .end_device_ids.device_id, "type": .uplink_message.locations."frm-payload".source, "recorded_at": .uplink_message.settings.time, "location": { "lat": .uplink_message.locations."frm-payload".latitude, "lon": .uplink_message.locations."frm-payload".longitude}, "additional": {"application_id": .end_device_ids.application_ids.application_id, "dev_eui": .end_device_ids.dev_eui, "dev_addr": .end_device_ids.dev_addr, "batterypercent": .uplink_message.decoded_payload.batterypercent, "gps": .uplink_message.decoded_payload.gps}}',
                'output_type': 'patrol'
            },
            error='Invalid output type: patrol. Please review the configuration.'
        )
    )


@pytest.fixture
def webhook_custom_activity_log_event():
    return IntegrationWebhookCustomLog(
        payload=CustomWebhookLog(
            integration_id='ed8ed116-efb4-4fb1-9d68-0ecc4b0996a1',
            webhook_id='lionguards_webhook',
            config_data={},
            title='Webhook data transformed successfully',
            level=LogLevel.DEBUG,
            data={
                'transformed_data': [
                    {
                        'source': 'test-webhooks-mm',
                        'source_name': 'test-webhooks-mm',
                        'type': 'SOURCE_GPS',
                        'recorded_at': '2024-06-07T15:08:19.841Z',
                        'location': {'lat': -4.1234567, 'lon': 32.01234567890123},
                        'additional': {
                            'application_id': 'lt10-globalsat',
                            'dev_eui': '123456789ABCDEF0',
                            'dev_addr': '12345ABC',
                            'batterypercent': 100,
                            'gps': '3D fix'
                        }
                    }
                ]
            }
        )
    )


@pytest.fixture
def system_event(
        request, action_started_event, action_complete_event, action_failed_event, custom_activity_log_event,
        webhook_started_event, webhook_complete_event, webhook_failed_event, webhook_custom_activity_log_event
):
    if request.param == "action_started_event":
        return action_started_event
    if request.param == "action_complete_event":
        return action_complete_event
    if request.param == "action_failed_event":
        return action_failed_event
    if request.param == "custom_activity_log_event":
        return custom_activity_log_event
    if request.param == "webhook_started_event":
        return webhook_started_event
    if request.param == "webhook_complete_event":
        return webhook_complete_event
    if request.param == "webhook_failed_event":
        return webhook_failed_event
    if request.param == "webhook_custom_activity_log_event":
        return webhook_custom_activity_log_event
    return None


@pytest.fixture
def mock_webhook_handler():
    return AsyncMock()


@pytest.fixture
def mock_get_webhook_handler_for_generic_json_payload(mocker, mock_webhook_handler):
    mock_get_webhook_handler = mocker.MagicMock()
    payload_model = GenericJsonPayload
    config_model = GenericJsonTransformConfig
    mock_get_webhook_handler.return_value = mock_webhook_handler, payload_model, config_model
    return mock_get_webhook_handler


class MockWebhookPayloadModel(WebhookPayload):
    device_id: str
    received_at: str
    lat: float
    lon: float


class MockWebhookConfigModel(WebhookConfiguration):
    allowed_devices_list: list = FieldWithUIOptions(
        ...,
        title="Allowed Devices List",
        ui_options=UIOptions(
            widget="list",
        )
    )
    deduplication_enabled: bool = FieldWithUIOptions(
        ...,
        title="Deduplication Enabled",
        ui_options=UIOptions(
            widget="radio",
        )
    )


@pytest.fixture
def mock_get_webhook_handler_for_fixed_json_payload(mocker, mock_webhook_handler):
    mock_get_webhook_handler = mocker.MagicMock()
    payload_model = MockWebhookPayloadModel
    config_model = MockWebhookConfigModel
    mock_get_webhook_handler.return_value = mock_webhook_handler, payload_model, config_model
    return mock_get_webhook_handler


@pytest.fixture
def mock_webhook_request_headers_onyesha():
    return {
        "apikey": "testapikey",
        "x-consumer-username": "integration:testintegrationid",
        "x-gundi-integration-type": "onyesha_wh"
    }


@pytest.fixture
def mock_webhook_request_payload_for_dynamic_schema():
    return {
        "end_device_ids": {
            "device_id": "lt10-1234",
            "application_ids": {
                "application_id": "lt10-myapp"
            },
            "dev_eui": "0123456789ABCDEF",
            "dev_addr": "789ABCDE"
        },
        "correlation_ids": [
            "gs:uplink:FAKEWXYZK41B1ZE12346578ABC"
        ],
        "received_at": "2024-06-07T15:08:20.179713582Z",
        "uplink_message": {
            "f_port": 2,
            "f_cnt": 2904,
            "frm_payload": "gFAKExojovxCZCE=",
            "decoded_payload": {
                "batterypercent": 100,
                "gps": "3D fix",
                "latitude": -2.3828796,
                "longitude": 37.338060999999996
            },
            "rx_metadata": [
                {
                    "gateway_ids": {
                        "gateway_id": "my-gateway-006",
                        "eui": "123ABCDEFF1234A1"
                    },
                    "time": "2024-06-07T15:08:19.841Z",
                    "timestamp": 1569587228,
                    "rssi": -60,
                    "channel_rssi": -60,
                    "snr": 6.5,
                    "uplink_token": "FakeTokenlvbi1ndWFyZGlhbnMtMDA2Eghk13r//gFake123LjsBRoMCOPEjLMGELbnpsADIODawZXXgw4qDAjjxTestBhDAyIKRAw==",
                    "channel_index": 7,
                    "gps_time": "2024-06-07T15:08:19.841Z",
                    "received_at": "2024-06-07T15:08:19.880458765Z"
                }
            ],
            "settings": {
                "data_rate": {
                    "lora": {
                        "bandwidth": 125000,
                        "spreading_factor": 11,
                        "coding_rate": "4/5"
                    }
                },
                "frequency": "867900000",
                "timestamp": 1569587228,
                "time": "2024-06-07T15:08:19.841Z"
            },
            "received_at": "2024-06-07T15:08:19.940799259Z",
            "consumed_airtime": "1.482752s",
            "locations": {
                "frm-payload": {
                    "latitude": -5.1234567,
                    "longitude": 32.132456789999999,
                    "source": "SOURCE_GPS"
                }
            },
            "network_ids": {
                "net_id": "000015",
                "ns_id": "ABC1230000456789",
                "tenant_id": "faketenant",
                "cluster_id": "eu1",
                "cluster_address": "eu1.cloud.thethings.industries",
                "tenant_address": "faketenant.eu1.cloud.thethings.industries"
            }
        }
    }


@pytest.fixture
def mock_webhook_request_payload_for_fixed_schema():
    return {
        "device_id": "device1",
        "received_at": "2024-06-07T15:08:20.179713582Z",
        "lat": -2.3828796,
        "lon": 35.3380609
    }
