import pydantic
import pytest

from app.services.redaction import (
    REDACTED,
    redact_config_data,
    redact_secrets,
    secret_field_names,
)
from app.services.utils import FieldWithUIOptions, UIOptions


def test_redacts_values_under_sensitive_key_names_at_any_depth():
    data = {
        "username": "me@example.com",
        "password": "hunter2",
        "nested": {
            "api_key": "k-123",
            "items": [{"client_secret": "s-456", "name": "kept"}],
        },
    }

    assert redact_secrets(data) == {
        "username": "me@example.com",
        "password": REDACTED,
        "nested": {
            "api_key": REDACTED,
            "items": [{"client_secret": REDACTED, "name": "kept"}],
        },
    }


@pytest.mark.parametrize(
    "key",
    ["Password", "PASSWD", "API-Key", "apiKey", "AccessToken", "refresh_token", "Authorization",
     "private_key", "credentials", "client_secret"],
)
def test_key_match_is_case_insensitive_and_ignores_separators(key):
    assert redact_secrets({key: "value"}) == {key: REDACTED}


def test_leaves_non_secret_values_untouched():
    data = {"lookback_hours": 4, "device_refs": ["a", "b"], "enabled": True, "base_url": "https://x"}

    assert redact_secrets(data) == data


def test_leaves_an_unset_secret_visible_as_unset():
    # None or "" says "nothing configured", which is what an operator debugging
    # a failed auth needs to see, and discloses nothing.
    assert redact_secrets({"password": None, "token": ""}) == {"password": None, "token": ""}


def test_masks_a_secret_str_value_whatever_its_key():
    assert redact_secrets({"pin": pydantic.SecretStr("1234")}) == {"pin": REDACTED}


def test_does_not_mutate_the_input():
    data = {"password": "hunter2", "nested": {"token": "t"}}

    redact_secrets(data)

    assert data == {"password": "hunter2", "nested": {"token": "t"}}


class _Config(pydantic.BaseModel):
    username: str
    password: pydantic.SecretStr
    pin: str = pydantic.Field(..., format="password")
    code: str = FieldWithUIOptions(..., ui_options=UIOptions(widget="password"))
    plain: str


def test_secret_field_names_reads_secret_str_password_format_and_password_widget():
    assert secret_field_names(_Config) == frozenset({"password", "pin", "code"})


def test_secret_field_names_is_empty_for_a_model_without_secrets():
    class Plain(pydantic.BaseModel):
        lookback_hours: int

    assert secret_field_names(Plain) == frozenset()


def test_redacts_fields_the_model_declares_as_secret_even_with_innocent_names():
    data = {"username": "u", "pin": "1234", "code": "abcd", "plain": "p"}

    out = redact_secrets(data, secret_fields=secret_field_names(_Config))

    assert out == {"username": "u", "pin": REDACTED, "code": REDACTED, "plain": "p"}


class _AuthConfig(pydantic.BaseModel):
    username: str
    login_code: pydantic.SecretStr


class _PullConfig(pydantic.BaseModel):
    start_datetime: str


_CONFIG_MODELS = {"auth": _AuthConfig, "pull_events": _PullConfig}


def test_redact_config_data_uses_each_configuration_rows_own_model():
    # The shape _handle_error attaches on a saved-integration failure: the
    # integration's IntegrationActionConfiguration rows, serialized.
    config_data = {
        "configurations": [
            {
                "id": "30f8878c",
                "integration": "779ff3ab",
                "action": {"id": "80448d1c", "type": "auth", "name": "Authenticate", "value": "auth"},
                "data": {"username": "u", "login_code": "abc"},
            },
            {
                "id": "431af42b",
                "integration": "779ff3ab",
                "action": {"id": "4b721b37", "type": "pull", "name": "Pull Events", "value": "pull_events"},
                "data": {"start_datetime": "2023-11-16T00:00:00-03:00"},
            },
        ]
    }

    out = redact_config_data(config_data, config_models=_CONFIG_MODELS)

    assert out["configurations"][0]["data"] == {"username": "u", "login_code": REDACTED}
    assert out["configurations"][0]["action"] == config_data["configurations"][0]["action"]
    assert out["configurations"][1] == config_data["configurations"][1]


def test_redact_config_data_uses_the_action_id_model_for_a_flat_config():
    # The shape attached when the action's own config fails validation.
    out = redact_config_data({"username": "u", "login_code": "abc"}, action_id="auth", config_models=_CONFIG_MODELS)

    assert out == {"username": "u", "login_code": REDACTED}


def test_redact_config_data_without_a_model_still_redacts_by_name():
    config_data = {"configurations": [{"action": {"value": "unknown"}, "data": {"password": "p", "site": "s"}}]}

    out = redact_config_data(config_data, config_models={})

    assert out["configurations"][0]["data"] == {"password": REDACTED, "site": "s"}


def test_redact_config_data_tolerates_rows_without_an_action_or_data():
    config_data = {"configurations": [{"id": "x"}, {"action": None, "data": None}, "not-a-row"]}

    assert redact_config_data(config_data, config_models=_CONFIG_MODELS) == config_data


def test_redact_config_data_normalizes_none_to_an_empty_dict():
    assert redact_config_data(None, config_models={}) == {}
