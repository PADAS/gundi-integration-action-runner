import typing

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

    out = redact_secrets(data, model=_Config)

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


# --- Review on PR #120: aliases and nested models ---

class _AliasedAuthConfig(pydantic.BaseModel):
    username: str
    login_code: pydantic.SecretStr = pydantic.Field(..., alias="code")


def test_secret_field_names_includes_aliases():
    assert secret_field_names(_AliasedAuthConfig) == frozenset({"login_code", "code"})


def test_redacts_a_model_declared_secret_saved_under_its_alias():
    # The portal saves a configuration under the field's alias; the decorator
    # path serializes it under the field name. Both spellings are the secret.
    models = {"auth": _AliasedAuthConfig}

    by_alias = redact_config_data({"username": "u", "code": "alias-secret"}, action_id="auth", config_models=models)
    by_name = redact_config_data({"username": "u", "login_code": "name-secret"}, action_id="auth", config_models=models)

    assert by_alias == {"username": "u", "code": REDACTED}
    assert by_name == {"username": "u", "login_code": REDACTED}


def test_redacts_a_model_declared_secret_saved_under_its_alias_in_a_configuration_row():
    config_data = {"configurations": [{"action": {"value": "auth"}, "data": {"username": "u", "code": "alias-secret"}}]}

    out = redact_config_data(config_data, config_models={"auth": _AliasedAuthConfig})

    assert out["configurations"][0]["data"] == {"username": "u", "code": REDACTED}


class _NestedDetails(pydantic.BaseModel):
    pin: pydantic.SecretStr
    code: str = pydantic.Field(..., format="password")
    label: str


class _NestingConfig(pydantic.BaseModel):
    details: _NestedDetails
    history: typing.List[_NestedDetails]
    by_site: typing.Dict[str, _NestedDetails]
    optional_details: typing.Optional[_NestedDetails] = None
    plain: str


_NESTED_RAW = {"pin": "nested-pin", "code": "nested-password", "label": "kept"}
_NESTED_MASKED = {"pin": REDACTED, "code": REDACTED, "label": "kept"}


def test_redacts_model_declared_secrets_below_the_root():
    data = {
        "details": dict(_NESTED_RAW),
        "history": [dict(_NESTED_RAW), dict(_NESTED_RAW)],
        "by_site": {"site-a": dict(_NESTED_RAW)},
        "optional_details": dict(_NESTED_RAW),
        "plain": "p",
    }

    out = redact_secrets(data, model=_NestingConfig)

    assert out == {
        "details": _NESTED_MASKED,
        "history": [_NESTED_MASKED, _NESTED_MASKED],
        "by_site": {"site-a": _NESTED_MASKED},
        "optional_details": _NESTED_MASKED,
        "plain": "p",
    }


def test_redact_config_data_applies_nested_model_declarations_to_a_configuration_row():
    config_data = {"configurations": [{"action": {"value": "pull"}, "data": {"details": dict(_NESTED_RAW), "history": [], "by_site": {}, "plain": "p"}}]}

    out = redact_config_data(config_data, config_models={"pull": _NestingConfig})

    assert out["configurations"][0]["data"]["details"] == _NESTED_MASKED


def test_a_sensitive_named_container_keeps_its_structure_and_masks_every_leaf_beneath_it():
    # A stored OAuth token dict: the inner keys say nothing about secrecy,
    # the outer one says everything.
    data = {"auth_token": {"access": "a", "refresh": "r", "expires_in": 3600, "scopes": ["read", "write"]}, "site": "s"}

    assert redact_secrets(data) == {
        "auth_token": {"access": REDACTED, "refresh": REDACTED, "expires_in": REDACTED, "scopes": [REDACTED, REDACTED]},
        "site": "s",
    }


# --- Second review on PR #120: typed mappings and union variants ---

class _TypedMappingConfig(pydantic.BaseModel):
    options: typing.Dict[str, str]
    sites: typing.Dict[str, _NestedDetails]


def test_typed_mapping_entries_keep_the_key_name_check():
    # Supplying the model must never switch off the protection the data gets
    # without one.
    data = {"options": {"password": "map-secret", "api_key": "map-key", "site": "kept"}, "sites": {}}

    out = redact_secrets(data, model=_TypedMappingConfig)

    assert out["options"] == {"password": REDACTED, "api_key": REDACTED, "site": "kept"}
    assert out == redact_secrets(data)


def test_typed_mapping_with_a_sensitive_key_masks_every_leaf_of_that_entry():
    data = {"options": {}, "sites": {"credentials": dict(_NESTED_RAW), "main": dict(_NESTED_RAW)}}

    out = redact_secrets(data, model=_TypedMappingConfig)

    assert out["sites"]["credentials"] == {"pin": REDACTED, "code": REDACTED, "label": REDACTED}
    assert out["sites"]["main"] == _NESTED_MASKED


class _FirstVariant(pydantic.BaseModel):
    kind: typing.Literal["first"]
    label: str


class _SecondVariant(pydantic.BaseModel):
    kind: typing.Literal["second"]
    pin: pydantic.SecretStr
    code: str = pydantic.Field(..., format="password")
    label: str


class _DiscriminatedUnionConfig(pydantic.BaseModel):
    details: typing.Union[_FirstVariant, _SecondVariant] = pydantic.Field(..., discriminator="kind")


class _PlainUnionConfig(pydantic.BaseModel):
    details: typing.Union[_FirstVariant, _SecondVariant, None] = None
    many: typing.List[typing.Union[_FirstVariant, _SecondVariant]] = []


@pytest.mark.parametrize("config_model", [_DiscriminatedUnionConfig, _PlainUnionConfig])
def test_secrets_declared_on_a_later_union_variant_are_masked(config_model):
    data = {"details": {"kind": "second", "pin": "variant-pin", "code": "variant-password", "label": "kept"}}

    out = redact_secrets(data, model=config_model)

    assert out["details"] == {"kind": "second", "pin": REDACTED, "code": REDACTED, "label": "kept"}


def test_secrets_declared_on_a_later_union_variant_are_masked_inside_a_list():
    data = {"many": [{"kind": "first", "label": "a"}, {"kind": "second", "pin": "p", "code": "c", "label": "b"}]}

    out = redact_secrets(data, model=_PlainUnionConfig)

    assert out["many"] == [{"kind": "first", "label": "a"}, {"kind": "second", "pin": REDACTED, "code": REDACTED, "label": "b"}]


def test_redact_config_data_applies_a_later_union_variants_declarations_to_a_configuration_row():
    row = {"action": {"value": "pull"}, "data": {"details": {"kind": "second", "pin": "p", "code": "c", "label": "l"}}}

    out = redact_config_data({"configurations": [row]}, config_models={"pull": _DiscriminatedUnionConfig})

    assert out["configurations"][0]["data"]["details"] == {"kind": "second", "pin": REDACTED, "code": REDACTED, "label": "l"}


# --- Third review on PR #120: nested containers and scalar unions ---

class _NestedContainersConfig(pydantic.BaseModel):
    groups: typing.List[typing.Dict[str, _NestedDetails]] = []
    by_site: typing.Dict[str, typing.List[_NestedDetails]] = {}
    matrix: typing.List[typing.List[_NestedDetails]] = []
    either: typing.Union[typing.Dict[str, _NestedDetails], typing.List[_NestedDetails], None] = None


def test_model_declarations_survive_a_list_of_mappings_and_other_nested_containers_in_raw_data():
    data = {
        "groups": [{"main": dict(_NESTED_RAW)}],
        "by_site": {"site-a": [dict(_NESTED_RAW)]},
        "matrix": [[dict(_NESTED_RAW)]],
        "either": {"x": dict(_NESTED_RAW)},
    }

    out = redact_secrets(data, model=_NestedContainersConfig)

    assert out == {
        "groups": [{"main": _NESTED_MASKED}],
        "by_site": {"site-a": [_NESTED_MASKED]},
        "matrix": [[_NESTED_MASKED]],
        "either": {"x": _NESTED_MASKED},
    }


def test_model_declarations_survive_a_list_of_mappings_in_a_parsed_configuration():
    # The decorator path: a parsed config serialized with .dict(). The
    # SecretStr is masked by its type; the password-format str only by the
    # declaration, which has to be found through the containers.
    config = _NestedContainersConfig(groups=[{"main": _NESTED_RAW}], either=[_NESTED_RAW])

    out = redact_secrets(config.dict(), model=type(config))

    assert out["groups"] == [{"main": _NESTED_MASKED}]
    assert out["either"] == [_NESTED_MASKED]


def test_redact_config_data_follows_a_list_of_mappings_in_a_configuration_row():
    row = {"action": {"value": "pull"}, "data": {"groups": [{"main": dict(_NESTED_RAW)}]}}

    out = redact_config_data({"configurations": [row]}, config_models={"pull": _NestedContainersConfig})

    assert out["configurations"][0]["data"]["groups"] == [{"main": _NESTED_MASKED}]


class _ScalarUnionConfig(pydantic.BaseModel):
    code: typing.Union[pydantic.SecretStr, int]
    maybe: typing.Optional[typing.Union[int, pydantic.SecretBytes]] = None
    codes: typing.List[typing.Union[pydantic.SecretStr, int]] = []
    plain: typing.Union[str, int] = ""


def test_secret_field_names_sees_a_secret_type_inside_a_scalar_union():
    assert secret_field_names(_ScalarUnionConfig) == frozenset({"code", "maybe", "codes"})


def test_a_secret_type_inside_a_scalar_union_masks_the_raw_value():
    data = {"code": "sensitive-code", "maybe": "bytes", "codes": ["a", 1], "plain": "kept"}

    out = redact_secrets(data, model=_ScalarUnionConfig)

    assert out == {"code": REDACTED, "maybe": REDACTED, "codes": [REDACTED, REDACTED], "plain": "kept"}


# --- Request and response bodies attached to failure events ---

from app.services.redaction import redact_body, redact_text, redact_url  # noqa: E402


def test_redact_body_masks_secrets_in_a_form_encoded_token_post():
    # The password-grant body httpx sends to an OAuth token endpoint, which
    # _handle_error attaches as request_data when that POST fails.
    body = b"grant_type=password&client_id=api-telemetry&username=me%40example.com&password=s3cr3t%21"

    assert redact_body(body) == (
        "grant_type=password&client_id=api-telemetry&username=me%40example.com&password=**********"
    )


def test_redact_body_masks_secrets_in_a_json_body_at_any_depth():
    body = b'{"username": "u", "password": "p", "auth": {"api_key": "k", "site": "s"}}'

    assert redact_body(body) == '{"username": "u", "password": "**********", "auth": {"api_key": "**********", "site": "s"}}'


def test_redact_body_masks_a_token_echoed_in_a_json_response():
    assert redact_body('{"access_token": "abc", "expires_in": 300}') == '{"access_token": "**********", "expires_in": 300}'


def test_redact_body_leaves_a_body_without_secrets_as_decoded_text():
    assert redact_body(b'{"start_time": "2024-01-10T05:30:00-00:00"}') == '{"start_time": "2024-01-10T05:30:00-00:00"}'
    assert redact_body("<html><body>403 Forbidden</body></html>") == "<html><body>403 Forbidden</body></html>"
    assert redact_body(b"") == ""
    assert redact_body(None) == ""


def test_redact_body_replaces_an_unparseable_body_that_names_a_secret():
    # Neither JSON nor a form (a SOAP login, say): the keys cannot be walked,
    # so the whole body goes rather than guess where the value ends.
    assert redact_body(b"<login><user>u</user><password>p</password></login>") == REDACTED


def test_redact_body_does_not_take_xml_holding_an_equals_sign_for_a_form():
    # Without whitespace and with one "=", this once matched the form shape,
    # and per-field masking then folded the password into a "field name"
    # that was kept in clear.
    body = b"<login><password>hunter2</password><note>a=b</note></login>"

    assert redact_body(body) == REDACTED
    assert "hunter2" not in redact_body(body)


def test_redact_body_reserializes_a_json_object_that_repeats_a_key():
    # json.loads keeps the last value, so the walk sees an empty password
    # and masks nothing; the original text still holds the first one.
    assert redact_body(b'{"password":"hunter2","password":""}') == '{"password": ""}'
    assert redact_body(b'{"auth":{"token":"tok-1","token":""},"a":1}') == '{"auth": {"token": ""}, "a": 1}'
    assert redact_body(b'[{"secret":"s","secret":""}]') == '[{"secret": ""}]'


def test_redact_body_tolerates_bytes_that_are_not_utf8():
    assert redact_body(b"\xff\xfe plain") == "�� plain"


def test_redact_body_masks_secrets_in_a_dict_or_list_body():
    assert redact_body({"password": "p", "a": 1}) == '{"password": "**********", "a": 1}'


def test_redact_url_masks_secret_query_parameters_and_keeps_the_rest():
    assert redact_url("https://api.example.com/v1/items?api_key=k123&page=2&token=t") == (
        "https://api.example.com/v1/items?api_key=**********&page=2&token=**********"
    )
    assert redact_url("https://api.example.com/v1/items") == "https://api.example.com/v1/items"
    assert redact_url("https://api.example.com/v1/items?page=2") == "https://api.example.com/v1/items?page=2"


def test_redact_text_masks_secret_query_parameters_quoted_in_free_text():
    # What httpx's raise_for_status() puts in the exception message, and
    # what the traceback repeats.
    text = (
        "Client error '403 Forbidden' for url 'https://api.example.com/v1/items?api_key=k-999&page=2'\n"
        "For more information check: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/403"
    )

    assert redact_text(text) == (
        f"Client error '403 Forbidden' for url 'https://api.example.com/v1/items?api_key={REDACTED}&page=2'\n"
        "For more information check: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/403"
    )


def test_redact_text_masks_every_secret_pair_and_keeps_the_rest():
    assert redact_text("token=t1 then password=p%21 but page=2 and user=u") == (
        f"token={REDACTED} then password={REDACTED} but page=2 and user=u"
    )
    assert redact_text("https://x.test/a?token=t\"https://y.test/b?secret=s") == (
        f"https://x.test/a?token={REDACTED}\"https://y.test/b?secret={REDACTED}"
    )


def test_redact_text_leaves_text_without_secret_pairs_as_is():
    assert redact_text("ConnectError: [Errno 61] Connection refused") == "ConnectError: [Errno 61] Connection refused"
    assert redact_text("https://api.example.com/v1/items?page=2") == "https://api.example.com/v1/items?page=2"
    assert redact_text("    response = client.get(url, params=params)") == "    response = client.get(url, params=params)"
    assert redact_text("") == ""
    assert redact_text(None) == ""


def test_redact_text_masks_the_url_as_redact_url_does_from_a_real_status_error():
    # Encoded parameter names and apostrophes in values are legal in a URL
    # and httpx keeps them; the free-text pass must parse the query like
    # redact_url does, or the structured request_url and the error text
    # disagree on what is secret.
    import httpx
    url = "https://api.example.com/v1/items?api%5Fkey=k-999&%74oken=alpha'beta&page=2"
    request = httpx.Request("GET", url)
    try:
        httpx.Response(403, request=request).raise_for_status()
    except httpx.HTTPStatusError as e:
        error = e
    assert "k-999" in str(error) and "alpha'beta" in str(error)

    text = redact_text(str(error))

    for secret in ("k-999", "alpha", "beta"):
        assert secret not in text
    assert text.startswith(
        f"Client error '403 Forbidden' for url 'https://api.example.com/v1/items?api_key={REDACTED}&token={REDACTED}&page=2'\n"
    )
    assert redact_text(f"for url '{url}'") == f"for url '{redact_url(url)}'"


def test_redact_text_masks_encoded_names_and_apostrophes_outside_a_url_too():
    assert redact_text("%74oken=abc api%5Fkey=k page=2") == f"%74oken={REDACTED} api%5Fkey={REDACTED} page=2"
    assert redact_text("token='alpha'beta' then page=2") == f"token={REDACTED} then page=2"
    assert redact_text('for url "https://x.test/a?token=t&page=2" (HTTP 401)') == (
        f'for url "https://x.test/a?token={REDACTED}&page=2" (HTTP 401)'
    )
    # A URL as the value of an innocent pair is still parsed as a URL.
    assert redact_text("url=https://x.test/a?token=t&page=2 next") == f"url=https://x.test/a?token={REDACTED}&page=2 next"
