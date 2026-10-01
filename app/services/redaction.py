"""Redaction of secrets in the configuration data attached to activity-log events.

Every event the runner publishes for the portal's activity feed carries a
``config_data`` dict: the action's own configuration on the decorator path,
or the integration's full saved configurations (auth row included) when a
run fails. Those come from the portal as raw dicts, so without this module a
connector's password reaches the activity log in plaintext.

Two signals mark a value as secret, and either one is enough:

- Its key name. Any key whose normalized form (lower case, ``-`` folded to
  ``_``) contains one of ``SENSITIVE_KEY_FRAGMENTS``. Deliberately broad:
  ``token_url`` is redacted too, and that over-redaction is the safe side.
- Its field declaration. The connector's config model marks the field as
  ``SecretStr``/``SecretBytes``, ``format="password"``, or a ``password`` UI
  widget. That catches secrets with innocent names such as ``client_id``.

Unset secrets (``None`` or ``""``) stay as they are: "nothing configured" is
what an operator debugging a failed auth needs to see, and discloses nothing.
"""
from typing import Any, Collection, FrozenSet, Mapping, Optional, Type

import pydantic

REDACTED = "**********"  # the mask pydantic itself uses for SecretStr

SENSITIVE_KEY_FRAGMENTS = (
    "password",
    "passwd",
    "secret",
    "token",
    "api_key",
    "apikey",
    "private_key",
    "authorization",
    "credential",
)


def _is_sensitive_key(key: Any) -> bool:
    normalized = str(key).lower().replace("-", "_")
    return any(fragment in normalized for fragment in SENSITIVE_KEY_FRAGMENTS)


def redact_secrets(value: Any, *, secret_fields: Collection[str] = ()) -> Any:
    """Return a copy of ``value`` with every secret replaced by ``REDACTED``.

    Walks dicts and lists. A dict entry is redacted when its key is sensitive
    by name or listed in ``secret_fields`` (the model-declared secrets for
    this level of the data). Non-empty values only; see the module docstring.
    """
    if isinstance(value, (pydantic.SecretStr, pydantic.SecretBytes)):
        # A plain string, so the event does not depend on how its serializer
        # prints a SecretStr. An empty one stays empty, like an unset key.
        return REDACTED if _is_set(value) else value.get_secret_value()
    if isinstance(value, dict):
        return {
            key: (
                REDACTED
                if (_is_sensitive_key(key) or key in secret_fields) and _is_set(item)
                else redact_secrets(item)
            )
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [redact_secrets(item) for item in value]
    return value


def _is_set(value: Any) -> bool:
    if isinstance(value, (pydantic.SecretStr, pydantic.SecretBytes)):
        return bool(value.get_secret_value())
    return value is not None and value != ""


def secret_field_names(config_model: Optional[Type[pydantic.BaseModel]]) -> FrozenSet[str]:
    """The fields a connector's config model declares as secret.

    Reads three declarations: a ``SecretStr``/``SecretBytes`` annotation,
    ``Field(..., format="password")`` (the JSON-schema hint the portal renders
    as a password input), and ``FieldWithUIOptions(ui_options=UIOptions(
    widget="password"))``. Returns an empty set for anything that is not a
    pydantic model.
    """
    fields = getattr(config_model, "__fields__", None)
    if not isinstance(fields, dict):
        return frozenset()
    names = set()
    for name, field in fields.items():
        field_type = getattr(field, "type_", None)
        if isinstance(field_type, type) and issubclass(field_type, (pydantic.SecretStr, pydantic.SecretBytes)):
            names.add(name)
            continue
        field_info = getattr(field, "field_info", None)
        if getattr(field_info, "extra", {}).get("format") == "password":
            names.add(name)
            continue
        ui_options = getattr(field_info, "ui_options", None)
        if getattr(ui_options, "widget", None) == "password":
            names.add(name)
    return frozenset(names)


def redact_config_data(
        config_data: Optional[dict], *, config_models: Mapping[str, Type[pydantic.BaseModel]],
        action_id: Optional[str] = None,
) -> dict:
    """Redact the ``config_data`` the runner attaches to an activity event.

    Handles the two shapes the runner produces. ``{"configurations": [...]}``
    holds the integration's serialized ``IntegrationActionConfiguration``
    rows: each row's ``data`` is redacted with the model registered for its
    ``action.value``. Anything else is one action's own configuration, redacted
    with the model registered for ``action_id``. ``config_models`` maps an
    action id to its config model (``{id: config_model for id, (_, config_model, _)
    in action_handlers.items()}``); an action with no model still gets the
    key-name redaction.
    """
    if not config_data:
        return {}
    rows = config_data.get("configurations")
    if isinstance(rows, list):
        redacted = redact_secrets(config_data)
        redacted["configurations"] = [_redact_configuration_row(row, config_models) for row in rows]
        return redacted
    return redact_secrets(config_data, secret_fields=secret_field_names(config_models.get(action_id)))


def _redact_configuration_row(row: Any, config_models: Mapping[str, Type[pydantic.BaseModel]]) -> Any:
    if not isinstance(row, dict):
        return row
    action = row.get("action")
    action_value = action.get("value") if isinstance(action, dict) else None
    secret_fields = secret_field_names(config_models.get(action_value)) if action_value else frozenset()
    redacted = redact_secrets(row)
    if isinstance(row.get("data"), dict):
        redacted["data"] = redact_secrets(row["data"], secret_fields=secret_fields)
    return redacted
