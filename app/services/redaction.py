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
  The model is walked alongside the data, so a declaration on a nested
  model (direct, ``Optional``, in a ``List`` or a ``Dict`` value) counts
  too, and a field is matched by its name or its alias: the portal saves a
  configuration under the alias, ``.dict()`` serializes it under the name.
  For a ``Union`` of models every variant's declarations apply: masking by
  a variant the data did not select costs at most a readable value, while
  guessing the variant wrong would leak one.

Unset secrets (``None`` or ``""``) stay as they are: "nothing configured" is
what an operator debugging a failed auth needs to see, and discloses nothing.
"""
from typing import Any, Dict, FrozenSet, Iterable, List, Mapping, Optional, Sequence, Tuple, Type, Union

import pydantic
from pydantic.fields import ModelField

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

_SECRET_TYPES = (pydantic.SecretStr, pydantic.SecretBytes)


def _is_sensitive_key(key: Any) -> bool:
    normalized = str(key).lower().replace("-", "_")
    return any(fragment in normalized for fragment in SENSITIVE_KEY_FRAGMENTS)


def _is_set(value: Any) -> bool:
    if isinstance(value, _SECRET_TYPES):
        return bool(value.get_secret_value())
    return value is not None and value != ""


Models = Union[None, Type[pydantic.BaseModel], Sequence[Type[pydantic.BaseModel]]]


def redact_secrets(value: Any, *, model: Models = None, _inherited: bool = False) -> Any:
    """Return a copy of ``value`` with every secret replaced by ``REDACTED``.

    Walks dicts and lists. A dict entry is secret when its key is sensitive
    by name, or when ``model`` (the pydantic model this level of the data was
    saved from; several when the data may be any of a union's variants)
    declares the field as secret. A secret leaf becomes ``REDACTED``; a
    secret container (a stored OAuth token dict, say) keeps its structure
    and every leaf beneath it is masked, whatever the inner keys are called.
    Nested model declarations are followed as the walk descends. Non-empty
    values only; see the module docstring.
    """
    if isinstance(value, _SECRET_TYPES):
        # A plain string, so the event does not depend on how its serializer
        # prints a SecretStr. An empty one stays empty, like an unset key.
        return REDACTED if _is_set(value) else value.get_secret_value()
    if isinstance(value, dict):
        fields = _fields_by_key(_as_models(model))
        redacted = {}
        for key, item in value.items():
            declared = fields.get(key, [])
            secret = _inherited or _is_sensitive_key(key) or any(_is_secret_field(f) for f in declared)
            nested = _nested_models(declared)
            if secret and not isinstance(item, (dict, list)):
                redacted[key] = REDACTED if _is_set(item) else item
            elif any(f.key_field is not None for f in declared) and isinstance(item, dict):
                # A Dict[str, ...] field: the keys are data, so they get the
                # same name check as any other key; the values are instances
                # of the field's model(s), if it has any.
                redacted[key] = {
                    k: redact_secrets(v, model=nested, _inherited=secret or _is_sensitive_key(k))
                    for k, v in item.items()
                }
            else:
                redacted[key] = redact_secrets(item, model=nested, _inherited=secret)
        return redacted
    if isinstance(value, list):
        return [redact_secrets(item, model=model, _inherited=_inherited) for item in value]
    if _inherited:
        return REDACTED if _is_set(value) else value
    return value


def secret_field_names(config_model: Models) -> FrozenSet[str]:
    """The keys under which a connector's config model stores a secret.

    Each secret field contributes its name and its alias (the portal saves
    under the alias). Reads three declarations: a ``SecretStr``/``SecretBytes``
    annotation, ``Field(..., format="password")`` (the JSON-schema hint the
    portal renders as a password input), and ``FieldWithUIOptions(ui_options=
    UIOptions(widget="password"))``. Root level only; ``redact_secrets``
    follows nested models itself. Empty for anything that is not a model.
    """
    return frozenset(
        key for key, fields in _fields_by_key(_as_models(config_model)).items()
        if any(_is_secret_field(f) for f in fields)
    )


def _as_models(model: Models) -> Tuple[Type[pydantic.BaseModel], ...]:
    if model is None:
        return ()
    if isinstance(model, type):
        return (model,) if issubclass(model, pydantic.BaseModel) else ()
    return tuple(m for m in model if isinstance(m, type) and issubclass(m, pydantic.BaseModel))


def _fields_by_key(models: Iterable[Type[pydantic.BaseModel]]) -> Dict[str, List[ModelField]]:
    """Every field of ``models`` reachable under a key, by name and alias.
    Several when the models are union variants sharing a key."""
    by_key: Dict[str, List[ModelField]] = {}
    for model in models:
        fields = getattr(model, "__fields__", None)
        if not isinstance(fields, dict):
            continue
        for name, field in fields.items():
            by_key.setdefault(name, []).append(field)
            if field.alias and field.alias != name:
                by_key.setdefault(field.alias, []).append(field)
    return by_key


def _is_secret_field(field: ModelField) -> bool:
    if isinstance(field.type_, type) and issubclass(field.type_, _SECRET_TYPES):
        return True
    field_info = field.field_info
    if (getattr(field_info, "extra", None) or {}).get("format") == "password":
        return True
    ui_options = getattr(field_info, "ui_options", None)
    return getattr(ui_options, "widget", None) == "password"


def _nested_models(fields: Iterable[ModelField]) -> Tuple[Type[pydantic.BaseModel], ...]:
    """The pydantic models the values of ``fields`` may be instances of.

    pydantic v1 unwraps Optional, List and Dict to the inner type in
    ``type_``. A Union is left as is, with one ``sub_fields`` entry per
    member, and a List or Dict of a Union nests those one level deeper, so
    the sub-field tree is walked. Every variant is returned.
    """
    found: List[Type[pydantic.BaseModel]] = []

    def visit(field: ModelField) -> None:
        candidate = field.type_
        if isinstance(candidate, type) and issubclass(candidate, pydantic.BaseModel) and candidate not in found:
            found.append(candidate)
        for sub in field.sub_fields or []:
            visit(sub)

    for field in fields:
        visit(field)
    return tuple(found)


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
    return redact_secrets(config_data, model=config_models.get(action_id))


def _redact_configuration_row(row: Any, config_models: Mapping[str, Type[pydantic.BaseModel]]) -> Any:
    if not isinstance(row, dict):
        return row
    action = row.get("action")
    action_value = action.get("value") if isinstance(action, dict) else None
    redacted = redact_secrets(row)
    if isinstance(row.get("data"), dict):
        redacted["data"] = redact_secrets(row["data"], model=config_models.get(action_value) if action_value else None)
    return redacted
