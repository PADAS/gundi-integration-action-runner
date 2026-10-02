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
  Containers of any nesting (``List[Dict[str, Model]]``, ...) are followed
  level by level. For a ``Union`` every variant's declarations apply, a
  ``Union[SecretStr, int]`` included: masking by a variant the data did not
  select costs at most a readable value, guessing wrong would leak one.

Unset secrets (``None`` or ``""``) stay as they are: "nothing configured" is
what an operator debugging a failed auth needs to see, and discloses nothing.

A failure event also carries the failed HTTP request and response
(``request_url``, ``request_data``, ``server_response_body``). A connector's
password-grant token POST that the provider rejects puts the password in
``request_data``, a key in the URL query, and a token in the response:
``redact_body`` and ``redact_url`` mask those by the same key rules.
"""
import json
import re
from typing import Any, Dict, FrozenSet, Iterable, List, Mapping, Optional, Sequence, Tuple, Type, Union
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import pydantic
from pydantic.fields import ModelField, SHAPE_SINGLETON

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


def redact_secrets(value: Any, *, model: Models = None) -> Any:
    """Return a copy of ``value`` with every secret replaced by ``REDACTED``.

    Walks dicts and lists. A dict entry is secret when its key is sensitive
    by name, or when ``model`` (the pydantic model this level of the data was
    saved from; several when the data may be any of a union's variants)
    declares the field as secret. A secret leaf becomes ``REDACTED``; a
    secret container (a stored OAuth token dict, say) keeps its structure
    and every leaf beneath it is masked, whatever the inner keys are called.
    The model's field metadata is followed level by level as the walk
    descends, through nested models and containers alike. Non-empty values
    only; see the module docstring.
    """
    return _walk(value, specs=(), models=_as_models(model), inherited=False)


def _walk(value: Any, *, specs: Sequence[ModelField], models: Sequence[Type[pydantic.BaseModel]], inherited: bool) -> Any:
    """``specs`` are the fields whose value this may be (several under a
    union), ``models`` the model types it may be an instance of; both may be
    empty, leaving the key-name check. ``inherited`` is set below a secret
    container and masks every leaf."""
    specs = _expand_unions(specs)
    if isinstance(value, _SECRET_TYPES):
        # A plain string, so the event does not depend on how its serializer
        # prints a SecretStr. An empty one stays empty, like an unset key.
        return REDACTED if _is_set(value) else value.get_secret_value()
    if isinstance(value, dict):
        # The dict is an instance of one of these models...
        instance_of = list(models) + [
            s.type_ for s in specs
            if s.shape == SHAPE_SINGLETON and isinstance(s.type_, type) and issubclass(s.type_, pydantic.BaseModel)
        ]
        # ...or a mapping whose values these sub-fields describe; a key gets
        # both readings, since under a union it may be either.
        mapping_values = [sub for s in specs if s.key_field is not None for sub in (s.sub_fields or [])]
        fields_by_key = _fields_by_key(instance_of)
        redacted = {}
        for key, item in value.items():
            child = fields_by_key.get(key, []) + mapping_values
            secret = inherited or _is_sensitive_key(key) or any(_is_secret_field(c) for c in child)
            if secret and not isinstance(item, (dict, list)):
                redacted[key] = REDACTED if _is_set(item) else item
            else:
                redacted[key] = _walk(item, specs=child, models=(), inherited=secret)
        return redacted
    if isinstance(value, list):
        elements = [sub for s in specs if s.key_field is None and s.shape != SHAPE_SINGLETON for sub in (s.sub_fields or [])]
        return [_walk(item, specs=elements, models=models, inherited=inherited) for item in value]
    if inherited:
        return REDACTED if _is_set(value) else value
    return value


def secret_field_names(config_model: Models) -> FrozenSet[str]:
    """The keys under which a connector's config model stores a secret.

    Each secret field contributes its name and its alias (the portal saves
    under the alias). Reads three declarations: a ``SecretStr``/``SecretBytes``
    annotation (directly, in a container or in a union), ``Field(...,
    format="password")`` (the JSON-schema hint the portal renders as a
    password input), and ``FieldWithUIOptions(ui_options=UIOptions(widget=
    "password"))``. Root level only; ``redact_secrets`` follows nested models
    itself. Empty for anything that is not a model.
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


def _expand_unions(specs: Sequence[ModelField]) -> List[ModelField]:
    """Replace a field whose single value may be any of several types (a
    Union: singleton shape with one sub-field per variant) by those variants,
    so each level of the walk sees concrete shapes. Container fields keep
    their sub-fields, which describe their elements, not alternatives."""
    expanded: List[ModelField] = []
    for spec in specs:
        if spec.shape == SHAPE_SINGLETON and spec.sub_fields:
            expanded.extend(_expand_unions(spec.sub_fields))
        else:
            expanded.append(spec)
    return expanded


def _is_secret_field(field: ModelField) -> bool:
    """Whether a field's value is a secret: its own type, its schema hint or
    widget say so, or a type anywhere inside its containers or union does
    (``List[SecretStr]``, ``Union[SecretStr, int]``), in which case the whole
    value is masked."""
    if isinstance(field.type_, type) and issubclass(field.type_, _SECRET_TYPES):
        return True
    field_info = field.field_info
    if (getattr(field_info, "extra", None) or {}).get("format") == "password":
        return True
    ui_options = getattr(field_info, "ui_options", None)
    if getattr(ui_options, "widget", None) == "password":
        return True
    return any(_is_secret_field(sub) for sub in (field.sub_fields or []))


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


_FORM_BODY = re.compile(r"^[^=&\s]+=[^&\s]*(?:&[^=&\s]+=[^&\s]*)*$")


def redact_body(body: Any) -> str:
    """The text of an HTTP request or response body with secrets masked.

    Bytes are decoded as UTF-8 (undecodable bytes replaced). A JSON object
    or array, or a dict/list given directly, is walked by ``redact_secrets``
    and re-serialized when something was masked. A form-encoded body (``grant_type=password&...``) is
    masked per field and re-encoded. Anything else cannot be walked by key,
    so if it so much as names a secret-looking key (a SOAP login with a
    ``<password>`` element, say) the whole body is replaced; otherwise it is
    returned as is. Over-redaction of a plain-text error that merely mentions
    "token" is the accepted cost.
    """
    if body is None:
        return ""
    if isinstance(body, (dict, list)):
        return json.dumps(redact_secrets(body))
    if isinstance(body, (bytes, bytearray)):
        text = bytes(body).decode("utf-8", errors="replace")
    else:
        text = str(body)
    if not text.strip():
        return text
    try:
        parsed = json.loads(text)
    except ValueError:
        parsed = None
    if isinstance(parsed, (dict, list)):
        redacted = redact_secrets(parsed)
        # Re-serialized only when something was masked, so an innocent body
        # is attached exactly as it went over the wire.
        return text if redacted == parsed else json.dumps(redacted)
    if _FORM_BODY.match(text):
        redacted = _redact_query(text)
        return text if redacted == urlencode(parse_qsl(text, keep_blank_values=True), safe="*") else redacted
    if _is_sensitive_key(text):
        return REDACTED
    return text


def redact_url(url: str) -> str:
    """``url`` with secret-looking query parameters masked; untouched when it
    has no query string."""
    parts = urlsplit(str(url))
    if not parts.query:
        return str(url)
    return urlunsplit(parts._replace(query=_redact_query(parts.query)))


def _redact_query(query: str) -> str:
    pairs = parse_qsl(query, keep_blank_values=True)
    return urlencode(
        [(key, REDACTED if _is_sensitive_key(key) and value else value) for key, value in pairs],
        safe="*",
    )
