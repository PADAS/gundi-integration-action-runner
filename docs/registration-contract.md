# Registration contract

Registration is the single moment a connector tells Gundi what it is:
one idempotent `POST /v2/integrations/types/` carrying the type, every
action (with its JSON Schema and ui schema), crontab defaults, and the
optional webhook block. Everything the portal renders and everything the
scheduler fires derives from this payload.

## Who calls it, and when

- `gundi-runner register --slug my_connector --name "My Connector" …` — the
  normal path (see the [quickstart](quickstart.md#register-in-gundi)).
- `REGISTER_ON_START=true` — the FastAPI lifespan self-registers on boot.
  Useful for first deploys; turn it off afterwards.
- Legacy template forks: `python -m app.register`.

All three build the same payload (`services/self_registration.py`) and send
it through `gundi-client`'s `register_integration_type()` with retry
(3 attempts on HTTP errors).

!!! note "Credentials"
    The endpoint's write path effectively requires a **superuser** service
    account — org-scoped roles can read integration types but not register
    them. Registration failing with 403 means your credential lacks that
    role, not that the payload is wrong.

## The payload

```jsonc
{
  "name": "My Connector",
  "value": "my_connector",              // slug: ^[a-z0-9_]+$, unique
  "description": "Default type for integrations with My Connector",
  "service_url": "https://my-connector-…run.app",   // omitted unless set
  "actions": [
    {
      "type": "auth",                    // auth | pull | push | generic
      "name": "Auth",                    // decorator title=, else Title Case of id
      "value": "auth",                   // the action id
      "description": "My Connector Auth action",
      "schema": { "...": "Model.schema_json()", "is_executable": true },
      "ui_schema": { "...": "Model.ui_schema()" },
      "is_periodic_action": false
    },
    {
      "type": "pull",
      "value": "pull_observations",
      "schema": { "...": "..." },
      "ui_schema": { "...": "..." },
      "is_periodic_action": true,        // always true for pull configs
      "crontab_schedule": {              // only for pull actions
        "minute": "0", "hour": "*/4",
        "day_of_week": "*", "day_of_month": "*", "month_of_year": "*",
        "tz_offset": 0
      }
    }
  ],
  "webhook": {                           // only if a @webhook handler exists
    "name": "My Connector Webhook",
    "value": "my_connector_webhook",
    "description": "Webhook Integration with My Connector",
    "schema": { "...": "..." },
    "ui_schema": { "...": "..." }
  }
}
```

How each field is derived on the runner side:

| Field | Source |
|---|---|
| `actions[].type` | Which base the config model subclasses (`AuthActionConfiguration` → `auth`, etc.) |
| `actions[].value` | Action id: function name or decorator `id=` |
| `actions[].name` | Decorator `title=`, else `value.replace("_", " ").title()` |
| `actions[].schema` | `config_model.schema_json()` — Pydantic validation constraints included |
| `actions[].schema.is_executable` | Added when the config model mixes in `ExecutableActionMixin` (nested in the schema, not a sibling field) |
| `actions[].ui_schema` | `config_model.ui_schema()` — see [portal rendering](portal-rendering.md) |
| `actions[].is_periodic_action` | `True` iff the config subclasses `PullActionConfiguration` |
| `actions[].crontab_schedule` | CLI `--schedule "action_id:MIN HOUR DOM MON DOW [TZ]"` wins over the `@crontab_schedule` decorator; omitted if neither |
| `webhook` | Introspected from the registered `@webhook` handler's config model; silently omitted when there's no handler |

Actions whose config subclasses `InternalActionConfiguration` are **not
registered** — they stay invisible to the platform (used for runner-internal
sub-actions).

## What the platform does with it

On the Gundi API side (`cdip`), the create serializer is **idempotent**:
`IntegrationType` is `update_or_create`d by `value`, and each action by
`(integration_type, value)`. Re-registering with changed schemas updates the
stored rows in place — that's how schema changes ship.

Stored rows:

| Model | Fields |
|---|---|
| `IntegrationType` | `name`, `value` (unique slug), `description`, `service_url`, `help_center_url` |
| `IntegrationAction` | `type`, `name`, `value`, `description`, `schema`, `ui_schema`, `is_periodic_action`, `crontab_schedule` (FK to `django_celery_beat.CrontabSchedule`) |
| `IntegrationWebhook` | `name`, `value`, `description`, `schema`, `ui_schema` — **one per type** (OneToOne) |

Notable server-side behavior:

- **Crontab conversion:** the `crontab_schedule` dict becomes a
  `django_celery_beat.CrontabSchedule` row; `tz_offset` (hours) maps to an
  `Etc/GMT±N` timezone.
- **Configuration backfill:** adding a *new* action to an existing type
  triggers an async backfill creating empty `IntegrationConfiguration` rows
  for every existing integration of that type.
- **Kong webhook route:** the *first* registration that creates a webhook
  also registers `integration_type → service_url` with Kong, which is what
  makes `https://hooks.gundiservice.org/webhooks` reach your runner. Pass
  `register_webhook_in_kong: false` to skip (e.g. for local/test registries).
- **`service_url` is load-bearing:** the manual execute proxy and Kong
  routing both resolve your runner by it. A type registered without one
  cannot serve portal-triggered executes.

## Action types

| `type` | Config base | Triggered by | Notes |
|---|---|---|---|
| `auth` | `AuthActionConfiguration` | Portal Test button (usually `ExecutableActionMixin`) | Validates credentials |
| `pull` | `PullActionConfiguration` | Scheduler (Pub/Sub) + manual execute | Always `is_periodic_action: true` |
| `push` | `PushActionConfiguration` | Platform push-data Pub/Sub | Routed by the `data` model's type |
| `generic` | `GenericActionConfiguration` | Manual execute / `trigger_action()` | Utility actions |
| `reference` | `ReferenceActionConfiguration` | Portal reference dropdowns | **Not yet merged** platform-side (branch `feature/reference-action-type`, as of 2026-08-11); see [portal rendering](portal-rendering.md#live-reference-data-gundireference) |

## Ground rules

- **Slugs are `^[a-z0-9_]+$`** — type `value` and action `value` both;
  anything else is rejected server-side.
- **Registration is the only schema-sync mechanism.** Editing config models
  does nothing until you re-register. Saved operator configs are *not*
  migrated — keep schema changes backward compatible (new fields get
  defaults).
- **Don't rename action ids casually.** The id (`value`) keys saved
  configurations, scheduler tasks, and execute calls; renaming orphans all
  three.
