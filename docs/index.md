# Gundi Action Runner

A framework for building [Gundi](https://gundiservice.org) integration
connectors in Python. Register handlers with decorators, and the framework
provides the FastAPI service, scheduling, state, activity logging, portal
schemas, and delivery to Gundi.

```python
from gundi_action_runner import action, create_app

@action.pull(config=PullObservationsConfig, title="Pull Observations")
async def pull_observations(integration, action_config):
    ...

app = create_app(handlers_modules=["myconnector.handlers"])
```

## Install

```bash
pip install "gundi-action-runner[cli]"
```

## Where to go

- **[Quickstart](quickstart.md)** — scaffold, run, and register a connector
  with `gundi-runner`.
- **[Extension API](extension-api.md)** — the `@action.*` / `@webhook`
  decorators, `create_app()`, framework services, and testing fixtures.
- **[Architecture](architecture.md)** — how a connector fits into the Gundi
  platform, and the contracts it lives by:
  [registration](registration-contract.md),
  [runtime](runtime-contracts.md) (scheduling, execute proxy, webhooks,
  config events, data path), and
  [portal rendering](portal-rendering.md).
- **[Migrating a Fork](fork-migration.md)** — moving an existing
  template fork onto the library, step by step.

Maintainers: releases are cut per
[RELEASING.md](https://github.com/PADAS/gundi-integration-action-runner/blob/main/RELEASING.md);
source lives on
[GitHub](https://github.com/PADAS/gundi-integration-action-runner).
