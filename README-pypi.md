# gundi-action-runner

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

> The current release is a release candidate — until the first stable release,
> pin it exactly: `pip install "gundi-action-runner[cli]==0.1.0rc3"`
> (avoid `--pre`, which cascades pre-releases to all dependencies).

## What you get

- **`gundi-runner new`** — scaffold a complete connector project interactively
  (copier-backed; generated projects can pull template updates later)
- **`gundi-runner run` / `register` / `add-action`** — local dev server,
  Gundi registration, and handler codegen
- **Pytest fixtures** — installing the package registers a pytest plugin with
  ready-made Gundi mocks (`integration_v2`, `mock_gundi_client_v2`, ...)
- **Local dev stack** — scaffolds include a docker-compose environment
  (connector + redis + Pub/Sub emulator with sub-action loopback)
- **Two auth modes** — service client credentials, or your own Gundi login
  via the OAuth2 password grant for local development

## Documentation

- Source, quickstart, and guides:
  https://github.com/PADAS/gundi-integration-action-runner
- Docs site:
  https://padas.github.io/gundi-integration-action-runner/

Apache-2.0. Maintained by the [Gundi](https://gundiservice.org) team at
EarthRanger.
