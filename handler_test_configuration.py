import asyncio

# ---------- remove this ------------
from gundi_core.schemas.v2 import (
    Integration,
    IntegrationAction,
    IntegrationType,
    UUID,
    IntegrationActionConfiguration,
    IntegrationActionSummary,
    ConnectionRoute,
    Organization,
)
from app.actions.handlers import action_pull_events
from app.actions.configurations import PullEventsConfig

if __name__ == "__main__":
    integration = Integration(
        id=UUID("e9c1eef0-7c28-46bb-8155-fe9b31dedce7"),
        name="Test eBird Connection",
        type=IntegrationType(
            id=UUID("cd401782-cf42-4c38-90c9-8248536139af"),
            name="eBird",
            value="ebird",
            description="A type for eBird connections",
            actions=[
            ],
            webhook=None,
        ),
        base_url="https://api.ebird.org/v2",
        enabled=True,
        owner=Organization(
            id=UUID("b56b585d-7f94-4a45-b8af-bb7dc6a9c731"),
            name="EarthRanger Developers",
            description="",
        ),
        configurations=[
            IntegrationActionConfiguration(
                id=UUID("7c0cbf42-ad62-4725-8380-e4cf29acc406"),
                integration=UUID("5ac83294-bda3-43c8-b58e-566024b03543"),
                action=IntegrationActionSummary(
                    id=UUID("1306da74-7e87-45a0-a5de-c11974e4e63e"),
                    type="pull",
                    name="Pull Events",
                    value="pull_events",
                ),
                data={
                    "latitude": 47.5218082,
                    "longitude": -122.3864506,
                    "distance": 30,
                    "num_days": 1
                },
            ),
            IntegrationActionConfiguration(
                id=UUID("91930701-0cf3-4201-a4a5-02b458c460e1"),
                integration=UUID("5ac83294-bda3-43c8-b58e-566024b03543"),
                action=IntegrationActionSummary(
                    id=UUID("e0d2b2de-a277-4f67-89ef-13ef0e07623d"),
                    type="auth",
                    name="Auth",
                    value="auth",
                ),
                data={
                    "api_key": "cvgvv4eimjck",
                },
            ),
        ],
        webhook_configuration=None,
        default_route=ConnectionRoute(
            id=UUID("4a98b8bb-1be7-479a-8814-e1c9843159e3"),
            name="Test eBird Connection - Default Route",
        ),
        additional={},
        status={
            "id": "mockid-b16a-4dbd-ad32-197c58aeef59",
            "is_healthy": True,
            "details": "Last observation has been delivered with success.",
            "observation_delivered_24hrs": 50231,
            "last_observation_delivered_at": "2023-03-31T11:20:00+0200",
        },
    )
    asyncio.run(action_pull_events(integration, integration.configurations[0]))
