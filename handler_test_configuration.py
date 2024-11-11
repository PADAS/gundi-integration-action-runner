import asyncio
import os

from app.actions.core import discover_actions
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
        id=UUID(""),
        name="Test iNat Integration",
        type=IntegrationType(
            id=UUID(""),
            name="iNaturalist",
            value="intaturalist",
            description="A type for iNat connections",
            actions=[
            ],
            webhook=None,
        ),
        base_url="",
        enabled=True,
        owner=Organization(
            id=UUID("b56b585d-7f94-4a45-b8af-bb7dc6a9c731"),
            name="EarthRanger Developers",
            description="",
        ),
        configurations=[
            IntegrationActionConfiguration(
                id=UUID("GUID"),
                integration=UUID("GUID1"),
                action=IntegrationActionSummary(
                    id=UUID("GUID"),
                    type="pull",
                    name="Pull Events",
                    value="pull_events",
                ),
                data={
                },
            ),
        ],
        webhook_configuration=None,
        default_route=ConnectionRoute(
            id=UUID("GUID"),
            name="Test iNat Connection - Default Route",
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

    pull_action_config = integration.configurations[0]
    pull_action_config = PullEventsConfig(**pull_action_config.data)
    asyncio.run(action_pull_events(integration=integration, action_config=pull_action_config))
