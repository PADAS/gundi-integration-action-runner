import asyncio

from app.services.action_runner import _portal
from app.services.self_registration import register_integration_in_gundi


# Main
if __name__ == "__main__":
    # Register the integration in Gundi
    asyncio.run(register_integration_in_gundi(gundi_client=_portal))
