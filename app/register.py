import asyncio
import sys
from app.services.action_runner import _portal
from app.services.self_registration import register_integration_in_gundi


# Main
if __name__ == "__main__":
    # Read service_url from args (optional)
    service_url = sys.argv[1] if len(sys.argv) > 1 else None
    # Register the integration in Gundi
    asyncio.run(register_integration_in_gundi(gundi_client=_portal, service_url=service_url))
