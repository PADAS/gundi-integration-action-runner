# Load the runner's settings before anything else in the package. app.actions
# executes the connector's handlers module at import, and app.settings installs
# the shared token cache URL into gundi-client-v2's settings, so a GundiClient()
# built at module scope in connector code only picks it up if this runs first.
from app import settings  # noqa: F401
