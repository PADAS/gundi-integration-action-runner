from .base import *

# Add your integration-specific settings here
INTEGRATION_TYPE_SLUG = env.str("INTEGRATION_TYPE_SLUG", None)  # Define a string id here e.g. "my_tracker"
