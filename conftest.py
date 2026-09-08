# Root conftest: runs before pytest imports anything under app/, so an
# environment default set here is in place before app.settings loads.
import os

# Tests share Gundi OAuth tokens within the process only. The runner's default
# is redis://<REDIS_HOST>:<REDIS_PORT>/2, and a developer with a local Redis up
# would otherwise have a token minted by one pytest run persisted and served to
# the next, while CI (no Redis) sees none of it. An explicit value in the
# environment is respected.
os.environ.setdefault("GUNDI_TOKEN_CACHE_URL", "")
