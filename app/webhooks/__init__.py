"""Deprecated compatibility package — webhook base classes moved to
gundi_action_runner.webhooks. This package remains so forks keep their
app/webhooks/handlers.py and app/webhooks/configurations.py files."""
import warnings

from gundi_action_runner.webhooks import *  # noqa: F401,F403

warnings.warn(
    "'app.webhooks' is deprecated; import 'gundi_action_runner.webhooks' instead.",
    DeprecationWarning,
    stacklevel=2,
)
