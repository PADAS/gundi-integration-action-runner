"""Deprecated compatibility package — framework settings moved to
gundi_action_runner.settings. app/settings/integration.py remains the
fork-specific settings extension point."""
import warnings

from gundi_action_runner.settings import *  # noqa: F401,F403
from .integration import *  # noqa: F401,F403

warnings.warn(
    "'app.settings' is deprecated; import 'gundi_action_runner.settings' instead.",
    DeprecationWarning,
    stacklevel=2,
)
