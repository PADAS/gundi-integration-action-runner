"""Deprecated compatibility shim — this module moved to gundi_action_runner.routers.config_events."""
import importlib
import sys
import warnings

warnings.warn(
    "'app.routers.config_events' is deprecated; import 'gundi_action_runner.routers.config_events' instead.",
    DeprecationWarning,
    stacklevel=2,
)
sys.modules[__name__] = importlib.import_module("gundi_action_runner.routers.config_events")
