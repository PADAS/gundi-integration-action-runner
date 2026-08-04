"""Deprecated compatibility shim — this module moved to gundi_action_runner.services.config_events_consumer."""
import importlib
import sys
import warnings

warnings.warn(
    "'app.services.config_events_consumer' is deprecated; import 'gundi_action_runner.services.config_events_consumer' instead.",
    DeprecationWarning,
    stacklevel=2,
)
sys.modules[__name__] = importlib.import_module("gundi_action_runner.services.config_events_consumer")
