"""Deprecated compatibility shim — this module moved to gundi_action_runner.services.state."""
import importlib
import sys
import warnings

warnings.warn(
    "'app.services.state' is deprecated; import 'gundi_action_runner.services.state' instead.",
    DeprecationWarning,
    stacklevel=2,
)
sys.modules[__name__] = importlib.import_module("gundi_action_runner.services.state")
