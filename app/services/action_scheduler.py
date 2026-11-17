"""Deprecated compatibility shim — this module moved to gundi_action_runner.services.action_scheduler."""
import importlib
import sys
import warnings

warnings.warn(
    "'app.services.action_scheduler' is deprecated; import 'gundi_action_runner.services.action_scheduler' instead.",
    DeprecationWarning,
    stacklevel=2,
)
sys.modules[__name__] = importlib.import_module("gundi_action_runner.services.action_scheduler")
