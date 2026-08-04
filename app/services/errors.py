"""Deprecated compatibility shim — this module moved to gundi_action_runner.services.errors."""
import importlib
import sys
import warnings

warnings.warn(
    "'app.services.errors' is deprecated; import 'gundi_action_runner.services.errors' instead.",
    DeprecationWarning,
    stacklevel=2,
)
sys.modules[__name__] = importlib.import_module("gundi_action_runner.services.errors")
