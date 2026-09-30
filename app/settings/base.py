"""Deprecated compatibility shim — this module moved to gundi_action_runner.settings."""
import importlib
import sys
import warnings

warnings.warn(
    "'app.settings.base' is deprecated; import 'gundi_action_runner.settings' instead.",
    DeprecationWarning,
    stacklevel=2,
)
sys.modules[__name__] = importlib.import_module("gundi_action_runner.settings")
