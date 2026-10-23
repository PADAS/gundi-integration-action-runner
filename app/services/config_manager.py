"""Deprecated compatibility shim — this module moved to gundi_action_runner.services.config_manager."""
import importlib
import sys
import warnings

warnings.warn(
    "'app.services.config_manager' is deprecated; import 'gundi_action_runner.services.config_manager' instead.",
    DeprecationWarning,
    stacklevel=2,
)
sys.modules[__name__] = importlib.import_module("gundi_action_runner.services.config_manager")
