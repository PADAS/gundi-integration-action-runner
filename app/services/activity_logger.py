"""Deprecated compatibility shim — this module moved to gundi_action_runner.services.activity_logger."""
import importlib
import sys
import warnings

warnings.warn(
    "'app.services.activity_logger' is deprecated; import 'gundi_action_runner.services.activity_logger' instead.",
    DeprecationWarning,
    stacklevel=2,
)
sys.modules[__name__] = importlib.import_module("gundi_action_runner.services.activity_logger")
