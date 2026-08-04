"""Deprecated compatibility shim — this module moved to gundi_action_runner.actions.core."""
import importlib
import sys
import warnings

warnings.warn(
    "'app.actions.core' is deprecated; import 'gundi_action_runner.actions.core' instead.",
    DeprecationWarning,
    stacklevel=2,
)
sys.modules[__name__] = importlib.import_module("gundi_action_runner.actions.core")
