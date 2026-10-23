"""Deprecated compatibility shim — this module moved to gundi_action_runner.api_schemas."""
import importlib
import sys
import warnings

warnings.warn(
    "'app.api_schemas' is deprecated; import 'gundi_action_runner.api_schemas' instead.",
    DeprecationWarning,
    stacklevel=2,
)
sys.modules[__name__] = importlib.import_module("gundi_action_runner.api_schemas")
