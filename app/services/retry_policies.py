"""Deprecated compatibility shim — this module moved to gundi_action_runner.services.retry_policies."""
import importlib
import sys
import warnings

warnings.warn(
    "'app.services.retry_policies' is deprecated; import 'gundi_action_runner.services.retry_policies' instead.",
    DeprecationWarning,
    stacklevel=2,
)
sys.modules[__name__] = importlib.import_module("gundi_action_runner.services.retry_policies")
