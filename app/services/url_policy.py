"""Deprecated compatibility shim — this module moved to gundi_action_runner.services.url_policy."""
import importlib
import sys
import warnings

warnings.warn(
    "'app.services.url_policy' is deprecated; import 'gundi_action_runner.services.url_policy' instead.",
    DeprecationWarning,
    stacklevel=2,
)
sys.modules[__name__] = importlib.import_module("gundi_action_runner.services.url_policy")
