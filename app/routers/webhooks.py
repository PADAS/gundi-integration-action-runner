"""Deprecated compatibility shim — this module moved to gundi_action_runner.routers.webhooks."""
import importlib
import sys
import warnings

warnings.warn(
    "'app.routers.webhooks' is deprecated; import 'gundi_action_runner.routers.webhooks' instead.",
    DeprecationWarning,
    stacklevel=2,
)
sys.modules[__name__] = importlib.import_module("gundi_action_runner.routers.webhooks")
