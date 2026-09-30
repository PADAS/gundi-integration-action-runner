"""Deprecated compatibility entry point — the app is now built by
gundi_action_runner.create_app(). `uvicorn app.main:app` keeps working."""
import warnings

from gundi_action_runner import create_app

warnings.warn(
    "'app.main' is deprecated; build the app with 'gundi_action_runner.create_app()' instead.",
    DeprecationWarning,
    stacklevel=2,
)

app = create_app()
