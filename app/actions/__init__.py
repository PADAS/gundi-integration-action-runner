"""Deprecated compatibility package — base classes and the handler registry moved
to gundi_action_runner.actions. This package remains so forks keep their
app/actions/handlers.py and app/actions/configurations.py files."""
import warnings

from gundi_action_runner.actions.core import *  # noqa: F401,F403

warnings.warn(
    "'app.actions' is deprecated; import 'gundi_action_runner.actions' instead.",
    DeprecationWarning,
    stacklevel=2,
)


def __getattr__(name):
    # PEP 562: resolve registry-level names (action_handlers, get_actions,
    # setup_action_handlers, get_action_handler_by_data_type) lazily to avoid
    # an import cycle with the library's eager legacy discovery.
    import gundi_action_runner.actions as _lib
    return getattr(_lib, name)
