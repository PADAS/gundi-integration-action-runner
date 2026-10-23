import importlib
import inspect
import logging

from gundi_action_runner.actions.core import PushActionConfiguration, discover_actions

logger = logging.getLogger(__name__)


def _is_module_or_parent(missing_name, target):
    """True when `missing_name` is `target` itself or a parent package of it."""
    if not missing_name:
        return False
    return target == missing_name or target.startswith(missing_name + ".")


class RegistryError(Exception):
    """Raised for invalid or conflicting handler registrations."""


class ActionRegistry:
    """Holds the connector's action and webhook handlers.

    Populated by the @action.* / @webhook decorators at import time, or by the
    legacy loaders that scan `action_`-prefixed functions (the template fork
    convention). `action_handlers` keeps the same tuple shape the template's
    discover_actions() produced — (func, config_model, data_model) — so the
    services bind this dict directly.
    """

    def __init__(self):
        self.action_handlers = {}
        self.webhook_handler = None  # (func, payload_model, config_model) or None

    def reset(self):
        self.action_handlers.clear()
        self.webhook_handler = None

    def register_action(self, func, *, config_model, expected_config_base,
                        action_id=None, title=None):
        if not inspect.iscoroutinefunction(func):
            raise RegistryError(
                f"Action handler '{func.__module__}.{func.__qualname__}' must be an async function."
            )
        action_id = action_id or func.__name__
        if action_id in self.action_handlers:
            existing = self.action_handlers[action_id][0]
            raise RegistryError(
                f"Duplicate action id '{action_id}': already registered by "
                f"'{existing.__module__}.{existing.__qualname__}'."
            )
        if not (inspect.isclass(config_model) and issubclass(config_model, expected_config_base)):
            raise RegistryError(
                f"Action '{action_id}' ('{func.__module__}.{func.__qualname__}'): config must "
                f"subclass {expected_config_base.__name__}, got {config_model!r}."
            )
        params = inspect.signature(func).parameters
        for required in ("integration", "action_config"):
            if required not in params:
                raise RegistryError(
                    f"Action '{action_id}' ('{func.__module__}.{func.__qualname__}') must accept "
                    f"an '{required}' parameter."
                )
        data_model = None
        if issubclass(config_model, PushActionConfiguration):
            data_param = params.get("data")
            if data_param is None or data_param.annotation is inspect.Parameter.empty:
                raise RegistryError(
                    f"Push action '{action_id}' must accept a 'data' parameter annotated "
                    f"with a data model."
                )
            if "metadata" not in params:
                raise RegistryError(f"Push action '{action_id}' must accept a 'metadata' parameter.")
            data_model = data_param.annotation
        if title:
            func.action_title = title  # read by self-registration (PR #77 convention)
        self.action_handlers[action_id] = (func, config_model, data_model)
        return func

    def register_webhook(self, func, *, payload_model=None, config_model=None):
        if not inspect.iscoroutinefunction(func):
            raise RegistryError(
                f"Webhook handler '{func.__module__}.{func.__qualname__}' must be an async function."
            )
        if self.webhook_handler is not None:
            existing = self.webhook_handler[0]
            raise RegistryError(
                f"A webhook handler is already registered "
                f"('{existing.__module__}.{existing.__qualname__}'); only one is allowed."
            )
        params = inspect.signature(func).parameters
        for required in ("payload", "webhook_config"):
            if required not in params:
                raise RegistryError(
                    f"Webhook handler '{func.__module__}.{func.__qualname__}' must accept "
                    f"a '{required}' parameter."
                )
        if payload_model is None and params["payload"].annotation is not inspect.Parameter.empty:
            payload_model = params["payload"].annotation
        if config_model is None and params["webhook_config"].annotation is not inspect.Parameter.empty:
            config_model = params["webhook_config"].annotation
        self.webhook_handler = (func, payload_model, config_model)
        return func

    def load_modules(self, module_names):
        """Import handler modules so their decorators register with this registry."""
        for name in module_names:
            importlib.import_module(name)

    def load_legacy_actions(self, module_name):
        """Scan a fork-convention module for `action_`-prefixed handler functions.

        Decorator registrations always win over legacy scans of the same id.
        """
        for action_id, entry in discover_actions(module_name=module_name, prefix="action_").items():
            if action_id not in self.action_handlers:
                self.action_handlers[action_id] = entry

    def load_legacy_webhook(self, module_name):
        """Adopt a fork-convention `webhook_handler` function if none is registered."""
        if self.webhook_handler is not None:
            return
        module = importlib.import_module(module_name)
        # AttributeError when the module defines no webhook_handler — the same
        # failure mode the template had; callers decide whether that is fatal.
        self.register_webhook(module.webhook_handler)


    def ensure_loaded(self):
        """Populate the registry from env-configured modules if it is empty.

        Called by create_app() and register_integration_in_gundi() so both the
        server path and the CLI registration path see the connector's handlers.
        """
        from gundi_action_runner import settings  # deferred: avoid import-time env coupling

        if not self.action_handlers and settings.GUNDI_HANDLERS_MODULES:
            self.load_modules(
                [m.strip() for m in settings.GUNDI_HANDLERS_MODULES.split(",") if m.strip()]
            )
        if not self.action_handlers:
            try:
                self.load_legacy_actions(settings.GUNDI_LEGACY_ACTIONS_MODULE)
            except ModuleNotFoundError as e:
                if not _is_module_or_parent(e.name, settings.GUNDI_LEGACY_ACTIONS_MODULE):
                    raise  # a broken import INSIDE the module must fail loudly
                logger.warning(
                    f"No legacy actions module '{settings.GUNDI_LEGACY_ACTIONS_MODULE}' found; "
                    f"no actions registered."
                )
        if self.webhook_handler is None:
            try:
                self.load_legacy_webhook(settings.GUNDI_LEGACY_WEBHOOKS_MODULE)
            except ModuleNotFoundError as e:
                if not _is_module_or_parent(e.name, settings.GUNDI_LEGACY_WEBHOOKS_MODULE):
                    raise
            except AttributeError:
                pass  # module exists but defines no webhook_handler — fine


registry = ActionRegistry()
