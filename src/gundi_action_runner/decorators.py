from gundi_action_runner.actions.core import (
    AuthActionConfiguration,
    GenericActionConfiguration,
    PullActionConfiguration,
    PushActionConfiguration,
)
from gundi_action_runner.registry import registry


def _action_decorator(expected_config_base):
    def outer(*, config, id=None, title=None):
        def register(func):
            return registry.register_action(
                func,
                config_model=config,
                expected_config_base=expected_config_base,
                action_id=id,
                title=title,
            )
        return register
    return outer


class _ActionDecorators:
    """Namespace for the @action.* handler decorators."""
    auth = staticmethod(_action_decorator(AuthActionConfiguration))
    pull = staticmethod(_action_decorator(PullActionConfiguration))
    push = staticmethod(_action_decorator(PushActionConfiguration))
    generic = staticmethod(_action_decorator(GenericActionConfiguration))


action = _ActionDecorators()


def webhook(func=None, *, payload=None, config=None):
    """Register the connector's webhook handler.

    Usable bare (`@webhook`, models introspected from annotations) or with
    explicit overrides (`@webhook(payload=..., config=...)`).
    """
    def register(f):
        return registry.register_webhook(f, payload_model=payload, config_model=config)

    if func is not None:
        return register(func)
    return register
