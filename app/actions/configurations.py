from .core import PullActionConfiguration, AuthActionConfiguration


class AuthenticateConfig(AuthActionConfiguration):
    username: str
    password: str


class PullObservationsConfig(PullActionConfiguration):
    endpoint: str = "mobile/vehicles"