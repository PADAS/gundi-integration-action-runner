from .core import PullActionConfiguration, AuthActionConfiguration


class AuthenticateConfig(AuthActionConfiguration):
    endpoint: str = "oauth/token"
    username: str
    password: str
    grant_type: str = "password"
    refresh_token: str = "string"


class PullObservationsConfig(PullActionConfiguration):
    endpoint: str = "mobile/vehicles"
