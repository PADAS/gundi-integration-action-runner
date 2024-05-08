from .core import ActionConfiguration


class AuthenticateConfig(ActionConfiguration):
    endpoint: str = "oauth/token"
    username: str
    password: str
    grant_type: str = "password"
    refresh_token: str = "string"


class PullObservationsConfig(ActionConfiguration):
    endpoint: str = "mobile/vehicles"
