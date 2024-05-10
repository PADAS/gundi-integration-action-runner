from .core import PullActionConfiguration, AuthActionConfiguration


class AuthenticateConfig(AuthActionConfiguration):
    email: str
    password: str


class PullEventsConfig(PullActionConfiguration):
    carto_url: str = "https://rw-nrt.carto.com:443/api/v2/sql"
    url: str
    include_fire_alerts: bool = True
    include_tree_losses_alerts: bool = True
