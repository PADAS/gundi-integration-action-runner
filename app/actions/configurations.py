# actions/configurations.py
from .core import PullActionConfiguration


class PullRmwHubObservationsConfiguration(PullActionConfiguration):
    sync_interval_minutes: int = 5
