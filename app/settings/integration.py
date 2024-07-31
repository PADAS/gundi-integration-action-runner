# Add your integration-specific settings here

from environs import Env

env = Env()
env.read_env()

INTEGRATED_ALERTS_REQUEST_CONCURRENCY = env.int("INTEGRATED_ALERTS_REQUEST_CONCURRENCY", 5)
