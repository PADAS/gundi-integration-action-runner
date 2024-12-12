from environs import Env

env = Env()
env.read_env()

OBSERVATIONS_BATCH_SIZE = env.int("OBSERVATIONS_BATCH_SIZE", default=200)
