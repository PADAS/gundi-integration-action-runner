import json

from unittest.mock import MagicMock
from app.services.tests.storyboards.common import async_return


class RedisStoryboard(object):
    @staticmethod
    def mock_integration_state():
        return {"last_execution": "2024-01-29T11:20:00+0200"}

    def given_redis_mock(self):
        redis = MagicMock()
        redis_client = MagicMock()
        redis_client.set.return_value = async_return(MagicMock())
        redis_client.get.return_value = async_return(json.dumps(self.mock_integration_state(), default=str))
        redis_client.setex.return_value = async_return(None)
        redis_client.incr.return_value = redis_client
        redis_client.decr.return_value = async_return(None)
        redis_client.expire.return_value = redis_client
        redis_client.execute.return_value = async_return((1, True))
        redis_client.__aenter__.return_value = redis_client
        redis_client.__aexit__.return_value = None
        redis_client.pipeline.return_value = redis_client
        redis.Redis.return_value = redis_client
        return redis
