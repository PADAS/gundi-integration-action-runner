import pytest

from app.services.tests.storyboards.action_runner_storyboard import ActionRunnerStoryboard
from app.services.tests.storyboards.activity_logger_storyboard import ActivityLoggerStoryboard
from app.services.tests.storyboards.gundi_api_storyboard import GundiApiStoryboard
from app.services.tests.storyboards.redis_storyboard import RedisStoryboard
from app.services.tests.storyboards.state_manager_storyboard import StateManagerStoryboard


@pytest.fixture
def action_runner_story(mocker):
    return ActionRunnerStoryboard(mocker)


@pytest.fixture()
def activity_logger_story():
    return ActivityLoggerStoryboard()


@pytest.fixture
def gundi_api_story(mocker):
    return GundiApiStoryboard(mocker)


@pytest.fixture
def redis_story():
    return RedisStoryboard()


@pytest.fixture
def state_manager_story():
    return StateManagerStoryboard()
