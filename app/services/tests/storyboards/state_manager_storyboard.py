from unittest.mock import MagicMock
from app.services.tests.storyboards.common import async_return


class StateManagerStoryboard(object):
    def mock_state_manager(self):
        mock_state_manager = MagicMock()
        mock_state_manager.get_state.return_value = async_return(
            {'last_execution': '2023-11-17T11:20:00+0200'}
        )
        mock_state_manager.set_state.return_value = async_return(None)
        return mock_state_manager
