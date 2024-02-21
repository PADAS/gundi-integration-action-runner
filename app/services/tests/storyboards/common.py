import asyncio
from unittest.mock import MagicMock


class AsyncMock(MagicMock):
    async def __call__(self, *args, **kwargs):
        return super(AsyncMock, self).__call__(*args, **kwargs)


def async_return(result):
    f = asyncio.Future()
    f.set_result(result)
    return f
