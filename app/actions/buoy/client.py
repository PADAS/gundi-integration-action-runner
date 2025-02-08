import json
import logging
from typing import List

import aiohttp

logger = logging.getLogger(__name__)


class BuoyClient:
    def __init__(self, er_token: str, er_site: str):
        self.er_token = er_token
        self.er_site = er_site
        self.headers = {
            "Authorization": f"Bearer {self.er_token}",
        }

    # TODO: Validate include details works as expected
    async def get_er_subjects(self, start_datetime: str = None) -> List:
        query_params = {
            "include_details": True,
            "include_inactive": True,
        }
        if start_datetime:
            query_params["updated_since"] = start_datetime

        url = f"{self.er_site}/subjects/"

        with aiohttp.ClientSession() as session:
            async with session.get(
                url, headers=self.headers, params=query_params
            ) as response:
                if response.status != 200:
                    logger.error(
                        f"Failed to make request. Status code: {response.status}"
                    )
                    return []
                data = await response.json()
                if len(data["data"]) == 0:
                    logger.error("No subjects found")
                    return []
                return data["data"]
