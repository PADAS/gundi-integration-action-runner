import json
import logging
from typing import List, Optional

import aiohttp

from .types import ObservationSubject

logger = logging.getLogger(__name__)


class BuoyClient:
    def __init__(self, er_token: str, er_site: str):
        self.er_token = er_token
        self.er_site = er_site
        self.headers = {
            "Authorization": f"Bearer {self.er_token}",
        }

    async def get_er_subjects(
        self,
        params: Optional[dict] = None,
    ) -> List[ObservationSubject]:
        url = f"{self.er_site}api/v1.0/subjects/"

        async with aiohttp.ClientSession() as session:
            async with session.get(
                url, headers=self.headers, params=params
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
                data = data["data"]
                items = []
                for item in data:
                    try:
                        items.append(ObservationSubject.parse_obj(item))
                    except Exception as e:
                        logger.error(f"Error parsing subject: {e}\n{json.dumps(item)})")
                return items
