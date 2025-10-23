import json
import logging
from datetime import timezone
from typing import List, Optional

import aiohttp

from .types import BuoyGear, ObservationSubject

logger = logging.getLogger(__name__)


class BuoyClient:
    def __init__(self, er_token: str, er_site: str):
        self.er_token = er_token
        self.er_site = er_site
        self.headers = {
            "Authorization": f"Bearer {self.er_token}",
        }

    async def get_er_gears(
        self,
        params: Optional[dict] = None,
    ) -> List[BuoyGear]:
        url = f"{self.er_site}api/v1.0/gear/"
        items = []

        async with aiohttp.ClientSession() as session:
            while url:
                async with session.get(
                    url, headers=self.headers, params=params
                ) as response:
                    if response.status != 200:
                        logger.error(
                            f"Failed to make request. Status code: {response.status} Body: {await response.text()}"
                        )
                        break

                    data = await response.json()

                    if "data" not in data:
                        logger.error("Unexpected response structure")
                        break

                    page_data = data["data"]

                    if "results" not in page_data:
                        logger.error("No results field in response")
                        break

                    results = page_data["results"]

                    items.extend(results)

                    url = page_data.get("next")

        if len(items) == 0:
            logger.error("No gears found")

        gears = []
        try:
            for item in items:
                buoy = BuoyGear.parse_obj(item)
                if buoy.manufacturer != "edgetech":
                    continue
                buoy.last_updated = buoy.last_updated.astimezone(timezone.utc)
                gears.append(buoy)
        except Exception as e:
            logger.error(f"Error parsing gear items: {e} (item: {json.dumps(item)})")

        return gears