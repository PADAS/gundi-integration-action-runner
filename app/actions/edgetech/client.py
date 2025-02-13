import asyncio
import gzip
import json
import logging
import re
import time
from typing import List

import aiohttp

from app.actions.configurations import EdgeTechConfiguration
from app.actions.edgetech.enums import Buoy

logger = logging.getLogger(__name__)


# TODO: Verify how will the token update behave
class EdgeTechClient:
    def __init__(self, config: EdgeTechConfiguration, *args, **kwargs):
        self._token_json = config.token_json.get_secret_value()
        self._config = config

    def _set_token(self, token_response, refresh_token=None, *args, **kwargs):
        token_response["expires_at"] = time.time() + token_response["expires_in"]
        if "refresh_token" not in token_response and refresh_token:
            token_response["refresh_token"] = refresh_token

        self._token_json = token_response

    async def _update_token(self):
        refresh_token = self._token_json.get("refresh_token")

        refresh_params = {
            "client_id": self._config.client_id,
            "refresh_token": refresh_token,
            "redirect_uri": self._config.redirect_uri,
            "scope": self._config.scope,
        }

        with aiohttp.ClientSession() as session:
            async with session.post(
                self._config.token_url, data=refresh_params
            ) as response:
                token_response = await response.json()
                self._set_token(token_response, self._token_json["refresh_token"])

        return self._token_json

    async def _get_token(self):
        now = time.time()

        if now >= self._token_json["expires_at"]:
            self._token_json = await self._update_token(self._token_json)

        return self._token_json

    async def download_data(self) -> List[Buoy]:
        token = await self._get_token()

        access_token = token["access_token"]

        headers = {"Authorization": f"Bearer {access_token}"}

        async with aiohttp.ClientSession() as session:
            async with session.post(
                self._config.database_dump_url, headers=headers
            ) as start_response:
                if start_response.status != 303:
                    raise ValueError(f"Invalid response: {start_response.status}")

                dump_location = start_response.headers.get("Location")
                if not dump_location:
                    raise ValueError("Missing Location header in response")

            dump_url = f"{self._config.api_base_url}{dump_location}"

            for attempt in range(self._config.num_get_retry):
                logger.debug(
                    f"Get Dump Attempt {attempt + 1}/{self._config.num_get_retry}"
                )
                async with session.get(dump_url, headers=headers) as get_response:
                    if get_response.status == 200:
                        logger.info(await get_response.text())
                        await asyncio.sleep(1)
                    elif get_response.status == 303:
                        logger.debug("Success - downloading")

                        # Download the file
                        download_url = get_response.headers.get("Location")
                        if not download_url:
                            raise ValueError(
                                "Missing Location header in download response"
                            )

                        async with session.get(download_url) as download_response:
                            content_disposition = download_response.headers.get(
                                "Content-Disposition", ""
                            )
                            match = re.search(r'filename="(.*)"', content_disposition)
                            if not match:
                                raise ValueError(
                                    "Filename not found in Content-Disposition header"
                                )

                            fname = match.group(1)
                            logger.info(f"Downloaded file: {fname}")

                            compressed_data = await download_response.read()
                            with gzip.GzipFile(fileobj=compressed_data) as fo:
                                data = json.load(fo)
                        break

        return [Buoy.parse_obj(item) for item in data]
