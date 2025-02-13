import asyncio
import gzip
import json
import logging
import re
import time
from typing import List

import aiohttp

from app.actions.configurations import EdgeTechConfiguration
from app.actions.edgetech.types import Buoy

logger = logging.getLogger(__name__)


# TODO: Verify how will the token update behave
class EdgeTechClient:
    """
    Client for interacting with the EdgeTech API.

    This client handles authentication token management and data downloading from the EdgeTech service.
    It uses the provided configuration (EdgeTechConfiguration) for setting up token refresh, API endpoints,
    and other parameters.
    """

    def __init__(self, config: EdgeTechConfiguration, *args, **kwargs):
        """
        Initialize an EdgeTechClient instance with the given configuration.

        Args:
            config (EdgeTechConfiguration): Configuration settings for connecting to the EdgeTech API.
            *args: Additional positional arguments.
            **kwargs: Additional keyword arguments.
        """
        self._token_json = config.token_json.get_secret_value()
        self._config = config

    def _set_token(self, token_response, refresh_token=None, *args, **kwargs):
        """
        Update the token information based on the token response.

        This method calculates the token's expiry time and updates the internal token JSON.
        Optionally, if a refresh token is provided and missing in the token response, it is added.

        Args:
            token_response (dict): The response dictionary containing token details.
            refresh_token (str, optional): The refresh token to use if not present in token_response.
            *args: Additional positional arguments.
            **kwargs: Additional keyword arguments.
        """
        token_response["expires_at"] = time.time() + token_response["expires_in"]
        if "refresh_token" not in token_response and refresh_token:
            token_response["refresh_token"] = refresh_token

        self._token_json = token_response

    async def _update_token(self) -> dict:
        """
        Refresh the authentication token using the refresh token.

        Sends a POST request to the token URL with the necessary refresh parameters.
        Upon receiving the new token, it updates the internal token JSON using the _set_token method.

        Returns:
            dict: The updated token JSON containing the new access token and expiry information.
        """
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

    async def _get_token(self) -> dict:
        """
        Retrieve a valid authentication token, refreshing it if expired.

        Checks if the current token has expired based on its 'expires_at' field.
        If expired, triggers a token update; otherwise, returns the current token.

        Returns:
            dict: The valid token JSON with access token and expiry details.
        """
        now = time.time()

        if now >= self._token_json["expires_at"]:
            self._token_json = await self._update_token(self._token_json)

        return self._token_json

    async def download_data(self) -> List[Buoy]:
        """
        Download buoy data from the EdgeTech API.

        This method performs the following steps:
            1. Retrieves a valid authentication token.
            2. Initiates a data dump request to the API.
            3. Follows redirection to download the compressed data file.
            4. Decompresses the downloaded gzip data and loads it as JSON.
            5. Parses the JSON data into a list of Buoy objects.

        Returns:
            List[Buoy]: A list of Buoy objects parsed from the downloaded data.

        Raises:
            ValueError: If the API responses are invalid or missing required headers.
        """
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
