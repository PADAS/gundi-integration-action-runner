import asyncio
import gzip
import io
import json
import logging
import re
import time
from typing import List

import aiohttp

from app.actions.configurations import EdgeTechAuthConfiguration, EdgeTechConfiguration
from app.actions.edgetech.exceptions import InvalidCredentials
from app.actions.edgetech.types import Buoy

logger = logging.getLogger(__name__)


TOKEN_URL = "https://trap-tracker.auth0.com/oauth/token"
REDIRECT_URI = "https://app.local"
SCOPE = "offline_access database:dump openid profile email"


class EdgeTechClient:
    """
    Client for interacting with the EdgeTech API.

    This client handles authentication token management and data downloading from the EdgeTech service.
    It uses the provided configuration (EdgeTechConfiguration) for setting up token refresh, API endpoints,
    and other parameters.
    """

    def __init__(
        self,
        auth_config: EdgeTechAuthConfiguration,
        pull_config: EdgeTechConfiguration,
        *args,
        **kwargs,
    ):
        """
        Initialize an EdgeTechClient instance with the given configuration.

        Args:
            config (EdgeTechConfiguration): Configuration settings for connecting to the EdgeTech API.
            *args: Additional positional arguments.
            **kwargs: Additional keyword arguments.
        """
        self._token_json = json.loads(auth_config.token_json.get_secret_value())
        self._auth_config = auth_config
        self._pull_config = pull_config

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
            "client_id": self._auth_config.client_id,
            "refresh_token": refresh_token,
            "redirect_uri": REDIRECT_URI,
            "scope": SCOPE,
            "grant_type": "refresh_token",
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(TOKEN_URL, data=refresh_params) as response:
                response_json = await response.json()
                if response.status != 200:
                    raise InvalidCredentials(response_json)

                self._set_token(response_json, self._token_json["refresh_token"])

        return self._token_json

    async def get_token(self) -> dict:
        """
        Retrieve a valid authentication token, refreshing it if expired.

        Checks if the current token has expired based on its 'expires_at' field.
        If expired, triggers a token update; otherwise, returns the current token.

        Returns:
            dict: The valid token JSON with access token and expiry details.
        """
        now = time.time()

        if now >= self._token_json["expires_at"]:
            self._token_json = await self._update_token()

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
        return [
            {
                "serialNumber": "88CE9978AE",
                "currentState": {
                    "etag": '"1733842319546"',
                    "isDeleted": False,
                    "positionSetByCapri": False,
                    "serialNumber": "88CE9978AE",
                    "releaseCommand": "C8AB8C73AE",
                    "statusCommand": "88CE9978AE",
                    "idCommand": "CCCCCCCCCC",
                    "isNfcTag": False,
                    "modelNumber": "5112",
                    "dateStatus": "2024-10-07T19:31:24.469Z",
                    "statusRangeM": 0,
                    "statusIsTilted": False,
                    "statusBatterySoC": 86,
                    "lastUpdated": "2024-12-10T14:51:59.546Z",
                },
                "changeRecords": [],
            },
            {
                "serialNumber": "88CE999763",
                "currentState": {
                    "etag": '"1733842320416"',
                    "isDeleted": False,
                    "positionSetByCapri": False,
                    "serialNumber": "88CE999763",
                    "releaseCommand": "C8AB8CCAE3",
                    "statusCommand": "88CE999763",
                    "idCommand": "CCCCCCCCCC",
                    "isNfcTag": False,
                    "modelNumber": "5112",
                    "dateOfManufacture": "2021-12-07T05:00:00.000Z",
                    "dateOfBatteryCharge": "2023-03-28T04:00:00.000Z",
                    "isDeployed": False,
                    "dateRecovered": "2024-05-28T14:22:54.151Z",
                    "dateStatus": "2023-09-23T20:07:48.458Z",
                    "statusRangeM": 257.656,
                    "statusIsTilted": False,
                    "statusBatterySoC": 82,
                    "lastUpdated": "2024-12-10T14:52:00.416Z",
                },
                "changeRecords": [],
            },
            {
                "serialNumber": "88CE99C99A",
                "currentState": {
                    "etag": '"1733842326885"',
                    "isDeleted": False,
                    "positionSetByCapri": False,
                    "serialNumber": "88CE99C99A",
                    "releaseCommand": "C8AB8CCC9A",
                    "statusCommand": "88CE99C99A",
                    "idCommand": "CCCCCCCCCC",
                    "isNfcTag": False,
                    "modelNumber": "5112",
                    "dateStatus": "2023-08-01T18:15:44.563Z",
                    "statusRangeM": 0,
                    "statusIsTilted": False,
                    "statusBatterySoC": 62,
                    "lastUpdated": "2024-12-10T14:52:06.885Z",
                },
                "changeRecords": [],
            },
            {
                "serialNumber": "88CE9978AE",
                "currentState": {
                    "etag": '"1729614890401"',
                    "isDeleted": False,
                    "positionSetByCapri": False,
                    "serialNumber": "88CE9978AE",
                    "releaseCommand": "C8AB8C73AE",
                    "statusCommand": "88CE9978AE",
                    "idCommand": "CCCCCCCCCC",
                    "isNfcTag": False,
                    "latDeg": 41.82907459248435,
                    "lonDeg": -71.41540430869928,
                    "modelNumber": "5112",
                    "dateOfManufacture": "2024-10-07T15:26:33.362Z",
                    "dateOfBatteryCharge": "2024-10-07T15:26:34.034Z",
                    "isDeployed": True,
                    "dateDeployed": "2024-10-22T16:34:46.981Z",
                    "lastUpdated": "2024-10-22T16:34:50.401Z",
                },
                "changeRecords": [],
            },
            {
                "serialNumber": "88CE99C99A",
                "currentState": {
                    "etag": '"1741954819256"',
                    "isDeleted": False,
                    "positionSetByCapri": False,
                    "serialNumber": "88CE99C99A",
                    "releaseCommand": "C8AB8CCC9A",
                    "statusCommand": "88CE99C99A",
                    "idCommand": "CCCCCCCCCC",
                    "isNfcTag": True,
                    "modelNumber": "5112",
                    "dateOfManufacture": "2022-12-21T05:00:00.000Z",
                    "isDeployed": False,
                    "dateRecovered": "2025-03-14T12:07:27.199Z",
                    "recoveredLatDeg": 41.7832483,
                    "recoveredLonDeg": -70.7527803,
                    "recoveredRangeM": 0,
                    "recoveredTemperatureC": 24,
                    "dateStatus": "2025-03-14T12:20:13.201Z",
                    "statusRangeM": 0,
                    "statusIsTilted": True,
                    "statusBatterySoC": 3,
                    "lastUpdated": "2025-03-14T12:20:19.256Z",
                },
                "changeRecords": [],
            },
            {
                "serialNumber": "88CE9978AE",
                "currentState": {
                    "etag": '"1739896745003"',
                    "isDeleted": True,
                    "positionSetByCapri": False,
                    "serialNumber": "88CE9978AE",
                    "releaseCommand": "C8AB8C73AE",
                    "statusCommand": "88CE9978AE",
                    "idCommand": "CCCCCCCCCC",
                    "isNfcTag": False,
                    "modelNumber": "5112",
                    "isDeployed": False,
                    "dateRecovered": "2025-02-13T15:30:42.492Z",
                    "recoveredRangeM": 0,
                    "recoveredTemperatureC": 7,
                    "dateStatus": "2025-02-11T17:14:15.690Z",
                    "statusRangeM": 0,
                    "statusIsTilted": False,
                    "statusBatterySoC": 0,
                    "lastUpdated": "2025-02-18T16:39:05.003Z",
                },
                "changeRecords": [],
            },
            {
                "serialNumber": "88CE9978AE",
                "currentState": {
                    "etag": '"1717697928807"',
                    "isDeleted": False,
                    "positionSetByCapri": False,
                    "serialNumber": "88CE9978AE",
                    "releaseCommand": "C8AB8C73AE",
                    "statusCommand": "88CE9978AE",
                    "idCommand": "CCCCCCCCCC",
                    "isNfcTag": False,
                    "modelNumber": "5112",
                    "dateStatus": "2024-06-06T18:16:22.470Z",
                    "statusRangeM": 1.498,
                    "statusIsTilted": True,
                    "statusBatterySoC": 94,
                    "lastUpdated": "2024-06-06T18:18:48.807Z",
                },
                "changeRecords": [],
            },
            {
                "serialNumber": "88CE99C99A",
                "currentState": {
                    "etag": '"1682088076708"',
                    "isDeleted": False,
                    "positionSetByCapri": False,
                    "serialNumber": "88CE99C99A",
                    "releaseCommand": "C8AB8CCC9A",
                    "statusCommand": "88CE99C99A",
                    "idCommand": "CCCCCCCCCC",
                    "isNfcTag": True,
                    "modelNumber": "5112",
                    "dateOfManufacture": "2022-12-21T05:00:00.000Z",
                    "dateStatus": "2023-04-21T14:41:08.627Z",
                    "statusRangeM": 1575.89599609375,
                    "lastUpdated": "2023-04-21T14:41:16.708Z",
                },
                "changeRecords": [],
            },
            {
                "serialNumber": "88CE99C99A",
                "currentState": {
                    "etag": '"1733241895379"',
                    "isDeleted": False,
                    "positionSetByCapri": False,
                    "serialNumber": "88CE99C99A",
                    "releaseCommand": "C8AB8CCC9A",
                    "statusCommand": "88CE99C99A",
                    "idCommand": "CCCCCCCCCC",
                    "isNfcTag": False,
                    "modelNumber": "5112",
                    "dateOfManufacture": "2022-12-21T05:00:00.000Z",
                    "isDeployed": False,
                    "dateRecovered": "2024-12-03T16:04:55.230Z",
                    "recoveredLatDeg": 41.5740898,
                    "recoveredLonDeg": -70.8831463,
                    "dateStatus": "2024-12-03T15:23:51.886Z",
                    "statusRangeM": 58.422,
                    "statusIsTilted": False,
                    "statusBatterySoC": 104,
                    "lastUpdated": "2024-12-03T16:04:55.379Z",
                },
                "changeRecords": [],
            },
            {
                "serialNumber": "88CE999763",
                "currentState": {
                    "etag": '"1726594987576"',
                    "isDeleted": False,
                    "positionSetByCapri": False,
                    "serialNumber": "88CE999763",
                    "releaseCommand": "C8AB8CCAE3",
                    "statusCommand": "88CE999763",
                    "idCommand": "CCCCCCCCCC",
                    "isNfcTag": False,
                    "lastUpdated": "2024-09-17T17:43:07.576Z",
                },
                "changeRecords": [],
            },
            {
                "serialNumber": "88CE99C99A",
                "currentState": {
                    "etag": '"1726083572993"',
                    "isDeleted": False,
                    "positionSetByCapri": False,
                    "serialNumber": "88CE99C99A",
                    "releaseCommand": "C8AB8CCC9A",
                    "statusCommand": "88CE99C99A",
                    "idCommand": "CCCCCCCCCC",
                    "isNfcTag": False,
                    "dateStatus": "2024-09-11T19:34:41.744Z",
                    "statusRangeM": 0,
                    "statusIsTilted": True,
                    "statusBatterySoC": 69,
                    "lastUpdated": "2024-09-11T19:39:32.993Z",
                },
                "changeRecords": [],
            },
            {
                "serialNumber": "88CE9978AE",
                "currentState": {
                    "etag": '"1728330307444"',
                    "isDeleted": False,
                    "positionSetByCapri": False,
                    "serialNumber": "88CE9978AE",
                    "releaseCommand": "C8AB8C73AE",
                    "statusCommand": "88CE9978AE",
                    "idCommand": "CCCCCCCCCC",
                    "isNfcTag": False,
                    "modelNumber": "5112",
                    "dateStatus": "2024-10-07T19:31:24.469Z",
                    "statusRangeM": 0,
                    "statusIsTilted": False,
                    "statusBatterySoC": 86,
                    "lastUpdated": "2024-10-07T19:45:07.444Z",
                },
                "changeRecords": [],
            },
            {
                "serialNumber": "88CE999763",
                "currentState": {
                    "etag": '"1742232994823"',
                    "isDeleted": False,
                    "positionSetByCapri": False,
                    "serialNumber": "88CE999763",
                    "releaseCommand": "C8AB8CCAE3",
                    "statusCommand": "88CE999763",
                    "idCommand": "CCCCCCCCCC",
                    "isNfcTag": False,
                    "modelNumber": "5112",
                    "dateOfManufacture": "2021-12-07T05:00:00.000Z",
                    "dateOfBatteryCharge": "2023-03-28T04:00:00.000Z",
                    "isDeployed": False,
                    "dateRecovered": "2025-03-17T17:36:32.643Z",
                    "dateStatus": "2023-09-23T20:07:48.458Z",
                    "statusRangeM": 257.656,
                    "statusIsTilted": False,
                    "statusBatterySoC": 82,
                    "lastUpdated": "2025-03-17T17:36:34.823Z",
                },
                "changeRecords": [
                    {
                        "type": "MODIFY",
                        "timestamp": "2025-03-17T17:36:34.000Z",
                        "changes": [
                            {
                                "key": "dateDeployed",
                                "oldValue": "2025-03-17T16:43:40.228Z",
                                "newValue": None,
                            },
                            {
                                "key": "dateRecovered",
                                "oldValue": None,
                                "newValue": "2025-03-17T17:36:32.643Z",
                            },
                            {
                                "key": "endLatDeg",
                                "oldValue": 41.52537796592242,
                                "newValue": None,
                            },
                            {
                                "key": "endLonDeg",
                                "oldValue": -70.6738777899687,
                                "newValue": None,
                            },
                            {"key": "geoHash", "oldValue": "89e4d", "newValue": "X"},
                            {"key": "isDeployed", "oldValue": True, "newValue": False},
                            {
                                "key": "lastUpdated",
                                "oldValue": "2025-03-17T16:43:40.831Z",
                                "newValue": "2025-03-17T17:36:34.823Z",
                            },
                            {
                                "key": "latDeg",
                                "oldValue": 41.52546746182916,
                                "newValue": None,
                            },
                            {
                                "key": "lonDeg",
                                "oldValue": -70.67401171221228,
                                "newValue": None,
                            },
                        ],
                    },
                    {
                        "type": "MODIFY",
                        "timestamp": "2025-03-17T16:43:40.000Z",
                        "changes": [
                            {
                                "key": "dateDeployed",
                                "oldValue": None,
                                "newValue": "2025-03-17T16:43:40.228Z",
                            },
                            {
                                "key": "dateRecovered",
                                "oldValue": "2025-03-17T16:41:22.508Z",
                                "newValue": None,
                            },
                            {
                                "key": "endLatDeg",
                                "oldValue": None,
                                "newValue": 41.52537796592242,
                            },
                            {
                                "key": "endLonDeg",
                                "oldValue": None,
                                "newValue": -70.6738777899687,
                            },
                            {"key": "geoHash", "oldValue": "X", "newValue": "89e4d"},
                            {"key": "isDeployed", "oldValue": False, "newValue": True},
                            {
                                "key": "lastUpdated",
                                "oldValue": "2025-03-17T16:43:14.071Z",
                                "newValue": "2025-03-17T16:43:40.831Z",
                            },
                            {
                                "key": "latDeg",
                                "oldValue": None,
                                "newValue": 41.52546746182916,
                            },
                            {
                                "key": "lonDeg",
                                "oldValue": None,
                                "newValue": -70.67401171221228,
                            },
                        ],
                    },
                    {
                        "type": "MODIFY",
                        "timestamp": "2025-03-17T16:43:14.000Z",
                        "changes": [
                            {
                                "key": "dateDeployed",
                                "oldValue": "2025-03-17T16:26:12.059Z",
                                "newValue": None,
                            },
                            {
                                "key": "dateRecovered",
                                "oldValue": None,
                                "newValue": "2025-03-17T16:41:22.508Z",
                            },
                            {"key": "geoHash", "oldValue": "89e4d", "newValue": "X"},
                            {"key": "isDeployed", "oldValue": True, "newValue": False},
                            {
                                "key": "lastUpdated",
                                "oldValue": "2025-03-17T16:26:12.457Z",
                                "newValue": "2025-03-17T16:43:14.071Z",
                            },
                            {
                                "key": "latDeg",
                                "oldValue": 41.52546746182916,
                                "newValue": None,
                            },
                            {
                                "key": "lonDeg",
                                "oldValue": -70.67401171221228,
                                "newValue": None,
                            },
                        ],
                    },
                    {
                        "type": "MODIFY",
                        "timestamp": "2025-03-17T16:26:12.000Z",
                        "changes": [
                            {
                                "key": "dateDeployed",
                                "oldValue": None,
                                "newValue": "2025-03-17T16:26:12.059Z",
                            },
                            {
                                "key": "dateRecovered",
                                "oldValue": "2024-05-28T14:22:54.151Z",
                                "newValue": None,
                            },
                            {"key": "geoHash", "oldValue": "X", "newValue": "89e4d"},
                            {"key": "isDeployed", "oldValue": False, "newValue": True},
                            {
                                "key": "lastUpdated",
                                "oldValue": "2024-05-28T14:22:55.449Z",
                                "newValue": "2025-03-17T16:26:12.457Z",
                            },
                            {
                                "key": "latDeg",
                                "oldValue": None,
                                "newValue": 41.52546746182916,
                            },
                            {
                                "key": "lonDeg",
                                "oldValue": None,
                                "newValue": -70.67401171221228,
                            },
                        ],
                    },
                ],
            },
            {
                "serialNumber": "88CE99C99A",
                "currentState": {
                    "etag": '"1691005965618"',
                    "isDeleted": False,
                    "positionSetByCapri": False,
                    "serialNumber": "88CE99C99A",
                    "releaseCommand": "C8AB8CCC9A",
                    "statusCommand": "88CE99C99A",
                    "idCommand": "CCCCCCCCCC",
                    "isNfcTag": False,
                    "modelNumber": "5112",
                    "dateStatus": "2023-08-01T18:15:44.563Z",
                    "statusRangeM": 0,
                    "statusIsTilted": False,
                    "statusBatterySoC": 62,
                    "lastUpdated": "2023-08-02T19:52:45.618Z",
                },
                "changeRecords": [],
            },
            {
                "serialNumber": "88CE99C99A",
                "currentState": {
                    "etag": '"1692717424234"',
                    "isDeleted": False,
                    "positionSetByCapri": False,
                    "serialNumber": "88CE99C99A",
                    "releaseCommand": "C8AB8CCC9A",
                    "statusCommand": "88CE99C99A",
                    "idCommand": "CCCCCCCCCC",
                    "isNfcTag": True,
                    "modelNumber": "5112",
                    "dateOfManufacture": "2022-12-21T05:00:00.000Z",
                    "dateStatus": "2023-08-22T15:17:01.599Z",
                    "statusRangeM": 7.489999771118164,
                    "statusIsTilted": False,
                    "statusBatterySoC": 71,
                    "lastUpdated": "2023-08-22T15:17:04.234Z",
                },
                "changeRecords": [],
            },
        ]

        token = await self.get_token()

        access_token = token["access_token"]

        headers = {"Authorization": f"Bearer {access_token}"}

        async with aiohttp.ClientSession() as session:
            async with session.post(
                self._pull_config.database_dump_url,
                headers=headers,
                allow_redirects=False,
            ) as start_response:
                if start_response.status != 303:
                    raise ValueError(f"Invalid response: {start_response.status}")

                dump_location = start_response.headers.get("Location")
                if not dump_location:
                    raise ValueError("Missing Location header in response")

            dump_url = f"{self._pull_config.api_base_url}{dump_location}"

            for attempt in range(self._pull_config.num_get_retry):
                logger.debug(
                    f"Get Dump Attempt {attempt + 1}/{self._pull_config.num_get_retry}"
                )
                async with session.get(
                    dump_url, headers=headers, allow_redirects=False
                ) as get_response:
                    if get_response.status == 200:
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
                            with gzip.GzipFile(
                                fileobj=io.BytesIO(compressed_data)
                            ) as fo:
                                data = json.load(fo)
                        break

        return [Buoy.parse_obj(item) for item in data]
