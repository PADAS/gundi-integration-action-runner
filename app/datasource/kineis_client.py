"""
CLS Kineis API Client for GPS Location Data

This module provides an async client for fetching GPS location data from the CLS Kineis
telemetry API. It supports both realtime polling (with checkpoint mechanism) and
bulk historical data retrieval. Uses httpx.AsyncClient.

Usage:
    from app.datasource.kineis_client import KineisClient

    client = KineisClient(username="your_username", password="your_password")

    # Get all accessible devices
    devices = await client.get_devices()

    # Start fetching realtime GPS data (async iterator)
    async for locations in client.poll_realtime_locations(interval_seconds=60):
        for loc in locations:
            print(f"Device {loc.device_ref}: {loc.lat}, {loc.lon}")

Environment variables:
    KINEIS_USERNAME: API username
    KINEIS_PASSWORD: API password
    KINEIS_AUTH_URL: OAuth2 token endpoint (optional)
    KINEIS_API_URL: Telemetry API base URL (optional)
"""

import asyncio
import os
import time
import logging
from datetime import datetime, timezone
from typing import Optional, AsyncIterator
from dataclasses import dataclass, field

import httpx

logger = logging.getLogger(__name__)


# Default endpoints
DEFAULT_AUTH_URL = "https://account.groupcls.com/auth/realms/cls/protocol/openid-connect/token"
DEFAULT_API_URL = "https://api.groupcls.com/telemetry/api/v1"


@dataclass
class GPSLocation:
    """Represents a GPS location from a Kineis device."""
    device_uid: int
    device_ref: str
    modem_ref: str
    message_uid: int
    message_timestamp: datetime
    acquisition_timestamp: datetime
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    altitude: Optional[float] = None
    speed: Optional[float] = None
    heading: Optional[float] = None
    # Optional doppler location (if no GPS available)
    doppler_latitude: Optional[float] = None
    doppler_longitude: Optional[float] = None
    doppler_error_radius: Optional[float] = None
    # Raw message data
    raw_data: Optional[dict] = field(default=None, repr=False)

    @property
    def lat(self) -> Optional[float]:
        """Returns GPS latitude if available, otherwise doppler latitude."""
        return self.latitude if self.latitude is not None else self.doppler_latitude
    
    @property
    def lon(self) -> Optional[float]:
        """Returns GPS longitude if available, otherwise doppler longitude."""
        return self.longitude if self.longitude is not None else self.doppler_longitude
    
    @property
    def has_gps(self) -> bool:
        """True if GPS location is available."""
        return self.latitude is not None and self.longitude is not None
    
    @property
    def has_doppler(self) -> bool:
        """True if Doppler location is available."""
        return self.doppler_latitude is not None and self.doppler_longitude is not None
    
    @property
    def has_location(self) -> bool:
        """True if any location (GPS or Doppler) is available."""
        return self.has_gps or self.has_doppler


@dataclass 
class Device:
    """Represents a Kineis device."""
    device_uid: int
    device_ref: str
    program_ref: Optional[str] = None
    customer_id: Optional[int] = None
    customer_name: Optional[str] = None
    modems: list = field(default_factory=list)


class KineisAuthError(Exception):
    """Raised when authentication fails."""
    pass


class KineisAPIError(Exception):
    """Raised when an API call fails."""
    def __init__(self, message: str, status_code: Optional[int] = None, response: Optional[dict] = None):
        super().__init__(message)
        self.status_code = status_code
        self.response = response


class KineisClient:
    """
    Client for the CLS Kineis Telemetry API.
    
    Handles OAuth2 authentication and provides methods for fetching device
    information and GPS location data.
    
    Args:
        username: API username (or set KINEIS_USERNAME env var)
        password: API password (or set KINEIS_PASSWORD env var)
        auth_url: OAuth2 token endpoint URL
        api_url: Telemetry API base URL
        client_id: OAuth2 client ID (default: "api-telemetry")
    """
    
    def __init__(
        self,
        username: Optional[str] = None,
        password: Optional[str] = None,
        auth_url: Optional[str] = None,
        api_url: Optional[str] = None,
        client_id: str = "api-telemetry",
    ):
        self.username = username or os.environ.get("KINEIS_USERNAME")
        self.password = password or os.environ.get("KINEIS_PASSWORD")
        self.auth_url = auth_url or os.environ.get("KINEIS_AUTH_URL", DEFAULT_AUTH_URL)
        self.api_url = api_url or os.environ.get("KINEIS_API_URL", DEFAULT_API_URL)
        self.client_id = client_id
        
        if not self.username or not self.password:
            raise ValueError(
                "Username and password are required. "
                "Pass them as arguments or set KINEIS_USERNAME and KINEIS_PASSWORD env vars."
            )
        
        self._access_token: Optional[str] = None
        self._token_expires_at: float = 0.0

        # Realtime polling state
        self._checkpoint: int = 0

    async def _get_token(self, client: httpx.AsyncClient) -> str:
        """Get a valid access token, refreshing if necessary."""
        if self._access_token and time.time() < (self._token_expires_at - 60):
            return self._access_token

        logger.debug("Fetching new access token")

        try:
            response = await client.post(
                self.auth_url,
                data={
                    "grant_type": "password",
                    "client_id": self.client_id,
                    "username": self.username,
                    "password": self.password,
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            response.raise_for_status()

            token_data = response.json()
            self._access_token = token_data["access_token"]
            expires_in = token_data.get("expires_in", 300)
            self._token_expires_at = time.time() + expires_in

            logger.debug("Token obtained, expires in %ss", expires_in)
            return self._access_token

        except httpx.HTTPStatusError as e:
            raise KineisAuthError(f"Authentication failed: {e}") from e
        except (KeyError, ValueError) as e:
            raise KineisAuthError(f"Invalid token response: {e}") from e

    async def _api_request(
        self,
        client: httpx.AsyncClient,
        method: str,
        endpoint: str,
        json_data: Optional[dict] = None,
        params: Optional[dict] = None,
    ) -> dict:
        """Make an authenticated API request."""
        url = f"{self.api_url.rstrip('/')}/{endpoint.lstrip('/')}"
        token = await self._get_token(client)

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

        logger.debug("API request: %s %s", method, endpoint)

        response = await client.request(
            method=method,
            url=url,
            json=json_data,
            params=params,
            headers=headers,
        )

        if response.status_code >= 400:
            error_data = None
            try:
                error_data = response.json()
            except ValueError:
                pass

            msg = f"API error: {response.status_code}"
            if error_data and "msg" in error_data:
                msg = f"{msg} - {error_data['msg']}"

            raise KineisAPIError(msg, response.status_code, error_data)

        return response.json()

    async def get_devices(self) -> list[Device]:
        """
        Get all accessible devices.

        Returns a list of Device objects including owned devices and
        devices shared by other customers.
        """
        async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
            response = await self._api_request(client, "POST", "/retrieve-device-list")

        devices = []
        for item in response.get("contents", []):
            device = Device(
                device_uid=item["deviceUid"],
                device_ref=item["deviceRef"],
                program_ref=item.get("programRef"),
                customer_id=item.get("customerId"),
                customer_name=item.get("customerName"),
                modems=item.get("modems", []),
            )
            devices.append(device)

        logger.info("Retrieved %d devices", len(devices))
        return devices
    
    def _parse_location(self, msg: dict) -> GPSLocation:
        """Parse a message into a GPSLocation object."""
        # Parse timestamps
        msg_ts = msg.get("msgTs")
        acq_ts = msg.get("acqTs")
        
        msg_datetime = datetime.fromtimestamp(msg_ts / 1000, tz=timezone.utc) if msg_ts else None
        acq_datetime = datetime.fromtimestamp(acq_ts / 1000, tz=timezone.utc) if acq_ts else None
        
        return GPSLocation(
            device_uid=msg.get("deviceUid"),
            device_ref=msg.get("deviceRef"),
            modem_ref=msg.get("modemRef"),
            message_uid=msg.get("deviceMsgUid"),
            message_timestamp=msg_datetime,
            acquisition_timestamp=acq_datetime,
            # GPS location
            latitude=msg.get("gpsLocLat"),
            longitude=msg.get("gpsLocLon"),
            altitude=msg.get("gpsLocAlt"),
            speed=msg.get("gpsLocSpeed"),
            heading=msg.get("gpsLocHeading"),
            # Doppler location
            doppler_latitude=msg.get("dopplerLocLat"),
            doppler_longitude=msg.get("dopplerLocLon"),
            doppler_error_radius=msg.get("dopplerLocErrorRadius"),
            raw_data=msg,
        )
    
    async def fetch_realtime(
        self,
        checkpoint: int = 0,
        device_refs: Optional[list[str]] = None,
        include_doppler: bool = True,
    ) -> tuple[list[GPSLocation], int]:
        """
        Fetch realtime GPS locations since the given checkpoint.

        Args:
            checkpoint: Checkpoint from previous call (0 for first call, returns last 6 hours)
            device_refs: Optional list of device references to filter by
            include_doppler: Whether to include Doppler location data

        Returns:
            Tuple of (list of GPSLocation objects, new checkpoint for next call)
        """
        criteria = {
            "fromCheckpoint": checkpoint,
            "retrieveGpsLoc": True,
            "retrieveDoppler": include_doppler,
            "datetimeFormat": "TS",
        }

        if device_refs:
            criteria["deviceRefs"] = device_refs

        async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as client:
            response = await self._api_request(
                client, "POST", "/retrieve-realtime", json_data=criteria
            )

        new_checkpoint = response.get("checkpoint", checkpoint)
        messages = response.get("contents", [])

        locations = [self._parse_location(msg) for msg in messages]
        locations_with_data = [loc for loc in locations if loc.has_location]

        logger.info(
            "Realtime fetch: %d locations with GPS/Doppler (of %d total messages)",
            len(locations_with_data),
            len(locations),
        )

        return locations_with_data, new_checkpoint

    async def poll_realtime_locations(
        self,
        interval_seconds: int = 60,
        device_refs: Optional[list[str]] = None,
        include_doppler: bool = True,
        max_iterations: Optional[int] = None,
    ) -> AsyncIterator[list[GPSLocation]]:
        """
        Async generator that polls for realtime GPS locations at regular intervals.

        This handles the checkpoint mechanism automatically. Each yield returns
        only new locations since the previous poll.

        Args:
            interval_seconds: Seconds between polls (minimum 60, API rate limit)
            device_refs: Optional list of device references to filter by
            include_doppler: Whether to include Doppler location data
            max_iterations: Maximum number of polls (None for infinite)

        Yields:
            List of new GPSLocation objects

        Example:
            async for locations in client.poll_realtime_locations(interval_seconds=300):
                for loc in locations:
                    await forward_to_other_system(loc)
        """
        if interval_seconds < 60:
            logger.warning(
                "Interval %ds is below API minimum of 60s, using 60s",
                interval_seconds,
            )
            interval_seconds = 60

        iteration = 0

        while max_iterations is None or iteration < max_iterations:
            try:
                locations, self._checkpoint = await self.fetch_realtime(
                    checkpoint=self._checkpoint,
                    device_refs=device_refs,
                    include_doppler=include_doppler,
                )

                yield locations

            except KineisAPIError as e:
                logger.error("API error during poll: %s", e)

            iteration += 1

            if max_iterations is None or iteration < max_iterations:
                logger.debug("Sleeping %ds until next poll", interval_seconds)
                await asyncio.sleep(interval_seconds)

    async def fetch_bulk(
        self,
        from_datetime: datetime,
        to_datetime: Optional[datetime] = None,
        device_refs: Optional[list[str]] = None,
        include_doppler: bool = True,
        page_size: int = 100,
    ) -> list[GPSLocation]:
        """
        Fetch historical GPS locations for a time range.

        Handles pagination automatically to retrieve all matching records.

        Args:
            from_datetime: Start of time range (UTC)
            to_datetime: End of time range (UTC), defaults to now
            device_refs: Optional list of device references to filter by
            include_doppler: Whether to include Doppler location data
            page_size: Number of records per page

        Returns:
            List of all GPSLocation objects in the time range
        """
        from_ts = int(from_datetime.timestamp() * 1000)
        to_ts = int(
            (to_datetime or datetime.now(timezone.utc)).timestamp() * 1000
        )

        all_locations = []
        cursor: Optional[str] = None

        async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as client:
            while True:
                criteria = {
                    "fromTs": from_ts,
                    "toTs": to_ts,
                    "retrieveGpsLoc": True,
                    "retrieveDoppler": include_doppler,
                    "datetimeFormat": "TS",
                    "pagination": {"first": page_size},
                }

                if cursor:
                    criteria["pagination"]["after"] = cursor

                if device_refs:
                    criteria["deviceRefs"] = device_refs

                response = await self._api_request(
                    client, "POST", "/retrieve-bulk", json_data=criteria
                )

                messages = response.get("contents", [])
                locations = [self._parse_location(msg) for msg in messages]
                locations_with_data = [
                    loc for loc in locations if loc.has_location
                ]
                all_locations.extend(locations_with_data)

                page_info = response.get("pageInfo", {})
                if page_info.get("hasNextPage"):
                    cursor = page_info.get("endCursor")
                    logger.debug(
                        "Fetched page, %d total so far", len(all_locations)
                    )
                else:
                    break

        logger.info("Bulk fetch complete: %d locations", len(all_locations))
        return all_locations

    def reset_checkpoint(self) -> None:
        """Reset the realtime checkpoint to 0 (will fetch last 6 hours on next poll)."""
        self._checkpoint = 0
        logger.info("Checkpoint reset to 0")


# Convenience function for one-off usage
async def fetch_latest_locations(
    username: Optional[str] = None,
    password: Optional[str] = None,
    device_refs: Optional[list[str]] = None,
) -> list[GPSLocation]:
    """
    Convenience function to fetch the latest GPS locations (last 6 hours).

    Args:
        username: API username (or set KINEIS_USERNAME env var)
        password: API password (or set KINEIS_PASSWORD env var)
        device_refs: Optional list of device references to filter by

    Returns:
        List of GPSLocation objects
    """
    client = KineisClient(username=username, password=password)
    locations, _ = await client.fetch_realtime(
        checkpoint=0, device_refs=device_refs
    )
    return locations


async def _main_async(parser: "argparse.ArgumentParser", args: "argparse.Namespace") -> None:
    client = KineisClient(username=args.username, password=args.password)

    if args.list_devices:
        devices = await client.get_devices()
        print(f"\nFound {len(devices)} devices:")
        for d in devices:
            print(f"  - {d.device_ref} (UID: {d.device_uid}, Program: {d.program_ref})")

    elif args.fetch_latest:
        locations = await fetch_latest_locations(
            args.username, args.password
        )
        print(f"\nFound {len(locations)} locations:")
        for loc in locations:
            loc_type = "GPS" if loc.has_gps else "Doppler"
            print(
                f"  - {loc.device_ref}: {loc.lat:.5f}, {loc.lon:.5f} ({loc_type}) @ {loc.message_timestamp}"
            )

    elif args.poll:
        print(f"\nPolling for updates every {args.interval}s (Ctrl+C to stop)...")
        async for locations in client.poll_realtime_locations(
            interval_seconds=args.interval
        ):
            if locations:
                print(
                    f"\n[{datetime.now()}] Received {len(locations)} new locations:"
                )
                for loc in locations:
                    loc_type = "GPS" if loc.has_gps else "Doppler"
                    print(
                        f"  - {loc.device_ref}: {loc.lat:.5f}, {loc.lon:.5f} ({loc_type})"
                    )
            else:
                print(".", end="", flush=True)

    else:
        parser.print_help()


if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Fetch GPS locations from Kineis API"
    )
    parser.add_argument("--username", "-u", help="API username")
    parser.add_argument("--password", "-p", help="API password")
    parser.add_argument(
        "--list-devices", action="store_true", help="List all devices"
    )
    parser.add_argument(
        "--fetch-latest",
        action="store_true",
        help="Fetch latest locations",
    )
    parser.add_argument(
        "--poll", action="store_true", help="Poll for realtime updates"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=300,
        help="Poll interval in seconds",
    )
    args = parser.parse_args()

    try:
        asyncio.run(_main_async(parser, args))
    except KeyboardInterrupt:
        print("\nStopped.")
    except (KineisAuthError, KineisAPIError) as e:
        print(f"Error: {e}")
        exit(1)