"""
Kineis/CLS API client for bulk telemetry retrieval (CONNECTORS-836).

- Authentication: username/password → Bearer token via account.groupcls.com
- Bulk telemetry: POST /telemetry/api/v1/retrieve-bulk with pagination
"""

import logging
import time
from typing import Any, Dict, List, Optional, Tuple

import httpx
import stamina

from app import settings

logger = logging.getLogger(__name__)

def _auth_path() -> str:
    return getattr(
        settings,
        "KINEIS_AUTH_PATH",
        "/auth/realms/cls/protocol/openid-connect/token",
    )


async def get_access_token(
    username: str,
    password: str,
    client_id: str = "api-telemetry",
    auth_base_url: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Obtain a Bearer token from the CLS/Kineis auth endpoint.
    Uses password grant: grant_type=password, client_id, username, password.
    Returns dict with access_token, expires_in (seconds), and optionally refresh_token.
    """
    base = auth_base_url or settings.KINEIS_AUTH_BASE_URL
    path = _auth_path()
    url = base.rstrip("/") + path
    data = {
        "grant_type": "password",
        "client_id": client_id,
        "username": username,
        "password": password,
    }
    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
        response = await client.post(
            url,
            data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        response.raise_for_status()
        return response.json()


def _token_cache_key(integration_id: str) -> str:
    return f"kineis_token:{integration_id}"


# Simple in-memory token cache: (token, expires_at_ts)
_token_cache: Dict[str, tuple] = {}


async def get_cached_token(
    integration_id: str,
    username: str,
    password: str,
    client_id: str = "api-telemetry",
    auth_base_url: Optional[str] = None,
    min_ttl_seconds: int = 60,
) -> str:
    """
    Return a valid Bearer token, using cache if still valid (with min_ttl_seconds
    until expiry). On 401 from telemetry API, caller should clear cache and retry.
    """
    key = _token_cache_key(integration_id)
    now = time.time()
    if key in _token_cache:
        token, expires_at = _token_cache[key]
        if expires_at - now >= min_ttl_seconds:
            return token
        del _token_cache[key]
    result = await get_access_token(
        username=username,
        password=password,
        client_id=client_id,
        auth_base_url=auth_base_url,
    )
    token = result["access_token"]
    expires_in = int(result.get("expires_in", 300))
    _token_cache[key] = (token, now + expires_in)
    return token


def clear_token_cache(integration_id: Optional[str] = None) -> None:
    """Clear cached token for integration_id, or all if integration_id is None."""
    if integration_id is None:
        _token_cache.clear()
        return
    key = _token_cache_key(integration_id)
    _token_cache.pop(key, None)


def _format_datetime_utc(dt: "datetime") -> str:
    """Format datetime as YYYY-MM-DDTHH:mm:ss.SSSZ (UTC)."""
    from datetime import datetime, timezone

    if getattr(dt, "tzinfo", None) is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


async def retrieve_bulk_telemetry(
    access_token: str,
    from_datetime: str,
    to_datetime: str,
    page_size: int = 100,
    device_refs: Optional[List[str]] = None,
    device_uids: Optional[List[int]] = None,
    retrieve_metadata: bool = True,
    retrieve_raw_data: bool = True,
    retrieve_gps_loc: bool = True,
    retrieve_doppler: bool = True,
    api_base_url: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Retrieve all telemetry messages in the time window via the bulk endpoint.
    Request GPS and Doppler so responses include gpsLocLat/Lon, gpsLocDatetime, etc.
    Paginates until hasNextPage is false. Returns a flat list of telemetry messages.
    """
    base = api_base_url or settings.KINEIS_API_BASE_URL
    url = base.rstrip("/") + "/telemetry/api/v1/retrieve-bulk"

    all_messages: List[Dict[str, Any]] = []
    after: Optional[str] = None

    async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as client:
        while True:
            body: Dict[str, Any] = {
                "fromDatetime": from_datetime,
                "toDatetime": to_datetime,
                "datetimeFormat": "DATETIME",
                "pagination": {"first": page_size},
                "retrieveMetadata": retrieve_metadata,
                "retrieveRawData": retrieve_raw_data,
                "retrieveGpsLoc": retrieve_gps_loc,
                "retrieveDoppler": retrieve_doppler,
            }
            if after is not None:
                body["pagination"]["after"] = after
            # API allows only one of deviceRefs or deviceUids (manual 1.3.1.2); prefer refs
            if device_refs:
                body["deviceRefs"] = device_refs
            elif device_uids:
                body["deviceUids"] = device_uids

            response = await client.post(
                url,
                json=body,
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
            )

            if response.status_code == 401:
                raise httpx.HTTPStatusError(
                    "Unauthorized",
                    request=response.request,
                    response=response,
                )

            response.raise_for_status()
            data = response.json()

            # Collect messages from this page (structure may be data.contents or data.edges/node)
            contents = data.get("contents") or data.get("data") or []
            if isinstance(contents, list):
                all_messages.extend(contents)
            else:
                edges = data.get("edges", [])
                for edge in edges:
                    node = edge.get("node") if isinstance(edge, dict) else edge
                    if node:
                        all_messages.append(node)

            page_info = data.get("pageInfo") or data.get("page_info") or {}
            has_next = page_info.get("hasNextPage", page_info.get("has_next_page", False))
            if not has_next:
                break
            after = page_info.get("endCursor") or page_info.get("end_cursor")
            if not after:
                break

    return all_messages


async def retrieve_realtime_telemetry(
    access_token: str,
    checkpoint: int = 0,
    device_refs: Optional[List[str]] = None,
    device_uids: Optional[List[int]] = None,
    retrieve_metadata: bool = True,
    retrieve_raw_data: bool = True,
    retrieve_gps_loc: bool = True,
    retrieve_doppler: bool = True,
    api_base_url: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Retrieve realtime telemetry since the given checkpoint (pull interface).
    First call with checkpoint=0 returns messages from the last 6 hours.
    Returns (list of message dicts, new_checkpoint for next call).
    """
    base = api_base_url or settings.KINEIS_API_BASE_URL
    url = base.rstrip("/") + "/telemetry/api/v1/retrieve-realtime"
    body: Dict[str, Any] = {
        "fromCheckpoint": checkpoint,
        "retrieveMetadata": retrieve_metadata,
        "retrieveRawData": retrieve_raw_data,
        "retrieveGpsLoc": retrieve_gps_loc,
        "retrieveDoppler": retrieve_doppler,
        "datetimeFormat": "DATETIME",
    }
    if device_refs:
        body["deviceRefs"] = device_refs
    elif device_uids:
        body["deviceUids"] = device_uids

    async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as client:
        response = await client.post(
            url,
            json=body,
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )
        if response.status_code == 401:
            raise httpx.HTTPStatusError(
                "Unauthorized",
                request=response.request,
                response=response,
            )
        response.raise_for_status()
        data = response.json()

    contents = data.get("contents") or []
    new_checkpoint = data.get("checkpoint", checkpoint)
    return list(contents) if isinstance(contents, list) else [], new_checkpoint


async def retrieve_device_list(
    access_token: str,
    api_base_url: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Retrieve the list of accessible devices (device list).
    Returns list of device dicts with deviceUid, deviceRef, customerName, etc.
    """
    base = api_base_url or settings.KINEIS_API_BASE_URL
    url = base.rstrip("/") + "/telemetry/api/v1/retrieve-device-list"

    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
        response = await client.post(
            url,
            json={},
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )
        if response.status_code == 401:
            raise httpx.HTTPStatusError(
                "Unauthorized",
                request=response.request,
                response=response,
            )
        response.raise_for_status()
        data = response.json()

    contents = data.get("contents") or []
    return list(contents) if isinstance(contents, list) else []


@stamina.retry(on=httpx.HTTPError, wait_initial=10.0, wait_jitter=10.0, wait_max=300.0)
async def fetch_device_list(
    integration_id: str,
    username: str,
    password: str,
    client_id: str = "api-telemetry",
    auth_base_url: Optional[str] = None,
    api_base_url: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Get Bearer token (cached) and fetch device list.
    On 401, clears cache and raises.
    """
    try:
        token = await get_cached_token(
            integration_id=integration_id,
            username=username,
            password=password,
            client_id=client_id,
            auth_base_url=auth_base_url,
            min_ttl_seconds=60,
        )
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 401:
            clear_token_cache(integration_id)
        raise

    try:
        return await retrieve_device_list(
            access_token=token,
            api_base_url=api_base_url,
        )
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 401:
            clear_token_cache(integration_id)
        raise


@stamina.retry(on=httpx.HTTPError, wait_initial=10.0, wait_jitter=10.0, wait_max=300.0)
async def fetch_telemetry(
    integration_id: str,
    username: str,
    password: str,
    from_datetime: str,
    to_datetime: str,
    page_size: int = 100,
    device_refs: Optional[List[str]] = None,
    device_uids: Optional[List[int]] = None,
    retrieve_metadata: bool = True,
    retrieve_raw_data: bool = True,
    retrieve_gps_loc: bool = True,
    retrieve_doppler: bool = True,
    client_id: str = "api-telemetry",
    auth_base_url: Optional[str] = None,
    api_base_url: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Get Bearer token (cached) and fetch all bulk telemetry in the time window.
    Requests GPS and Doppler by default so responses include location fields.
    On 401, clears cache and raises so caller can retry once after re-auth.
    """
    try:
        token = await get_cached_token(
            integration_id=integration_id,
            username=username,
            password=password,
            client_id=client_id,
            auth_base_url=auth_base_url,
            min_ttl_seconds=60,
        )
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 401:
            clear_token_cache(integration_id)
        raise

    try:
        return await retrieve_bulk_telemetry(
            access_token=token,
            from_datetime=from_datetime,
            to_datetime=to_datetime,
            page_size=page_size,
            device_refs=device_refs,
            device_uids=device_uids,
            retrieve_metadata=retrieve_metadata,
            retrieve_raw_data=retrieve_raw_data,
            retrieve_gps_loc=retrieve_gps_loc,
            retrieve_doppler=retrieve_doppler,
            api_base_url=api_base_url,
        )
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 401:
            clear_token_cache(integration_id)
        raise


@stamina.retry(on=httpx.HTTPError, wait_initial=10.0, wait_jitter=10.0, wait_max=300.0)
async def fetch_telemetry_realtime(
    integration_id: str,
    username: str,
    password: str,
    checkpoint: int = 0,
    device_refs: Optional[List[str]] = None,
    device_uids: Optional[List[int]] = None,
    retrieve_metadata: bool = True,
    retrieve_raw_data: bool = True,
    retrieve_gps_loc: bool = True,
    retrieve_doppler: bool = True,
    client_id: str = "api-telemetry",
    auth_base_url: Optional[str] = None,
    api_base_url: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Get Bearer token (cached) and fetch realtime telemetry since checkpoint.
    Returns (messages, new_checkpoint). On 401, clears cache and raises.
    """
    try:
        token = await get_cached_token(
            integration_id=integration_id,
            username=username,
            password=password,
            client_id=client_id,
            auth_base_url=auth_base_url,
            min_ttl_seconds=60,
        )
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 401:
            clear_token_cache(integration_id)
        raise

    try:
        return await retrieve_realtime_telemetry(
            access_token=token,
            checkpoint=checkpoint,
            device_refs=device_refs,
            device_uids=device_uids,
            retrieve_metadata=retrieve_metadata,
            retrieve_raw_data=retrieve_raw_data,
            retrieve_gps_loc=retrieve_gps_loc,
            retrieve_doppler=retrieve_doppler,
            api_base_url=api_base_url,
        )
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 401:
            clear_token_cache(integration_id)
        raise
