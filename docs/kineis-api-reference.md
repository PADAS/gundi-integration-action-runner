# Kineis API Reference

Documentation for the CLS Kineis Telemetry API used by this integration.

## API Endpoints

| Endpoint | Description | Auth Role Required |
|----------|-------------|-------------------|
| `/retrieve-device-list` | List accessible devices | - |
| `/retrieve-bulk` | Historical data with pagination | `api_telemetry_bulk` |
| `/retrieve-bulk-count` | Count messages (with optional groupBy) | `api_telemetry_bulk` |
| `/retrieve-realtime` | Poll for new messages (checkpoint-based) | `api_telemetry_realtime` |
| `/retrieve-doppler` | Doppler location data | `api_telemetry_doppler` |
| `/retrieve-kineis-aop` | Satellite orbital parameters | None |

**Base URL:** `https://api.groupcls.com/telemetry/api/v1`  
**Auth URL:** `https://account.groupcls.com/auth/realms/cls/protocol/openid-connect/token`

## Device identification

The API does **not** expose a per-device human-friendly name or label. Identifiers available:

| Source | Field | Description |
|--------|--------|-------------|
| Messages (bulk/realtime) | `deviceRef` | Provider device reference (e.g. Argos ID); string, stable identifier |
| Messages | `deviceUid` | Internal numeric device ID |
| Messages | `programRef` | Program reference (e.g. `"56780"`) |
| Device list (`/retrieve-device-list`) | `deviceRef`, `deviceUid`, `programRef` | Same as above |
| Device list only | `customerName` | Name of the **customer/organization** that owns the device (e.g. "WILDLIFE COMPUTER"), not a device-specific label |

For a display name in UIs, use **`deviceRef`** (or fall back to `deviceUid`) unless you maintain your own device→name mapping.

**This integration:** The pull action calls `/retrieve-device-list` before mapping telemetry to observations and builds a `deviceUid` → `customerName` map. Each observation's **source_name** is set to `"deviceUid (customerName)"` (e.g. `"67899 (WILDLIFE COMPUTER)"`) when the device is in the list and has a non-empty `customerName`; otherwise **source_name** equals **source** (deviceRef or deviceUid).

## Message Types

The `msgType` field indicates the type and origin of a message.

| msgType | Description |
|---------|-------------|
| `operation-mo-pdrgroup` | Mobile Originated PDR (Processed Data Record) group - scheduled/periodic transmissions, typically contain full GPS fixes |
| `operation-mo-event` | Mobile Originated event - triggered transmissions (motion, geofence, alert), often shorter payloads without GPS |

### Naming Convention

- **`operation`** - Operational message (vs test/administrative)
- **`mo`** - Mobile Originated (device → satellite)
- **`pdrgroup`** - Scheduled periodic data records
- **`event`** - Event-triggered transmission

## Message Structure

Example message from `/retrieve-realtime`:

```json
{
  "deviceMsgUid": 4663303027064391301,
  "providerMsgId": 1471625421375148038,
  "msgType": "operation-mo-event",
  "deviceUid": 106499,
  "deviceRef": "45009",
  "modemRef": "45009",
  "msgDatetime": "2026-02-12T22:44:38.285",
  "acqDatetime": "2026-02-12T22:53:32.268",
  "kineisMetadata": {
    "sat": "MC",
    "mod": "LDA2",
    "level": -118.55,
    "snr": 49.15,
    "freq": 401678798.524
  },
  "rawData": "1b8f1c",
  "bitLength": 24
}
```

### Core Fields

| Field | Type | Description |
|-------|------|-------------|
| `deviceMsgUid` | int64 | Internal unique message ID |
| `providerMsgId` | int64 | Provider's message ID |
| `msgType` | string | Message type (see above) |
| `deviceUid` | int64 | Internal device ID |
| `deviceRef` | string | Device reference (e.g., Argos ID) |
| `modemRef` | string | Modem reference |
| `msgDatetime` | datetime | Timestamp set by device (UTC) |
| `acqDatetime` | datetime | Acquisition time by satellite (UTC) |
| `rawData` | string | Hex-encoded payload |
| `bitLength` | int | Payload length in bits |

### Optional Fields (when requested)

GPS Location (set `retrieveGpsLoc: true`):

| Field | Type | Description |
|-------|------|-------------|
| `gpsLocLat` | double | GPS latitude |
| `gpsLocLon` | double | GPS longitude |
| `gpsLocAlt` | double | GPS altitude |
| `gpsLocSpeed` | double | Speed |
| `gpsLocHeading` | double | Heading |
| `gpsLocDatetime` | datetime | GPS fix timestamp |

Doppler Location (set `retrieveDoppler: true`):

| Field | Type | Description |
|-------|------|-------------|
| `dopplerLocLat` | double | Doppler-calculated latitude |
| `dopplerLocLon` | double | Doppler-calculated longitude |
| `dopplerLocErrorRadius` | double | Error radius in meters |
| `dopplerLocClass` | string | Location class |

### Kineis Metadata

| Field | Description |
|-------|-------------|
| `sat` | Satellite identifier (see Satellite Table) |
| `mod` | Modulation type (e.g., LDA2) |
| `level` | Signal level (dBm) |
| `snr` | Signal-to-noise ratio |
| `freq` | Frequency (Hz) |

## Satellite Identification

| Satellite | Code | Payload |
|-----------|------|---------|
| NOAA-19 | NP | ARGOS-3 |
| METOP-B | MB | ARGOS-3 |
| METOP-C | MC | ARGOS-3 |
| SARAL | SR | ARGOS-3 |
| ANGELS | A1 | ARGOS-NEO |
| OCEANSAT-3 | O3 | ARGOS-4 |
| KINEIS-1A | 1A | KINEIS V1 |
| KINEIS-1B | 1B | KINEIS V1 |
| ... | ... | ... |

(Full list of 30+ satellites in the OpenAPI spec)

## Realtime Polling

The `/retrieve-realtime` endpoint uses a checkpoint mechanism:

1. First call: `fromCheckpoint: 0` → returns last 6 hours of data
2. Response includes new `checkpoint` value
3. Subsequent calls: pass previous `checkpoint` → returns only new messages
4. **Minimum interval: 60 seconds** between calls (API rate limit)

```python
# Pseudocode
checkpoint = 0
while True:
    response = api.retrieve_realtime(fromCheckpoint=checkpoint)
    checkpoint = response.checkpoint
    process(response.contents)
    sleep(60)  # minimum interval
```

## Raw Data Decoding

When GPS coordinates are not provided as separate fields, they may be encoded in `rawData`. Decoding depends on the specific device/tag manufacturer.

Common scenarios:
- **Event messages** (`operation-mo-event`): Often short payloads (24-48 bits) without GPS
- **PDR messages** (`operation-mo-pdrgroup`): More likely to contain full GPS data

For device-specific decoding, consult the tag manufacturer's documentation.

## Authentication

OAuth2 password grant flow:

```bash
curl -X POST $AUTH_URL \
  -d "grant_type=password" \
  -d "client_id=api-telemetry" \
  -d "username=$USERNAME" \
  -d "password=$PASSWORD"
```

Returns JWT token to use as `Authorization: Bearer <token>` header.

## References

- [CLS API Telemetry User Manual v1.2](./API-Telemetry_UserManual-v1_2.pdf)
- [Swagger UI](https://api.groupcls.com/telemetry/api/v1/q/swagger-ui/#/)
- OpenAPI spec: `api-telemetry.openapi.yaml`
