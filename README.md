# EdgeTech Integration Documentation

## Overview

This document describes the integration between EdgeTech's Trap Tracker API and our buoy tracking system (Earth Ranger destinations). It focuses on:

1. **EdgeTech API Connection** - Authentication and data retrieval mechanisms
2. **Data Structure** - Expected format from EdgeTech
3. **Record Filtering** - Which buoy records we process vs. discard
4. **Observation Mapping** - How EdgeTech data transforms into our observation format

The integration runs automatically every 3 minutes via a scheduled task and syncs the last 90 days of buoy data.

---

## EdgeTech API Connection

### Authentication

**OAuth2 Flow**
- **Provider**: Auth0 (EdgeTech)
- **Token Endpoint**: `https://trap-tracker.auth0.com/oauth/token`
- **Grant Type**: `refresh_token`
- **Required Scopes**: `offline_access database:dump openid profile email`

**Configuration**
```python
EdgeTechAuthConfiguration:
  - token_json: OAuth token JSON (contains access_token, refresh_token, expires_at)
  - client_id: EdgeTech API client identifier
```

**Token Management**
- Tokens expire after a set period (tracked via `expires_at` timestamp)
- Automatic refresh occurs when `current_time >= expires_at`
- Refresh uses the `refresh_token` from the original token JSON

**Refresh Request**
```python
POST https://trap-tracker.auth0.com/oauth/token
{
  "client_id": "<client_id>",
  "refresh_token": "<refresh_token>",
  "redirect_uri": "https://app.local",
  "scope": "offline_access database:dump openid profile email",
  "grant_type": "refresh_token"
}
```

### Data Retrieval Process

EdgeTech provides buoy data through a **database dump mechanism** that involves multiple steps:

#### Step 1: Initiate Database Dump
```python
POST {api_base_url}/v1/database-dump/tasks
Headers:
  Authorization: Bearer <access_token>

Response: 303 See Other
Location: /v1/database-dump/tasks/{task_id}
```

#### Step 2: Poll for Completion
```python
GET {api_base_url}/v1/database-dump/tasks/{task_id}
Headers:
  Authorization: Bearer <access_token>

Response (while processing): 200 OK
Response (when ready): 303 See Other
Location: <download_url>
```

**Polling Configuration**
- Maximum attempts: 60 (configurable via `num_get_retry`)
- Retry interval: 1 second between attempts
- Total max wait time: ~60 seconds

#### Step 3: Download Data
```python
GET <download_url>

Response Headers:
  Content-Disposition: attachment; filename="database-dump-{timestamp}.json.gz"

Response Body: GZIP-compressed JSON
```

#### Step 4: Process Data
1. Download GZIP file
2. Decompress in-memory
3. Parse JSON array
4. Convert each item to `Buoy` object
5. Apply temporal filters (optional `start_datetime`)

**Download Implementation**
```python
async def download_data(start_datetime: Optional[datetime]) -> List[Buoy]:
    # 1. Get valid token (refresh if needed)
    token = await self.get_token()
    
    # 2. Initiate dump
    # 3. Poll until ready
    # 4. Download GZIP file
    # 5. Decompress and parse JSON
    # 6. Filter by start_datetime if provided
    
    return buoys
```

---

## EdgeTech Data Structure

### Buoy Record Format

EdgeTech provides buoy data as a JSON array. Each element represents a buoy with its complete state and change history.

**Top-Level Structure**
```json
{
  "currentState": { /* CurrentState object */ },
  "serialNumber": "ET-12345",
  "userId": "user@example.com",
  "changeRecords": [ /* Array of ChangeRecord objects */ ]
}
```

### CurrentState Object

The `currentState` contains all current information about a buoy:

```json
{
  "etag": "\"abc123def456\"",
  "isDeleted": false,
  "serialNumber": "ET-12345",
  "releaseCommand": "RELEASE_CMD",
  "statusCommand": "STATUS_CMD",
  "idCommand": "ID_CMD",
  "isNfcTag": false,
  "modelNumber": "ET-2000",
  "dateOfManufacture": "2024-01-15T00:00:00Z",
  "dateOfBatteryChange": "2024-06-01T00:00:00Z",
  "dateDeployed": "2025-10-01T08:30:00Z",
  "isDeployed": true,
  "dateRecovered": null,
  "recoveredLatDeg": null,
  "recoveredLonDeg": null,
  "recoveredRangeM": null,
  "dateStatus": "2025-10-20T10:00:00Z",
  "statusRangeM": 150.5,
  "statusIsTilted": false,
  "statusBatterySoC": 85,
  "lastUpdated": "2025-10-20T10:00:00Z",
  "latDeg": 42.123456,
  "lonDeg": -70.654321,
  "endLatDeg": 42.234567,
  "endLonDeg": -70.765432,
  "isTwoUnitLine": true,
  "endUnit": "ET-12346",
  "startUnit": null
}
```

**Key Fields**

| Field | Type | Description |
|-------|------|-------------|
| `etag` | string | Version identifier (quotes stripped during parsing) |
| `isDeleted` | boolean | Whether the buoy has been deleted from the system |
| `serialNumber` | string | Unique buoy identifier |
| `isDeployed` | boolean | Whether the buoy is currently deployed in water |
| `lastUpdated` | datetime | Last modification timestamp (ISO 8601) |
| `latDeg` / `lonDeg` | float? | Deployment location (start point) |
| `endLatDeg` / `endLonDeg` | float? | End location (for two-unit lines) |
| `recoveredLatDeg` / `recoveredLonDeg` | float? | Recovery location |
| `isTwoUnitLine` | boolean? | Whether this is a two-unit line system |
| `endUnit` | string? | Serial number of the end unit buoy |
| `startUnit` | string? | Serial number of the start unit buoy |
| `statusBatterySoC` | int? | Battery state of charge (0-100%) |
| `statusIsTilted` | boolean? | Whether the buoy is tilted |

**Location Hierarchy**
1. **Deployed Location**: `latDeg` / `lonDeg` (primary deployment point)
2. **End Location**: `endLatDeg` / `endLonDeg` (for two-unit systems)
3. **Recovery Location**: `recoveredLatDeg` / `recoveredLonDeg` (when retrieved)

### Two-Unit Line Systems

EdgeTech supports buoy systems with two physical units connected by a line:

**Start Unit Record**
```json
{
  "serialNumber": "ET-12345",
  "currentState": {
    "latDeg": 42.123456,
    "lonDeg": -70.654321,
    "isTwoUnitLine": true,
    "endUnit": "ET-12346",
    "startUnit": null
  }
}
```

**End Unit Record**
```json
{
  "serialNumber": "ET-12346",
  "currentState": {
    "latDeg": 42.234567,
    "lonDeg": -70.765432,
    "isTwoUnitLine": true,
    "endUnit": null,
    "startUnit": "ET-12345"
  }
}
```

**Processing Logic**
- Both units share the same `userId`
- The start unit (`startUnit: null`) initiates processing
- The end unit (`endUnit: null`) is skipped (processed as part of start unit)
- If end unit is missing, the start unit is skipped with a warning

---

## Record Filtering

### Records We Process

✅ **Included Records**
- `isDeleted: false` - Active buoys in the system
- `isDeployed: true` - Currently deployed in water
- Has valid location data (at least one of):
  - `latDeg` AND `lonDeg` (deployment location)
  - `endLatDeg` AND `endLonDeg` (two-unit end location)
  - `recoveredLatDeg` AND `recoveredLonDeg` (recovery location)

✅ **Additional Processing Rules**
- Only the **most recent state** per buoy (grouped by `serialNumber` + `userId`)
  - Key: `{serialNumber}/{hashedUserId}`
  - Comparison: Latest `lastUpdated` timestamp wins
- Temporal filter applied if configured:
  - Only buoys with `lastUpdated > start_datetime`
  - Default: Last 90 days of data

### Records We Discard

❌ **Excluded Records**

| Condition | Reason | Log Level |
|-----------|--------|-----------|
| `isDeleted: true` | Buoy removed from system | WARNING |
| `isDeployed: false` | Buoy not in water | WARNING |
| No location data | Cannot track position | WARNING |
| Older duplicate | Superseded by newer state | (Implicit filtering) |

**Filter Implementation**
```python
def _should_skip_buoy(record: Buoy) -> Tuple[bool, Optional[str]]:
    if record.currentState.isDeleted:
        return True, f"Deleted buoy {record.serialNumber}"
    
    if not record.currentState.isDeployed:
        return True, f"Not deployed {record.serialNumber}"
    
    if not record.has_location:
        return True, f"No location data {record.serialNumber}"
    
    return False, None
```

**Location Validation**
```python
@property
def has_location(self) -> bool:
    has_deployed = (self.latDeg is not None and self.lonDeg is not None)
    has_end = (self.endLatDeg is not None and self.endLonDeg is not None)
    has_recovered = (self.recoveredLatDeg is not None and self.recoveredLonDeg is not None)
    
    return has_deployed or has_end or has_recovered
```

### Deduplication Strategy

**Step 1: Group by Buoy Identity**
```python
key = f"{serialNumber}/{hashed_user_id}"
# Example: "ET-12345/a1b2c3d4"
```

**Step 2: Select Latest State**
```python
if record.currentState.lastUpdated > existing.currentState.lastUpdated:
    latest[key] = record
```

**Result**: One record per unique buoy (serial + user combination)

---

## Change Detection and Operations

After filtering, we compare EdgeTech data with our existing Earth Ranger records to determine required actions:

### Operation Categories

**1. DEPLOY (New Deployments)**
- Buoy exists in EdgeTech but **not** in Earth Ranger
- Checked device IDs:
  - Primary: `edgetech_{serialNumber}_{hashedUserId}_A`
  - Standard: `edgetech_{serialNumber}_{hashedUserId}`
- Action: Create deployment observations

**2. UPDATE (Location Changes)**
- Buoy exists in both systems
- EdgeTech `lastUpdated` > Earth Ranger `last_updated`
- Location coordinates have changed
- Action: Create update observations

**3. HAUL (Retrievals)**
- Buoy exists in Earth Ranger but **not** in EdgeTech filtered data
- Indicates the buoy was recovered or removed
- Action: Create retrieval observations

**4. NO-OP (Skip)**
- Buoy exists in both systems
- No location change detected
- Same `lastUpdated` timestamp
- Action: None (logged and skipped)

---

## Observation Mapping

### Our Observation Schema

We transform EdgeTech buoy data into standardized observation records:

```json
{
  "source_name": "<subject_uuid>",
  "source": "<device_id>",
  "subject_type": "ropeless_buoy_gearset",
  "recorded_at": "<iso8601_timestamp>",
  "source_type": "ropeless_buoy",
  "location": {
    "lat": <latitude>,
    "lon": <longitude>
  },
  "additional": {
    "event_type": "trap_deployed" | "trap_retrieved",
    "raw": { /* Original EdgeTech data */ }
  }
}
```

### Field Mapping

| Our Field | EdgeTech Source | Notes |
|-----------|----------------|-------|
| `source_name` | Generated UUID | Unique subject identifier for the gearset |
| `source` | Derived | Device ID: `{serialNumber}_{hashedUserId}[_A/B]` |
| `subject_type` | Static | Always `"ropeless_buoy_gearset"` |
| `source_type` | Static | Always `"ropeless_buoy"` |
| `recorded_at` | `currentState.lastUpdated` | Microseconds removed for consistency |
| `location.lat` | `currentState.latDeg` or `endLatDeg` | Depends on device position |
| `location.lon` | `currentState.lonDeg` or `endLonDeg` | Depends on device position |
| `event_type` | Derived | `trap_deployed` or `trap_retrieved` |
| `raw` | `currentState` | Complete state (minus `changeRecords`) |

### Device ID Generation

Device IDs uniquely identify each tracking device within a buoy system.

#### User ID Hashing

**Purpose**: Anonymize user identifiers while maintaining uniqueness

```python
def get_hashed_user_id(user_id: str) -> str:
    # Convert to hex
    user_id_hex = user_id.encode("utf-8").hex()
    
    # Hash with Hashids (min length: 8)
    hashids = Hashids(min_length=8)
    hashed = hashids.encode_hex(user_id_hex)
    
    return hashed  # e.g., "a1b2c3d4"
```

#### Single-Unit Buoys

For standard buoys (not two-unit lines), we create **two devices** representing start and end of the trap line:

```
Device A: edgetech_{serialNumber}_{hashedUserId}_A
Device B: edgetech_{serialNumber}_{hashedUserId}_B
```

**Example**:
```
EdgeTech Serial: ET-12345
User ID: user@example.com
Hashed User ID: a1b2c3d4

Device A: edgetech_ET-12345_a1b2c3d4_A (start point)
Device B: edgetech_ET-12345_a1b2c3d4_B (end point)
```

**Observation Creation**:
- Both devices share the same `subject_name` (UUID)
- Device A uses `latDeg` / `lonDeg`
- Device B uses `endLatDeg` / `endLonDeg` (if present)
- If end coordinates missing, only Device A observation created

#### Two-Unit Line Buoys

For two-unit systems, each physical buoy becomes a separate device:

```
Start Device: edgetech_{startSerialNumber}_{hashedUserId}
End Device: edgetech_{endSerialNumber}_{hashedUserId}
```

**Example**:
```
Start Buoy Serial: ET-12345
End Buoy Serial: ET-12346
User ID: user@example.com
Hashed User ID: a1b2c3d4

Start Device: edgetech_ET-12345_a1b2c3d4
End Device: edgetech_ET-12346_a1b2c3d4
```

**Observation Creation**:
- Both devices share the same `subject_name` (UUID)
- Start device uses start buoy's `latDeg` / `lonDeg`
- End device uses end buoy's `latDeg` / `lonDeg`
- Processing triggered only by start unit record

### Event Types

#### trap_deployed

Created when:
- New buoy appears in EdgeTech (DEPLOY operation)
- Buoy location updated in EdgeTech (UPDATE operation)

**Indicates**: Buoy is actively deployed in water

```json
{
  "additional": {
    "event_type": "trap_deployed",
    "raw": { /* currentState */ }
  }
}
```

#### trap_retrieved

Created when:
- Buoy disappears from EdgeTech filtered data (HAUL operation)
- Buoy no longer meets deployment criteria

**Indicates**: Buoy has been recovered from water

```json
{
  "additional": {
    "event_type": "trap_retrieved"
  }
}
```

---

## Mapping Examples

### Example 1: Single-Unit Deployment

**EdgeTech Input**:
```json
{
  "currentState": {
    "serialNumber": "ET-12345",
    "isDeleted": false,
    "isDeployed": true,
    "lastUpdated": "2025-10-20T10:00:00Z",
    "latDeg": 42.123456,
    "lonDeg": -70.654321,
    "endLatDeg": 42.234567,
    "endLonDeg": -70.765432,
    "isTwoUnitLine": false
  },
  "serialNumber": "ET-12345",
  "userId": "user@example.com"
}
```

**Our Observations** (2 devices):
```json
[
  {
    "source_name": "550e8400-e29b-41d4-a716-446655440000",
    "source": "edgetech_ET-12345_a1b2c3d4_A",
    "subject_type": "ropeless_buoy_gearset",
    "source_type": "ropeless_buoy",
    "recorded_at": "2025-10-20T10:00:00+00:00",
    "location": {
      "lat": 42.123456,
      "lon": -70.654321
    },
    "additional": {
      "event_type": "trap_deployed",
      "raw": { /* currentState */ }
    }
  },
  {
    "source_name": "550e8400-e29b-41d4-a716-446655440000",
    "source": "edgetech_ET-12345_a1b2c3d4_B",
    "subject_type": "ropeless_buoy_gearset",
    "source_type": "ropeless_buoy",
    "recorded_at": "2025-10-20T10:00:00+00:00",
    "location": {
      "lat": 42.234567,
      "lon": -70.765432
    },
    "additional": {
      "event_type": "trap_deployed",
      "raw": { /* currentState */ }
    }
  }
]
```

### Example 2: Two-Unit Deployment

**EdgeTech Input** (Start Unit):
```json
{
  "currentState": {
    "serialNumber": "ET-12345",
    "isDeleted": false,
    "isDeployed": true,
    "lastUpdated": "2025-10-20T10:00:00Z",
    "latDeg": 42.123456,
    "lonDeg": -70.654321,
    "isTwoUnitLine": true,
    "endUnit": "ET-12346",
    "startUnit": null
  },
  "serialNumber": "ET-12345",
  "userId": "user@example.com"
}
```

**EdgeTech Input** (End Unit):
```json
{
  "currentState": {
    "serialNumber": "ET-12346",
    "isDeleted": false,
    "isDeployed": true,
    "lastUpdated": "2025-10-20T10:00:00Z",
    "latDeg": 42.234567,
    "lonDeg": -70.765432,
    "isTwoUnitLine": true,
    "endUnit": null,
    "startUnit": "ET-12345"
  },
  "serialNumber": "ET-12346",
  "userId": "user@example.com"
}
```

**Our Observations** (2 devices from different buoys):
```json
[
  {
    "source_name": "550e8400-e29b-41d4-a716-446655440000",
    "source": "edgetech_ET-12345_a1b2c3d4",
    "subject_type": "ropeless_buoy_gearset",
    "source_type": "ropeless_buoy",
    "recorded_at": "2025-10-20T10:00:00+00:00",
    "location": {
      "lat": 42.123456,
      "lon": -70.654321
    },
    "additional": {
      "event_type": "trap_deployed",
      "raw": { /* start unit currentState */ }
    }
  },
  {
    "source_name": "550e8400-e29b-41d4-a716-446655440000",
    "source": "edgetech_ET-12346_a1b2c3d4",
    "subject_type": "ropeless_buoy_gearset",
    "source_type": "ropeless_buoy",
    "recorded_at": "2025-10-20T10:00:00+00:00",
    "location": {
      "lat": 42.234567,
      "lon": -70.765432
    },
    "additional": {
      "event_type": "trap_deployed",
      "raw": { /* end unit currentState */ }
    }
  }
]
```

### Example 3: Buoy Retrieval (Haul)

When a buoy disappears from EdgeTech data, we create retrieval observations using Earth Ranger's existing record:

**Earth Ranger Record**:
```json
{
  "device_id": "edgetech_ET-12345_a1b2c3d4_A",
  "last_updated": "2025-10-19T15:00:00Z",
  "location": {
    "latitude": 42.123456,
    "longitude": -70.654321
  }
}
```

**Our Observation**:
```json
{
  "source_name": "<existing_display_id>",
  "source": "edgetech_ET-12345_a1b2c3d4_A",
  "subject_type": "ropeless_buoy_gearset",
  "source_type": "ropeless_buoy",
  "recorded_at": "2025-10-20T10:00:00+00:00",
  "location": {
    "lat": 42.123456,
    "lon": -70.654321
  },
  "additional": {
    "event_type": "trap_retrieved"
  }
}
```

**Note**: Uses last known location from Earth Ranger, not EdgeTech recovery location

---

## Data Quality and Edge Cases

### Handling Missing Data

| Missing Field | Behavior |
|--------------|----------|
| `latDeg` / `lonDeg` | Buoy filtered out (no location) |
| `endLatDeg` / `endLonDeg` | Single observation created (only Device A) |
| End unit in two-unit system | Start unit skipped with warning |
| `lastUpdated` | Required field - parsing would fail |

### Timestamp Handling

**Input Format**: ISO 8601 with timezone
```
2025-10-20T10:00:00.123456Z
```

**Normalization**:
```python
recorded_at = state.lastUpdated.replace(microsecond=0)
# Result: 2025-10-20T10:00:00Z
```

**Purpose**: Ensure consistent timestamp comparison and prevent false positives in change detection

### Location Change Detection

We skip UPDATE operations if location hasn't changed:

```python
if (er_device_lat == edgetech_buoy_lat and 
    er_device_long == edgetech_buoy_long):
    # No change - skip update
    continue
```

**Comparison**: Direct float equality (no tolerance threshold)

### Two-Unit Coordination

**Requirements**:
- Both start and end units must exist in EdgeTech data
- Both must have same `userId`
- End unit lookup: `f"{endUnitSerial}/{hashedUserId}"`

**Failure Modes**:
```python
if not end_unit_buoy:
    logger.warning(f"End unit {endUnit} not found for {serialNumber}, skipping")
    continue
```

Result: Entire system skipped if either unit missing

---

## Processing Flow Summary

```
┌─────────────────────────────────────────────────────────────┐
│ 1. DOWNLOAD FROM EDGETECH                                   │
│    - OAuth token refresh                                    │
│    - Database dump request                                  │
│    - Poll until ready                                       │
│    - Download GZIP                                          │
│    - Parse JSON → List[Buoy]                                │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│ 2. FILTER EDGETECH DATA                                     │
│    ✓ isDeleted = false                                      │
│    ✓ isDeployed = true                                      │
│    ✓ Has location data                                      │
│    ✓ Most recent per serial/user                            │
│    ✓ Within time window (90 days)                           │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│ 3. FETCH EARTH RANGER GEARS                                 │
│    - GET /api/v1.0/gear/                                    │
│    - Filter manufacturer = "edgetech"                       │
│    - Map by device_id                                       │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│ 4. IDENTIFY OPERATIONS                                      │
│    DEPLOY:  In EdgeTech, not in ER                          │
│    UPDATE:  In both, EdgeTech newer + location changed      │
│    HAUL:    In ER, not in EdgeTech                          │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│ 5. GENERATE OBSERVATIONS                                    │
│    For each operation:                                      │
│    - Determine device IDs                                   │
│    - Extract locations                                      │
│    - Set event_type                                         │
│    - Build observation JSON                                 │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│ 6. SEND TO DESTINATIONS                                     │
│    - One observation list per destination                   │
│    - Log activity to Gundi                                  │
│    - Return counts                                          │
└─────────────────────────────────────────────────────────────┘
```

---

## Configuration

### Sync Window

**Default**: 30 minutes
**Override**: 90 days (configured in handler)

```python
# In action_pull_edgetech_observations:
action_config.minutes_to_sync = 90 * 24 * 60  # 129,600 minutes
```

**Reasoning**: Ensures full historical sync on each run to catch any missed updates

### Polling Limits

```python
num_get_retry: 60  # Maximum polling attempts
retry_interval: 1 second
```

**Total max wait**: 60 seconds for dump generation

### Execution Schedule

```python
@crontab_schedule("*/3 * * * *")  # Every 3 minutes
```

**Frequency**: 480 executions per day

---

## Error Handling and Logging

### Critical Errors

**InvalidCredentials**
```python
# Raised when OAuth token refresh fails
return {"valid_credentials": False, "error": str(e)}
```

**Missing End Unit**
```python
logger.warning(
    f"End unit {endUnit} not found for {serialNumber}, skipping deployment."
)
continue  # Skip entire system, don't fail
```

### Information Logging

**Filtered Buoys**
```python
logger.warning(f"Skipping deleted buoy {serialNumber}. Last updated: {lastUpdated}")
logger.warning(f"Skipping buoy {serialNumber} that is not deployed.")
logger.warning(f"Skipping buoy {serialNumber} with no location data.")
```

**Operation Counts**
```python
logger.info(f"Buoys to deploy: {to_deploy}")
logger.info(f"Buoys to haul: {to_haul}")
logger.info(f"Buoys to update: {to_update}")
```

**Observations Summary**
```python
logger.info(
    f"Sending {len(observations)} observations:\n"
    f"{json.dumps(observations, indent=4, default=str)}"
)
```

### Activity Logging

Each execution logs to Gundi:
```python
await log_action_activity(
    integration_id=integration.id,
    action_id="pull_edgetech",
    level=LogLevel.INFO,
    title="Pulled data from EdgeTech API"
)
```

---

## API Reference

### EdgeTech API Endpoints

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/oauth/token` | POST | Refresh authentication token |
| `/v1/database-dump/tasks` | POST | Initiate database dump |
| `/v1/database-dump/tasks/{id}` | GET | Check dump status |
| Download URL | GET | Retrieve GZIP file |

### Earth Ranger API Endpoints

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/v1.0/gear/` | GET | List existing buoy gears |

### Response Codes

| Code | Meaning | Action |
|------|---------|--------|
| 200 | OK | Continue processing |
| 303 | See Other | Follow Location header |
| 401 | Unauthorized | Refresh token |
| 404 | Not Found | Resource doesn't exist |

---

## Conclusion

This integration provides robust synchronization between EdgeTech's Trap Tracker system and our buoy tracking platform:

✅ **Automated OAuth management** with token refresh  
✅ **Efficient database dump** mechanism for bulk data retrieval  
✅ **Intelligent filtering** to process only active, deployed buoys  
✅ **Change detection** to minimize redundant updates  
✅ **Support for complex systems** including two-unit lines  
✅ **Standardized observation format** for downstream processing  
✅ **Comprehensive logging** for monitoring and debugging  

The system runs every 3 minutes, maintaining near real-time synchronization while respecting API limits and ensuring data quality.
