# gundi-integration-ornitela

Gundi v2 connector for Ornitela GPS bird tracking collars. Reads telemetry CSV files deposited by Ornitela into a Google Cloud Storage bucket and forwards observations to Gundi.

---

## How it works

Ornitela deposits CSV files into a GCS bucket. This connector runs on a schedule, picks up new files, processes them in chunks, and sends observations to Gundi. Each file goes through a four-stage lifecycle:

```
root/   →   in_progress/   →   archive/
                           →   dead_letter/
```

- **root/** — where Ornitela deposits raw CSV files
- **in_progress/** — a chunk is carved off the root file and placed here before processing
- **archive/** — chunk is moved here after observations are successfully sent
- **dead_letter/** — chunk is moved here if processing fails

---

## Actions

### `process_new_files` (runs every 5 minutes)
- Lists all files in the root folder of the bucket
- For each new file, carves off the first N rows (chunk size, default 3000) into `in_progress/`, writes remaining rows back to root
- Triggers `process_ornitela_file` sub-action for each chunk
- Deletes archived files older than `delete_after_archive_days`

### `process_ornitela_file` (triggered per chunk)
- Downloads the chunk from `in_progress/`
- Parses each row into a Gundi observation
- Sends observations in batches of 500
- Moves chunk to `archive/` on success, `dead_letter/` on failure

---

## CSV row types

Each row in an Ornitela CSV has a `datatype` field:

| datatype | handling |
|---|---|
| `GPS`, `GPSS` | One observation with real GPS location |
| `SEN_*` (SEN_ALL_20Hz, SEN_ALL_20Hz_START, SEN_ALL_20Hz_END, etc.) | One observation with location `(0, 0)` and sensor readings in `additional` |

Millisecond offsets from the `milliseconds` column are applied to `recorded_at`. Rows older than `historical_limit_days` are skipped.

---

## Configuration (`process_new_files`)

| Field | Default | Description |
|---|---|---|
| `bucket_path` | `ornitela/` | Path prefix within the GCS bucket |
| `chunk_size` | `5000` | Rows per chunk |
| `batch_size` | `500` | Observations per batch when sending to Gundi |
| `max_files_per_run` | `10` | Max files to process per cron run |
| `process_most_recent_first` | `true` | Sort files by modification time descending |
| `historical_limit_days` | `5` | Skip observations older than this many days |
| `delete_after_archive_days` | `5` | Delete archived chunks after this many days |

---

## Local development

```bash
docker compose -f local/docker-compose.yml up --build -d
```

Trigger manually:
```bash
curl -X POST http://localhost:8080/v1/actions/execute \
  -H "Content-Type: application/json" \
  -d '{"integration_id": "<your-integration-id>", "action_id": "process_new_files", "run_in_background": true}'
```
