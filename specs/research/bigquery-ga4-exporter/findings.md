# BigQuery GA4 Exporter — Research Findings

## Source Data

- **Project**: `your-gcp-project`
- **Dataset**: `analytics_XXXXXXXXX`
- **Table pattern**: `events_YYYYMMDD` (standard GA4 BigQuery export)
- **Schema**: 30 top-level fields including nested RECORDs (`event_params`, `user_properties`, `items`, `device`, `geo`, etc.)
- **Volume per day**: ~817K rows, 692 MB logical, 87 MB physical
- **365-day total**: ~298M rows, ~247 GB logical, ~31 GB physical
- **Estimated NDJSON output**: ~550 GB uncompressed, ~55 GB gzipped

## Cost Analysis

| Component | Cost |
|-----------|------|
| BQ Storage Read API | $0.27 (247 GB × $1.10/TiB) |
| BQ egress US → Mumbai | ~$30-35 (247 GB × $0.12/GiB) |
| S3 storage (gzipped) | ~$1.27/month |
| **Total one-time** | **~$31-36** |

`list_rows()` via REST is free but 10-50x slower than Storage Read API.
Storage Read API at $0.27 total is negligible — use it for speed.

## Existing Patterns (from codebase analysis)

### Exporter class structure
- Constructor: credentials, target, S3Store, ExportConfig, flags
- `run()`: checkpoint.load() → phase methods gated by is_phase_done → checkpoint.complete()
- Each phase: start_phase → iterate/paginate → mark_item_done → complete_phase → save(force=True)

### main() structure
- Deferred imports of lib.config, lib.input inside main()
- load_dotenv() → argparse → target resolution (CLI > CSV > env) → S3Store + ExportConfig → Exporter → run()

### Key lib modules
- `S3Store` (lib/s3.py): upload_json, upload_file, upload_bytes, download_json
- `NDJSONWriter` (lib/s3.py): append() → periodic upload, close() → final upload + cleanup
- `CheckpointManager` (lib/checkpoint.py): load, start_phase, complete_phase, mark_item_done, set_cursor, save
- `StatsCollector` (lib/stats.py): set, set_nested, increment, add_to_map, save
- `ExportConfig` (lib/types.py): s3_bucket, s3_prefix, max_workers, log_level

### Google auth precedent
- `google_workspace.py` uses `service_account.Credentials.from_service_account_file(key, scopes=SCOPES)`
- BigQuery client accepts the same credential type: `bigquery.Client(credentials=creds, project=project)`

### Compression precedent
- No existing exporter compresses output — this will be the first
- `NDJSONWriter` writes to temp file, then uploads via `S3Store.upload_file()`
- Can gzip the temp file before upload, or use `S3Store.upload_bytes()` with gzipped content

## BigQuery Storage Read API Notes

- Python package: `google-cloud-bigquery-storage` + `pyarrow`
- When installed, `client.list_rows(table).to_arrow_iterable()` automatically uses gRPC streaming
- Returns `pyarrow.RecordBatch` objects — efficient columnar format, low memory
- Can iterate batches without holding full result in memory
- Each RecordBatch can be converted to list of dicts for NDJSON serialization
