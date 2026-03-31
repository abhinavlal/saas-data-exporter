# BigQuery GA4 Exporter — Implementation Plan

## Overview

New exporter that reads GA4 event data from BigQuery daily-sharded `events_*` tables and writes gzipped NDJSON files to S3. Optimized for throughput using the BigQuery Storage Read API (gRPC + Arrow) and parallel day exports.

## Current State Analysis

- No BigQuery or GA4 code exists in the project
- 6 existing exporters follow a consistent pattern (class + `main()` + checkpointing + stats)
- Google auth precedent in `google_workspace.py` using service account credentials
- `NDJSONWriter` in `lib/s3.py:156` handles disk-backed streaming writes
- `S3Store.upload_file()` in `lib/s3.py:81` handles large file uploads with multipart
- No existing exporter uses compression — this will be the first

## Desired End State

```
$ uv run python -m exporters.bigquery
```

Exports 365 days of GA4 events from BigQuery to S3:
```
v31/bigquery/analytics_XXXXXXXXX/
    events/20250401.ndjson.gz    # ~150 MB gzipped per day
    events/20250402.ndjson.gz
    ...
    events/20260330.ndjson.gz
    _stats.json                  # aggregate stats
```

Checkpointed per day — can resume after interruption. Parallel day exports (default 4 concurrent) for throughput.

Verification: `_stats.json` shows `total_rows`, `total_bytes`, `days_exported: 365`, and `exported_at` timestamp.

## What We're NOT Doing

- Intraday tables (`events_intraday_*`)
- Flattening nested RECORD fields (raw JSON preserved)
- Aggregation, pre-computation, or derived tables
- Multi-property support (single dataset, extensible later)
- CSV output (NDJSON only — nested records don't flatten to CSV)
- BigQuery-to-GCS export path

## Implementation Approach

**Per-day streaming with parallel workers.** Enumerate all `events_*` tables for the date range, then process N days concurrently. Each day: `list_rows()` via Storage Read API → iterate Arrow batches → write rows to temp file as NDJSON → gzip → upload to S3. Per-day checkpointing allows resume.

Key performance choices:
- **Storage Read API** (gRPC + Arrow) — 10-50x faster than REST pagination
- **Parallel day exports** (default 4) — saturate network without hitting BQ slot limits
- **Arrow batch iteration** — `to_arrow_iterable()` yields `RecordBatch` objects, constant memory
- **Gzip on disk** — compress the temp file before upload, not in memory
- **No SQL queries** — `list_rows(table_ref)` reads directly from storage, $0 analysis cost

---

## Phase 1: Dependencies and Project Setup

### Overview
Add BigQuery dependencies and create the exporter file skeleton.

### Changes Required

#### 1. Add dependencies
**File**: `pyproject.toml`

Add a new optional dependency group and the core BigQuery packages:

```toml
[project.optional-dependencies]
dev = [...]
fast = [...]
bigquery = [
    "google-cloud-bigquery>=3.20",
    "google-cloud-bigquery-storage>=2.24",
    "pyarrow>=15.0",
]
```

Add to main dependencies (since this is a core exporter):

```toml
dependencies = [
    ...existing...,
    "google-cloud-bigquery>=3.20",
    "google-cloud-bigquery-storage>=2.24",
    "pyarrow>=15.0",
]
```

Decision: put in main `dependencies` since it's a first-class exporter (consistent with `google-api-python-client` being in main deps for google_workspace).

#### 2. Create exporter skeleton
**File**: `exporters/bigquery.py`

```python
"""BigQuery GA4 Exporter — exports daily GA4 event tables to S3 as gzipped NDJSON."""

import argparse
import gzip
import json
import logging
import os
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

from google.cloud import bigquery
from google.oauth2 import service_account

from lib.s3 import S3Store
from lib.checkpoint import CheckpointManager
from lib.stats import StatsCollector
from lib.logging import setup_logging
from lib.types import ExportConfig

log = logging.getLogger(__name__)
```

#### 3. Update .env.example
**File**: `.env.example`

```bash
# BigQuery GA4
BIGQUERY_KEY=path/to/service-account-key.json
BIGQUERY_PROJECT=your-gcp-project
BIGQUERY_DATASET=analytics_XXXXXXXXX
BIGQUERY_DAYS=365
BIGQUERY_PARALLEL=4
```

### Success Criteria

#### Automated Verification:
- [x] `uv sync` installs BigQuery dependencies without errors
- [x] `uv run python -c "from google.cloud import bigquery; import pyarrow"` succeeds

#### Manual Verification:
- [ ] `exporters/bigquery.py` exists with correct imports

---

## Phase 2: BigQueryExporter Class

### Overview
Implement the core exporter class with constructor, `run()`, and single-day export logic. This is the performance-critical code.

### Changes Required

#### 1. Class definition
**File**: `exporters/bigquery.py`

```python
# -- Constants -------------------------------------------------------------- #

# Preferred tmp dir (avoid RAM-backed /tmp on some systems)
_TMP_DIR = next((d for d in ("/var/tmp",) if os.path.isdir(d)), None)


class BigQueryExporter:
    """Exports GA4 daily event tables from BigQuery to S3 as gzipped NDJSON."""

    def __init__(
        self,
        key_path: str,
        project: str,
        dataset: str,
        s3: S3Store,
        config: ExportConfig,
        days: int = 365,
        parallel: int = 4,
        end_date: str | None = None,
    ):
        self.project = project
        self.dataset = dataset
        self.s3 = s3
        self.config = config
        self.days = days
        self.parallel = parallel
        self.s3_base = f"bigquery/{dataset}"

        # Parse end_date or default to yesterday (latest complete events_* table)
        if end_date:
            self.end_date = datetime.strptime(end_date, "%Y%m%d").date()
        else:
            self.end_date = (datetime.now(timezone.utc) - timedelta(days=1)).date()
        self.start_date = self.end_date - timedelta(days=days - 1)

        # BigQuery client with service account credentials
        credentials = service_account.Credentials.from_service_account_file(key_path)
        self.client = bigquery.Client(
            credentials=credentials,
            project=project,
        )

        self.checkpoint = CheckpointManager(s3, f"bigquery/{dataset}")
        self.stats = StatsCollector(s3, f"{self.s3_base}/_stats.json")
```

#### 2. run() method — parallel day dispatch
```python
    def run(self):
        self.checkpoint.load()
        self.stats.load()
        self.stats.set("exporter", "bigquery")
        self.stats.set("target", f"{self.project}.{self.dataset}")
        self.stats.set("date_range", {
            "start": self.start_date.strftime("%Y%m%d"),
            "end": self.end_date.strftime("%Y%m%d"),
        })

        # Enumerate date strings
        dates = []
        d = self.start_date
        while d <= self.end_date:
            dates.append(d.strftime("%Y%m%d"))
            d += timedelta(days=1)

        # Filter to incomplete days
        if not self.checkpoint.is_phase_done("events"):
            self.checkpoint.start_phase("events", total=len(dates))
            self._export_events(dates)
            self.checkpoint.complete_phase("events")
            self.checkpoint.save(force=True)

        self.stats.set("exported_at", datetime.now(timezone.utc).isoformat())
        self.stats.save(force=True)
        self.checkpoint.complete()
        log.info("BigQuery export complete for %s (%d days)",
                 self.dataset, len(dates))
```

#### 3. Parallel day export with per-day checkpointing
```python
    def _export_events(self, dates: list[str]):
        to_export = [d for d in dates
                     if not self.checkpoint.is_item_done("events", d)]
        log.info("Exporting %d days (%d already done)",
                 len(to_export), len(dates) - len(to_export))

        if not to_export:
            return

        with ThreadPoolExecutor(max_workers=self.parallel) as pool:
            futures = {pool.submit(self._export_one_day, d): d
                       for d in to_export}
            for future in as_completed(futures):
                date_str = futures[future]
                try:
                    future.result()
                    self.checkpoint.mark_item_done("events", date_str)
                    self.checkpoint.save()
                except Exception:
                    log.error("Failed to export day %s", date_str,
                              exc_info=True)
```

#### 4. Single-day export — performance-critical path
```python
    def _export_one_day(self, date_str: str):
        """Export one events_YYYYMMDD table to gzipped NDJSON in S3."""
        table_id = f"{self.project}.{self.dataset}.events_{date_str}"

        # Check table exists
        try:
            table = self.client.get_table(table_id)
        except Exception:
            log.warning("Table %s not found, skipping", table_id)
            return

        row_count = table.num_rows
        log.info("Exporting %s (%s rows)", table_id, f"{row_count:,}")

        # Stream rows via Storage Read API → Arrow batches → NDJSON → gzip
        s3_path = f"{self.s3_base}/events/{date_str}.ndjson.gz"
        rows_written = 0
        bytes_written = 0

        # Write NDJSON to temp file, then gzip, then upload
        tmp_ndjson = tempfile.NamedTemporaryFile(
            mode="w", suffix=".ndjson", delete=False, dir=_TMP_DIR,
        )
        try:
            # list_rows with Storage Read API (auto-enabled when
            # google-cloud-bigquery-storage + pyarrow are installed)
            row_iter = self.client.list_rows(table)

            for batch in row_iter.to_arrow_iterable():
                # Convert Arrow RecordBatch to list of dicts
                for row in batch.to_pylist():
                    line = json.dumps(row, default=str)
                    tmp_ndjson.write(line)
                    tmp_ndjson.write("\n")
                    rows_written += 1
                    bytes_written += len(line) + 1

            tmp_ndjson.close()

            # Gzip the temp file
            tmp_gz_path = tmp_ndjson.name + ".gz"
            with open(tmp_ndjson.name, "rb") as f_in, \
                 gzip.open(tmp_gz_path, "wb", compresslevel=6) as f_out:
                while True:
                    chunk = f_in.read(1024 * 1024)  # 1 MB chunks
                    if not chunk:
                        break
                    f_out.write(chunk)

            # Upload gzipped file to S3
            gz_size = os.path.getsize(tmp_gz_path)
            self.s3.upload_file(
                tmp_gz_path, s3_path,
                content_type="application/gzip",
            )

            log.info("Exported %s: %s rows, %s raw, %s gzipped",
                     date_str, f"{rows_written:,}",
                     _human_bytes(bytes_written), _human_bytes(gz_size))

            # Update stats (thread-safe via StatsCollector's internal locking)
            self.stats.increment("events.total_rows", rows_written)
            self.stats.increment("events.total_bytes_raw", bytes_written)
            self.stats.increment("events.total_bytes_gzipped", gz_size)
            self.stats.increment("events.days_exported")
            self.stats.save()

        finally:
            # Clean up temp files
            for path in (tmp_ndjson.name, tmp_ndjson.name + ".gz"):
                try:
                    os.unlink(path)
                except OSError:
                    pass


def _human_bytes(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if abs(n) < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"
```

**Memory profile for one day:**
- Arrow `RecordBatch` default ~64K rows → ~50 MB per batch (transient)
- `batch.to_pylist()` produces list of dicts for that batch → ~50 MB (transient)
- `json.dumps()` per row → written to disk immediately, not accumulated
- Only 1 batch in memory at a time per worker
- 4 parallel workers × ~100 MB peak each = ~400 MB total peak
- Gzip reads 1 MB chunks — negligible

### Success Criteria

#### Automated Verification:
- [x] `uv run pytest tests/test_bigquery.py -v` passes
- [ ] No memory leaks: running with 1 day stays under 200 MB

#### Manual Verification:
- [ ] `uv run python -m exporters.bigquery --days 1` exports yesterday's data successfully
- [ ] S3 file is valid gzipped NDJSON: `aws s3 cp s3://.../.ndjson.gz - | gunzip | head -1 | python -m json.tool`

**Implementation Note**: After completing this phase and all automated verification passes, pause for manual confirmation before proceeding to the next phase.

---

## Phase 3: main() Entry Point

### Overview
CLI entry point following the standard pattern.

### Changes Required

**File**: `exporters/bigquery.py` — append to bottom

```python
def main():
    from lib.config import load_dotenv, env, env_int, env_bool
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Export GA4 event data from BigQuery to S3",
    )
    parser.add_argument("--key", default=env("BIGQUERY_KEY"),
                        help="Path to service account JSON key")
    parser.add_argument("--project", default=env("BIGQUERY_PROJECT",
                        "your-gcp-project"),
                        help="GCP project ID")
    parser.add_argument("--dataset", default=env("BIGQUERY_DATASET",
                        "analytics_XXXXXXXXX"),
                        help="BigQuery dataset ID")
    parser.add_argument("--days", type=int,
                        default=env_int("BIGQUERY_DAYS", 365),
                        help="Number of days to export (default 365)")
    parser.add_argument("--end-date",
                        default=env("BIGQUERY_END_DATE"),
                        help="End date YYYYMMDD (default: yesterday)")
    parser.add_argument("--parallel", type=int,
                        default=env_int("BIGQUERY_PARALLEL", 4),
                        help="Parallel day exports (default 4)")
    parser.add_argument("--s3-bucket", default=env("S3_BUCKET"))
    parser.add_argument("--s3-prefix", default=env("S3_PREFIX", ""))
    parser.add_argument("--max-workers", type=int,
                        default=env_int("MAX_WORKERS", 10),
                        help="Max parallel workers (default 10)")
    parser.add_argument("--log-level", default=env("LOG_LEVEL", "INFO"))
    parser.add_argument("--no-json-logs", action="store_true",
                        default=not env_bool("JSON_LOGS", True))
    parser.add_argument("--log-dir", default=env("LOG_DIR", "logs"))
    args = parser.parse_args()

    if not args.key:
        parser.error("--key is required (or set BIGQUERY_KEY)")
    if not args.s3_bucket:
        parser.error("--s3-bucket is required (or set S3_BUCKET)")

    log_file = os.path.join(args.log_dir, "bigquery.log")
    setup_logging(
        level=args.log_level,
        json_output=not args.no_json_logs,
        log_file=log_file,
    )
    s3 = S3Store(bucket=args.s3_bucket, prefix=args.s3_prefix)
    config = ExportConfig(
        s3_bucket=args.s3_bucket,
        s3_prefix=args.s3_prefix,
        max_workers=args.max_workers,
        log_level=args.log_level,
    )

    log.info("Exporting GA4 data: %s.%s, %d days ending %s, parallel=%d",
             args.project, args.dataset, args.days,
             args.end_date or "yesterday", args.parallel)

    exporter = BigQueryExporter(
        key_path=args.key,
        project=args.project,
        dataset=args.dataset,
        s3=s3,
        config=config,
        days=args.days,
        parallel=args.parallel,
        end_date=args.end_date,
    )
    exporter.run()


if __name__ == "__main__":
    main()
```

### Success Criteria

#### Automated Verification:
- [x] `uv run python -m exporters.bigquery --help` shows all flags

#### Manual Verification:
- [ ] `uv run python -m exporters.bigquery --days 3` exports 3 days end-to-end

---

## Phase 4: Tests

### Overview
Test suite following existing patterns — mock BigQuery client, real S3 via moto.

### Changes Required

**File**: `tests/test_bigquery.py`

Tests to write:
1. `test_exports_single_day` — mock `client.list_rows()` to return Arrow table, verify gzipped NDJSON in S3
2. `test_skips_missing_table` — mock `client.get_table()` raising NotFound, verify graceful skip
3. `test_checkpoint_resume` — pre-populate checkpoint with some days done, verify only remaining days exported
4. `test_stats_accumulated` — verify `_stats.json` has correct totals after multi-day export
5. `test_parallel_export` — mock 5 days, verify all 5 exported with parallel=2
6. `test_gzip_valid` — verify output file is valid gzip containing valid NDJSON lines

**Mocking strategy:**
- `@patch("exporters.bigquery.bigquery.Client")` for the BigQuery client
- `@patch("exporters.bigquery.service_account.Credentials")` for auth
- Use `pyarrow.table()` to create real Arrow tables for realistic data flow
- `moto mock_aws()` + `S3Store` for S3 (same `s3_env` fixture as other tests)

```python
import pyarrow as pa

def _make_arrow_table(num_rows=10):
    """Create a minimal Arrow table mimicking GA4 schema."""
    return pa.table({
        "event_date": ["20250401"] * num_rows,
        "event_timestamp": list(range(num_rows)),
        "event_name": ["page_view"] * num_rows,
        "user_pseudo_id": [f"user_{i}" for i in range(num_rows)],
        "platform": ["WEB"] * num_rows,
    })
```

### Success Criteria

#### Automated Verification:
- [x] `uv run pytest tests/test_bigquery.py -v` — all tests pass (7/7)
- [x] `uv run pytest tests/ -v` — no regressions (215/215 passed)

---

## Phase 5: Documentation and .env Updates

### Overview
Update README, .env.example, and CLAUDE.md references.

### Changes Required

1. **`.env.example`** — add BigQuery section
2. **`README.md`** — add BigQuery exporter to the list with usage example
3. **`CLAUDE.md`** — add `exporters/bigquery.py` to the exporter list and run command

### Success Criteria

#### Manual Verification:
- [ ] README accurately describes BigQuery exporter usage
- [ ] `.env.example` has all BigQuery vars documented

---

## Testing Strategy

### Unit Tests:
- Mock BigQuery client (`list_rows`, `get_table`)
- Real Arrow tables for data serialization path
- Real S3 via moto for upload verification
- Gzip decompression + NDJSON parsing for end-to-end validation

### Integration Tests (manual):
- Export 1 day: `--days 1`
- Export 7 days: `--days 7 --parallel 4`
- Interrupt mid-export (Ctrl+C), restart, verify resume
- Verify gzipped NDJSON can be read by: `gunzip | jq`, Athena, pandas

### Manual Testing Steps:
1. `uv sync` installs BigQuery deps
2. `uv run python -m exporters.bigquery --days 1` completes
3. S3 has `bigquery/analytics_XXXXXXXXX/events/YYYYMMDD.ndjson.gz`
4. `_stats.json` has correct row count matching BQ table
5. `gunzip` the file, verify valid JSON per line
6. Run full `--days 365 --parallel 4`, monitor memory stays < 500 MB

## Performance Considerations

- **Storage Read API throughput**: ~100-500 MB/s per stream. With 4 parallel days, expect ~400 MB-2 GB/s aggregate read rate from BQ
- **Bottleneck**: Network egress from GCP US to AWS Mumbai (~100-200 Mbps typical). 247 GB at 150 Mbps ≈ ~3.6 hours. Compression doesn't help here (data is compressed after receipt)
- **Parallel tuning**: `--parallel 4` is conservative. BQ allows 100 concurrent API sessions. Could go to 8-10 if network allows, but each worker holds ~100 MB memory
- **Gzip compresslevel=6**: Good balance of speed vs ratio. Level 1 is 2x faster but 20% larger. Level 9 is 30% slower for 5% smaller
- **Temp disk space**: 2 temp files per worker (ndjson + gz) × ~1.5 GB each × 4 workers = ~12 GB peak temp disk. Ensure /var/tmp has space

## Migration Notes

N/A — new exporter, no migration needed.

## References

- Research findings: `specs/research/bigquery-ga4-exporter/findings.md`
- Similar implementation: `exporters/google_workspace.py` (Google auth pattern)
- Closest structural pattern: `exporters/confluence.py` (cleanest recent exporter)
- BigQuery Storage Read API docs: cloud.google.com/bigquery/docs/reference/storage
- GA4 BigQuery export schema: support.google.com/analytics/answer/7029846
