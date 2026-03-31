"""BigQuery GA4 Exporter — exports daily GA4 event tables to S3 as Parquet."""

import argparse
import logging
import os
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import bigquery
from google.oauth2 import service_account

from lib.s3 import S3Store
from lib.checkpoint import CheckpointManager
from lib.stats import StatsCollector
from lib.logging import setup_logging
from lib.types import ExportConfig

log = logging.getLogger(__name__)

# -- Constants -------------------------------------------------------------- #

# Preferred tmp dir (avoid RAM-backed /tmp on some systems)
_TMP_DIR = next((d for d in ("/var/tmp",) if os.path.isdir(d)), None)


# -- Exporter --------------------------------------------------------------- #

class BigQueryExporter:
    """Exports GA4 daily event tables from BigQuery to S3 as Parquet."""

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
        table_prefix: str = "events_intraday_",
    ):
        self.project = project
        self.dataset = dataset
        self.s3 = s3
        self.config = config
        self.days = days
        self.parallel = parallel
        self.table_prefix = table_prefix
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

    # -- Main entry --------------------------------------------------------- #

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

        # Export all days (checkpoint-gated)
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

    # -- Parallel day dispatch ---------------------------------------------- #

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

    # -- Single-day export (performance-critical) --------------------------- #

    def _export_one_day(self, date_str: str):
        """Export one daily table to Parquet in S3.

        Optimized path: Arrow batches streamed directly to Parquet writer.
        No Python dict conversion — stays in Arrow columnar format end-to-end.
        """
        table_id = f"{self.project}.{self.dataset}.{self.table_prefix}{date_str}"

        # Check table exists
        try:
            table = self.client.get_table(table_id)
        except Exception:
            log.warning("Table %s not found, skipping", table_id)
            return

        row_count = table.num_rows
        log.info("Exporting %s (%s rows)", table_id, f"{row_count:,}")

        s3_path = f"{self.s3_base}/events/{date_str}.parquet"
        rows_written = 0

        tmp_pq = tempfile.NamedTemporaryFile(
            suffix=".parquet", delete=False, dir=_TMP_DIR,
        )
        try:
            # Stream Arrow batches from BQ Storage Read API directly to
            # Parquet writer — zero Python dict conversion, columnar
            # end-to-end with snappy compression.
            row_iter = self.client.list_rows(table)
            writer = None

            for batch in row_iter.to_arrow_iterable():
                if writer is None:
                    writer = pq.ParquetWriter(
                        tmp_pq.name, batch.schema,
                        compression="snappy",
                    )
                writer.write_batch(batch)
                rows_written += batch.num_rows

            if writer is not None:
                writer.close()

            tmp_pq.close()

            # Upload parquet file to S3
            file_size = os.path.getsize(tmp_pq.name)
            self.s3.upload_file(
                tmp_pq.name, s3_path,
                content_type="application/vnd.apache.parquet",
            )

            log.info("Exported %s: %s rows, %s parquet",
                     date_str, f"{rows_written:,}",
                     _human_bytes(file_size))

            # Update stats
            self.stats.increment("events.total_rows", rows_written)
            self.stats.increment("events.total_bytes", file_size)
            self.stats.increment("events.days_exported")
            self.stats.save()

        finally:
            try:
                os.unlink(tmp_pq.name)
            except OSError:
                pass


# -- Helpers ---------------------------------------------------------------- #

def _human_bytes(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if abs(n) < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


# -- CLI -------------------------------------------------------------------- #

def main():
    from lib.config import load_dotenv, env, env_int, env_bool
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Export GA4 event data from BigQuery to S3 as Parquet",
    )
    parser.add_argument("--key", default=env("BIGQUERY_KEY"),
                        help="Path to service account JSON key")
    parser.add_argument("--project", default=env("BIGQUERY_PROJECT"),
                        help="GCP project ID")
    parser.add_argument("--dataset", default=env("BIGQUERY_DATASET"),
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
    parser.add_argument("--table-prefix",
                        default=env("BIGQUERY_TABLE_PREFIX", "events_intraday_"),
                        help="Table name prefix (default events_intraday_)")
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
    if not args.project:
        parser.error("--project is required (or set BIGQUERY_PROJECT)")
    if not args.dataset:
        parser.error("--dataset is required (or set BIGQUERY_DATASET)")
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
        table_prefix=args.table_prefix,
    )
    exporter.run()


if __name__ == "__main__":
    main()
