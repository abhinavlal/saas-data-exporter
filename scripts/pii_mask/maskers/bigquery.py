"""BigQuery masker — DuckDB-based Parquet PII masking.

Wraps the existing DuckDB/SQL masking logic from the deprecated module
and integrates it into the pipeline as a BaseMasker subclass.

Each mask_file() call creates its own DuckDB connection (thread-safe).
Supports httpfs for direct S3 read/write, or local temp files for testing.
"""

import logging
import os
import tempfile

import duckdb

from lib.s3 import S3Store
from scripts.pii_mask.maskers.base import BaseMasker
from scripts.pii_mask.scanner import TextScanner

# Import the SQL expression builders from the deprecated module
from scripts.deprecated.pii_mask_bigquery import (
    REGEX_PATTERNS,
    _col_expr,
    _configure_connection,
    _configure_httpfs,
    _parse_struct_fields,
    mask_parquet,
)

log = logging.getLogger(__name__)

DEFAULT_SOURCE_DOMAIN = "org_name.com"
DEFAULT_TARGET_DOMAIN = "example-health.com"

_SPILL_DIR = next((d for d in ("/var/tmp",) if os.path.isdir(d)), None)


class BigQueryMasker(BaseMasker):
    """Masks BigQuery GA4 Parquet exports via DuckDB SQL.

    Unlike JSON maskers, this operates on Parquet files using SQL
    ``regexp_replace`` and ``struct_pack``.  Each ``mask_file`` call
    gets its own DuckDB connection for thread safety.
    """

    prefix = "bigquery/"

    def __init__(self, scanner: TextScanner,
                 dataset: str = "",
                 source_domain: str = DEFAULT_SOURCE_DOMAIN,
                 target_domain: str = DEFAULT_TARGET_DOMAIN,
                 s3_region: str | None = None,
                 use_httpfs: bool = True):
        super().__init__(scanner)
        self.dataset = dataset
        self.source_domain = source_domain
        self.target_domain = target_domain
        self.s3_region = s3_region
        self.use_httpfs = use_httpfs
        # Cap DuckDB threads to avoid oversubscription
        cpu_count = os.cpu_count() or 4
        self._threads_per_worker = max(1, cpu_count // 4)

    def list_keys(self, src: S3Store) -> list[str]:
        prefix = f"bigquery/{self.dataset}/events/" if self.dataset \
            else "bigquery/"
        return [k for k in src.list_keys(prefix) if self.should_process(k)]

    def should_process(self, key: str) -> bool:
        return key.endswith(".parquet")

    def mask_file(self, src: S3Store, dst: S3Store, key: str) -> str:
        """Mask one Parquet file via DuckDB SQL."""
        con = duckdb.connect()
        try:
            _configure_connection(con, threads=self._threads_per_worker)

            if self.use_httpfs:
                _configure_httpfs(con, self.s3_region)
                src_url = self._s3_url(src, key)
                dst_url = self._s3_url(dst, key)
                rows = mask_parquet(con, src_url, dst_url,
                                    self.source_domain, self.target_domain)
            else:
                rows = self._mask_local(con, src, dst, key)

            return f"ok ({rows:,} rows)"
        finally:
            con.close()

    def _mask_local(self, con, src: S3Store, dst: S3Store,
                    key: str) -> int:
        """Fallback for testing with moto — download, mask locally, upload."""
        with tempfile.TemporaryDirectory(dir=_SPILL_DIR) as tmpdir:
            local_src = os.path.join(tmpdir, "input.parquet")
            local_dst = os.path.join(tmpdir, "output.parquet")
            src._client.download_file(
                Bucket=src.bucket, Key=src._key(key),
                Filename=local_src,
            )
            rows = mask_parquet(con, local_src, local_dst,
                                self.source_domain, self.target_domain)
            dst.upload_file(
                local_dst, key,
                content_type="application/vnd.apache.parquet",
            )
            return rows

    @staticmethod
    def _s3_url(store: S3Store, key: str) -> str:
        full_key = store._key(key)
        return f"s3://{store.bucket}/{full_key}"

