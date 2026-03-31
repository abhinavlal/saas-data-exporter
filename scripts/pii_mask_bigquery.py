"""PII Mask — BigQuery Parquet full PII masking.

Reads BigQuery GA4 Parquet exports from a source S3 bucket, masks all
PII (domains, user identifiers, doctor names in URLs, geo locations,
tracking IDs, sensitive URL paths), and writes masked Parquet files to
a destination S3 bucket.

Parallelized with ProcessPoolExecutor — each worker independently
downloads, masks, and uploads one Parquet file.
"""

import argparse
import logging
import os
import secrets
import tempfile
from concurrent.futures import ProcessPoolExecutor, as_completed

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from lib.checkpoint import CheckpointManager
from lib.logging import setup_logging
from lib.s3 import S3Store

log = logging.getLogger(__name__)

# Preferred tmp dir (avoid RAM-backed /tmp on some systems)
_TMP_DIR = next((d for d in ("/var/tmp",) if os.path.isdir(d)), None)

# Default domain mapping
DEFAULT_SOURCE_DOMAIN = "org_name.com"
DEFAULT_TARGET_DOMAIN = "example-health.com"

# -- Regex patterns applied to ALL string values recursively --------------- #
#
# Order matters — more specific patterns first, domain patterns last.

REGEX_PATTERNS = [
    # URL path: doctor name slugs → redacted
    (r"/doctor/[a-zA-Z0-9_-]+", "/doctor/redacted"),
    # URL path: consult question slugs → redacted
    (r"/consult/[^/?#]+", "/consult/redacted"),
    # URL path: feedback upload IDs → redacted
    (r"/feedback/upload/[0-9]+", "/feedback/upload/0"),
    # URL query params: session/practice/tracking IDs
    (r"practice_id=[0-9]+", "practice_id=0"),
    (r"[cf]_sid=[0-9]+", "sid=0"),
    (r"gad_source=[^&]+", "gad_source=0"),
    # Domain: hyphenated variant (AMP cache, Google Translate)
    (r"www-org_name-com", "www-example-health-com"),
    (r"org_name-com", "example-health-com"),
    # Brand name in text (e.g. page titles "... | Org_Name Consult")
    (r"Org_Name", "ExampleHealth"),
]

# Event param keys whose string_value should be randomized
_RANDOMIZE_PARAM_KEYS = frozenset({"gclid", "transaction_id"})

# Event param keys whose string_value should be redacted (sensitive text)
_REDACT_PARAM_KEYS = frozenset({"term"})

# Struct fields in collected_traffic_source to randomize
_TRACKING_ID_FIELDS = frozenset({"gclid", "dclid", "srsltid"})

# Geo fields to redact (keep country/continent, redact city-level)
_GEO_REDACT_FIELDS = frozenset({"city", "region", "metro"})


# -- Helpers ---------------------------------------------------------------- #

def _random_token(length: int = 32) -> str:
    return secrets.token_urlsafe(length)


def _random_id(length: int = 16) -> str:
    return secrets.token_hex(length)


# -- Column-level regex masking --------------------------------------------- #

def mask_column(col: pa.Array, pattern: str, replacement: str) -> pa.Array:
    """Replace *pattern* with *replacement* in all string values, recursively.

    Handles flat strings, structs (recursive fields), and lists of
    structs (e.g. GA4 event_params).  Non-string leaf types are
    returned unchanged.
    """
    if pa.types.is_string(col.type) or pa.types.is_large_string(col.type):
        return pc.replace_substring_regex(col, pattern=pattern,
                                          replacement=replacement)

    if pa.types.is_struct(col.type):
        fields = []
        arrays = []
        for i in range(col.type.num_fields):
            field = col.type.field(i)
            child = col.field(field.name)
            masked = mask_column(child, pattern, replacement)
            arrays.append(masked)
            fields.append(field.with_type(masked.type))
        return pa.StructArray.from_arrays(
            arrays,
            fields=fields,
            mask=col.is_null(),
        )

    if pa.types.is_list(col.type) or pa.types.is_large_list(col.type):
        masked_values = mask_column(col.values, pattern, replacement)
        return type(col).from_arrays(col.offsets, masked_values)

    # int, float, bool, timestamp, etc. — unchanged
    return col


def _apply_regex_patterns(table: pa.Table, patterns: list[tuple[str, str]],
                          domain_pattern: str, domain_replacement: str,
                          ) -> pa.Table:
    """Apply all regex patterns + domain replacement to every string in table."""
    all_patterns = patterns + [(domain_pattern, domain_replacement)]
    for pattern, replacement in all_patterns:
        new_columns = []
        for i in range(table.num_columns):
            col = table.column(i)
            masked = mask_column(col.combine_chunks(), pattern, replacement)
            new_columns.append(masked)
        table = pa.table(
            dict(zip(table.schema.names, new_columns)),
            schema=table.schema,
        )
    return table


# -- Column-specific masking ------------------------------------------------ #

def _randomize_string_column(col: pa.ChunkedArray, generator) -> pa.Array:
    """Replace all non-null values with random strings from *generator*."""
    arr = col.combine_chunks()
    values = [generator() if v.is_valid else None for v in arr]
    return pa.array(values, type=arr.type)


def _redact_struct_fields(col: pa.ChunkedArray, field_names: frozenset,
                          redacted_value: str = "(redacted)") -> pa.Array:
    """Replace specific string fields within a struct with a fixed value."""
    arr = col.combine_chunks()
    fields = []
    arrays = []
    for i in range(arr.type.num_fields):
        field = arr.type.field(i)
        child = arr.field(field.name)
        if field.name in field_names and pa.types.is_string(child.type):
            redacted = pa.array(
                [redacted_value if v.is_valid else None for v in child],
                type=child.type,
            )
            arrays.append(redacted)
        else:
            arrays.append(child)
        fields.append(field)
    return pa.StructArray.from_arrays(arrays, fields=fields, mask=arr.is_null())


def _mask_event_params(col: pa.ChunkedArray) -> pa.Array:
    """Mask PII in event_params: randomize gclid/transaction_id, redact terms."""
    rows = col.to_pylist()
    for row in rows:
        if row is None:
            continue
        for param in row:
            key = param["key"]
            val = param.get("value")
            if val is None:
                continue
            sv = val.get("string_value")
            if sv is None:
                continue
            if key in _RANDOMIZE_PARAM_KEYS:
                val["string_value"] = _random_token()
            elif key in _REDACT_PARAM_KEYS:
                val["string_value"] = "(redacted)"
    return pa.array(rows, type=col.type)


def _mask_tracking_struct(col: pa.ChunkedArray,
                          id_fields: frozenset) -> pa.Array:
    """Randomize tracking ID fields within a struct column."""
    rows = col.to_pylist()
    for row in rows:
        if row is None:
            continue
        for field in id_fields:
            if row.get(field):
                row[field] = _random_token()
    return pa.array(rows, type=col.type)


def _mask_session_traffic_source(col: pa.ChunkedArray) -> pa.Array:
    """Randomize tracking IDs nested inside session_traffic_source_last_click."""
    rows = col.to_pylist()
    for row in rows:
        if row is None:
            continue
        # Each sub-struct (google_ads_campaign, etc.) may have ID fields
        for campaign_key, campaign in row.items():
            if campaign is None or not isinstance(campaign, dict):
                continue
            for field in ("customer_id", "ad_group_id", "campaign_id",
                          "advertiser_id", "creative_id", "insertion_order_id",
                          "line_item_id", "partner_id", "site_id",
                          "rendering_id", "placement_id", "account_id"):
                if campaign.get(field):
                    campaign[field] = _random_id(8)
    return pa.array(rows, type=col.type)


# -- Table-level orchestration ---------------------------------------------- #

def mask_table(table: pa.Table, source_domain: str,
               target_domain: str) -> pa.Table:
    """Apply full PII masking to a GA4 Arrow table."""
    n = table.num_rows
    domain_pattern = source_domain.replace(".", r"\.")

    # -- Step 1: Column-specific masking (before regex pass) ----------------

    # Randomize user identifiers
    for col_name, gen in [
        ("user_id", lambda: _random_id(16)),
        ("user_pseudo_id", lambda: _random_id(32)),
    ]:
        if col_name in table.schema.names:
            table = table.set_column(
                table.schema.get_field_index(col_name),
                col_name,
                _randomize_string_column(table.column(col_name), gen),
            )

    # Redact geo city/region/metro (keep country, continent, sub_continent)
    if "geo" in table.schema.names:
        table = table.set_column(
            table.schema.get_field_index("geo"),
            "geo",
            _redact_struct_fields(table.column("geo"), _GEO_REDACT_FIELDS),
        )

    # Randomize device identifiers (vendor_id, advertising_id)
    if "device" in table.schema.names:
        table = table.set_column(
            table.schema.get_field_index("device"),
            "device",
            _redact_struct_fields(
                table.column("device"),
                frozenset({"vendor_id", "advertising_id"}),
                redacted_value=_random_id(16),
            ),
        )

    # Mask event_params (gclid, transaction_id, term)
    if "event_params" in table.schema.names:
        table = table.set_column(
            table.schema.get_field_index("event_params"),
            "event_params",
            _mask_event_params(table.column("event_params")),
        )

    # Mask collected_traffic_source tracking IDs
    if "collected_traffic_source" in table.schema.names:
        table = table.set_column(
            table.schema.get_field_index("collected_traffic_source"),
            "collected_traffic_source",
            _mask_tracking_struct(
                table.column("collected_traffic_source"),
                _TRACKING_ID_FIELDS,
            ),
        )

    # Mask session_traffic_source_last_click IDs
    if "session_traffic_source_last_click" in table.schema.names:
        table = table.set_column(
            table.schema.get_field_index("session_traffic_source_last_click"),
            "session_traffic_source_last_click",
            _mask_session_traffic_source(
                table.column("session_traffic_source_last_click"),
            ),
        )

    # -- Step 2: Regex patterns on ALL strings (domain, URLs, brand) --------

    table = _apply_regex_patterns(
        table, REGEX_PATTERNS, domain_pattern, target_domain,
    )

    return table


# -- S3 pipeline ----------------------------------------------------------- #

# Default workers: 6 fits comfortably on c5.4xlarge (16 vCPU, 32 GB RAM)
# Each worker peaks at ~4.3 GB — 6 × 4.3 = 26 GB, leaves headroom.
DEFAULT_WORKERS = 6


def _mask_one_file(
    src_bucket: str,
    src_prefix: str,
    dst_bucket: str,
    dst_prefix: str,
    key: str,
    source_domain: str,
    target_domain: str,
) -> tuple[str, bool, str]:
    """Worker function: download, mask, upload one Parquet file.

    Creates its own S3Store instances (boto3 clients cannot be pickled
    across process boundaries).  Returns (key, success, message).
    """
    try:
        src = S3Store(bucket=src_bucket, prefix=src_prefix)
        dst = S3Store(bucket=dst_bucket, prefix=dst_prefix)

        table = _download_parquet(src, key)
        if table is None:
            return (key, True, "skipped (download failed)")

        masked = mask_table(table, source_domain, target_domain)
        _upload_parquet(dst, key, masked)

        return (key, True, f"{table.num_rows:,} rows")
    except Exception as exc:
        return (key, False, str(exc))


def mask_bigquery_parquet(
    src: S3Store,
    dst: S3Store,
    dataset: str,
    source_domain: str,
    target_domain: str,
    checkpoint: CheckpointManager,
    max_workers: int = DEFAULT_WORKERS,
):
    """Mask all Parquet files under bigquery/{dataset}/events/."""
    prefix = f"bigquery/{dataset}/events/"
    keys = [k for k in src.list_keys(prefix) if k.endswith(".parquet")]
    log.info("Found %d parquet files under %s", len(keys), prefix)

    if not checkpoint.is_phase_done("mask"):
        checkpoint.start_phase("mask", total=len(keys))

        to_mask = [k for k in keys if not checkpoint.is_item_done("mask", k)]
        already_done = len(keys) - len(to_mask)
        log.info("Masking %d files (%d already done), workers=%d",
                 len(to_mask), already_done, max_workers)

        if to_mask and max_workers > 1:
            # Parallel: each worker creates its own S3Store (boto3
            # clients cannot be pickled across process boundaries).
            with ProcessPoolExecutor(max_workers=max_workers) as pool:
                futures = {
                    pool.submit(
                        _mask_one_file,
                        src.bucket, src.prefix,
                        dst.bucket, dst.prefix,
                        key, source_domain, target_domain,
                    ): key
                    for key in to_mask
                }
                for future in as_completed(futures):
                    key = futures[future]
                    try:
                        _, success, msg = future.result()
                        if success:
                            log.info("Masked %s — %s", key, msg)
                        else:
                            log.error("Failed %s — %s", key, msg)
                    except Exception:
                        log.error("Worker crashed on %s", key, exc_info=True)

                    checkpoint.mark_item_done("mask", key)
                    checkpoint.save()
        elif to_mask:
            # Sequential: used when max_workers <= 1 (or in tests where
            # moto mock_aws context cannot cross process boundaries).
            for key in to_mask:
                log.info("Masking %s", key)
                table = _download_parquet(src, key)
                if table is None:
                    log.warning("Could not read %s, skipping", key)
                else:
                    masked = mask_table(table, source_domain, target_domain)
                    _upload_parquet(dst, key, masked)
                    log.info("Masked %s — %s rows", key,
                             f"{table.num_rows:,}")
                checkpoint.mark_item_done("mask", key)
                checkpoint.save()

        checkpoint.complete_phase("mask")
        checkpoint.save(force=True)

    # Copy _stats.json if it exists (no masking needed, it has no PII)
    stats_key = f"bigquery/{dataset}/_stats.json"
    stats = src.download_json(stats_key)
    if stats is not None:
        dst.upload_json(stats, stats_key)
        log.info("Copied %s", stats_key)

    checkpoint.complete()
    log.info("BigQuery masking complete for dataset %s", dataset)


def _download_parquet(s3: S3Store, key: str) -> pa.Table | None:
    """Download a Parquet file from S3 into an Arrow table."""
    tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False,
                                      dir=_TMP_DIR)
    try:
        tmp.close()
        s3._client.download_file(
            Bucket=s3.bucket,
            Key=s3._key(key),
            Filename=tmp.name,
        )
        return pq.read_table(tmp.name)
    except Exception:
        log.error("Failed to download %s", key, exc_info=True)
        return None
    finally:
        try:
            os.unlink(tmp.name)
        except OSError:
            pass


def _upload_parquet(s3: S3Store, key: str, table: pa.Table) -> None:
    """Write an Arrow table to S3 as a Snappy-compressed Parquet file."""
    tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False,
                                      dir=_TMP_DIR)
    try:
        tmp.close()
        pq.write_table(table, tmp.name, compression="snappy")
        s3.upload_file(
            tmp.name, key,
            content_type="application/vnd.apache.parquet",
        )
    finally:
        try:
            os.unlink(tmp.name)
        except OSError:
            pass


# -- CLI -------------------------------------------------------------------- #

def main():
    from lib.config import load_dotenv, env, env_int
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Mask PII in BigQuery Parquet exports",
    )
    parser.add_argument("--src-bucket", default=env("S3_BUCKET"),
                        help="Source S3 bucket (default: S3_BUCKET)")
    parser.add_argument("--dst-bucket", default=env("S3_MASKED_BUCKET"),
                        help="Destination S3 bucket (default: S3_MASKED_BUCKET)")
    parser.add_argument("--s3-prefix", default=env("S3_PREFIX", ""),
                        help="S3 key prefix (shared by src and dst)")
    parser.add_argument("--dataset", default=env("BIGQUERY_DATASET"),
                        help="BigQuery dataset ID (e.g. analytics_123456789)")
    parser.add_argument("--source-domain",
                        default=env("PII_SOURCE_DOMAIN", DEFAULT_SOURCE_DOMAIN),
                        help=f"Domain to replace (default: {DEFAULT_SOURCE_DOMAIN})")
    parser.add_argument("--target-domain",
                        default=env("PII_TARGET_DOMAIN", DEFAULT_TARGET_DOMAIN),
                        help=f"Replacement domain (default: {DEFAULT_TARGET_DOMAIN})")
    parser.add_argument("--max-workers", type=int,
                        default=env_int("PII_MAX_WORKERS", DEFAULT_WORKERS),
                        help=f"Parallel workers (default: {DEFAULT_WORKERS})")
    parser.add_argument("--log-level", default=env("LOG_LEVEL", "INFO"))
    parser.add_argument("--no-json-logs", action="store_true")
    parser.add_argument("--log-dir", default=env("LOG_DIR", "logs"))
    args = parser.parse_args()

    if not args.src_bucket:
        parser.error("--src-bucket is required (or set PII_SRC_BUCKET)")
    if not args.dst_bucket:
        parser.error("--dst-bucket is required (or set PII_DST_BUCKET)")
    if not args.dataset:
        parser.error("--dataset is required (or set BIGQUERY_DATASET)")

    log_file = os.path.join(args.log_dir, "pii_mask_bigquery.log")
    setup_logging(
        level=args.log_level,
        json_output=not args.no_json_logs,
        log_file=log_file,
    )

    src = S3Store(bucket=args.src_bucket, prefix=args.s3_prefix)
    dst = S3Store(bucket=args.dst_bucket, prefix=args.s3_prefix)
    checkpoint = CheckpointManager(dst, f"pii_mask/bigquery/{args.dataset}")

    checkpoint.load()

    log.info("Masking BigQuery dataset %s: %s -> %s (domain: %s -> %s, workers: %d)",
             args.dataset, args.src_bucket, args.dst_bucket,
             args.source_domain, args.target_domain, args.max_workers)

    mask_bigquery_parquet(
        src=src,
        dst=dst,
        dataset=args.dataset,
        source_domain=args.source_domain,
        target_domain=args.target_domain,
        checkpoint=checkpoint,
        max_workers=args.max_workers,
    )


if __name__ == "__main__":
    main()
