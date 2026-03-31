"""PII Mask — BigQuery Parquet masking via DuckDB.

Reads BigQuery GA4 Parquet exports from a source S3 bucket, masks all
PII (domains, user identifiers, doctor names in URLs, geo locations,
tracking IDs, sensitive URL paths), and writes masked Parquet files to
a destination S3 bucket.

Uses DuckDB's httpfs extension to read/write S3 directly — no temp
files, streaming execution with bounded memory.  All masking logic is
expressed in SQL using regexp_replace, list_transform, and struct_pack.
"""

import argparse
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import duckdb

from lib.checkpoint import CheckpointManager
from lib.logging import setup_logging
from lib.s3 import S3Store

log = logging.getLogger(__name__)

# -- Constants -------------------------------------------------------------- #

DEFAULT_SOURCE_DOMAIN = "org_name.com"
DEFAULT_TARGET_DOMAIN = "example-health.com"
DEFAULT_WORKERS = 4

# Regex patterns applied to all string values.
# Order matters — more specific patterns first, domain patterns last.
REGEX_PATTERNS = [
    # URL path: doctor name slugs
    (r"/doctor/[a-zA-Z0-9_-]+", "/doctor/redacted"),
    # URL path: consult question slugs
    (r"/consult/[^/?#]+", "/consult/redacted"),
    # URL path: feedback upload IDs
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
_RANDOMIZE_PARAM_KEYS = ("gclid", "transaction_id")

# Event param keys whose string_value should be redacted
_REDACT_PARAM_KEYS = ("term",)

# Struct fields in collected_traffic_source to randomize
_TRACKING_ID_FIELDS = ("gclid", "dclid", "srsltid")

# Geo fields to redact (keep country/continent, redact city-level)
_GEO_REDACT_FIELDS = ("city", "region", "metro")

# Device fields to randomize
_DEVICE_ID_FIELDS = ("vendor_id", "advertising_id")

# Session traffic source campaign ID fields to randomize
_CAMPAIGN_ID_FIELDS = (
    "customer_id", "ad_group_id", "campaign_id", "advertiser_id",
    "creative_id", "insertion_order_id", "line_item_id", "partner_id",
    "site_id", "rendering_id", "placement_id", "account_id",
)


# -- SQL Expression Builders ----------------------------------------------- #

def _regex_chain_sql(expr: str, domain_re: str,
                     target_domain: str) -> str:
    """Wrap *expr* in nested regexp_replace calls for all patterns + domain."""
    all_patterns = REGEX_PATTERNS + [(domain_re, target_domain)]
    result = expr
    for pattern, replacement in all_patterns:
        # Escape single quotes in pattern/replacement for SQL
        p = pattern.replace("'", "''")
        r = replacement.replace("'", "''")
        result = f"regexp_replace({result}, '{p}', '{r}', 'g')"
    return result


def _parse_struct_fields(type_str: str) -> list[tuple[str, str]]:
    """Parse a DuckDB STRUCT(...) type string into (name, type) pairs.

    Handles nested structs and lists by tracking parenthesis depth.
    Field names may be quoted with double quotes.
    """
    if not type_str.startswith("STRUCT("):
        return []

    inner = type_str[7:-1]  # Strip STRUCT( and )
    fields = []
    depth = 0
    current = ""

    for ch in inner + ",":  # Trailing comma to flush last field
        if ch in "([":
            depth += 1
            current += ch
        elif ch in ")]":
            depth -= 1
            current += ch
        elif ch == "," and depth == 0:
            token = current.strip()
            if token:
                if token.startswith('"'):
                    end_quote = token.index('"', 1)
                    name = token[1:end_quote]
                    ftype = token[end_quote + 1:].strip()
                else:
                    parts = token.split(None, 1)
                    name = parts[0]
                    ftype = parts[1] if len(parts) > 1 else ""
                fields.append((name, ftype))
            current = ""
        else:
            current += ch

    return fields


def _col_expr(name: str, dtype: str, domain_re: str,
              target_domain: str) -> str:
    """Build the SELECT expression for a single column."""

    # -- Known PII columns with specific handling --------------------------

    if name == "user_pseudo_id":
        return 'md5(gen_random_uuid()::text) AS "user_pseudo_id"'

    if name == "user_id":
        return ('CASE WHEN "user_id" IS NOT NULL '
                'THEN md5(gen_random_uuid()::text) ELSE NULL '
                'END AS "user_id"')

    if name == "event_params":
        return _event_params_expr(dtype, domain_re, target_domain)

    if name == "user_properties":
        # Same structure as event_params
        return _event_params_expr(
            dtype, domain_re, target_domain, col_name="user_properties")

    if name == "geo" and "STRUCT" in dtype:
        return _geo_expr(dtype)

    if name == "device" and "STRUCT" in dtype:
        return _device_expr(dtype, domain_re, target_domain)

    if name == "collected_traffic_source" and "STRUCT" in dtype:
        return _tracking_struct_expr(
            "collected_traffic_source", dtype, _TRACKING_ID_FIELDS,
            domain_re, target_domain)

    if name == "session_traffic_source_last_click" and "STRUCT" in dtype:
        return _session_traffic_source_expr(dtype, domain_re, target_domain)

    # -- Generic handling --------------------------------------------------

    if "VARCHAR" in dtype and "STRUCT" not in dtype and "[]" not in dtype:
        # Top-level string column: apply regex chain
        chain = _regex_chain_sql(f'"{name}"', domain_re, target_domain)
        return f'{chain} AS "{name}"'

    if dtype.startswith("STRUCT"):
        # Unknown struct: apply regex to string fields, pass through others
        return _generic_struct_expr(name, dtype, domain_re, target_domain)

    # Non-string (int, float, bool, timestamp, etc.) — pass through
    return f'"{name}"'


def _event_params_expr(dtype: str, domain_re: str, target_domain: str,
                       col_name: str = "event_params") -> str:
    """Build SQL for event_params: list of struct(key, value struct)."""
    # Determine value struct fields from the type
    # dtype looks like: STRUCT("key" VARCHAR, "value" STRUCT(...))[]
    # Strip [] suffix if present
    struct_type = dtype.rstrip("[]")
    outer_fields = _parse_struct_fields(struct_type)

    # Find the value field's struct type
    value_fields = []
    for fname, ftype in outer_fields:
        if fname == "value" and ftype.startswith("STRUCT"):
            value_fields = _parse_struct_fields(ftype)
            break

    if not value_fields:
        # Fallback: standard GA4 value struct
        value_fields = [
            ("string_value", "VARCHAR"),
            ("int_value", "BIGINT"),
            ("float_value", "DOUBLE"),
            ("double_value", "DOUBLE"),
        ]

    # Build value struct_pack for each CASE branch
    def _value_pack(string_expr: str) -> str:
        parts = []
        for vname, vtype in value_fields:
            if vname == "string_value":
                parts.append(f'"string_value" := {string_expr}')
            else:
                parts.append(f'"{vname}" := ep."value"."{vname}"')
        return f"struct_pack({', '.join(parts)})"

    randomize_keys = ", ".join(f"'{k}'" for k in _RANDOMIZE_PARAM_KEYS)
    redact_keys = ", ".join(f"'{k}'" for k in _REDACT_PARAM_KEYS)

    regex_sv = _regex_chain_sql('ep."value"."string_value"',
                                domain_re, target_domain)

    return f"""list_transform("{col_name}", ep -> struct_pack(
        "key" := ep."key",
        "value" := CASE
            WHEN ep."key" IN ({randomize_keys})
            THEN {_value_pack("md5(gen_random_uuid()::text)")}
            WHEN ep."key" IN ({redact_keys})
            THEN {_value_pack("'(redacted)'")}
            ELSE {_value_pack(regex_sv)}
        END
    )) AS "{col_name}" """


def _geo_expr(dtype: str) -> str:
    """Redact city/region/metro, keep everything else."""
    struct_type = dtype.rstrip("[]")
    fields = _parse_struct_fields(struct_type)
    if not fields:
        return '"geo"'

    parts = []
    for fname, ftype in fields:
        if fname in _GEO_REDACT_FIELDS:
            parts.append(
                f'"{fname}" := CASE WHEN "geo"."{fname}" IS NOT NULL '
                f"THEN '(redacted)' ELSE NULL END")
        else:
            parts.append(f'"{fname}" := "geo"."{fname}"')
    return f'struct_pack({", ".join(parts)}) AS "geo"'


def _device_expr(dtype: str, domain_re: str, target_domain: str) -> str:
    """Randomize vendor_id/advertising_id, regex on string fields."""
    struct_type = dtype.rstrip("[]")
    fields = _parse_struct_fields(struct_type)
    if not fields:
        return '"device"'

    parts = []
    for fname, ftype in fields:
        if fname in _DEVICE_ID_FIELDS:
            parts.append(
                f'"{fname}" := CASE WHEN "device"."{fname}" IS NOT NULL '
                f"THEN md5(gen_random_uuid()::text) ELSE NULL END")
        elif "VARCHAR" in ftype and "STRUCT" not in ftype:
            chain = _regex_chain_sql(f'"device"."{fname}"',
                                     domain_re, target_domain)
            parts.append(f'"{fname}" := {chain}')
        else:
            parts.append(f'"{fname}" := "device"."{fname}"')
    return f'struct_pack({", ".join(parts)}) AS "device"'


def _tracking_struct_expr(col_name: str, dtype: str,
                          id_fields: tuple, domain_re: str,
                          target_domain: str) -> str:
    """Randomize tracking ID fields, regex on string fields."""
    struct_type = dtype.rstrip("[]")
    fields = _parse_struct_fields(struct_type)
    if not fields:
        return f'"{col_name}"'

    parts = []
    for fname, ftype in fields:
        ref = f'"{col_name}"."{fname}"'
        if fname in id_fields:
            parts.append(
                f'"{fname}" := CASE WHEN {ref} IS NOT NULL '
                f"THEN md5(gen_random_uuid()::text) ELSE NULL END")
        elif "VARCHAR" in ftype and "STRUCT" not in ftype:
            chain = _regex_chain_sql(ref, domain_re, target_domain)
            parts.append(f'"{fname}" := {chain}')
        else:
            parts.append(f'"{fname}" := {ref}')
    return f'struct_pack({", ".join(parts)}) AS "{col_name}"'


def _session_traffic_source_expr(dtype: str, domain_re: str,
                                 target_domain: str) -> str:
    """Randomize campaign IDs nested inside sub-structs."""
    struct_type = dtype.rstrip("[]")
    fields = _parse_struct_fields(struct_type)
    if not fields:
        return '"session_traffic_source_last_click"'

    col = "session_traffic_source_last_click"
    parts = []
    for fname, ftype in fields:
        ref = f'"{col}"."{fname}"'
        if ftype.startswith("STRUCT"):
            # Sub-struct (google_ads_campaign, etc.): randomize ID fields
            sub_fields = _parse_struct_fields(ftype)
            if sub_fields:
                sub_parts = []
                for sf_name, sf_type in sub_fields:
                    sf_ref = f'{ref}."{sf_name}"'
                    if sf_name in _CAMPAIGN_ID_FIELDS:
                        sub_parts.append(
                            f'"{sf_name}" := CASE WHEN {sf_ref} IS NOT NULL '
                            f"THEN md5(gen_random_uuid()::text) ELSE NULL END")
                    elif "VARCHAR" in sf_type:
                        chain = _regex_chain_sql(sf_ref,
                                                 domain_re, target_domain)
                        sub_parts.append(f'"{sf_name}" := {chain}')
                    else:
                        sub_parts.append(f'"{sf_name}" := {sf_ref}')
                parts.append(
                    f'"{fname}" := struct_pack({", ".join(sub_parts)})')
            else:
                parts.append(f'"{fname}" := {ref}')
        elif "VARCHAR" in ftype:
            chain = _regex_chain_sql(ref, domain_re, target_domain)
            parts.append(f'"{fname}" := {chain}')
        else:
            parts.append(f'"{fname}" := {ref}')
    return f'struct_pack({", ".join(parts)}) AS "{col}"'


def _generic_struct_expr(name: str, dtype: str, domain_re: str,
                         target_domain: str) -> str:
    """Apply regex to string fields in an unknown struct, pass through rest."""
    struct_type = dtype.rstrip("[]")
    fields = _parse_struct_fields(struct_type)
    if not fields:
        return f'"{name}"'

    parts = []
    for fname, ftype in fields:
        ref = f'"{name}"."{fname}"'
        if "VARCHAR" in ftype and "STRUCT" not in ftype:
            chain = _regex_chain_sql(ref, domain_re, target_domain)
            parts.append(f'"{fname}" := {chain}')
        else:
            parts.append(f'"{fname}" := {ref}')
    return f'struct_pack({", ".join(parts)}) AS "{name}"'


# -- Core masking function ------------------------------------------------- #

def mask_parquet(con: duckdb.DuckDBPyConnection, src: str, dst: str,
                 source_domain: str, target_domain: str) -> int:
    """Mask PII in a single Parquet file using DuckDB SQL.

    *src* and *dst* can be local file paths or ``s3://`` URLs.
    Returns the number of rows written.
    """
    domain_re = source_domain.replace(".", r"\.")

    # Escape single quotes in paths to prevent SQL injection
    safe_src = src.replace("'", "''")
    safe_dst = dst.replace("'", "''")

    # Get schema from the parquet file
    schema = con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{safe_src}')"
    ).fetchall()

    exprs = []
    for row in schema:
        name, dtype = row[0], row[1]
        exprs.append(_col_expr(name, dtype, domain_re, target_domain))

    select_clause = ",\n        ".join(exprs)
    sql = f"""
        COPY (
            SELECT {select_clause}
            FROM read_parquet('{safe_src}')
        ) TO '{safe_dst}' (FORMAT PARQUET, COMPRESSION UNCOMPRESSED)
    """

    result = con.execute(sql)
    return result.fetchone()[0]


# -- S3 httpfs configuration ----------------------------------------------- #

# Preferred spill directory (avoid RAM-backed /tmp on some systems)
_SPILL_DIR = next((d for d in ("/var/tmp",) if os.path.isdir(d)), None)


def _configure_connection(con: duckdb.DuckDBPyConnection,
                          threads: int | None = None) -> None:
    """Set DuckDB temp directory and thread limit.

    *threads*: cap internal parallelism to avoid oversubscription when
    running multiple workers.  None = DuckDB default (all cores).
    """
    if _SPILL_DIR:
        con.execute(f"SET temp_directory = '{_SPILL_DIR}'")
    if threads is not None:
        con.execute(f"SET threads = {threads}")


def _configure_httpfs(con: duckdb.DuckDBPyConnection,
                      region: str | None = None) -> None:
    """Install and configure httpfs for S3 access.

    DuckDB picks up AWS credentials from environment variables
    (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN)
    or the default credential provider chain.
    """
    con.install_extension("httpfs")
    con.load_extension("httpfs")
    if region:
        con.execute(f"SET s3_region = '{region}'")


# -- S3 pipeline ----------------------------------------------------------- #

def _s3_url(store: S3Store, key: str) -> str:
    """Build an s3:// URL for a key relative to the store."""
    full_key = store._key(key)
    return f"s3://{store.bucket}/{full_key}"


def mask_bigquery_parquet(
    src: S3Store,
    dst: S3Store,
    dataset: str,
    source_domain: str,
    target_domain: str,
    checkpoint: CheckpointManager,
    max_workers: int = DEFAULT_WORKERS,
    s3_region: str | None = None,
    use_httpfs: bool = True,
):
    """Mask all Parquet files under bigquery/{dataset}/events/.

    When *use_httpfs* is True (default), DuckDB reads/writes S3 directly
    via httpfs — no temp files.  Set to False for testing with moto
    (falls back to local temp files with boto3 download/upload).
    """
    prefix = f"bigquery/{dataset}/events/"
    keys = [k for k in src.list_keys(prefix) if k.endswith(".parquet")]
    log.info("Found %d parquet files under %s", len(keys), prefix)

    if not checkpoint.is_phase_done("mask"):
        checkpoint.start_phase("mask", total=len(keys))

        to_mask = [k for k in keys if not checkpoint.is_item_done("mask", k)]
        already_done = len(keys) - len(to_mask)
        log.info("Masking %d files (%d already done), workers=%d",
                 len(to_mask), already_done, max_workers)

        # Cap DuckDB's internal threads so N workers don't each grab
        # all cores.  E.g. 16-core machine with 4 workers → 4 threads each.
        cpu_count = os.cpu_count() or 4
        threads_per_worker = max(1, cpu_count // max_workers)

        if to_mask and max_workers > 1 and use_httpfs:
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                futures = {
                    pool.submit(
                        _mask_one_file_s3,
                        src, dst, key,
                        source_domain, target_domain, s3_region,
                        threads_per_worker,
                    ): key
                    for key in to_mask
                }
                for future in as_completed(futures):
                    key = futures[future]
                    try:
                        rows = future.result()
                        log.info("Masked %s — %s rows", key, f"{rows:,}")
                        checkpoint.mark_item_done("mask", key)
                        checkpoint.save()
                    except Exception:
                        log.error("Failed %s", key, exc_info=True)
        elif to_mask:
            # Sequential: used for single-worker or local/test mode
            con = duckdb.connect()
            try:
                _configure_connection(con)
                if use_httpfs:
                    _configure_httpfs(con, s3_region)
                for key in to_mask:
                    log.info("Masking %s", key)
                    try:
                        rows = _mask_one_file(
                            con, src, dst, key,
                            source_domain, target_domain, use_httpfs)
                        log.info("Masked %s — %s rows", key, f"{rows:,}")
                    except Exception:
                        log.error("Failed %s", key, exc_info=True)
                    checkpoint.mark_item_done("mask", key)
                    checkpoint.save()
            finally:
                con.close()

        checkpoint.complete_phase("mask")
        checkpoint.save(force=True)

    # Copy _stats.json (no PII)
    stats_key = f"bigquery/{dataset}/_stats.json"
    stats = src.download_json(stats_key)
    if stats is not None:
        dst.upload_json(stats, stats_key)
        log.info("Copied %s", stats_key)

    checkpoint.complete()
    log.info("BigQuery masking complete for dataset %s", dataset)


def _mask_one_file_s3(src: S3Store, dst: S3Store, key: str,
                      source_domain: str, target_domain: str,
                      s3_region: str | None,
                      threads: int | None = None) -> int:
    """Worker: mask one file via httpfs (each thread gets its own connection)."""
    con = duckdb.connect()
    try:
        _configure_connection(con, threads=threads)
        _configure_httpfs(con, s3_region)
        src_url = _s3_url(src, key)
        dst_url = _s3_url(dst, key)
        return mask_parquet(con, src_url, dst_url,
                            source_domain, target_domain)
    finally:
        con.close()


def _mask_one_file(con: duckdb.DuckDBPyConnection, src: S3Store,
                   dst: S3Store, key: str, source_domain: str,
                   target_domain: str, use_httpfs: bool) -> int:
    """Mask one file — httpfs or local fallback."""
    if use_httpfs:
        src_url = _s3_url(src, key)
        dst_url = _s3_url(dst, key)
        return mask_parquet(con, src_url, dst_url,
                            source_domain, target_domain)
    else:
        # Local fallback (for testing with moto)
        import tempfile
        _TMP_DIR = next((d for d in ("/var/tmp",) if os.path.isdir(d)), None)
        with tempfile.TemporaryDirectory(dir=_TMP_DIR) as tmpdir:
            local_src = os.path.join(tmpdir, "input.parquet")
            local_dst = os.path.join(tmpdir, "output.parquet")
            src._client.download_file(
                Bucket=src.bucket, Key=src._key(key),
                Filename=local_src,
            )
            rows = mask_parquet(con, local_src, local_dst,
                                source_domain, target_domain)
            dst.upload_file(
                local_dst, key,
                content_type="application/vnd.apache.parquet",
            )
            return rows


# -- CLI -------------------------------------------------------------------- #

def main():
    from lib.config import load_dotenv, env, env_int
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Mask PII in BigQuery Parquet exports (DuckDB)",
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
    parser.add_argument("--s3-region",
                        default=env("AWS_DEFAULT_REGION"),
                        help="AWS region for DuckDB httpfs")
    parser.add_argument("--log-level", default=env("LOG_LEVEL", "INFO"))
    parser.add_argument("--no-json-logs", action="store_true")
    parser.add_argument("--log-dir", default=env("LOG_DIR", "logs"))
    args = parser.parse_args()

    if not args.src_bucket:
        parser.error("--src-bucket is required (or set S3_BUCKET)")
    if not args.dst_bucket:
        parser.error("--dst-bucket is required (or set S3_MASKED_BUCKET)")
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

    log.info("Masking BigQuery dataset %s: %s -> %s "
             "(domain: %s -> %s, workers: %d)",
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
        s3_region=args.s3_region,
    )


if __name__ == "__main__":
    main()
