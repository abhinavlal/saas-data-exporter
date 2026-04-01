"""Sample extractor — fast S3 sampling for smoke testing.

Uses delimiter-based prefix listing to avoid enumerating millions of
keys.  Strategy: list top-level prefixes (repos/projects/channels/users),
pick a random subset, then grab files from each.

Usage:
    python -m scripts.pii_mask.sample \\
        --bucket my-export-bucket --s3-prefix v31/ \\
        --sample-prefix _smoke_test/input/ \\
        --files-per-exporter 300
"""

import argparse
import logging
import random
from concurrent.futures import ThreadPoolExecutor, as_completed

from lib.s3 import S3Store

log = logging.getLogger(__name__)

DEFAULT_FILES_PER_EXPORTER = 300
DEFAULT_WORKERS = 32


def _list_prefixes(client, bucket: str, prefix: str,
                   delimiter: str = "/") -> list[str]:
    """List common prefixes (subdirectories) under a prefix. Fast — one API call."""
    prefixes = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix,
                                   Delimiter=delimiter):
        for cp in page.get("CommonPrefixes", []):
            prefixes.append(cp["Prefix"])
    return prefixes


def _list_keys_fast(client, bucket: str, prefix: str,
                    max_keys: int = 1000,
                    suffix: str = "") -> list[str]:
    """List up to max_keys under a prefix. Stops after first page if enough."""
    keys = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix,
                                   PaginationConfig={"MaxItems": max_keys,
                                                     "PageSize": 1000}):
        for obj in page.get("Contents", []):
            k = obj["Key"]
            if suffix and not k.endswith(suffix):
                continue
            keys.append(k)
            if len(keys) >= max_keys:
                return keys
    return keys


def _strip_store_prefix(keys: list[str], store_prefix: str) -> list[str]:
    """Strip the store prefix from S3 keys to get relative paths."""
    if not store_prefix:
        return keys
    p = store_prefix.rstrip("/") + "/"
    return [k[len(p):] if k.startswith(p) else k for k in keys]


def _copy_keys(src: S3Store, dst: S3Store, keys: list[str],
               max_workers: int) -> int:
    """Copy files from src to dst. Returns count copied."""
    def _copy_one(key):
        data = src.download_bytes(key)
        if data is None:
            return False
        ct = "application/json"
        if key.endswith(".eml"):
            ct = "message/rfc822"
        elif key.endswith(".parquet"):
            ct = "application/vnd.apache.parquet"
        dst.upload_bytes(data, key, content_type=ct)
        return True

    copied = 0
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_copy_one, k): k for k in keys}
        for future in as_completed(futures):
            try:
                if future.result():
                    copied += 1
            except Exception:
                log.error("Copy failed: %s", futures[future], exc_info=True)
    return copied


# -- Per-exporter samplers ------------------------------------------------- #
# Each returns a list of relative S3 keys to copy.
# Strategy: list sub-prefixes (fast), pick random ones, list within those.

def sample_github(client, bucket: str, prefix: str,
                  n: int, rng: random.Random) -> list[str]:
    """github/{org}__repo/ → pick repos, grab PRs + metadata from each."""
    repo_prefixes = _list_prefixes(client, bucket, prefix + "github/")
    if not repo_prefixes:
        return []
    repos = rng.sample(repo_prefixes, min(len(repo_prefixes), max(5, n // 50)))
    log.info("  github: %d repos available, sampling from %d",
             len(repo_prefixes), len(repos))

    keys = []
    per_repo = max(10, n // len(repos))
    for repo_prefix in repos:
        # Grab PRs
        pr_keys = _list_keys_fast(client, bucket, repo_prefix + "prs/",
                                  max_keys=per_repo, suffix=".json")
        keys.extend(pr_keys)
        # Grab metadata files
        for fname in ("contributors.json", "repo_metadata.json", "_stats.json"):
            meta_keys = _list_keys_fast(client, bucket, repo_prefix + fname,
                                        max_keys=1)
            keys.extend(meta_keys)
    return _strip_store_prefix(rng.sample(keys, min(n, len(keys))),
                               prefix)


def sample_jira(client, bucket: str, prefix: str,
                n: int, rng: random.Random) -> list[str]:
    """jira/{project}/tickets/ → pick projects, grab tickets."""
    proj_prefixes = _list_prefixes(client, bucket, prefix + "jira/")
    if not proj_prefixes:
        return []
    projects = rng.sample(proj_prefixes, min(len(proj_prefixes), max(3, n // 100)))
    log.info("  jira: %d projects available, sampling from %d",
             len(proj_prefixes), len(projects))

    keys = []
    per_proj = max(20, n // len(projects))
    for proj in projects:
        ticket_keys = _list_keys_fast(client, bucket, proj + "tickets/",
                                      max_keys=per_proj, suffix=".json")
        keys.extend(ticket_keys)
    return _strip_store_prefix(rng.sample(keys, min(n, len(keys))),
                               prefix)


def sample_slack(client, bucket: str, prefix: str,
                 n: int, rng: random.Random) -> list[str]:
    """slack/{channel_id}/ → pick channels, grab messages + info."""
    chan_prefixes = _list_prefixes(client, bucket, prefix + "slack/")
    if not chan_prefixes:
        return []
    channels = rng.sample(chan_prefixes, min(len(chan_prefixes), max(10, n // 30)))
    log.info("  slack: %d channels available, sampling from %d",
             len(chan_prefixes), len(channels))

    keys = []
    per_chan = max(10, n // len(channels))
    for chan in channels:
        chan_keys = _list_keys_fast(client, bucket, chan,
                                   max_keys=per_chan, suffix=".json")
        # Filter out attachments
        chan_keys = [k for k in chan_keys if "/attachments/" not in k]
        keys.extend(chan_keys)
    return _strip_store_prefix(rng.sample(keys, min(n, len(keys))),
                               prefix)


def sample_confluence(client, bucket: str, prefix: str,
                      n: int, rng: random.Random) -> list[str]:
    """confluence/{space}/pages/ → pick spaces, grab pages."""
    space_prefixes = _list_prefixes(client, bucket, prefix + "confluence/")
    if not space_prefixes:
        return []
    spaces = rng.sample(space_prefixes, min(len(space_prefixes), 5))
    log.info("  confluence: %d spaces available, sampling from %d",
             len(space_prefixes), len(spaces))

    keys = []
    per_space = max(20, n // len(spaces))
    for space in spaces:
        page_keys = _list_keys_fast(client, bucket, space + "pages/",
                                    max_keys=per_space, suffix=".json")
        keys.extend(page_keys)
    return _strip_store_prefix(rng.sample(keys, min(n, len(keys))),
                               prefix)


def sample_google(client, bucket: str, prefix: str,
                  n: int, rng: random.Random) -> list[str]:
    """google/{user_slug}/ → pick users, grab calendar + gmail + drive."""
    user_prefixes = _list_prefixes(client, bucket, prefix + "google/")
    if not user_prefixes:
        return []
    users = rng.sample(user_prefixes, min(len(user_prefixes), max(5, n // 50)))
    log.info("  google: %d users available, sampling from %d",
             len(user_prefixes), len(users))

    keys = []
    per_user = max(10, n // len(users))
    for user in users:
        # Calendar events
        cal_keys = _list_keys_fast(client, bucket, user + "calendar/",
                                   max_keys=per_user // 3, suffix=".json")
        keys.extend(cal_keys)
        # Gmail EMLs (sample a few)
        eml_keys = _list_keys_fast(client, bucket, user + "gmail/",
                                   max_keys=per_user // 3, suffix=".eml")
        keys.extend(eml_keys)
        # Gmail index
        idx_keys = _list_keys_fast(client, bucket, user + "gmail/_index.json",
                                   max_keys=1)
        keys.extend(idx_keys)
        # Drive
        drive_keys = _list_keys_fast(client, bucket, user + "drive/",
                                     max_keys=per_user // 3, suffix=".json")
        keys.extend(drive_keys)

    return _strip_store_prefix(rng.sample(keys, min(n, len(keys))),
                               prefix)


def sample_bigquery(client, bucket: str, prefix: str,
                    n: int, rng: random.Random) -> list[str]:
    """bigquery/{dataset}/events/ → grab a few parquet files."""
    n = min(n, 5)  # parquet files are large
    keys = _list_keys_fast(client, bucket, prefix + "bigquery/",
                           max_keys=50, suffix=".parquet")
    if not keys:
        return []
    sampled = rng.sample(keys, min(n, len(keys)))
    log.info("  bigquery: %d parquet files available, sampling %d",
             len(keys), len(sampled))
    return _strip_store_prefix(sampled, prefix)


SAMPLERS = {
    "github": sample_github,
    "jira": sample_jira,
    "slack": sample_slack,
    "confluence": sample_confluence,
    "google": sample_google,
    "bigquery": sample_bigquery,
}


# -- CLI ------------------------------------------------------------------- #

def main():
    from lib.config import load_dotenv, env, env_int
    from lib.logging import setup_logging
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Extract a random sample of exported data for smoke testing",
    )
    parser.add_argument("--bucket", default=env("S3_BUCKET"),
                        help="Source S3 bucket")
    parser.add_argument("--s3-prefix", default=env("S3_PREFIX", ""),
                        help="Source S3 prefix")
    parser.add_argument("--dst-bucket", default=None,
                        help="Destination bucket (default: same as --bucket)")
    parser.add_argument("--sample-prefix",
                        default="_smoke_test/input/",
                        help="Destination prefix for sample files")
    parser.add_argument("--files-per-exporter", type=int,
                        default=DEFAULT_FILES_PER_EXPORTER)
    parser.add_argument("--exporters", nargs="*", default=None,
                        choices=list(SAMPLERS.keys()))
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--max-workers", type=int,
                        default=env_int("PII_MAX_WORKERS", DEFAULT_WORKERS))
    parser.add_argument("--log-level", default=env("LOG_LEVEL", "INFO"))
    args = parser.parse_args()

    setup_logging(level=args.log_level, json_output=False)

    if not args.bucket:
        parser.error("--bucket is required (or set S3_BUCKET)")

    import boto3
    from botocore.config import Config as BotocoreConfig
    client = boto3.client("s3", config=BotocoreConfig(
        retries={"max_attempts": 5, "mode": "adaptive"}))

    # Build full prefix for S3 API calls
    full_prefix = args.s3_prefix.strip("/") + "/" if args.s3_prefix else ""

    src = S3Store(bucket=args.bucket, prefix=args.s3_prefix)
    dst_bucket = args.dst_bucket or args.bucket
    dst = S3Store(bucket=dst_bucket, prefix=args.sample_prefix)

    selected = args.exporters or list(SAMPLERS.keys())
    rng = random.Random(args.seed)
    total_copied = 0

    log.info("Sampling %d files/exporter from %s/%s → %s/%s",
             args.files_per_exporter, args.bucket, args.s3_prefix,
             dst_bucket, args.sample_prefix)

    for exporter in selected:
        log.info("Sampling %s...", exporter)
        sampler = SAMPLERS[exporter]
        keys = sampler(client, args.bucket, full_prefix,
                       args.files_per_exporter, rng)
        if not keys:
            log.warning("  %s: no files found — skipping", exporter)
            continue

        log.info("  %s: copying %d files...", exporter, len(keys))
        copied = _copy_keys(src, dst, keys, args.max_workers)
        total_copied += copied
        log.info("  %s: done (%d copied)", exporter, copied)

    log.info("Sample complete: %d total files → %s/%s",
             total_copied, dst_bucket, args.sample_prefix)


if __name__ == "__main__":
    main()
