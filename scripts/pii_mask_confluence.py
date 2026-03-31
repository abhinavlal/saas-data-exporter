"""PII Mask — Confluence JSON export masking.

Reads Confluence page exports from a source S3 bucket, hashes all PII
fields (author IDs, titles, body HTML, comments) with deterministic
SHA-256, and writes masked JSON to a destination bucket.

Skips attachments/ folders (binary files).
Parallelized with ThreadPoolExecutor (I/O-bound: ~4K page JSON files).
"""

import argparse
import hashlib
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from lib.checkpoint import CheckpointManager
from lib.logging import setup_logging
from lib.s3 import S3Store

log = logging.getLogger(__name__)

DEFAULT_WORKERS = 32

_ORG_SOURCE = "org_name"
_ORG_TARGET = "medica"
_HASH_SALT = "pii-mask-confluence-v1:"


# -- Hashing helpers -------------------------------------------------------- #

def _hash(value: str, length: int = 12) -> str:
    digest = hashlib.sha256((_HASH_SALT + value).encode()).hexdigest()
    return digest[:length]


def _hash_account_id(account_id: str) -> str:
    return f"acct-{_hash(account_id, 16)}" if account_id else account_id


def _hash_text(text: str) -> str:
    if not text:
        return text
    return _hash(text, 24)


# -- Page masking ----------------------------------------------------------- #

def mask_page(page: dict) -> dict:
    """Mask all PII fields in a Confluence page JSON."""
    if page.get("author_id"):
        page["author_id"] = _hash_account_id(page["author_id"])

    page["title"] = _hash_text(page.get("title", ""))
    page["body"] = _hash_text(page.get("body", ""))

    for comment in page.get("comments", []):
        if comment.get("author_id"):
            comment["author_id"] = _hash_account_id(comment["author_id"])
        comment["body"] = _hash_text(comment.get("body", ""))

    return page


# -- Org name replacement -------------------------------------------------- #

def _replace_org_in_obj(obj):
    if isinstance(obj, str):
        return obj.replace(_ORG_SOURCE, _ORG_TARGET).replace(
            _ORG_SOURCE.capitalize(), _ORG_TARGET.capitalize()).replace(
            _ORG_SOURCE.upper(), _ORG_TARGET.upper())
    if isinstance(obj, dict):
        return {k: _replace_org_in_obj(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_replace_org_in_obj(v) for v in obj]
    return obj


# -- S3 pipeline ----------------------------------------------------------- #

def _mask_one_file(src: S3Store, dst: S3Store, key: str) -> str:
    data = src.download_json(key)
    if data is None:
        return "skipped (not found)"

    filename = key.rsplit("/", 1)[-1]

    if "/pages/" in key and filename != "_index.json":
        data = mask_page(data)
    elif filename in ("_stats.json", "_index.json"):
        pass  # no PII
    else:
        return "skipped (unknown type)"

    data = _replace_org_in_obj(data)
    dst.upload_json(data, key)
    return "ok"


def mask_confluence_exports(
    src: S3Store,
    dst: S3Store,
    checkpoint: CheckpointManager,
    max_workers: int = DEFAULT_WORKERS,
):
    """Mask all Confluence page JSON files (skip attachments/)."""
    keys = src.list_keys("confluence/")
    json_keys = [k for k in keys
                 if k.endswith(".json") and "/attachments/" not in k]
    log.info("Found %d JSON files under confluence/ (skipped attachments)",
             len(json_keys))

    if not checkpoint.is_phase_done("mask"):
        checkpoint.start_phase("mask", total=len(json_keys))

        to_mask = [k for k in json_keys
                   if not checkpoint.is_item_done("mask", k)]
        done = len(json_keys) - len(to_mask)
        log.info("Masking %d files (%d already done), workers=%d",
                 len(to_mask), done, max_workers)

        if to_mask and max_workers > 1:
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                futures = {
                    pool.submit(_mask_one_file, src, dst, key): key
                    for key in to_mask
                }
                completed = 0
                for future in as_completed(futures):
                    key = futures[future]
                    try:
                        future.result()
                        completed += 1
                        if completed % 500 == 0:
                            log.info("Progress: %d/%d files",
                                     completed, len(to_mask))
                    except Exception:
                        log.error("Failed %s", key, exc_info=True)

                    checkpoint.mark_item_done("mask", key)
                    checkpoint.save()
        elif to_mask:
            for key in to_mask:
                try:
                    _mask_one_file(src, dst, key)
                except Exception:
                    log.error("Failed %s", key, exc_info=True)
                checkpoint.mark_item_done("mask", key)
                checkpoint.save()

        checkpoint.complete_phase("mask")
        checkpoint.save(force=True)

    checkpoint.complete()
    log.info("Confluence masking complete — %d files", len(json_keys))


# -- CLI -------------------------------------------------------------------- #

def main():
    from lib.config import load_dotenv, env, env_int
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Mask PII in Confluence JSON exports (deterministic hashing)",
    )
    parser.add_argument("--src-bucket", default=env("S3_BUCKET"),
                        help="Source S3 bucket (default: S3_BUCKET)")
    parser.add_argument("--dst-bucket", default=env("S3_MASKED_BUCKET"),
                        help="Destination S3 bucket (default: S3_MASKED_BUCKET)")
    parser.add_argument("--s3-prefix", default=env("S3_PREFIX", ""),
                        help="S3 key prefix")
    parser.add_argument("--max-workers", type=int,
                        default=env_int("PII_MAX_WORKERS", DEFAULT_WORKERS),
                        help=f"Parallel workers (default: {DEFAULT_WORKERS})")
    parser.add_argument("--log-level", default=env("LOG_LEVEL", "INFO"))
    parser.add_argument("--no-json-logs", action="store_true")
    parser.add_argument("--log-dir", default=env("LOG_DIR", "logs"))
    args = parser.parse_args()

    if not args.src_bucket:
        parser.error("--src-bucket is required (or set S3_BUCKET)")
    if not args.dst_bucket:
        parser.error("--dst-bucket is required (or set S3_MASKED_BUCKET)")

    import os
    log_file = os.path.join(args.log_dir, "pii_mask_confluence.log")
    setup_logging(
        level=args.log_level,
        json_output=not args.no_json_logs,
        log_file=log_file,
    )

    src = S3Store(bucket=args.src_bucket, prefix=args.s3_prefix)
    dst = S3Store(bucket=args.dst_bucket, prefix=args.s3_prefix)
    checkpoint = CheckpointManager(dst, "pii_mask/confluence")
    checkpoint.load()

    log.info("Masking Confluence exports: %s -> %s (workers: %d)",
             args.src_bucket, args.dst_bucket, args.max_workers)

    mask_confluence_exports(
        src=src, dst=dst,
        checkpoint=checkpoint,
        max_workers=args.max_workers,
    )


if __name__ == "__main__":
    main()
