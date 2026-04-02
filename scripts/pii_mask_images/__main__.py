"""CLI entry point for the image PII masking pipeline.

Standalone pipeline — runs independently from the text masking
pipeline. Can be deployed on a separate system.

Usage:
    python -m scripts.pii_mask_images --store pii_store.db \
        --src-bucket my-bucket --s3-prefix v31/ \
        --dst-bucket my-bucket --dst-prefix masked-v1/

    # Only process specific exporters:
    python -m scripts.pii_mask_images --store pii_store.db \
        --prefixes jira/ slack/ \
        --src-bucket ... --dst-bucket ...
"""

import argparse
import logging

from lib.checkpoint import CheckpointManager
from lib.logging import setup_logging
from lib.s3 import S3Store
from scripts.pii_mask_images.pipeline import (
    DEFAULT_WORKERS, IMAGE_PREFIXES, run_pipeline,
)

log = logging.getLogger(__name__)


def main():
    from lib.config import load_dotenv, env, env_int
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Image PII masking pipeline — OCR + blur",
    )

    # Store
    parser.add_argument("--store", required=True,
                        help="Path to PIIStore SQLite database")

    # S3
    parser.add_argument("--src-bucket", default=env("S3_BUCKET"))
    parser.add_argument("--dst-bucket", default=env("S3_MASKED_BUCKET"))
    parser.add_argument("--s3-prefix", default=env("S3_PREFIX", ""))
    parser.add_argument("--dst-prefix", default=env("S3_DST_PREFIX"))

    # Processing
    parser.add_argument("--prefixes", nargs="*", default=None,
                        help=f"S3 prefixes to scan (default: {IMAGE_PREFIXES})")
    parser.add_argument("--max-workers", type=int,
                        default=env_int("IMAGE_MAX_WORKERS", DEFAULT_WORKERS))
    parser.add_argument("--presidio-threshold", type=float, default=0.5)

    # Logging
    parser.add_argument("--log-level", default=env("LOG_LEVEL", "INFO"))
    parser.add_argument("--no-json-logs", action="store_true")
    parser.add_argument("--log-dir", default=env("LOG_DIR", "logs"))
    args = parser.parse_args()

    import os
    log_file = os.path.join(args.log_dir, "pii_mask_images.log")
    setup_logging(
        level=args.log_level,
        json_output=not args.no_json_logs,
        log_file=log_file,
    )

    if not args.src_bucket:
        parser.error("--src-bucket is required (or set S3_BUCKET)")
    if not args.dst_bucket:
        parser.error("--dst-bucket is required (or set S3_MASKED_BUCKET)")
    if not args.dst_prefix:
        parser.error("--dst-prefix is required (or set S3_DST_PREFIX)")

    src = S3Store(bucket=args.src_bucket, prefix=args.s3_prefix)
    dst = S3Store(bucket=args.dst_bucket, prefix=args.dst_prefix)
    checkpoint = CheckpointManager(dst, "pii_mask_images/pipeline")
    checkpoint.load()

    src_path = f"{args.src_bucket}/{args.s3_prefix}" if args.s3_prefix \
        else args.src_bucket
    dst_path = f"{args.dst_bucket}/{args.dst_prefix}"
    log.info("Starting image PII masking: %s -> %s (workers: %d)",
             src_path, dst_path, args.max_workers)

    run_pipeline(
        src=src, dst=dst,
        checkpoint=checkpoint,
        max_workers=args.max_workers,
        store_path=args.store,
        threshold=args.presidio_threshold,
        prefixes=args.prefixes,
    )


if __name__ == "__main__":
    main()
