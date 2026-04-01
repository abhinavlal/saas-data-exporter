"""CLI entry point for the PII masking pipeline.

Presidio-first architecture: detects ALL PII, replaces consistently
via PIIStore (SQLite). No --enable-ner flag — NER is always on.

Usage:
    # Import existing roster into SQLite store:
    python -m scripts.pii_mask --store pii_store.db --import-roster roster.json

    # Run masking:
    python -m scripts.pii_mask --store pii_store.db \\
        --src-bucket my-bucket --s3-prefix v31/ \\
        --dst-bucket my-bucket --dst-prefix masked-v1/
"""

import argparse
import logging

from lib.checkpoint import CheckpointManager
from lib.logging import setup_logging
from lib.s3 import S3Store
from scripts.pii_mask.manifest import Manifest
from scripts.pii_mask.pii_store import PIIStore
from scripts.pii_mask.pipeline import DEFAULT_WORKERS, run_pipeline
from scripts.pii_mask.scanner import TextScanner
from scripts.pii_mask.maskers.github import GitHubMasker
from scripts.pii_mask.maskers.jira import JiraMasker
from scripts.pii_mask.maskers.confluence import ConfluenceMasker
from scripts.pii_mask.maskers.slack import SlackMasker
from scripts.pii_mask.maskers.google import GoogleMasker
from scripts.pii_mask.maskers.bigquery import BigQueryMasker

log = logging.getLogger(__name__)

ALL_EXPORTERS = ("github", "jira", "confluence", "slack", "google", "bigquery")


def main():
    from lib.config import load_dotenv, env, env_int
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Presidio-first PII masking pipeline",
    )

    # Store
    parser.add_argument("--store", required=True,
                        help="Path to PIIStore SQLite database")
    parser.add_argument("--import-roster", default=None,
                        help="Import roster.json into the store (then exit)")

    # S3
    parser.add_argument("--src-bucket", default=env("S3_BUCKET"))
    parser.add_argument("--dst-bucket", default=env("S3_MASKED_BUCKET"))
    parser.add_argument("--s3-prefix", default=env("S3_PREFIX", ""))
    parser.add_argument("--dst-prefix", default=env("S3_DST_PREFIX"))

    # Exporters
    parser.add_argument("--exporters", nargs="*", default=None,
                        choices=ALL_EXPORTERS)
    parser.add_argument("--max-workers", type=int,
                        default=env_int("PII_MAX_WORKERS", DEFAULT_WORKERS))

    # BigQuery-specific
    parser.add_argument("--dataset", default=env("BIGQUERY_DATASET", ""))
    parser.add_argument("--source-domain",
                        default=env("PII_SOURCE_DOMAIN", "org_name.com"))
    parser.add_argument("--target-domain",
                        default=env("PII_TARGET_DOMAIN", "example-health.com"))
    parser.add_argument("--s3-region", default=env("AWS_DEFAULT_REGION"))

    # Scanner
    parser.add_argument("--presidio-threshold", type=float, default=0.5,
                        help="Presidio confidence threshold (default: 0.5)")

    # Logging
    parser.add_argument("--log-level", default=env("LOG_LEVEL", "INFO"))
    parser.add_argument("--no-json-logs", action="store_true")
    parser.add_argument("--log-dir", default=env("LOG_DIR", "logs"))
    args = parser.parse_args()

    import os
    log_file = os.path.join(args.log_dir, "pii_mask.log")
    setup_logging(
        level=args.log_level,
        json_output=not args.no_json_logs,
        log_file=log_file,
    )

    # Handle roster import
    if args.import_roster:
        store = PIIStore.from_json(args.import_roster, args.store)
        stats = store.stats()
        log.info("Import complete. Store stats: %s", stats)
        return

    # Validate required args for masking run
    if not args.src_bucket:
        parser.error("--src-bucket is required (or set S3_BUCKET)")
    if not args.dst_bucket:
        parser.error("--dst-bucket is required (or set S3_MASKED_BUCKET)")
    if not args.dst_prefix:
        parser.error("--dst-prefix is required (or set S3_DST_PREFIX)")

    # Load store + build scanner
    store = PIIStore(args.store)
    scanner = TextScanner(store, threshold=args.presidio_threshold)

    # Build masker list
    selected = args.exporters or list(ALL_EXPORTERS)
    maskers = []
    for name in selected:
        if name == "github":
            maskers.append(GitHubMasker(scanner))
        elif name == "jira":
            maskers.append(JiraMasker(scanner))
        elif name == "confluence":
            maskers.append(ConfluenceMasker(scanner))
        elif name == "slack":
            maskers.append(SlackMasker(scanner))
        elif name == "google":
            maskers.append(GoogleMasker(scanner))
        elif name == "bigquery":
            if not args.dataset:
                parser.error("--dataset is required for bigquery exporter")
            maskers.append(BigQueryMasker(
                scanner,
                dataset=args.dataset,
                source_domain=args.source_domain,
                target_domain=args.target_domain,
                s3_region=args.s3_region,
            ))

    src = S3Store(bucket=args.src_bucket, prefix=args.s3_prefix)
    dst = S3Store(bucket=args.dst_bucket, prefix=args.dst_prefix)
    checkpoint = CheckpointManager(dst, "pii_mask/pipeline")
    checkpoint.load()
    manifest = Manifest(args.src_bucket, args.dst_bucket)

    src_path = f"{args.src_bucket}/{args.s3_prefix}" if args.s3_prefix \
        else args.src_bucket
    dst_path = f"{args.dst_bucket}/{args.dst_prefix}"
    log.info("Starting PII masking pipeline: %s -> %s "
             "(store: %d entries, exporters: %s, workers: %d)",
             src_path, dst_path, len(store._cache),
             ", ".join(selected), args.max_workers)

    run_pipeline(
        src=src, dst=dst,
        maskers=maskers,
        checkpoint=checkpoint,
        manifest=manifest,
        max_workers=args.max_workers,
    )

    # Log final store stats
    stats = store.stats()
    log.info("PIIStore stats after run: %s", stats)


if __name__ == "__main__":
    main()
