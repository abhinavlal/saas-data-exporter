"""CLI entry point for the PII masking pipeline.

Usage:
    python -m scripts.pii_mask --roster roster.json
    python -m scripts.pii_mask --roster roster.json --enable-ner
    python -m scripts.pii_mask --roster roster.json --exporters github jira
    python -m scripts.pii_mask --roster roster.json --exporters bigquery --dataset analytics_123
"""

import argparse
import logging

from lib.checkpoint import CheckpointManager
from lib.logging import setup_logging
from lib.s3 import S3Store
from scripts.pii_mask.manifest import Manifest
from scripts.pii_mask.pipeline import DEFAULT_WORKERS, run_pipeline
from scripts.pii_mask.roster import Roster
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
        description="Roster-based PII masking pipeline for exported SaaS data",
    )
    parser.add_argument("--roster", required=True,
                        help="Path to roster.json (local file or S3 key)")
    parser.add_argument("--src-bucket", default=env("S3_BUCKET"),
                        help="Source S3 bucket (default: S3_BUCKET)")
    parser.add_argument("--dst-bucket", default=env("S3_MASKED_BUCKET"),
                        help="Destination S3 bucket (default: S3_MASKED_BUCKET)")
    parser.add_argument("--s3-prefix", default=env("S3_PREFIX", ""),
                        help="Source S3 key prefix")
    parser.add_argument("--dst-prefix", default=env("S3_DST_PREFIX"),
                        help="Destination S3 key prefix (required)")
    parser.add_argument("--exporters", nargs="*", default=None,
                        choices=ALL_EXPORTERS,
                        help="Exporters to mask (default: all)")
    parser.add_argument("--max-workers", type=int,
                        default=env_int("PII_MAX_WORKERS", DEFAULT_WORKERS),
                        help=f"Parallel workers (default: {DEFAULT_WORKERS})")

    # BigQuery-specific options
    parser.add_argument("--dataset", default=env("BIGQUERY_DATASET", ""),
                        help="BigQuery dataset ID (required for bigquery exporter)")
    parser.add_argument("--source-domain",
                        default=env("PII_SOURCE_DOMAIN", "org_name.com"),
                        help="Domain to replace in BigQuery data")
    parser.add_argument("--target-domain",
                        default=env("PII_TARGET_DOMAIN", "example-health.com"),
                        help="Replacement domain for BigQuery data")
    parser.add_argument("--s3-region", default=env("AWS_DEFAULT_REGION"),
                        help="AWS region for DuckDB httpfs")

    # NER options
    parser.add_argument("--enable-ner", action="store_true",
                        help="Enable Presidio NER for second-pass PII detection")
    parser.add_argument("--ner-threshold", type=float, default=0.7,
                        help="NER confidence threshold (default: 0.7)")

    # Logging
    parser.add_argument("--log-level", default=env("LOG_LEVEL", "INFO"))
    parser.add_argument("--no-json-logs", action="store_true")
    parser.add_argument("--log-dir", default=env("LOG_DIR", "logs"))
    args = parser.parse_args()

    if not args.src_bucket:
        parser.error("--src-bucket is required (or set S3_BUCKET)")
    if not args.dst_bucket:
        parser.error("--dst-bucket is required (or set S3_MASKED_BUCKET)")
    if not args.dst_prefix:
        parser.error("--dst-prefix is required (or set S3_DST_PREFIX). "
                     "This prevents accidental overwrites when src and "
                     "dst buckets are the same.")

    import os
    log_file = os.path.join(args.log_dir, "pii_mask.log")
    setup_logging(
        level=args.log_level,
        json_output=not args.no_json_logs,
        log_file=log_file,
    )

    # Load roster
    if args.roster.startswith("s3://"):
        parts = args.roster[5:].split("/", 1)
        roster_store = S3Store(bucket=parts[0])
        roster = Roster.from_s3(roster_store, parts[1])
    else:
        roster = Roster.from_file(args.roster)

    # Optional NER engine
    ner_engine = None
    if args.enable_ner:
        from scripts.pii_mask.ner import NEREngine
        ner_engine = NEREngine(score_threshold=args.ner_threshold)

    scanner = TextScanner(roster, ner_engine=ner_engine)

    # Build masker list
    selected = args.exporters or list(ALL_EXPORTERS)
    maskers = []
    for name in selected:
        if name == "github":
            maskers.append(GitHubMasker(roster, scanner))
        elif name == "jira":
            maskers.append(JiraMasker(roster, scanner))
        elif name == "confluence":
            maskers.append(ConfluenceMasker(roster, scanner))
        elif name == "slack":
            maskers.append(SlackMasker(roster, scanner))
        elif name == "google":
            maskers.append(GoogleMasker(roster, scanner))
        elif name == "bigquery":
            if not args.dataset:
                parser.error("--dataset is required for bigquery exporter")
            maskers.append(BigQueryMasker(
                roster, scanner,
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
             "(roster: %d users, exporters: %s, workers: %d, ner: %s)",
             src_path, dst_path, len(roster.users),
             ", ".join(selected), args.max_workers,
             "enabled" if args.enable_ner else "disabled")

    run_pipeline(
        src=src, dst=dst,
        maskers=maskers,
        checkpoint=checkpoint,
        manifest=manifest,
        max_workers=args.max_workers,
    )


if __name__ == "__main__":
    main()
