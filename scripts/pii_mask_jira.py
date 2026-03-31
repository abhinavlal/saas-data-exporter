"""PII Mask — Jira JSON export masking.

Reads Jira ticket exports from a source S3 bucket, hashes all PII fields
(names, emails, account IDs, ticket content, comments, changelog) with
deterministic SHA-256, and writes masked JSON to a destination bucket.

Skips attachments/ folders (binary files — handled separately).
Parallelized with ThreadPoolExecutor (I/O-bound: 138K small JSON files).
"""

import argparse
import hashlib
import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

from lib.checkpoint import CheckpointManager
from lib.logging import setup_logging
from lib.s3 import S3Store

log = logging.getLogger(__name__)

DEFAULT_WORKERS = 32

# Org name replacement (case-preserving)
_ORG_SOURCE = "org_name"
_ORG_TARGET = "medica"

# Salt prefix for hashing
_HASH_SALT = "pii-mask-jira-v1:"


# -- Hashing helpers -------------------------------------------------------- #

def _hash(value: str, length: int = 12) -> str:
    """Deterministic hash: same input always produces the same output."""
    digest = hashlib.sha256((_HASH_SALT + value).encode()).hexdigest()
    return digest[:length]


def _hash_email(email: str) -> str:
    if not email or "@" not in email:
        return _hash(email) if email else email
    local, _ = email.rsplit("@", 1)
    return f"{_hash(local, 8)}@example-health.com"


def _hash_name(name: str) -> str:
    return f"User {_hash(name, 8)}" if name else name


def _hash_account_id(account_id: str) -> str:
    return f"acct-{_hash(account_id, 16)}" if account_id else account_id


def _hash_text(text: str) -> str:
    """Hash the entire text content. Deterministic."""
    if not text:
        return text
    return _hash(text, 24)


def _hash_url(url: str) -> str:
    """Hash URLs — replace domain and identifiers."""
    if not url:
        return url
    url = url.replace(f"{_ORG_SOURCE}.atlassian.net",
                      f"{_ORG_TARGET}.atlassian.net")
    return url


# -- Ticket masking --------------------------------------------------------- #

def mask_ticket(ticket: dict) -> dict:
    """Mask all PII fields in a Jira ticket JSON."""
    # -- Person fields (assignee, reporter, creator) --
    for prefix in ("assignee", "reporter", "creator"):
        if ticket.get(prefix):
            ticket[prefix] = _hash_name(ticket[prefix])
        email_key = f"{prefix}_email"
        if ticket.get(email_key):
            ticket[email_key] = _hash_email(ticket[email_key])
        acct_key = f"{prefix}_account_id"
        if ticket.get(acct_key):
            ticket[acct_key] = _hash_account_id(ticket[acct_key])

    # -- Freeform text --
    ticket["summary"] = _hash_text(ticket.get("summary", ""))
    ticket["description_text"] = _hash_text(
        ticket.get("description_text", ""))
    if ticket.get("description_adf"):
        ticket["description_adf"] = _mask_adf(ticket["description_adf"])
    ticket["parent_summary"] = _hash_text(
        ticket.get("parent_summary", ""))

    # -- URL with domain --
    if ticket.get("self"):
        ticket["self"] = _hash_url(ticket["self"])

    # -- Comments --
    for comment in ticket.get("comments", []):
        _mask_comment(comment)

    # -- Attachments metadata (not the binary files) --
    for att in ticket.get("attachments", []):
        _mask_attachment_meta(att)

    # -- Changelog --
    for entry in ticket.get("changelog", []):
        _mask_changelog_entry(entry)

    # -- Custom fields --
    _mask_custom_fields(ticket)

    return ticket


def _mask_comment(comment: dict) -> None:
    """Mask PII in a comment dict."""
    comment["author"] = _hash_name(comment.get("author", ""))
    if comment.get("author_email"):
        comment["author_email"] = _hash_email(comment["author_email"])
    if comment.get("author_account_id"):
        comment["author_account_id"] = _hash_account_id(
            comment["author_account_id"])
    comment["body_text"] = _hash_text(comment.get("body_text", ""))
    if comment.get("body_adf"):
        comment["body_adf"] = _mask_adf(comment["body_adf"])
    if comment.get("rendered_body"):
        comment["rendered_body"] = _hash_text(comment["rendered_body"])


def _mask_attachment_meta(att: dict) -> None:
    """Mask PII in attachment metadata (not the binary file)."""
    att["author"] = _hash_name(att.get("author", ""))
    if att.get("author_email"):
        att["author_email"] = _hash_email(att["author_email"])
    if att.get("content_url"):
        att["content_url"] = _hash_url(att["content_url"])


def _mask_changelog_entry(entry: dict) -> None:
    """Mask PII in a changelog entry."""
    entry["author"] = _hash_name(entry.get("author", ""))
    # from/to can contain user names when assignment fields change
    if entry.get("field") in ("assignee", "reporter", "creator",
                               "Reviewer", "Approver"):
        entry["from"] = _hash_name(entry.get("from", ""))
        entry["to"] = _hash_name(entry.get("to", ""))


def _mask_custom_fields(ticket: dict) -> None:
    """Hash custom field values that might contain PII."""
    # Custom fields are keyed as "Custom field (Name)": value
    for key in list(ticket.keys()):
        if not key.startswith("Custom field ("):
            continue
        val = ticket[key]
        if isinstance(val, str) and val:
            # Hash non-empty string custom field values
            ticket[key] = _hash_text(val)


def _mask_adf(adf: dict) -> dict:
    """Mask PII in Atlassian Document Format JSON.

    Walks the ADF tree and:
    - Hashes text in 'text' nodes
    - Hashes mention attrs (id, text) in 'mention' nodes
    """
    if not isinstance(adf, dict):
        return adf

    if adf.get("type") == "text" and "text" in adf:
        adf["text"] = _hash_text(adf["text"])

    if adf.get("type") == "mention":
        attrs = adf.get("attrs", {})
        if attrs.get("id"):
            attrs["id"] = _hash_account_id(attrs["id"])
        if attrs.get("text"):
            attrs["text"] = _hash_name(attrs["text"])

    for child in adf.get("content", []):
        _mask_adf(child)

    return adf


# -- Org name replacement -------------------------------------------------- #

def _replace_org_in_obj(obj):
    """Recursively replace org name in all string values."""
    if isinstance(obj, str):
        return obj.replace(_ORG_SOURCE, _ORG_TARGET).replace(
            _ORG_SOURCE.capitalize(), _ORG_TARGET.capitalize()).replace(
            _ORG_SOURCE.upper(), _ORG_TARGET.upper())
    if isinstance(obj, dict):
        return {k: _replace_org_in_obj(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_replace_org_in_obj(v) for v in obj]
    return obj


def _rewrite_key(key: str) -> str:
    """Rewrite S3 key: replace org name in project paths."""
    return key.replace(_ORG_SOURCE, _ORG_TARGET)


# -- S3 pipeline ----------------------------------------------------------- #

def _mask_one_file(src: S3Store, dst: S3Store, key: str) -> str:
    """Download, mask, upload one Jira JSON file. Returns status message."""
    data = src.download_json(key)
    if data is None:
        return "skipped (not found)"

    filename = key.rsplit("/", 1)[-1]

    if "/tickets/" in key and filename != "_index.json":
        data = mask_ticket(data)
    elif filename == "_stats.json":
        pass  # no PII
    elif filename == "_index.json":
        pass  # ticket key list, no PII
    else:
        return "skipped (unknown type)"

    # Replace org name in all remaining strings
    data = _replace_org_in_obj(data)

    dst_key = _rewrite_key(key)
    dst.upload_json(data, dst_key)
    return "ok"


def mask_jira_exports(
    src: S3Store,
    dst: S3Store,
    checkpoint: CheckpointManager,
    max_workers: int = DEFAULT_WORKERS,
):
    """Mask all Jira ticket JSON files (skip attachments/)."""
    keys = src.list_keys("jira/")
    # Only JSON files, skip attachments/ directory
    json_keys = [k for k in keys
                 if k.endswith(".json") and "/attachments/" not in k]
    log.info("Found %d JSON files under jira/ (skipped attachments)",
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
                        msg = future.result()
                        completed += 1
                        if completed % 5000 == 0:
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
    log.info("Jira masking complete — %d files", len(json_keys))


# -- CLI -------------------------------------------------------------------- #

def main():
    from lib.config import load_dotenv, env, env_int
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Mask PII in Jira JSON exports (deterministic hashing)",
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
    log_file = os.path.join(args.log_dir, "pii_mask_jira.log")
    setup_logging(
        level=args.log_level,
        json_output=not args.no_json_logs,
        log_file=log_file,
    )

    src = S3Store(bucket=args.src_bucket, prefix=args.s3_prefix)
    dst = S3Store(bucket=args.dst_bucket, prefix=args.s3_prefix)
    checkpoint = CheckpointManager(dst, "pii_mask/jira")
    checkpoint.load()

    log.info("Masking Jira exports: %s -> %s (workers: %d)",
             args.src_bucket, args.dst_bucket, args.max_workers)

    mask_jira_exports(
        src=src, dst=dst,
        checkpoint=checkpoint,
        max_workers=args.max_workers,
    )


if __name__ == "__main__":
    main()
