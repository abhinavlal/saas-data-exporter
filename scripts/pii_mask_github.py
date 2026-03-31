"""PII Mask — GitHub JSON export masking.

Reads GitHub exports from a source S3 bucket, hashes all PII fields
(usernames, emails, names, profile URLs, PR/commit bodies) with
deterministic SHA-256, and writes masked JSON to a destination bucket.

Hashing is deterministic: same input → same hash everywhere, so
cross-references (author in PR matches author in commits) stay valid.

Parallelized with ThreadPoolExecutor (I/O-bound: 65K small JSON files).
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
DEFAULT_SOURCE_DOMAIN = "org_name.com"
DEFAULT_TARGET_DOMAIN = "example-health.com"

# Salt prefix for hashing — prevents rainbow table reversal.
# Not a secret, just domain separation.
_HASH_SALT = "pii-mask-github-v1:"


# -- Hashing helpers -------------------------------------------------------- #

def _hash(value: str, length: int = 12) -> str:
    """Deterministic hash: same input always produces the same output."""
    digest = hashlib.sha256((_HASH_SALT + value).encode()).hexdigest()
    return digest[:length]


def _hash_email(email: str) -> str:
    """Hash an email, preserving the @domain structure."""
    if not email or "@" not in email:
        return _hash(email) if email else email
    local, domain = email.rsplit("@", 1)
    return f"{_hash(local, 8)}@{DEFAULT_TARGET_DOMAIN}"


def _hash_login(login: str) -> str:
    return f"user-{_hash(login, 10)}" if login else login


def _hash_name(name: str) -> str:
    return f"User {_hash(name, 8)}" if name else name


def _hash_url(url: str) -> str:
    """Hash a GitHub profile URL, preserving structure."""
    if not url:
        return url
    # https://github.com/username → https://github.com/user-HASH
    m = re.match(r"(https?://github\.com/)([^/]+)(.*)", url)
    if m:
        return f"{m.group(1)}{_hash_login(m.group(2))}{m.group(3)}"
    return url


def _mask_body(text: str) -> str:
    """Replace @mentions and emails in freeform text."""
    if not text:
        return text
    # @username mentions
    text = re.sub(r"@([a-zA-Z0-9_-]+)",
                  lambda m: f"@{_hash_login(m.group(1))}", text)
    # email addresses
    text = re.sub(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
                  lambda m: _hash_email(m.group(0)), text)
    return text


# -- File-type maskers ------------------------------------------------------ #

def mask_pr(pr: dict) -> dict:
    """Mask PII in a pull request JSON object."""
    pr["author"] = _hash_login(pr.get("author", ""))
    if pr.get("author_id"):
        pr["author_id"] = 0

    pr["assignees"] = [_hash_login(a) for a in pr.get("assignees", [])]
    pr["requested_reviewers"] = [_hash_login(r)
                                 for r in pr.get("requested_reviewers", [])]

    pr["title"] = _mask_body(pr.get("title", ""))
    pr["body"] = _mask_body(pr.get("body", ""))
    pr["html_url"] = _hash_url(pr.get("html_url", ""))

    for review in pr.get("reviews", []):
        review["reviewer"] = _hash_login(review.get("reviewer", ""))
        review["body"] = _mask_body(review.get("body", ""))

    for rc in pr.get("review_comments", []):
        rc["author"] = _hash_login(rc.get("author", ""))
        rc["body"] = _mask_body(rc.get("body", ""))

    for comment in pr.get("comments", []):
        comment["author"] = _hash_login(comment.get("author", ""))
        comment["body"] = _mask_body(comment.get("body", ""))

    for commit in pr.get("commits", []):
        _mask_commit_fields(commit)

    return pr


def _mask_commit_fields(commit: dict) -> None:
    """Mask PII fields on a commit dict (used in PR commits and standalone)."""
    commit["author_name"] = _hash_name(commit.get("author_name", ""))
    commit["author_email"] = _hash_email(commit.get("author_email", ""))
    commit["author_login"] = _hash_login(commit.get("author_login", ""))
    if "committer_name" in commit:
        commit["committer_name"] = _hash_name(commit.get("committer_name", ""))
    if "committer_email" in commit:
        commit["committer_email"] = _hash_email(
            commit.get("committer_email", ""))
    if "committer_login" in commit:
        commit["committer_login"] = _hash_login(
            commit.get("committer_login", ""))
    commit["message"] = _mask_body(commit.get("message", ""))


def mask_contributors(contributors: list) -> list:
    """Mask PII in contributors.json array."""
    for c in contributors:
        c["login"] = _hash_login(c.get("login", ""))
        c["id"] = 0
        c["profile_url"] = _hash_url(c.get("profile_url", ""))
    return contributors


def mask_repo_metadata(meta: dict) -> dict:
    """Mask PII in repo_metadata.json (minimal — mostly non-PII)."""
    # full_name contains org/repo — mask org name
    if meta.get("full_name"):
        parts = meta["full_name"].split("/", 1)
        meta["full_name"] = f"{_hash_login(parts[0])}/{parts[1]}" \
            if len(parts) == 2 else _hash(meta["full_name"])
    meta["description"] = _mask_body(meta.get("description", ""))
    return meta


# -- S3 pipeline ----------------------------------------------------------- #

def _mask_one_file(src: S3Store, dst: S3Store, key: str) -> str:
    """Download, mask, upload one GitHub JSON file. Returns status message."""
    data = src.download_json(key)
    if data is None:
        return "skipped (not found)"

    filename = key.rsplit("/", 1)[-1]

    if "/prs/" in key:
        data = mask_pr(data)
    elif filename == "contributors.json":
        data = mask_contributors(data)
    elif filename == "repo_metadata.json":
        data = mask_repo_metadata(data)
    elif filename == "_stats.json":
        pass  # no PII in stats
    else:
        return "skipped (unknown type)"

    dst.upload_json(data, key)
    return "ok"


def mask_github_exports(
    src: S3Store,
    dst: S3Store,
    checkpoint: CheckpointManager,
    max_workers: int = DEFAULT_WORKERS,
):
    """Mask all GitHub JSON files."""
    keys = src.list_keys("github/")
    json_keys = [k for k in keys if k.endswith(".json")]
    log.info("Found %d JSON files under github/", len(json_keys))

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
                        if completed % 1000 == 0:
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
    log.info("GitHub masking complete — %d files", len(json_keys))


# -- CLI -------------------------------------------------------------------- #

def main():
    from lib.config import load_dotenv, env, env_int
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Mask PII in GitHub JSON exports (deterministic hashing)",
    )
    parser.add_argument("--src-bucket", default=env("PII_SRC_BUCKET"),
                        help="Source S3 bucket with raw exports")
    parser.add_argument("--dst-bucket", default=env("PII_DST_BUCKET"),
                        help="Destination S3 bucket for masked output")
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
        parser.error("--src-bucket is required (or set PII_SRC_BUCKET)")
    if not args.dst_bucket:
        parser.error("--dst-bucket is required (or set PII_DST_BUCKET)")

    import os
    log_file = os.path.join(args.log_dir, "pii_mask_github.log")
    setup_logging(
        level=args.log_level,
        json_output=not args.no_json_logs,
        log_file=log_file,
    )

    src = S3Store(bucket=args.src_bucket, prefix=args.s3_prefix)
    dst = S3Store(bucket=args.dst_bucket, prefix=args.s3_prefix)
    checkpoint = CheckpointManager(dst, "pii_mask/github")
    checkpoint.load()

    log.info("Masking GitHub exports: %s -> %s (workers: %d)",
             args.src_bucket, args.dst_bucket, args.max_workers)

    mask_github_exports(
        src=src, dst=dst,
        checkpoint=checkpoint,
        max_workers=args.max_workers,
    )


if __name__ == "__main__":
    main()
