"""Validate Office document masking on real S3 data.

Downloads a small sample of .docx/.xlsx/.pptx files from the production
bucket using known index structure (no S3 listing), masks them locally,
and writes originals + masked copies side-by-side for comparison.

Usage:
    uv run python scripts/validate_doc_masking.py

Output:
    /tmp/doc_masking_validation/
        original/
            google__<slug>__drive__<file>
            jira__<project>__attachments__<ticket>__<file>
        masked/
            google__<slug>__drive__<file>
            jira__<project>__attachments__<ticket>__<file>
"""

import json
import os
import sys

import boto3

# -- Config ---------------------------------------------------------------- #

BUCKET = os.environ.get("S3_BUCKET",
    "prod-mumbai-scaler-data-export-746161288457-ap-south-1-an")
PREFIX = os.environ.get("S3_PREFIX", "v31")
OUT_DIR = "/tmp/doc_masking_validation"

# How many files to sample per source
MAX_PER_SOURCE = 3

OFFICE_EXTS = (".docx", ".xlsx", ".pptx")


def main():
    s3 = boto3.client("s3")

    os.makedirs(f"{OUT_DIR}/original", exist_ok=True)
    os.makedirs(f"{OUT_DIR}/masked", exist_ok=True)

    files = []  # list of (s3_key, local_name, source_label)

    # -- 1. Google Drive: pick first user, read drive index ---------------- #
    print("Finding Google Drive office docs...")
    drive_files = find_google_drive_docs(s3)
    files.extend(drive_files)

    # -- 2. Jira: pick first project, read ticket index, find attachments -- #
    print("Finding Jira attachment office docs...")
    jira_files = find_jira_attachment_docs(s3)
    files.extend(jira_files)

    # -- 3. Slack: pick first channel with attachments --------------------- #
    print("Finding Slack attachment office docs...")
    slack_files = find_slack_attachment_docs(s3)
    files.extend(slack_files)

    if not files:
        print("No office documents found!")
        return

    # -- 4. Download originals --------------------------------------------- #
    print(f"\nDownloading {len(files)} files...")
    for s3_key, local_name, source in files:
        local_path = f"{OUT_DIR}/original/{local_name}"
        print(f"  [{source}] {s3_key} -> {local_name}")
        s3.download_file(BUCKET, s3_key, local_path)

    # -- 5. Mask each file ------------------------------------------------- #
    print("\nInitializing PII scanner...")

    # Find the PIIStore db
    store_path = os.environ.get("PII_STORE_PATH")
    if not store_path:
        # Try common locations
        for candidate in ["pii_store.db", "store.db",
                          os.path.expanduser("~/pii_store.db")]:
            if os.path.exists(candidate):
                store_path = candidate
                break
    if not store_path:
        print("ERROR: No PIIStore found. Set PII_STORE_PATH env var.")
        print("       Or run: python -m scripts.pii_mask --store pii_store.db --import-roster roster.json")
        sys.exit(1)

    from scripts.pii_mask.pii_store import PIIStore
    from scripts.pii_mask.scanner import TextScanner
    from scripts.pii_mask.documents import mask_docx, mask_xlsx, mask_pptx

    store = PIIStore(store_path)
    scanner = TextScanner(store, threshold=0.5)
    print(f"  PIIStore: {len(store._cache)} entries, store={store_path}")

    mask_fns = {
        ".docx": mask_docx,
        ".xlsx": mask_xlsx,
        ".pptx": mask_pptx,
    }

    results = []
    for s3_key, local_name, source in files:
        orig_path = f"{OUT_DIR}/original/{local_name}"
        masked_path = f"{OUT_DIR}/masked/{local_name}"
        ext = os.path.splitext(local_name)[1].lower()
        mask_fn = mask_fns.get(ext)
        if not mask_fn:
            continue

        print(f"  Masking {local_name}...", end=" ", flush=True)
        try:
            with open(orig_path, "rb") as f:
                raw = f.read()
            masked = mask_fn(raw, scanner)
            with open(masked_path, "wb") as f:
                f.write(masked)
            print(f"OK ({len(raw)} -> {len(masked)} bytes)")
            results.append((local_name, ext, "ok", len(raw), len(masked)))
        except Exception as e:
            print(f"FAILED: {e}")
            results.append((local_name, ext, f"error: {e}", 0, 0))

    # -- 6. Summary -------------------------------------------------------- #
    print(f"\n{'=' * 60}")
    print(f"Results: {OUT_DIR}")
    print(f"{'=' * 60}")
    ok = sum(1 for r in results if r[2] == "ok")
    err = len(results) - ok
    print(f"  Total: {len(results)}  OK: {ok}  Errors: {err}")
    print()
    for name, ext, status, orig_sz, masked_sz in results:
        marker = "OK" if status == "ok" else "FAIL"
        print(f"  [{marker}] {name} ({ext}) "
              f"{orig_sz:,} -> {masked_sz:,} bytes")
    print()
    print(f"Compare originals vs masked:")
    print(f"  open {OUT_DIR}/original/")
    print(f"  open {OUT_DIR}/masked/")


# -- Source-specific finders ----------------------------------------------- #

def find_google_drive_docs(s3) -> list:
    """Find office docs from Google Drive using drive/_index.json."""
    # List top-level user slugs under google/
    resp = s3.list_objects_v2(
        Bucket=BUCKET, Prefix=f"{PREFIX}/google/", Delimiter="/",
        MaxKeys=10)
    slugs = []
    for cp in resp.get("CommonPrefixes", []):
        slug = cp["Prefix"].rstrip("/").rsplit("/", 1)[-1]
        slugs.append(slug)

    files = []
    for slug in slugs:
        if len(files) >= MAX_PER_SOURCE:
            break
        idx_key = f"{PREFIX}/google/{slug}/drive/_index.json"
        idx = _download_json(s3, idx_key)
        if not idx or not isinstance(idx, list):
            continue
        for entry in idx:
            if len(files) >= MAX_PER_SOURCE:
                break
            if not isinstance(entry, dict) or not entry.get("downloaded"):
                continue
            name = entry.get("name", "")
            file_id = entry.get("id", "")
            if not any(name.lower().endswith(e) for e in OFFICE_EXTS):
                continue
            from lib.s3 import sanitize_filename
            s3_name = f"{file_id}_{sanitize_filename(name)}"
            s3_key = f"{PREFIX}/google/{slug}/drive/{s3_name}"
            local_name = f"google__{slug}__drive__{s3_name}"
            files.append((s3_key, local_name, "google-drive"))
    return files


def find_jira_attachment_docs(s3) -> list:
    """Find office docs from Jira attachments using ticket metadata."""
    # List projects
    resp = s3.list_objects_v2(
        Bucket=BUCKET, Prefix=f"{PREFIX}/jira/", Delimiter="/",
        MaxKeys=5)
    projects = []
    for cp in resp.get("CommonPrefixes", []):
        proj = cp["Prefix"].rstrip("/").rsplit("/", 1)[-1]
        projects.append(proj)

    files = []
    for proj in projects:
        if len(files) >= MAX_PER_SOURCE:
            break
        idx_key = f"{PREFIX}/jira/{proj}/tickets/_index.json"
        idx = _download_json(s3, idx_key)
        if not idx or not isinstance(idx, dict):
            continue
        ticket_keys = idx.get("keys", [])
        # Check first N tickets for office attachments
        for tk in ticket_keys[:50]:
            if len(files) >= MAX_PER_SOURCE:
                break
            ticket = _download_json(s3, f"{PREFIX}/jira/{proj}/tickets/{tk}.json")
            if not ticket:
                continue
            for att in ticket.get("attachments", []):
                if len(files) >= MAX_PER_SOURCE:
                    break
                fname = att.get("filename", "")
                if not any(fname.lower().endswith(e) for e in OFFICE_EXTS):
                    continue
                from lib.s3 import sanitize_filename
                safe = sanitize_filename(fname)
                s3_key = f"{PREFIX}/jira/{proj}/attachments/{tk}/{safe}"
                local_name = f"jira__{proj}__attachments__{tk}__{safe}"
                files.append((s3_key, local_name, "jira-att"))
    return files


def find_slack_attachment_docs(s3) -> list:
    """Find office docs from Slack using targeted S3 prefix listing."""
    # List channels
    resp = s3.list_objects_v2(
        Bucket=BUCKET, Prefix=f"{PREFIX}/slack/", Delimiter="/",
        MaxKeys=10)
    channels = []
    for cp in resp.get("CommonPrefixes", []):
        ch = cp["Prefix"].rstrip("/").rsplit("/", 1)[-1]
        channels.append(ch)

    files = []
    for ch in channels:
        if len(files) >= MAX_PER_SOURCE:
            break
        # List just the first page of attachments for this channel
        resp = s3.list_objects_v2(
            Bucket=BUCKET,
            Prefix=f"{PREFIX}/slack/{ch}/attachments/",
            MaxKeys=200)
        for obj in resp.get("Contents", []):
            if len(files) >= MAX_PER_SOURCE:
                break
            key = obj["Key"]
            if any(key.lower().endswith(e) for e in OFFICE_EXTS):
                fname = key.rsplit("/", 1)[-1]
                local_name = f"slack__{ch}__attachments__{fname}"
                files.append((key, local_name, "slack-att"))
    return files


def _download_json(s3, key):
    try:
        resp = s3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(resp["Body"].read())
    except Exception:
        return None


if __name__ == "__main__":
    main()
