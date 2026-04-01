"""Validation — scan masked output for PII leakage.

Searches for KNOWN real PII values from the PIIStore in the masked
output.  This has zero false positives — if a real email or name from
the store appears in the output, it's a genuine leak.

Checks:
1. Store leakage — search for every real_value from PIIStore
2. Domain leakage — search for real domains from domain_map
3. Readability — flag hex gibberish in freeform text fields
4. Structural integrity — JSON/EML parse correctly, keys rewritten
5. LLM spot-check (optional) — Claude reviews samples for unknown PII

Usage:
    python -m scripts.pii_mask.validate \\
        --store pii_store.db \\
        --bucket my-bucket --s3-prefix _smoke_test/masked-v1/ \\
        --report smoke_test_report.json
"""

import argparse
import email as email_mod
import email.policy
import json
import logging
import random
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

import ahocorasick

from lib.s3 import S3Store
from scripts.pii_mask.pii_store import PIIStore

log = logging.getLogger(__name__)

_HEX_GIBBERISH_RE = re.compile(r"\b[0-9a-f]{20,}\b", re.IGNORECASE)

_TEXT_FIELDS = {
    "github": ["title", "body", "message"],
    "jira": ["summary", "description_text", "body_text"],
    "slack": ["text"],
    "confluence": ["title", "body"],
    "google": ["summary", "description", "snippet", "subject"],
}

_INFRA_PREFIXES = ("_checkpoints/", "_manifest/")


# -- Check 1+2: Store + domain leakage via Aho-Corasick ------------------- #

def build_leak_automaton(store: PIIStore) -> ahocorasick.Automaton:
    """Build an AC automaton from all real PII values in the store.

    O(n) scan per file — fast even with thousands of search terms.
    Only includes terms >= 5 chars to avoid false positives from
    short strings like 'dev', 'user', 'test'.
    """
    A = ahocorasick.Automaton()
    con = store._get_connection()

    # Only search for entries from the roster import (known org members).
    # Entries auto-added by Presidio during masking (source != 'roster_import')
    # include common words and external URLs — not useful for leak detection.
    # Also include high-value types regardless of source.
    _HIGH_VALUE_TYPES = ("EMAIL_ADDRESS", "IN_PAN", "IN_AADHAAR",
                         "IN_UPI_ID", "CREDIT_CARD", "US_SSN", "IBAN_CODE")

    rows = con.execute(
        "SELECT entity_type, real_value FROM roster_entries "
        "WHERE source = 'roster_import' "
        "   OR entity_type IN (?, ?, ?, ?, ?, ?, ?)",
        _HIGH_VALUE_TYPES,
    ).fetchall()
    count = 0
    for entity_type, real_value in rows:
        key = real_value.lower()
        if len(key) < 5:
            continue
        A.add_word(key, (entity_type, real_value))
        count += 1

    # Real domains
    for real_domain in store.domain_map:
        key = real_domain.lower()
        A.add_word(key, ("DOMAIN", real_domain))
        count += 1

    if count > 0:
        A.make_automaton()
    log.info("Leak scanner: %d search terms from PIIStore", count)
    return A


def check_leakage(automaton: ahocorasick.Automaton,
                  content: str, key: str) -> list[dict]:
    """Scan content for any real PII values using AC automaton."""
    if not content:
        return []

    findings = []
    content_lower = content.lower()
    seen = set()  # dedup within one file

    for end_idx, (entity_type, real_value) in automaton.iter(content_lower):
        if real_value in seen:
            continue
        seen.add(real_value)

        start_idx = end_idx - len(real_value) + 1
        context_start = max(0, start_idx - 30)
        context_end = min(len(content), end_idx + 31)
        context = content[context_start:context_end].replace("\n", " ")

        findings.append({
            "file": key,
            "type": entity_type,
            "real_value": real_value,
            "context": context,
        })

    return findings


# -- Check 3: Readability ------------------------------------------------- #

def check_readability(data, key: str) -> list[dict]:
    findings = []
    exporter = key.split("/")[0] if "/" in key else ""
    text_fields = _TEXT_FIELDS.get(exporter, [])
    _walk_for_text(data, key, text_fields, findings)
    return findings


def _walk_for_text(obj, key, text_fields, findings, path=""):
    if isinstance(obj, dict):
        for k, v in obj.items():
            field_path = f"{path}.{k}" if path else k
            if k in text_fields and isinstance(v, str) and v:
                if _HEX_GIBBERISH_RE.fullmatch(v.strip()):
                    findings.append({
                        "file": key, "field": field_path,
                        "sample": v[:60], "issue": "hex_gibberish",
                    })
            _walk_for_text(v, key, text_fields, findings, field_path)
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            _walk_for_text(item, key, text_fields, findings, f"{path}[{i}]")


# -- Check 4: Structural integrity ---------------------------------------- #

def check_structure(key: str, raw_bytes: bytes) -> list[dict]:
    findings = []
    if key.endswith(".json"):
        try:
            json.loads(raw_bytes)
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            findings.append({"file": key, "issue": "invalid_json",
                             "detail": str(e)[:200]})
    elif key.endswith(".eml"):
        try:
            email_mod.message_from_bytes(
                raw_bytes, policy=email.policy.default)
        except Exception as e:
            findings.append({"file": key, "issue": "invalid_eml",
                             "detail": str(e)[:200]})
    return findings


def check_key_leakage(key: str, store: PIIStore) -> list[dict]:
    findings = []
    key_lower = key.lower()
    for real_domain in store.domain_map:
        if real_domain.lower() in key_lower:
            findings.append({"file": key, "issue": "domain_in_key",
                             "real_value": real_domain})
    return findings


# -- Check 5: LLM spot-check ---------------------------------------------- #

def llm_audit(files: dict[str, str], store: PIIStore,
              sample_size: int = 100) -> list[dict]:
    try:
        import anthropic
    except ImportError:
        log.error("anthropic package not installed — skip LLM audit")
        return []

    client = anthropic.Anthropic()
    rng = random.Random(42)

    all_keys = list(files.keys())
    sample_keys = rng.sample(all_keys, min(sample_size, len(all_keys)))

    con = store._get_connection()
    fake_names = [r[0] for r in con.execute(
        "SELECT masked_value FROM roster_entries "
        "WHERE entity_type = 'PERSON' LIMIT 50").fetchall()]
    fake_emails = [r[0] for r in con.execute(
        "SELECT masked_value FROM roster_entries "
        "WHERE entity_type = 'EMAIL_ADDRESS' LIMIT 50").fetchall()]

    findings = []
    log.info("LLM audit: reviewing %d files...", len(sample_keys))

    for i, key in enumerate(sample_keys):
        content = files[key][:8000]

        prompt = (
            "You are a PII auditor. This document went through a PII "
            "masking pipeline. All real names and emails should have been "
            "replaced with fake ones.\n\n"
            "EXPECTED fake names (do NOT flag these):\n"
            f"{', '.join(fake_names[:30])}\n\n"
            "EXPECTED fake emails (do NOT flag these):\n"
            f"{', '.join(fake_emails[:30])}\n\n"
            "Placeholders like [PHONE-abc123], [LOCATION-xyz], [REDACTED] "
            "are expected — do NOT flag them.\n\n"
            f"File: {key}\nContent:\n{content}\n\n"
            "Look for REAL PII that was NOT masked — real person names, "
            "real email addresses, phone numbers, physical addresses, "
            "or anything identifying a real person.\n\n"
            'Respond with JSON: [{"pii_type": "...", "value": "...", '
            '"reason": "..."}] or [] if clean.'
        )

        try:
            resp = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=1024,
                messages=[{"role": "user", "content": prompt}],
            )
            text = resp.content[0].text.strip()
            match = re.search(r"\[.*\]", text, re.DOTALL)
            if match:
                for f in json.loads(match.group()):
                    f["file"] = key
                    findings.append(f)
        except Exception as e:
            log.warning("LLM failed for %s: %s", key, e)

        if (i + 1) % 20 == 0:
            log.info("  LLM: %d/%d files", i + 1, len(sample_keys))

    log.info("LLM audit: %d findings from %d files",
             len(findings), len(sample_keys))
    return findings


# -- Main orchestrator ----------------------------------------------------- #

def run_validation(src: S3Store, store: PIIStore,
                   enable_llm: bool = False,
                   llm_sample: int = 100,
                   max_workers: int = 32) -> dict:
    log.info("Starting validation...")

    all_keys = [k for k in src.list_keys("")
                if not any(k.startswith(p) for p in _INFRA_PREFIXES)]
    log.info("Found %d files to validate", len(all_keys))

    # Download all files
    log.info("Downloading files...")
    raw_files: dict[str, bytes] = {}
    text_files: dict[str, str] = {}
    json_files: dict[str, dict | list] = {}

    def _download(key):
        return key, src.download_bytes(key)

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_download, k): k for k in all_keys}
        for future in as_completed(futures):
            key = futures[future]
            try:
                _, data = future.result()
                if data:
                    raw_files[key] = data
                    try:
                        text_files[key] = data.decode("utf-8", errors="replace")
                    except Exception:
                        pass
                    if key.endswith(".json"):
                        try:
                            json_files[key] = json.loads(data)
                        except Exception:
                            pass
            except Exception:
                log.error("Failed to download %s", key, exc_info=True)

    log.info("Downloaded %d files (%d JSON, %d text)",
             len(raw_files), len(json_files), len(text_files))

    # Build AC automaton from all real PII values
    automaton = build_leak_automaton(store)

    # Check 1+2: PII + domain leakage (AC scan — fast, zero false positives)
    log.info("Check 1: PII leakage scan (AC automaton)...")
    leaks = []
    for key, text in text_files.items():
        leaks.extend(check_leakage(automaton, text, key))
    log.info("  Leaks found: %d", len(leaks))

    leak_by_type: dict[str, int] = {}
    for l in leaks:
        leak_by_type[l["type"]] = leak_by_type.get(l["type"], 0) + 1

    # Check 3: Readability
    log.info("Check 2: Readability check...")
    unreadable = []
    for key, data in json_files.items():
        unreadable.extend(check_readability(data, key))
    log.info("  Unreadable text: %d", len(unreadable))

    # Check 4: Structural integrity
    log.info("Check 3: Structural integrity...")
    structural_errors = []
    for key, data in raw_files.items():
        structural_errors.extend(check_structure(key, data))
    key_leaks = []
    for key in all_keys:
        key_leaks.extend(check_key_leakage(key, store))
    log.info("  Structural errors: %d, key leaks: %d",
             len(structural_errors), len(key_leaks))

    # Check 5: LLM audit
    llm_findings = []
    if enable_llm:
        log.info("Check 4: LLM spot-check...")
        llm_findings = llm_audit(text_files, store, llm_sample)

    total_issues = (len(leaks) + len(unreadable) +
                    len(structural_errors) + len(key_leaks))

    report = {
        "passed": total_issues == 0,
        "files_checked": len(all_keys),
        "files_by_exporter": _count_by_exporter(all_keys),
        "summary": {
            "pii_leaks": len(leaks),
            "pii_leaks_by_type": leak_by_type,
            "unreadable_text": len(unreadable),
            "structural_errors": len(structural_errors),
            "key_leaks": len(key_leaks),
            "llm_findings": len(llm_findings),
        },
        "pii_leaks": leaks[:200],
        "unreadable_text": unreadable[:50],
        "structural_errors": structural_errors[:50],
        "key_leaks": key_leaks[:50],
        "llm_findings": llm_findings[:100],
    }
    return report


def _count_by_exporter(keys: list[str]) -> dict[str, int]:
    counts = {}
    for key in keys:
        exporter = key.split("/")[0] if "/" in key else "other"
        counts[exporter] = counts.get(exporter, 0) + 1
    return counts


# -- CLI ------------------------------------------------------------------- #

def main():
    from lib.config import load_dotenv, env, env_int
    from lib.logging import setup_logging
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Validate masked output — zero false-positive PII leak detection",
    )
    parser.add_argument("--store", required=True,
                        help="Path to PIIStore SQLite database")
    parser.add_argument("--bucket", default=env("S3_MASKED_BUCKET"))
    parser.add_argument("--s3-prefix", default="")
    parser.add_argument("--llm-check", action="store_true",
                        help="Enable LLM spot-check for unknown PII")
    parser.add_argument("--llm-sample", type=int, default=100)
    parser.add_argument("--max-workers", type=int,
                        default=env_int("PII_MAX_WORKERS", 32))
    parser.add_argument("--report", default="smoke_test_report.json")
    parser.add_argument("--log-level", default=env("LOG_LEVEL", "INFO"))
    args = parser.parse_args()

    setup_logging(level=args.log_level, json_output=False)

    if not args.bucket:
        parser.error("--bucket is required (or set S3_MASKED_BUCKET)")

    store = PIIStore(args.store)
    src = S3Store(bucket=args.bucket, prefix=args.s3_prefix)

    report = run_validation(
        src=src, store=store,
        enable_llm=args.llm_check,
        llm_sample=args.llm_sample,
        max_workers=args.max_workers,
    )

    with open(args.report, "w") as f:
        json.dump(report, f, indent=2)

    s = report["summary"]
    log.info("=" * 60)
    log.info("VALIDATION %s", "PASSED" if report["passed"] else "FAILED")
    log.info("=" * 60)
    log.info("Files checked:       %d", report["files_checked"])
    log.info("PII leaks:           %d", s["pii_leaks"])
    if s["pii_leaks_by_type"]:
        for t, c in sorted(s["pii_leaks_by_type"].items(), key=lambda x: -x[1]):
            log.info("  %-20s %d", t, c)
    log.info("Unreadable text:     %d", s["unreadable_text"])
    log.info("Structural errors:   %d", s["structural_errors"])
    log.info("Key path leaks:      %d", s["key_leaks"])
    if args.llm_check:
        log.info("LLM findings:        %d", s["llm_findings"])
    log.info("Report: %s", args.report)


if __name__ == "__main__":
    main()
