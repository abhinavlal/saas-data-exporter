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

def build_leak_scanner(store: PIIStore) -> dict:
    """Build search structures for PII leak detection.

    Returns separate matchers for different signal levels:
    - emails: AC automaton (exact substring, very high signal)
    - domains: simple list (exact substring)
    - names: AC automaton with word-boundary post-check (full names only, >= 8 chars)

    Skips opaque IDs (SLACK_USER_ID, JIRA_ACCOUNT_ID, GITHUB_LOGIN,
    SLACK_USERNAME) — these are system identifiers, not human-readable
    PII that someone would recognize.
    """
    con = store._get_connection()

    # -- Email AC (highest signal, exact match) --
    email_ac = ahocorasick.Automaton()
    email_count = 0
    rows = con.execute(
        "SELECT real_value FROM roster_entries "
        "WHERE entity_type = 'EMAIL_ADDRESS' AND source = 'roster_import'"
    ).fetchall()
    for (val,) in rows:
        key = val.lower()
        if len(key) >= 5:
            email_ac.add_word(key, val)
            email_count += 1
    if email_count > 0:
        email_ac.make_automaton()

    # -- Name AC (full names only, >= 8 chars, word-boundary check) --
    name_ac = ahocorasick.Automaton()
    name_count = 0
    rows = con.execute(
        "SELECT real_value FROM roster_entries "
        "WHERE entity_type = 'PERSON' AND source = 'roster_import' "
        "  AND LENGTH(real_value) >= 8"
    ).fetchall()
    for (val,) in rows:
        key = val.lower()
        # Only full names (contain a space = first + last)
        if " " not in key:
            continue
        name_ac.add_word(key, val)
        name_count += 1
    if name_count > 0:
        name_ac.make_automaton()

    # -- High-value types AC (PAN, Aadhaar, UPI, credit card, etc.) --
    sensitive_ac = ahocorasick.Automaton()
    sensitive_count = 0
    _SENSITIVE_TYPES = ("IN_PAN", "IN_AADHAAR", "IN_UPI_ID",
                        "CREDIT_CARD", "US_SSN", "IBAN_CODE", "IN_GST")
    rows = con.execute(
        "SELECT entity_type, real_value FROM roster_entries "
        "WHERE entity_type IN ({})".format(
            ",".join(f"'{t}'" for t in _SENSITIVE_TYPES))
    ).fetchall()
    for entity_type, val in rows:
        key = val.lower()
        if len(key) >= 5:
            sensitive_ac.add_word(key, (entity_type, val))
            sensitive_count += 1
    if sensitive_count > 0:
        sensitive_ac.make_automaton()

    # -- Domains --
    domains = [d.lower() for d in store.domain_map]

    log.info("Leak scanner: %d emails, %d full names, %d sensitive IDs, "
             "%d domains",
             email_count, name_count, sensitive_count, len(domains))

    return {
        "email_ac": email_ac if email_count > 0 else None,
        "name_ac": name_ac if name_count > 0 else None,
        "sensitive_ac": sensitive_ac if sensitive_count > 0 else None,
        "domains": domains,
        "email_count": email_count,
        "name_count": name_count,
    }


def check_leakage(scanner: dict, content: str,
                  key: str) -> list[dict]:
    """Scan content for real PII leaks. Tuned for zero false positives."""
    if not content:
        return []

    findings = []
    content_lower = content.lower()
    seen = set()

    # 1. Email leaks (highest signal — exact substring)
    if scanner["email_ac"]:
        for end_idx, real_email in scanner["email_ac"].iter(content_lower):
            if real_email in seen:
                continue
            seen.add(real_email)
            findings.append({
                "file": key, "type": "EMAIL_ADDRESS",
                "real_value": real_email,
                "context": _context(content, end_idx, real_email),
            })

    # 2. Full name leaks (word-boundary check to avoid substring matches)
    if scanner["name_ac"]:
        for end_idx, real_name in scanner["name_ac"].iter(content_lower):
            if real_name in seen:
                continue
            start = end_idx - len(real_name) + 1
            end = end_idx + 1
            # Word boundary: char before start and after end must not be
            # alphanumeric (avoids "robert" matching inside "Robertson")
            if start > 0 and content_lower[start - 1].isalnum():
                continue
            if end < len(content_lower) and content_lower[end].isalnum():
                continue
            seen.add(real_name)
            findings.append({
                "file": key, "type": "PERSON",
                "real_value": real_name,
                "context": _context(content, end_idx, real_name),
            })

    # 3. Sensitive ID leaks (PAN, Aadhaar, etc.)
    if scanner["sensitive_ac"]:
        for end_idx, (etype, real_val) in \
                scanner["sensitive_ac"].iter(content_lower):
            if real_val in seen:
                continue
            seen.add(real_val)
            findings.append({
                "file": key, "type": etype,
                "real_value": real_val,
                "context": _context(content, end_idx, real_val),
            })

    # 4. Domain leaks
    for domain in scanner["domains"]:
        if domain in content_lower and domain not in seen:
            seen.add(domain)
            idx = content_lower.find(domain)
            findings.append({
                "file": key, "type": "DOMAIN",
                "real_value": domain,
                "context": content[max(0, idx - 30):idx + len(domain) + 30],
            })

    return findings


def _context(content: str, end_idx: int, value: str) -> str:
    start = max(0, end_idx - len(value) - 29)
    end = min(len(content), end_idx + 31)
    return content[start:end].replace("\n", " ").strip()


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

    # Build targeted leak scanner
    leak_scanner = build_leak_scanner(store)

    # Check 1: PII leakage (emails, full names, sensitive IDs, domains)
    log.info("Check 1: PII leakage scan...")
    leaks = []
    for key, text in text_files.items():
        leaks.extend(check_leakage(leak_scanner, text, key))
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
