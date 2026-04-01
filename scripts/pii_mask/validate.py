"""Validation — scan masked output for PII leakage and quality issues.

Uses Microsoft Presidio for PII detection (already installed) — handles
names, emails, phones, addresses, IPs, credit cards etc. with proper
word-boundary and context awareness.

Checks:
1. PII leakage via Presidio — detect any remaining PII in masked output
2. Roster leakage — search for known real emails/domains from the roster
3. Readability — flag freeform text that looks like hex/hash gibberish
4. Structural integrity — JSON/EML parse correctly, keys rewritten
5. LLM audit (optional) — send sample files to Claude for review

Usage:
    python -m scripts.pii_mask.validate \\
        --roster roster.json \\
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

from presidio_analyzer import AnalyzerEngine

from lib.s3 import S3Store
from scripts.pii_mask.roster import Roster

log = logging.getLogger(__name__)

# Hex gibberish detector: 20+ contiguous hex chars
_HEX_GIBBERISH_RE = re.compile(r"\b[0-9a-f]{20,}\b", re.IGNORECASE)

# Freeform text field paths per exporter
_TEXT_FIELDS = {
    "github": ["title", "body", "message"],
    "jira": ["summary", "description_text", "body_text"],
    "slack": ["text"],
    "confluence": ["title", "body"],
    "google": ["summary", "description", "snippet", "subject"],
}

# Presidio entity types to scan for
_PRESIDIO_ENTITIES = [
    "PERSON", "EMAIL_ADDRESS", "PHONE_NUMBER", "LOCATION",
    "CREDIT_CARD", "IP_ADDRESS", "US_SSN", "IBAN_CODE",
]

# Infrastructure prefixes to skip
_INFRA_PREFIXES = ("_checkpoints/", "_manifest/")


# -- Check 1: PII leakage via Presidio ------------------------------------ #

def build_presidio_allow_list(roster: Roster) -> list[str]:
    """Build allow list of fake identities so Presidio doesn't flag them."""
    allow = []
    for entry in roster.users:
        masked = entry.masked
        for val in masked.values():
            if isinstance(val, str) and len(val) >= 3:
                allow.append(val)
    # Add common redaction placeholders
    allow.extend(["[PERSON]", "[PHONE]", "[EMAIL]", "[LOCATION]",
                  "[CREDIT_CARD]", "[IP]", "[SSN]"])
    return list(set(allow))


def check_pii_presidio(analyzer: AnalyzerEngine, text: str, key: str,
                       allow_list: list[str],
                       threshold: float = 0.5) -> list[dict]:
    """Run Presidio on text, return any PII findings above threshold."""
    if not text or len(text) < 5:
        return []

    # Truncate very large files to keep analysis time reasonable
    if len(text) > 50000:
        text = text[:50000]

    try:
        results = analyzer.analyze(
            text=text,
            language="en",
            entities=_PRESIDIO_ENTITIES,
            score_threshold=threshold,
            allow_list=allow_list,
        )
    except Exception as e:
        log.debug("Presidio failed on %s: %s", key, e)
        return []

    findings = []
    for r in results:
        value = text[r.start:r.end]
        findings.append({
            "file": key,
            "type": r.entity_type,
            "value": value,
            "score": round(r.score, 2),
            "start": r.start,
            "end": r.end,
        })
    return findings


# -- Check 2: Roster leakage (exact match for emails/domains) ------------- #

def check_roster_leakage(content: str, key: str,
                         roster: Roster) -> list[dict]:
    """Search for known real emails and domains from the roster."""
    findings = []
    content_lower = content.lower()

    # Check real emails (exact, high signal)
    for entry in roster.users:
        real_email = entry.real.get("email", "").lower()
        if real_email and real_email in content_lower:
            findings.append({
                "file": key,
                "type": "roster_email",
                "value": real_email,
                "context": _extract_context(content_lower, real_email),
            })

    # Check real domains
    for real_domain in roster.domain_map:
        if real_domain.lower() in content_lower:
            findings.append({
                "file": key,
                "type": "roster_domain",
                "value": real_domain,
                "context": _extract_context(content_lower, real_domain),
            })

    return findings


def _extract_context(text: str, term: str, window: int = 40) -> str:
    idx = text.find(term)
    if idx == -1:
        return ""
    start = max(0, idx - window)
    end = min(len(text), idx + len(term) + window)
    return text[start:end].replace("\n", " ").strip()


# -- Check 3: Readability ------------------------------------------------- #

def check_readability(data, key: str) -> list[dict]:
    """Flag freeform text fields that look like hex gibberish."""
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
            msg = email_mod.message_from_bytes(
                raw_bytes, policy=email.policy.default)
            if not msg["from"] and not msg["to"] and not msg["subject"]:
                findings.append({"file": key, "issue": "eml_missing_headers"})
        except Exception as e:
            findings.append({"file": key, "issue": "invalid_eml",
                             "detail": str(e)[:200]})
    return findings


def check_key_leakage(key: str, roster: Roster) -> list[dict]:
    findings = []
    key_lower = key.lower()
    for real_domain in roster.domain_map:
        if real_domain.lower() in key_lower:
            findings.append({"file": key, "issue": "domain_in_key",
                             "real_value": real_domain})
    return findings


# -- Check 5: LLM audit --------------------------------------------------- #

def llm_audit(files: dict[str, str], roster: Roster,
              sample_size: int = 100) -> list[dict]:
    """Send sample masked files to Claude for PII review."""
    try:
        import anthropic
    except ImportError:
        log.error("anthropic package not installed — skip LLM audit")
        return []

    client = anthropic.Anthropic()
    rng = random.Random(42)

    all_keys = list(files.keys())
    sample_keys = rng.sample(all_keys, min(sample_size, len(all_keys)))

    fake_names = [e.masked.get("name", "") for e in roster.users
                  if e.masked.get("name")][:30]
    fake_emails = [e.masked.get("email", "") for e in roster.users
                   if e.masked.get("email")][:30]

    findings = []
    log.info("LLM audit: reviewing %d files with Claude Haiku...",
             len(sample_keys))

    for i, key in enumerate(sample_keys):
        content = files[key]
        if len(content) > 8000:
            content = content[:8000] + "\n... (truncated)"

        prompt = (
            "You are a PII auditor. The following is a document that has been "
            "through a PII masking pipeline. Check if any REAL personally "
            "identifiable information remains — real names, real email "
            "addresses, phone numbers, physical addresses, ID numbers, "
            "medical info, or anything that could identify a real person.\n\n"
            "The following fake names/emails are EXPECTED and should NOT be "
            "flagged:\n"
            f"Fake names: {', '.join(fake_names)}\n"
            f"Fake emails: {', '.join(fake_emails)}\n\n"
            "Placeholders like [PERSON], [PHONE], [EMAIL], [LOCATION] are "
            "expected redactions — do NOT flag them.\n\n"
            f"File: {key}\n"
            f"Content:\n{content}\n\n"
            "Respond with a JSON array of findings. Each finding should have:\n"
            '{"pii_type": "name|email|phone|address|id|other", '
            '"value": "the PII found", '
            '"reason": "why you think this is real PII"}\n'
            "If no PII found, respond with: []"
        )

        try:
            resp = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=1024,
                messages=[{"role": "user", "content": prompt}],
            )
            text = resp.content[0].text.strip()
            if text.startswith("["):
                llm_findings = json.loads(text)
                for f in llm_findings:
                    f["file"] = key
                    findings.append(f)
            elif text != "[]":
                match = re.search(r"\[.*\]", text, re.DOTALL)
                if match:
                    llm_findings = json.loads(match.group())
                    for f in llm_findings:
                        f["file"] = key
                        findings.append(f)
        except Exception as e:
            log.warning("LLM audit failed for %s: %s", key, e)

        if (i + 1) % 20 == 0:
            log.info("LLM audit progress: %d/%d files", i + 1, len(sample_keys))

    log.info("LLM audit complete: %d findings from %d files",
             len(findings), len(sample_keys))
    return findings


# -- Main orchestrator ----------------------------------------------------- #

def run_validation(src: S3Store, roster: Roster,
                   enable_llm: bool = False,
                   llm_sample: int = 100,
                   max_workers: int = 32,
                   presidio_threshold: float = 0.5) -> dict:
    """Run all validation checks and return a report dict."""
    log.info("Starting validation...")

    # List all masked files (exclude infrastructure)
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

    # Initialize Presidio
    log.info("Initializing Presidio analyzer...")
    analyzer = AnalyzerEngine()
    allow_list = build_presidio_allow_list(roster)
    log.info("Allow list: %d fake values", len(allow_list))

    # Check 1: PII leakage via Presidio
    log.info("Check 1: Presidio PII scan (%d files)...", len(text_files))
    pii_findings = []
    done = 0
    for key, text in text_files.items():
        pii_findings.extend(
            check_pii_presidio(analyzer, text, key, allow_list,
                               presidio_threshold))
        done += 1
        if done % 200 == 0:
            log.info("  Presidio progress: %d/%d files (%d findings so far)",
                     done, len(text_files), len(pii_findings))
    log.info("  Presidio: %d PII findings", len(pii_findings))

    # Summarize by entity type
    pii_by_type: dict[str, int] = {}
    for f in pii_findings:
        pii_by_type[f["type"]] = pii_by_type.get(f["type"], 0) + 1

    # Check 2: Roster leakage (exact email/domain match)
    log.info("Check 2: Roster email/domain leak scan...")
    roster_leaks = []
    for key, text in text_files.items():
        roster_leaks.extend(check_roster_leakage(text, key, roster))
    log.info("  Roster leaks: %d", len(roster_leaks))

    # Check 3: Readability
    log.info("Check 3: Readability check...")
    unreadable = []
    for key, data in json_files.items():
        unreadable.extend(check_readability(data, key))
    log.info("  Unreadable text: %d", len(unreadable))

    # Check 4: Structural integrity
    log.info("Check 4: Structural integrity...")
    structural_errors = []
    for key, data in raw_files.items():
        structural_errors.extend(check_structure(key, data))
    key_leaks = []
    for key in all_keys:
        key_leaks.extend(check_key_leakage(key, roster))
    log.info("  Structural errors: %d, key leaks: %d",
             len(structural_errors), len(key_leaks))

    # Check 5: LLM audit (optional)
    llm_findings = []
    if enable_llm:
        log.info("Check 5: LLM audit...")
        llm_findings = llm_audit(text_files, roster, llm_sample)

    # Build report
    total_issues = (len(roster_leaks) + len(unreadable) +
                    len(structural_errors) + len(key_leaks))
    passed = total_issues == 0

    report = {
        "passed": passed,
        "files_checked": len(all_keys),
        "files_by_exporter": _count_by_exporter(all_keys),
        "summary": {
            "presidio_pii_findings": len(pii_findings),
            "presidio_by_type": pii_by_type,
            "roster_leaks": len(roster_leaks),
            "unreadable_text": len(unreadable),
            "structural_errors": len(structural_errors),
            "key_leaks": len(key_leaks),
            "llm_findings": len(llm_findings),
        },
        "presidio_pii_findings": pii_findings[:200],
        "roster_leaks": roster_leaks[:100],
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
        description="Validate masked output for PII leakage (Presidio-powered)",
    )
    parser.add_argument("--roster", required=True)
    parser.add_argument("--bucket", default=env("S3_MASKED_BUCKET"))
    parser.add_argument("--s3-prefix", default="")
    parser.add_argument("--presidio-threshold", type=float, default=0.5,
                        help="Presidio confidence threshold (default: 0.5)")
    parser.add_argument("--llm-check", action="store_true")
    parser.add_argument("--llm-sample", type=int, default=100)
    parser.add_argument("--max-workers", type=int,
                        default=env_int("PII_MAX_WORKERS", 32))
    parser.add_argument("--report", default="smoke_test_report.json")
    parser.add_argument("--log-level", default=env("LOG_LEVEL", "INFO"))
    args = parser.parse_args()

    setup_logging(level=args.log_level, json_output=False)

    if not args.bucket:
        parser.error("--bucket is required (or set S3_MASKED_BUCKET)")

    roster = Roster.from_file(args.roster)
    src = S3Store(bucket=args.bucket, prefix=args.s3_prefix)

    report = run_validation(
        src=src, roster=roster,
        enable_llm=args.llm_check,
        llm_sample=args.llm_sample,
        max_workers=args.max_workers,
        presidio_threshold=args.presidio_threshold,
    )

    with open(args.report, "w") as f:
        json.dump(report, f, indent=2)

    s = report["summary"]
    log.info("=" * 60)
    log.info("VALIDATION %s", "PASSED" if report["passed"] else "FAILED")
    log.info("=" * 60)
    log.info("Files checked:          %d", report["files_checked"])
    log.info("Presidio PII findings:  %d", s["presidio_pii_findings"])
    if s["presidio_by_type"]:
        for t, c in sorted(s["presidio_by_type"].items(), key=lambda x: -x[1]):
            log.info("  %-22s %d", t, c)
    log.info("Roster email/domain:    %d", s["roster_leaks"])
    log.info("Unreadable text:        %d", s["unreadable_text"])
    log.info("Structural errors:      %d", s["structural_errors"])
    log.info("Key path leaks:         %d", s["key_leaks"])
    if args.llm_check:
        log.info("LLM findings:           %d", s["llm_findings"])
    log.info("Report: %s", args.report)


if __name__ == "__main__":
    main()
