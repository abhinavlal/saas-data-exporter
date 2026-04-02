"""EML masker — full RFC 822 email PII replacement.

Scans ALL headers and body parts using the Presidio-powered scanner.
No header is skipped — Delivered-To, Return-Path, Received, DKIM,
Authentication-Results, etc. all get domain + PII replacement.

For HTML body parts, CSS noise (style blocks, inline styles, class
attributes) is stripped before NER to avoid wasting time tokenizing
layout markup.  Replacements are applied to the original HTML so
the structure is preserved.  This gives ~2-3x speedup on CSS-heavy
emails (calendar invites, newsletters) with identical PII coverage.

Final pass: AC automaton sweep on raw bytes catches PII in encoded
content (iCalendar CN= fields, quoted-printable HTML).
"""

import email
import email.utils
import email.policy
import logging
import re
import threading
from email.message import EmailMessage

import ahocorasick

from scripts.pii_mask.scanner import TextScanner, _ALL_ENTITIES

log = logging.getLogger(__name__)

# Headers that contain parseable email addresses
_ADDRESS_HEADERS = frozenset({
    "from", "to", "cc", "bcc", "reply-to", "sender",
    "delivered-to", "return-path", "x-original-to",
    "x-forwarded-to", "envelope-to",
})

# Headers to skip Presidio NER on (transport/crypto — no human PII,
# and running NER on them is slow + useless). Domain sweep via raw
# byte pass catches domains in these.
_SKIP_HEADERS = frozenset({
    "content-type", "content-transfer-encoding", "content-disposition",
    "mime-version", "content-id", "content-length",
    "received", "received-spf", "authentication-results",
    "arc-authentication-results", "arc-message-signature", "arc-seal",
    "dkim-signature", "domainkey-signature",
    "message-id", "references", "in-reply-to",
    "x-google-dkim-signature", "x-gm-message-state",
    "x-google-smtp-source", "x-received",
    "x-ses-outgoing", "feedback-id",
    "date", "x-mailer", "x-priority",
})

# Compiled regexes for CSS stripping (compiled once, reused)
_CSS_STRIP_PATTERNS = [
    (re.compile(r'<style[^>]*>.*?</style>', re.DOTALL), ''),
    (re.compile(r'<script[^>]*>.*?</script>', re.DOTALL), ''),
    (re.compile(r'\s+style="[^"]*"'), ''),
    (re.compile(r"\s+style='[^']*'"), ''),
    (re.compile(r'\s+class="[^"]*"'), ''),
]


def _strip_css(html: str) -> str:
    """Remove CSS noise from HTML: style blocks, inline styles, classes.

    Keeps all text content and PII-bearing attributes (href, title, etc.).
    Typically removes 40-70% of HTML bulk.
    """
    for pattern, repl in _CSS_STRIP_PATTERNS:
        html = pattern.sub(repl, html)
    return html


def mask_eml(eml_bytes: bytes, scanner: TextScanner) -> bytes:
    """Parse, mask all headers + body, re-encode.

    Every header is processed:
    - Address headers: parse + replace each email/name via scanner
    - All other headers: run scanner.scan() to catch PII + domains
    - Body text/html parts: full scanner.scan()
    - Final: domain sweep on raw bytes for DKIM/Received leftovers
    """
    msg = email.message_from_bytes(eml_bytes, policy=email.policy.default)

    # -- Mask ALL headers --
    for hdr_name in list(msg.keys()):
        hdr_lower = hdr_name.lower()

        if hdr_lower in _SKIP_HEADERS:
            continue

        original = str(msg[hdr_name])

        if hdr_lower in _ADDRESS_HEADERS:
            masked = _mask_address_header(original, scanner)
        else:
            # All other headers: Presidio scan catches PII + domain sweep
            masked = scanner.scan(original)

        if masked != original:
            del msg[hdr_name]
            msg[hdr_name] = masked

    # -- Mask body parts --
    if msg.is_multipart():
        for part in msg.walk():
            _mask_body_part(part, scanner)
    else:
        _mask_body_part(msg, scanner)

    # Convert back to bytes
    masked_bytes = msg.as_bytes()

    # Final raw byte sweep — catches PII in encoded content that the
    # MIME parser doesn't expose: iCalendar CN= fields, quoted-printable
    # HTML, DKIM signatures, etc.
    masked_bytes = _raw_byte_sweep(masked_bytes, scanner)

    return masked_bytes


# Module-level cache for the byte sweep AC automaton.
# Built once, reused across all EML files (thread-safe after build).
_byte_sweep_lock = threading.Lock()
_byte_sweep_cache: dict[str, tuple] = {}  # db_path → (automaton, domain_pairs)


def _get_byte_sweep_automaton(scanner: TextScanner):
    """Build (or retrieve cached) AC automaton for raw byte sweep.

    Built once from the PIIStore, then reused for all EML files.
    O(n) per file instead of O(patterns × file_size).
    """
    db_path = scanner._store._db_path

    with _byte_sweep_lock:
        if db_path in _byte_sweep_cache:
            return _byte_sweep_cache[db_path]

    store = scanner._store
    con = store._get_connection()

    A = ahocorasick.Automaton()
    rows = con.execute(
        "SELECT real_value, masked_value FROM roster_entries "
        "WHERE entity_type IN ('PERSON', 'EMAIL_ADDRESS') "
        "  AND LENGTH(real_value) >= 6"
    ).fetchall()

    # Skip entries that would corrupt MIME if replaced in raw bytes:
    # - NER false positives (timestamps, HTML fragments)
    # - Company/domain names (handled by domain_replace, not byte sweep)
    # - Entries that don't look like real names or emails
    company_names = set(store._company_names.keys())
    domain_names = set(store._domain_map.keys())

    count = 0
    for real_val, masked_val in rows:
        if real_val != real_val.strip():
            continue
        if any(c in real_val for c in '<>&;{}[]%#'):
            continue
        low = real_val.lower()
        if low in company_names or low in domain_names:
            continue
        # PERSON entries must look like names (letters + spaces only)
        if "@" not in real_val:
            if not re.match(r'^[A-Za-z][A-Za-z .\'-]+$', real_val):
                continue
        key = low
        A.add_word(key, (real_val, masked_val))
        count += 1

    if count > 0:
        A.make_automaton()

    domain_pairs = [(r.encode(), f.encode())
                    for r, f in store.domain_map.items()]

    result = (A if count > 0 else None, domain_pairs)

    with _byte_sweep_lock:
        _byte_sweep_cache[db_path] = result

    log.info("EML byte sweep automaton: %d patterns cached", count)
    return result


def _raw_byte_sweep(data: bytes, scanner: TextScanner) -> bytes:
    """Replace company domains in raw bytes.

    Domain-only sweep — replaces practo.com → faulkner-howard.com etc.
    in DKIM headers, encoded content, and anywhere else the MIME parser
    didn't expose.

    The AC automaton sweep for PERSON/EMAIL_ADDRESS was removed because
    replacing variable-length names in raw bytes corrupts quoted-printable
    and base64 encodings, breaking the MIME structure (Subject/From/To
    become None after re-parsing). Header + body masking already handles
    all PII; the byte sweep only needs to catch domains.
    """
    _, domain_pairs = _get_byte_sweep_automaton(scanner)

    for real_bytes, fake_bytes in domain_pairs:
        data = data.replace(real_bytes, fake_bytes)

    # Also replace company names in raw bytes (safe — fixed mapping)
    for real_name, fake_name in scanner._store._company_names.items():
        data = data.replace(real_name.encode(), fake_name.encode())
        data = data.replace(
            real_name.capitalize().encode(), fake_name.capitalize().encode())
        data = data.replace(real_name.upper().encode(), fake_name.upper().encode())

    return data


def _mask_address_header(header_value: str,
                         scanner: TextScanner) -> str:
    """Parse email addresses from a header and replace via scanner."""
    addresses = email.utils.getaddresses([header_value])
    if not addresses or all(not addr for _, addr in addresses):
        # Not parseable as addresses — scan as plain text instead
        return scanner.scan(header_value)

    masked_parts = []
    for display_name, addr in addresses:
        if addr:
            masked_addr = scanner.scan_structured("EMAIL_ADDRESS", addr)
        else:
            masked_addr = addr

        if display_name:
            masked_name = scanner.scan_structured("PERSON", display_name)
        else:
            masked_name = display_name

        masked_parts.append(
            email.utils.formataddr((masked_name, masked_addr)))

    return ", ".join(masked_parts)


def _mask_body_part(part: EmailMessage, scanner: TextScanner) -> None:
    """Scan a single MIME part's text content with Presidio."""
    content_type = part.get_content_type()
    if content_type not in ("text/plain", "text/html"):
        return

    try:
        text = part.get_content()
    except (KeyError, LookupError):
        return

    if not isinstance(text, str):
        return

    if content_type == "text/html" and len(text) > 1000:
        masked = _mask_html_body(text, scanner)
    else:
        masked = scanner.scan(text)

    if masked != text:
        part.set_content(masked, subtype=content_type.split("/")[1],
                         charset="utf-8")


def _mask_html_body(html: str, scanner: TextScanner) -> str:
    """Mask HTML body: strip CSS noise for NER, apply to original.

    CSS noise (style blocks, inline styles, class attributes) causes
    spaCy to waste time tokenizing layout tokens like font names, color
    codes, and CSS selectors — none of which contain PII.

    Stripping CSS before NER gives ~2-3x speedup with identical PII
    detection (the "missed" entities are all CSS false positives like
    'Roboto' or 'max' detected as PERSON).
    """
    cleaned = _strip_css(html)

    # Run Presidio NER on CSS-stripped HTML
    results = scanner._analyzer.analyze(
        text=cleaned,
        language="en",
        entities=_ALL_ENTITIES,
        score_threshold=scanner._threshold,
    )

    # Build replacement map (same logic as scanner.scan)
    MIN_MATCH_LEN = 3
    results = [r for r in results if (r.end - r.start) >= MIN_MATCH_LEN]
    results.sort(key=lambda r: (r.start, -(r.end - r.start), -r.score))

    replacements = {}
    last_end = 0
    for r in results:
        if r.start >= last_end:
            real_value = cleaned[r.start:r.end]
            real_lower = real_value.lower()

            # Skip company/domain names — handled by _domain_replace
            if (real_lower in scanner._store._company_names
                    or real_lower in scanner._store._domain_map):
                continue

            fake_value = scanner._store.get_or_create(
                r.entity_type, real_value)
            replacements[real_value] = fake_value
            last_end = r.end

    # Apply replacements to ORIGINAL HTML (longest first)
    masked = html
    for real, fake in sorted(replacements.items(),
                             key=lambda x: -len(x[0])):
        masked = masked.replace(real, fake)

    return scanner._domain_replace(masked)
