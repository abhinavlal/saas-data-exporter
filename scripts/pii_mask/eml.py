"""EML masker — full RFC 822 email PII replacement.

Scans ALL headers and body parts using the Presidio-powered scanner.
No header is skipped — Delivered-To, Return-Path, Received, DKIM,
Authentication-Results, etc. all get domain + PII replacement.

Final pass: AC automaton sweep on raw bytes catches PII in encoded
content (iCalendar CN= fields, quoted-printable HTML).
"""

import email
import email.utils
import email.policy
import logging
import threading
from email.message import EmailMessage

import ahocorasick

from scripts.pii_mask.scanner import TextScanner

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

    count = 0
    for real_val, masked_val in rows:
        key = real_val.lower()
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
    """Replace known PII in raw bytes using cached AC automaton.

    O(n) per file. Catches PII in iCalendar CN= fields,
    quoted-printable encoded HTML, DKIM signatures, etc.
    """
    automaton, domain_pairs = _get_byte_sweep_automaton(scanner)

    # AC sweep for PERSON + EMAIL_ADDRESS
    if automaton is not None:
        text = data.decode("utf-8", errors="replace")
        text_lower = text.lower()

        # Collect matches
        matches = []
        last_end = 0
        for end_idx, (real_val, masked_val) in automaton.iter(text_lower):
            start = end_idx - len(real_val) + 1
            if start >= last_end:
                # Find original-case match in text for replacement
                matches.append((start, end_idx + 1, masked_val))
                last_end = end_idx + 1

        # Apply replacements right-to-left
        if matches:
            chars = list(text)
            for start, end, replacement in reversed(matches):
                chars[start:end] = list(replacement)
            data = "".join(chars).encode("utf-8", errors="replace")

    # Domain sweep (only 3 entries — fast)
    for real_bytes, fake_bytes in domain_pairs:
        data = data.replace(real_bytes, fake_bytes)

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

    masked = scanner.scan(text)
    if masked != text:
        part.set_content(masked, subtype=content_type.split("/")[1],
                         charset="utf-8")
