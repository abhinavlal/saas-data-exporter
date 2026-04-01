"""EML masker — full RFC 822 email PII replacement.

Scans ALL headers and body parts using the Presidio-powered scanner.
No header is skipped — Delivered-To, Return-Path, Received, DKIM,
Authentication-Results, etc. all get domain + PII replacement.
"""

import email
import email.utils
import email.policy
import logging
from email.message import EmailMessage

from scripts.pii_mask.scanner import TextScanner

log = logging.getLogger(__name__)

# Headers that contain parseable email addresses
_ADDRESS_HEADERS = frozenset({
    "from", "to", "cc", "bcc", "reply-to", "sender",
    "delivered-to", "return-path", "x-original-to",
    "x-forwarded-to", "envelope-to",
})

# Headers to skip entirely (binary/structural, no PII)
_SKIP_HEADERS = frozenset({
    "content-type", "content-transfer-encoding", "content-disposition",
    "mime-version", "content-id", "content-length",
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

    # Final domain sweep on raw bytes — catches domains in DKIM signatures,
    # Received headers, and other places the parser might not expose
    for real_domain, fake_domain in scanner._store.domain_map.items():
        masked_bytes = masked_bytes.replace(
            real_domain.encode(), fake_domain.encode())

    return masked_bytes


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
