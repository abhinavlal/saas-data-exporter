"""EML masker — RFC 822 email parsing and PII replacement.

Parses raw .eml bytes, masks structured headers (From, To, CC, BCC,
Subject) and text/HTML body parts using the roster + scanner.
Binary MIME attachments are left untouched.
"""

import email
import email.utils
import email.policy
import logging
import re
from email.message import EmailMessage

from scripts.pii_mask.roster import Roster
from scripts.pii_mask.scanner import TextScanner

log = logging.getLogger(__name__)

# Headers that contain email addresses
_ADDRESS_HEADERS = ("from", "to", "cc", "bcc", "reply-to")

# Headers that contain freeform text
_TEXT_HEADERS = ("subject",)


def mask_eml(eml_bytes: bytes, roster: Roster,
             scanner: TextScanner) -> bytes:
    """Parse, mask, and re-encode an EML file.

    Returns the masked email as bytes, preserving MIME structure.
    """
    msg = email.message_from_bytes(eml_bytes, policy=email.policy.default)

    # -- Mask address headers --
    for hdr_name in _ADDRESS_HEADERS:
        if msg[hdr_name]:
            original = msg[hdr_name]
            masked = _mask_address_header(original, roster, scanner)
            if masked != original:
                del msg[hdr_name]
                msg[hdr_name] = masked

    # -- Mask text headers --
    for hdr_name in _TEXT_HEADERS:
        if msg[hdr_name]:
            original = msg[hdr_name]
            masked = scanner.scan(original)
            if masked != original:
                del msg[hdr_name]
                msg[hdr_name] = masked

    # -- Mask body parts --
    if msg.is_multipart():
        _mask_parts(msg, scanner)
    else:
        _mask_single_part(msg, scanner)

    return msg.as_bytes()


def _mask_address_header(header_value: str, roster: Roster,
                         scanner: TextScanner) -> str:
    """Mask email addresses and display names in an address header.

    Handles comma-separated lists: "John Doe <john@org.com>, Jane <jane@org.com>"
    """
    addresses = email.utils.getaddresses([header_value])
    masked_parts = []
    for display_name, addr in addresses:
        if addr:
            masked_addr = scanner.scan_email(addr)
        else:
            masked_addr = addr

        if display_name:
            masked_name = roster.map_name(display_name)
        else:
            masked_name = display_name

        masked_parts.append(
            email.utils.formataddr((masked_name, masked_addr)))

    return ", ".join(masked_parts)


def _mask_parts(msg: EmailMessage, scanner: TextScanner) -> None:
    """Walk multipart MIME and mask text/* parts."""
    for part in msg.walk():
        content_type = part.get_content_type()
        if content_type in ("text/plain", "text/html"):
            _mask_single_part(part, scanner)


def _mask_single_part(part: EmailMessage, scanner: TextScanner) -> None:
    """Mask the text content of a single MIME part."""
    content_type = part.get_content_type()
    if content_type not in ("text/plain", "text/html"):
        return

    try:
        text = part.get_content()
    except (KeyError, LookupError):
        # Can't decode — leave untouched
        return

    if not isinstance(text, str):
        return

    masked = scanner.scan(text)
    if masked != text:
        part.set_content(masked, subtype=content_type.split("/")[1],
                         charset="utf-8")
