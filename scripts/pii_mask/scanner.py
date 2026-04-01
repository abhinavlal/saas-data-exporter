"""TextScanner — Presidio-first PII detection with single-pass replacement.

Detects PII on the ORIGINAL text using Presidio, looks up consistent
replacements from PIIStore (auto-generating fakes for new entities),
then applies all replacements in a single right-to-left pass.

Never scans already-replaced output — no double-replacement risk.

Includes custom recognizers for Indian PII types (PAN, Aadhaar, UPI,
IFSC, bank account numbers) not covered by Presidio's defaults.
"""

import logging
import re

from presidio_analyzer import AnalyzerEngine, PatternRecognizer, Pattern

from scripts.pii_mask.pii_store import PIIStore

log = logging.getLogger(__name__)

# -- Entity types --------------------------------------------------------- #

# Built-in Presidio entities
_BUILTIN_ENTITIES = [
    "PERSON",
    "EMAIL_ADDRESS",
    "PHONE_NUMBER",
    "LOCATION",
    "CREDIT_CARD",
    "IP_ADDRESS",
    "US_SSN",
    "IBAN_CODE",
    "URL",
    "MEDICAL_LICENSE",
]

# Custom entities (Indian PII + financial)
_CUSTOM_ENTITIES = [
    "IN_PAN",
    "IN_AADHAAR",
    "IN_UPI_ID",
    "IN_IFSC",
    "IN_BANK_ACCOUNT",
    "GEO_COORDINATE",
]

_ALL_ENTITIES = _BUILTIN_ENTITIES + _CUSTOM_ENTITIES

DEFAULT_THRESHOLD = 0.5


# -- Custom recognizers ---------------------------------------------------- #

def _build_custom_recognizers() -> list[PatternRecognizer]:
    """Build regex-based recognizers for Indian PII types."""

    recognizers = []

    # Indian PAN: 5 uppercase letters + 4 digits + 1 uppercase letter
    # e.g. ABCPD1234E
    recognizers.append(PatternRecognizer(
        supported_entity="IN_PAN",
        name="IndianPANRecognizer",
        patterns=[Pattern(
            name="in_pan",
            regex=r"\b[A-Z]{5}[0-9]{4}[A-Z]\b",
            score=0.7,
        )],
        supported_language="en",
    ))

    # Indian Aadhaar: 12 digits starting with 2-9, optionally spaced as 4-4-4
    # e.g. 2345 6789 0123 or 234567890123
    recognizers.append(PatternRecognizer(
        supported_entity="IN_AADHAAR",
        name="IndianAadhaarRecognizer",
        patterns=[Pattern(
            name="in_aadhaar",
            regex=r"\b[2-9]\d{3}\s?\d{4}\s?\d{4}\b",
            score=0.6,
        )],
        supported_language="en",
    ))

    # UPI ID: identifier@bankhandle
    # e.g. john@okaxis, 9876543210@paytm, user.name@ybl
    _UPI_HANDLES = (
        "okaxis|oksbi|okicici|okhdfcbank|okkotak"
        "|ybl|paytm|upi|apl|ibl|axisbank"
        "|sbi|icici|hdfcbank|kotak|indus|citi"
        "|boi|pnb|unionbank|canara|bob"
        "|airtel|jio|freecharge|phonepe|gpay"
    )
    recognizers.append(PatternRecognizer(
        supported_entity="IN_UPI_ID",
        name="IndianUPIRecognizer",
        patterns=[Pattern(
            name="in_upi_id",
            regex=rf"\b[a-zA-Z0-9._-]+@(?:{_UPI_HANDLES})\b",
            score=0.8,
        )],
        supported_language="en",
    ))

    # IFSC Code: 4 uppercase letters + 0 + 6 alphanumeric
    # e.g. SBIN0001234, HDFC0001234
    recognizers.append(PatternRecognizer(
        supported_entity="IN_IFSC",
        name="IndianIFSCRecognizer",
        patterns=[Pattern(
            name="in_ifsc",
            regex=r"\b[A-Z]{4}0[A-Z0-9]{6}\b",
            score=0.7,
        )],
        supported_language="en",
    ))

    # Indian bank account number: 9-18 digits (with context keywords)
    # High false-positive risk without context, so require a keyword nearby
    recognizers.append(PatternRecognizer(
        supported_entity="IN_BANK_ACCOUNT",
        name="IndianBankAccountRecognizer",
        patterns=[Pattern(
            name="in_bank_account",
            regex=r"(?i)(?:account|a/c|acct)[^\d]{0,10}(\d{9,18})\b",
            score=0.5,
        )],
        supported_language="en",
    ))

    # Geo coordinates: latitude,longitude pairs
    # e.g. 12.9716,77.5946 or 28.6139, 77.2090
    recognizers.append(PatternRecognizer(
        supported_entity="GEO_COORDINATE",
        name="GeoCoordinateRecognizer",
        patterns=[Pattern(
            name="geo_latlon",
            regex=r"\b-?\d{1,3}\.\d{4,},\s?-?\d{1,3}\.\d{4,}\b",
            score=0.6,
        )],
        supported_language="en",
    ))

    return recognizers


# -- Scanner --------------------------------------------------------------- #

class TextScanner:
    """Presidio-first PII scanner with PIIStore-backed replacement.

    Build once, call ``scan()`` on every text field.
    Thread-safe — AnalyzerEngine and PIIStore are both thread-safe.
    """

    def __init__(self, store: PIIStore,
                 threshold: float = DEFAULT_THRESHOLD):
        self._store = store
        self._threshold = threshold

        log.info("Initializing Presidio analyzer with custom recognizers...")
        self._analyzer = AnalyzerEngine()

        # Register custom recognizers for Indian PII types
        for recognizer in _build_custom_recognizers():
            self._analyzer.registry.add_recognizer(recognizer)
            log.info("  Registered: %s → %s",
                     recognizer.name, recognizer.supported_entities)

        log.info("TextScanner ready (threshold=%.2f, entities=%d)",
                 threshold, len(_ALL_ENTITIES))

    # -- Public API -------------------------------------------------------- #

    def scan(self, text: str, source: str = "") -> str:
        """Detect PII on original text, replace in single pass.

        1. Presidio detects PII spans on the original text
        2. Each span → PIIStore lookup (consistent) or auto-generate
        3. All replacements applied right-to-left (no re-scan)
        """
        if not text or len(text) < 3:
            return text

        try:
            results = self._analyzer.analyze(
                text=text,
                language="en",
                entities=_ALL_ENTITIES,
                score_threshold=self._threshold,
            )
        except Exception:
            log.debug("Presidio analysis failed, falling back to domain-only",
                      exc_info=True)
            return self._domain_replace(text)

        if not results:
            return self._domain_replace(text)

        # Sort by start position, longest match first
        results.sort(key=lambda r: (r.start, -(r.end - r.start)))

        # Resolve overlaps: greedy left-to-right, longest wins
        resolved = []
        last_end = 0
        for r in results:
            if r.start >= last_end:
                real_value = text[r.start:r.end]
                fake_value = self._store.get_or_create(
                    r.entity_type, real_value, source=source)
                resolved.append((r.start, r.end, fake_value))
                last_end = r.end

        # Apply replacements right-to-left (preserves character offsets)
        chars = list(text)
        for start, end, replacement in reversed(resolved):
            chars[start:end] = list(replacement)
        result = "".join(chars)

        return self._domain_replace(result)

    def scan_structured(self, entity_type: str, value: str,
                        source: str = "") -> str:
        """Direct PIIStore lookup for known-type fields.

        Use this for fields where we KNOW the entity type (e.g., an
        email field, a username field). Skips Presidio NER — faster.
        """
        if not value:
            return value
        return self._store.get_or_create(entity_type, value, source=source)

    def scan_url(self, url: str) -> str:
        """Replace domains in a URL using the store's domain_map."""
        if not url:
            return url
        return self._domain_replace(url)

    # -- Internal ---------------------------------------------------------- #

    def _domain_replace(self, text: str) -> str:
        """Replace all known domains in text."""
        for real_domain, fake_domain in self._store.domain_map.items():
            text = text.replace(real_domain, fake_domain)
            text = text.replace(
                real_domain.capitalize(), fake_domain.capitalize())
        return text
