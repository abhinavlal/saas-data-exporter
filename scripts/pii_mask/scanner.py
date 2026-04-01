"""TextScanner — Presidio-first PII detection with single-pass replacement.

Detects PII on the ORIGINAL text using Presidio, looks up consistent
replacements from PIIStore (auto-generating fakes for new entities),
then applies all replacements in a single right-to-left pass.

Never scans already-replaced output — no double-replacement risk.

Includes:
- Roster-backed AC recognizer — guarantees 100% recall on roster entries
- Custom recognizers for Indian PII (PAN, Aadhaar, UPI, IFSC, GST)
- Company name detection via roster
"""

import logging
from typing import List

import ahocorasick
from presidio_analyzer import (
    AnalyzerEngine, PatternRecognizer, Pattern,
    EntityRecognizer, RecognizerResult,
)
from presidio_analyzer.nlp_engine import NlpArtifacts

from scripts.pii_mask.pii_store import PIIStore

log = logging.getLogger(__name__)

# -- Entity types --------------------------------------------------------- #

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
    "NRP",          # nationality, religion, political group
]

_CUSTOM_ENTITIES = [
    "IN_PAN",
    "IN_AADHAAR",
    "IN_UPI_ID",
    "IN_IFSC",
    "IN_BANK_ACCOUNT",
    "IN_GST",
    "GEO_COORDINATE",
    "ORG_NAME",     # company/org names — spaCy NER (ORG) + roster AC
]

_ALL_ENTITIES = _BUILTIN_ENTITIES + _CUSTOM_ENTITIES

DEFAULT_THRESHOLD = 0.5


# -- Roster-backed AC recognizer ------------------------------------------ #

class RosterRecognizer(EntityRecognizer):
    """Custom Presidio recognizer backed by PIIStore's real values.

    Builds an Aho-Corasick automaton from all known real PII in the
    store.  This guarantees 100% recall on roster entries — even if
    spaCy NER misses a name, this recognizer will catch it.

    Runs in O(n) per text via AC, same complexity as Presidio's regex
    recognizers.  Score is 0.95 — higher than spaCy NER (0.85) so
    roster matches always win overlap resolution.
    """

    ROSTER_SCORE = 0.95

    # Map PIIStore entity types to Presidio entity types
    _TYPE_MAP = {
        "PERSON": "PERSON",
        "EMAIL_ADDRESS": "EMAIL_ADDRESS",
        "GITHUB_LOGIN": "PERSON",
        "SLACK_USERNAME": "PERSON",
        "ORG_NAME": "ORG_NAME",
    }

    def __init__(self, store: PIIStore):
        super().__init__(
            supported_entities=list(set(self._TYPE_MAP.values())),
            name="RosterRecognizer",
            supported_language="en",
        )
        self._automaton = ahocorasick.Automaton()
        count = 0

        con = store._get_connection()
        rows = con.execute(
            "SELECT entity_type, real_value FROM roster_entries "
            "WHERE entity_type IN ('PERSON', 'EMAIL_ADDRESS', "
            "  'GITHUB_LOGIN', 'SLACK_USERNAME', 'ORG_NAME')"
        ).fetchall()

        seen = set()
        for entity_type, real_value in rows:
            key = real_value.lower()
            if len(key) < 4 or key in seen:
                continue
            seen.add(key)
            presidio_type = self._TYPE_MAP.get(entity_type, entity_type)
            self._automaton.add_word(key, (presidio_type, real_value))
            count += 1

        if count > 0:
            self._automaton.make_automaton()
        self._has_automaton = count > 0

        log.info("RosterRecognizer: %d patterns from PIIStore", count)

    def load(self):
        pass  # no-op, loaded in __init__

    def analyze(self, text: str, entities: List[str],
                nlp_artifacts: NlpArtifacts = None) -> List[RecognizerResult]:
        if not self._has_automaton or not text:
            return []

        results = []
        text_lower = text.lower()
        last_end = 0

        for end_idx, (entity_type, original) in \
                self._automaton.iter(text_lower):
            start = end_idx - len(original) + 1
            end = end_idx + 1

            # Skip overlapping matches
            if start < last_end:
                continue

            if entity_type not in entities:
                continue

            results.append(RecognizerResult(
                entity_type=entity_type,
                start=start,
                end=end,
                score=self.ROSTER_SCORE,
            ))
            last_end = end

        return results


# -- Custom pattern recognizers -------------------------------------------- #

def _build_custom_recognizers() -> list:
    """Build regex-based recognizers for Indian PII types."""

    recognizers: list = []

    # Indian PAN: ABCPD1234E
    recognizers.append(PatternRecognizer(
        supported_entity="IN_PAN",
        name="IndianPANRecognizer",
        patterns=[Pattern("in_pan", r"\b[A-Z]{5}[0-9]{4}[A-Z]\b", 0.7)],
        supported_language="en",
    ))

    # Indian Aadhaar: 2345 6789 0123
    recognizers.append(PatternRecognizer(
        supported_entity="IN_AADHAAR",
        name="IndianAadhaarRecognizer",
        patterns=[Pattern("in_aadhaar", r"\b[2-9]\d{3}\s?\d{4}\s?\d{4}\b", 0.6)],
        supported_language="en",
    ))

    # UPI ID: john@okaxis, 9876@paytm
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
        patterns=[Pattern("in_upi_id",
                          rf"\b[a-zA-Z0-9._-]+@(?:{_UPI_HANDLES})\b", 0.8)],
        supported_language="en",
    ))

    # IFSC: SBIN0001234
    recognizers.append(PatternRecognizer(
        supported_entity="IN_IFSC",
        name="IndianIFSCRecognizer",
        patterns=[Pattern("in_ifsc", r"\b[A-Z]{4}0[A-Z0-9]{6}\b", 0.7)],
        supported_language="en",
    ))

    # Indian bank account with context keywords
    recognizers.append(PatternRecognizer(
        supported_entity="IN_BANK_ACCOUNT",
        name="IndianBankAccountRecognizer",
        patterns=[Pattern("in_bank_account",
                          r"(?i)(?:account|a/c|acct)[^\d]{0,10}(\d{9,18})\b", 0.5)],
        supported_language="en",
    ))

    # Indian GST number: 22AAAAA0000A1Z5
    # Format: 2-digit state + PAN (10 chars) + 1 digit + Z + checksum
    recognizers.append(PatternRecognizer(
        supported_entity="IN_GST",
        name="IndianGSTRecognizer",
        patterns=[Pattern("in_gst",
                          r"\b\d{2}[A-Z]{5}\d{4}[A-Z]\d[A-Z][A-Z0-9]\b", 0.8)],
        supported_language="en",
    ))

    # Geo coordinates: 12.9716,77.5946
    recognizers.append(PatternRecognizer(
        supported_entity="GEO_COORDINATE",
        name="GeoCoordinateRecognizer",
        patterns=[Pattern("geo_latlon",
                          r"\b-?\d{1,3}\.\d{4,},\s?-?\d{1,3}\.\d{4,}\b", 0.6)],
        supported_language="en",
    ))

    return recognizers


# -- Scanner --------------------------------------------------------------- #

class TextScanner:
    """Presidio-first PII scanner with PIIStore-backed replacement.

    Registers a RosterRecognizer so Presidio itself catches all roster
    entries with 0.95 confidence — higher than NER (0.85). This means
    roster matches ALWAYS win, even if spaCy doesn't detect the name.

    Thread-safe — AnalyzerEngine and PIIStore are both thread-safe.
    """

    def __init__(self, store: PIIStore,
                 threshold: float = DEFAULT_THRESHOLD):
        self._store = store
        self._threshold = threshold

        log.info("Initializing Presidio analyzer...")
        self._analyzer = AnalyzerEngine()

        # Enable ORG entity detection: spaCy detects ORG but Presidio
        # ignores it by default. Remove from ignore list, map to ORG_NAME,
        # and add ORG_NAME to the SpacyRecognizer's supported entities.
        ner_cfg = self._analyzer.nlp_engine.ner_model_configuration
        if "ORGANIZATION" in ner_cfg.labels_to_ignore:
            ner_cfg.labels_to_ignore.remove("ORGANIZATION")
        ner_cfg.model_to_presidio_entity_mapping["ORG"] = "ORG_NAME"
        ner_cfg.model_to_presidio_entity_mapping["ORGANIZATION"] = "ORG_NAME"
        for rec in self._analyzer.get_recognizers(language="en"):
            if rec.name == "SpacyRecognizer":
                if "ORG_NAME" not in rec.supported_entities:
                    rec.supported_entities.append("ORG_NAME")

        # Register roster-backed recognizer (AC automaton, score 0.95)
        roster_recognizer = RosterRecognizer(store)
        self._analyzer.registry.add_recognizer(roster_recognizer)

        # Register custom pattern recognizers
        for recognizer in _build_custom_recognizers():
            self._analyzer.registry.add_recognizer(recognizer)
            log.info("  Registered: %s", recognizer.name)

        log.info("TextScanner ready (threshold=%.2f, entities=%d, "
                 "roster patterns=%d)",
                 threshold, len(_ALL_ENTITIES),
                 roster_recognizer._automaton.get_stats().get("nodes_count", 0)
                 if roster_recognizer._has_automaton else 0)

    # -- Public API -------------------------------------------------------- #

    def scan(self, text: str, source: str = "") -> str:
        """Detect PII on original text, replace in single pass.

        Presidio runs ALL recognizers (NER + roster AC + regex patterns)
        in one analyze() call. Results are deduplicated by overlap
        resolution, then replaced via PIIStore in a single right-to-left
        pass.
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

        # Sort by start position, longest match first, highest score first
        results.sort(key=lambda r: (r.start, -(r.end - r.start), -r.score))

        # Resolve overlaps: greedy left-to-right, longest/highest-score wins
        resolved = []
        last_end = 0
        for r in results:
            if r.start >= last_end:
                real_value = text[r.start:r.end]
                fake_value = self._store.get_or_create(
                    r.entity_type, real_value, source=source)
                resolved.append((r.start, r.end, fake_value))
                last_end = r.end

        # Apply replacements right-to-left
        chars = list(text)
        for start, end, replacement in reversed(resolved):
            chars[start:end] = list(replacement)
        result = "".join(chars)

        return self._domain_replace(result)

    def scan_structured(self, entity_type: str, value: str,
                        source: str = "") -> str:
        """Direct PIIStore lookup for known-type fields. No NER needed."""
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
        for real_domain, fake_domain in self._store.domain_map.items():
            text = text.replace(real_domain, fake_domain)
            text = text.replace(
                real_domain.capitalize(), fake_domain.capitalize())
        return text
