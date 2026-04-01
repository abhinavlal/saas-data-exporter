"""NER engine — Presidio-based PII detection as a second pass.

Wraps Microsoft Presidio to detect PII that the roster + Aho-Corasick
scanner missed: external people, addresses, phone numbers, etc.

This module is optional — it imports ``presidio_analyzer`` and
``presidio_anonymizer`` lazily so the pipeline works without them
when ``--enable-ner`` is not set.
"""

import logging

log = logging.getLogger(__name__)

# Entity types we care about (skip noisy ones like DATE_TIME, URL)
DEFAULT_ENTITIES = [
    "PERSON",
    "EMAIL_ADDRESS",
    "PHONE_NUMBER",
    "LOCATION",
    "CREDIT_CARD",
    "IP_ADDRESS",
    "US_SSN",
]

# Replacement labels per entity type
_REPLACEMENTS = {
    "PERSON": "[PERSON]",
    "EMAIL_ADDRESS": "[EMAIL]",
    "PHONE_NUMBER": "[PHONE]",
    "LOCATION": "[LOCATION]",
    "CREDIT_CARD": "[CREDIT_CARD]",
    "IP_ADDRESS": "[IP]",
    "US_SSN": "[SSN]",
}


class NEREngine:
    """Presidio-backed NER for PII detection and anonymization.

    Constructed once, reused across all scanner calls.  Thread-safe
    after construction (Presidio's AnalyzerEngine is thread-safe).
    """

    def __init__(self, language: str = "en",
                 score_threshold: float = 0.7,
                 entities: list[str] | None = None):
        from presidio_analyzer import AnalyzerEngine
        from presidio_anonymizer import AnonymizerEngine
        from presidio_anonymizer.entities import OperatorConfig

        self._language = language
        self._score_threshold = score_threshold
        self._entities = entities or DEFAULT_ENTITIES

        log.info("Initializing Presidio NER engine (language=%s, "
                 "threshold=%.2f, entities=%s)",
                 language, score_threshold, self._entities)

        self._analyzer = AnalyzerEngine()
        self._anonymizer = AnonymizerEngine()

        # Build operator config: each entity type → replace with label
        self._operators = {
            entity: OperatorConfig("replace", {
                "new_value": _REPLACEMENTS.get(entity, f"[{entity}]")
            })
            for entity in self._entities
        }

        log.info("Presidio NER engine ready")

    def mask(self, text: str,
             allow_list: list[str] | None = None) -> str:
        """Detect and replace PII in text.

        *allow_list*: tokens to skip (e.g., already-masked fake names).
        Returns the anonymized text.
        """
        if not text or len(text) < 3:
            return text

        results = self._analyzer.analyze(
            text=text,
            language=self._language,
            entities=self._entities,
            score_threshold=self._score_threshold,
            allow_list=allow_list or [],
        )

        if not results:
            return text

        anonymized = self._anonymizer.anonymize(
            text=text,
            analyzer_results=results,
            operators=self._operators,
        )

        return anonymized.text
