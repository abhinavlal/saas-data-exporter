"""Tests for scanner NER detection — Presidio integration.

The NEREngine module no longer exists as a standalone module. Its
functionality is embedded in TextScanner via Presidio AnalyzerEngine.
These tests verify that the scanner detects external people/PII that
are not pre-seeded in the store.
"""

import pytest

from scripts.pii_mask.pii_store import PIIStore
from scripts.pii_mask.scanner import TextScanner


@pytest.fixture(scope="module")
def _analyzer():
    from presidio_analyzer import AnalyzerEngine
    return AnalyzerEngine()


@pytest.fixture
def store(tmp_path):
    s = PIIStore(str(tmp_path / "test.db"))
    s.add_domain("org_name.com", "example.com")
    return s


@pytest.fixture
def scanner(store, _analyzer):
    s = TextScanner.__new__(TextScanner)
    s._store = store
    s._threshold = 0.5
    s._analyzer = _analyzer
    return s


# -- Presidio detection via scanner ---------------------------------------- #

class TestScannerNER:
    def test_detects_person_name(self, scanner):
        result = scanner.scan("Meeting with Sarah Johnson tomorrow")
        assert "Sarah Johnson" not in result

    def test_detects_email(self, scanner):
        result = scanner.scan("Email sarah.j@acme.com for details")
        assert "sarah.j@acme.com" not in result

    def test_preserves_non_pii(self, scanner):
        text = "The server returned a 500 error"
        result = scanner.scan(text)
        assert result == text

    def test_empty_text(self, scanner):
        assert scanner.scan("") == ""
        assert scanner.scan("ab") == "ab"  # too short


# -- Scanner with pre-seeded store ---------------------------------------- #

class TestScannerWithStore:
    def test_known_person_replaced_consistently(self, scanner, store):
        """Known person in the store is replaced with consistent fake."""
        store.get_or_create("PERSON", "John Doe")
        result = scanner.scan("John Doe met with Sarah Johnson to discuss the project")
        assert "John Doe" not in result
        assert "Sarah Johnson" not in result
        # Non-PII preserved
        assert "discuss the project" in result

    def test_consistent_replacement(self, scanner, store):
        """Same person always gets the same replacement in longer text."""
        store.get_or_create("PERSON", "John Doe")
        fake_name = store.lookup("PERSON", "John Doe")
        # Use longer sentences so Presidio detects the name with clear boundaries
        result1 = scanner.scan("The team lead John Doe will present the results today")
        result2 = scanner.scan("According to John Doe the deployment was successful")
        assert "John Doe" not in result1
        assert "John Doe" not in result2
        assert fake_name in result1
        assert fake_name in result2

    def test_scanner_works_on_short_text(self, scanner):
        """Scanner returns short text unchanged."""
        assert scanner.scan("Hi") == "Hi"


class TestRosterRecognizer:
    """Verify the AC-backed roster recognizer catches names that
    spaCy NER might miss (e.g., in short/ambiguous context).
    These tests use a real TextScanner (not the shortcut fixture)."""

    def test_roster_name_caught_in_short_context(self, store):
        """Names in the store are caught even in minimal context."""
        store.get_or_create("PERSON", "Rajesh Kumar")
        scanner = TextScanner(store, threshold=0.5)
        result = scanner.scan("assigned to Rajesh Kumar")
        assert "Rajesh Kumar" not in result

    def test_gst_number_detected(self, store):
        scanner = TextScanner(store, threshold=0.5)
        result = scanner.scan("GST: 27AABCU9603R1ZM for invoice")
        assert "27AABCU9603R1ZM" not in result
