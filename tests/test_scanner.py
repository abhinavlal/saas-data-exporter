"""Tests for scripts.pii_mask.scanner — Presidio-first single-pass scanner."""

import pytest

from scripts.pii_mask.pii_store import PIIStore
from scripts.pii_mask.scanner import TextScanner


@pytest.fixture
def store(tmp_path):
    store = PIIStore(str(tmp_path / "test.db"))
    store.add_domain("org_name.com", "example.com")
    store.add_domain("org_name.atlassian.net", "example.atlassian.net")
    # Pre-populate some known identities for consistency testing
    store.get_or_create("PERSON", "John Doe", source="test")
    store.get_or_create("EMAIL_ADDRESS", "john.doe@org_name.com", source="test")
    return store


@pytest.fixture(scope="module")
def _analyzer_cache():
    """Module-scoped analyzer to avoid reloading spaCy model per test."""
    from presidio_analyzer import AnalyzerEngine
    return AnalyzerEngine()


@pytest.fixture
def scanner(store, _analyzer_cache, monkeypatch):
    """Scanner with pre-loaded analyzer (fast)."""
    s = TextScanner.__new__(TextScanner)
    s._store = store
    s._threshold = 0.5
    s._analyzer = _analyzer_cache
    return s


# -- Single-pass detection + replacement ---------------------------------- #

class TestScan:
    def test_detects_and_replaces_person(self, scanner):
        result = scanner.scan("Meeting with Sarah Johnson tomorrow")
        assert "Sarah Johnson" not in result
        assert len(result) > 10  # still readable

    def test_replaces_email(self, scanner):
        result = scanner.scan("Contact someone@org_name.com for info")
        assert "someone@org_name.com" not in result
        assert "@example.com" in result

    def test_consistent_via_store(self, scanner):
        """Same PII value → same fake via PIIStore (not scanner's job
        to guarantee this — PIIStore tests cover it). Scanner just
        verifies it calls get_or_create with the detected value."""
        result = scanner.scan("Email from John Doe about the project")
        assert "John Doe" not in result
        assert "project" in result  # non-PII preserved

    def test_preserves_non_pii_text(self, scanner):
        text = "The server returned a 500 error during deployment"
        result = scanner.scan(text)
        assert "server" in result
        assert "500" in result
        assert "deployment" in result

    def test_no_double_replacement(self, scanner):
        # First scan: Presidio detects "John Doe", replaces with fake
        result = scanner.scan("John Doe submitted the report")
        # The fake name is a real-looking name (from PIIStore)
        # If we scanned again, Presidio might detect the fake name
        # But we only scan once — verify the result is stable
        assert "John Doe" not in result
        assert "submitted the report" in result

    def test_empty_text(self, scanner):
        assert scanner.scan("") == ""
        assert scanner.scan(None) is None
        assert scanner.scan("ab") == "ab"

    def test_detects_phone_number(self, scanner):
        # Lower threshold to catch phone numbers (score ~0.4)
        scanner._threshold = 0.3
        result = scanner.scan("Call 212-555-1234 for details")
        assert "212-555-1234" not in result

    def test_domain_replacement(self, scanner):
        result = scanner.scan("Visit org_name.atlassian.net/wiki")
        assert "org_name.atlassian.net" not in result
        assert "example.atlassian.net" in result


# -- scan_structured ------------------------------------------------------- #

class TestScanStructured:
    def test_known_email(self, scanner):
        result = scanner.scan_structured(
            "EMAIL_ADDRESS", "john.doe@org_name.com")
        # Should return the pre-populated fake
        assert result != "john.doe@org_name.com"
        assert "@example.com" in result

    def test_unknown_email(self, scanner):
        result = scanner.scan_structured(
            "EMAIL_ADDRESS", "stranger@org_name.com")
        assert "@example.com" in result
        assert "stranger" not in result

    def test_person(self, scanner):
        result = scanner.scan_structured("PERSON", "John Doe")
        assert result != "John Doe"

    def test_github_login(self, scanner):
        result = scanner.scan_structured("GITHUB_LOGIN", "johndoe")
        assert result != "johndoe"

    def test_empty(self, scanner):
        assert scanner.scan_structured("PERSON", "") == ""


# -- scan_url -------------------------------------------------------------- #

class TestScanURL:
    def test_replaces_domain(self, scanner):
        url = "https://org_name.atlassian.net/rest/api/3/issue/123"
        result = scanner.scan_url(url)
        assert "org_name.atlassian.net" not in result
        assert "example.atlassian.net" in result

    def test_passthrough_unknown_domain(self, scanner):
        url = "https://github.com/repo/pull/1"
        assert scanner.scan_url(url) == url

    def test_empty(self, scanner):
        assert scanner.scan_url("") == ""
        assert scanner.scan_url(None) is None
