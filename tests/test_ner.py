"""Tests for scripts.pii_mask.ner — Presidio NER integration."""

import pytest

from scripts.pii_mask.ner import NEREngine
from scripts.pii_mask.roster import Roster
from scripts.pii_mask.scanner import TextScanner


@pytest.fixture(scope="module")
def ner_engine():
    """Module-scoped NER engine (model load is expensive)."""
    return NEREngine(score_threshold=0.5)


# -- NEREngine unit tests -------------------------------------------------- #

class TestNEREngine:
    def test_detects_person_name(self, ner_engine):
        result = ner_engine.mask("Meeting with Sarah Johnson tomorrow")
        assert "Sarah Johnson" not in result
        assert "[PERSON]" in result

    def test_detects_phone_number(self):
        # Phone numbers score 0.4 in Presidio — use lower threshold
        engine = NEREngine(score_threshold=0.3)
        result = engine.mask("Call me at 212-555-1234")
        assert "212-555-1234" not in result
        assert "[PHONE]" in result

    def test_detects_email(self, ner_engine):
        result = ner_engine.mask("Email sarah.j@acme.com for details")
        assert "sarah.j@acme.com" not in result
        assert "[EMAIL]" in result

    def test_preserves_non_pii(self, ner_engine):
        text = "The server returned a 500 error"
        result = ner_engine.mask(text)
        assert result == text

    def test_allow_list_skips_known_tokens(self, ner_engine):
        # "Alice Chen" would normally be detected as PERSON
        result = ner_engine.mask(
            "Alice Chen and Sarah Johnson discussed it",
            allow_list=["Alice Chen"],
        )
        # Alice Chen should be preserved (in allow list)
        assert "Alice Chen" in result
        # Sarah Johnson should be masked (not in allow list)
        assert "Sarah Johnson" not in result

    def test_empty_text(self, ner_engine):
        assert ner_engine.mask("") == ""
        assert ner_engine.mask("ab") == "ab"  # too short


# -- Scanner + NER integration -------------------------------------------- #

class TestScannerWithNER:
    def test_ner_catches_external_person(self, ner_engine):
        """NER detects people not in the roster."""
        roster = Roster({
            "version": 1,
            "domain_map": {"org_name.com": "example.com"},
            "users": [{
                "id": "u1",
                "real": {"email": "john@org_name.com", "name": "John Doe",
                         "first_name": "John", "last_name": "Doe"},
                "masked": {"email": "alice@example.com", "name": "Alice Chen"},
            }],
        })
        scanner = TextScanner(roster, ner_engine=ner_engine)

        # John Doe is in roster → replaced by AC
        # Sarah Johnson is NOT in roster → caught by NER
        result = scanner.scan(
            "John Doe met with Sarah Johnson to discuss the project")
        assert "John Doe" not in result
        assert "Alice Chen" in result
        assert "Sarah Johnson" not in result
        assert "[PERSON]" in result
        # Non-PII preserved
        assert "discuss the project" in result

    def test_ner_allow_list_prevents_double_mask(self, ner_engine):
        """Masked names from roster don't get re-masked by NER."""
        roster = Roster({
            "version": 1,
            "users": [{
                "id": "u1",
                "real": {"email": "j@test.com", "name": "John Doe",
                         "first_name": "John", "last_name": "Doe"},
                "masked": {"email": "a@test.com", "name": "Alice Chen"},
            }],
        })
        scanner = TextScanner(roster, ner_engine=ner_engine)

        # After AC: "John Doe" → "Alice Chen"
        # NER should see "Alice Chen" in allow_list and skip it
        result = scanner.scan("Sent to John Doe for review")
        assert "Alice Chen" in result
        assert "[PERSON]" not in result  # Alice Chen not re-masked

    def test_scanner_works_without_ner(self):
        """Scanner still works when NER is None (default)."""
        roster = Roster({"version": 1, "users": []})
        scanner = TextScanner(roster)  # no ner_engine
        result = scanner.scan("Hello Sarah Johnson")
        # Without NER, external names are NOT caught
        assert "Sarah Johnson" in result
