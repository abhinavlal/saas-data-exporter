"""Tests for scripts.pii_mask.pii_store — SQLite PII identity store."""

import json
import pytest

from scripts.pii_mask.pii_store import PIIStore


@pytest.fixture
def store(tmp_path):
    return PIIStore(str(tmp_path / "test.db"))


class TestGetOrCreate:
    def test_creates_new_person(self, store):
        result = store.get_or_create("PERSON", "John Doe")
        assert result != "John Doe"
        assert len(result) > 0

    def test_consistent_lookups(self, store):
        r1 = store.get_or_create("PERSON", "John Doe")
        r2 = store.get_or_create("PERSON", "John Doe")
        assert r1 == r2

    def test_different_people_different_fakes(self, store):
        r1 = store.get_or_create("PERSON", "John Doe")
        r2 = store.get_or_create("PERSON", "Jane Smith")
        assert r1 != r2

    def test_email_gets_mapped_domain(self, store):
        store.add_domain("org_name.com", "example.com")
        result = store.get_or_create("EMAIL_ADDRESS", "john@org_name.com")
        assert "@example.com" in result
        assert "john@org_name.com" != result

    def test_email_unknown_domain_preserved(self, store):
        result = store.get_or_create("EMAIL_ADDRESS", "user@gmail.com")
        assert "@gmail.com" in result

    def test_phone_number_redacted(self, store):
        result = store.get_or_create("PHONE_NUMBER", "9876543210")
        assert "9876543210" not in result
        assert "[PHONE-" in result

    def test_ip_address_mapped(self, store):
        result = store.get_or_create("IP_ADDRESS", "192.168.1.1")
        assert result.startswith("10.0.")
        assert "192.168" not in result

    def test_credit_card_redacted(self, store):
        result = store.get_or_create("CREDIT_CARD", "4111111111111111")
        assert result == "[REDACTED]"

    def test_github_login(self, store):
        result = store.get_or_create("GITHUB_LOGIN", "johndoe")
        assert result != "johndoe"
        assert len(result) >= 2

    def test_jira_account_id(self, store):
        result = store.get_or_create("JIRA_ACCOUNT_ID", "557058:abc")
        assert result.startswith("mask-")

    def test_empty_value_passthrough(self, store):
        assert store.get_or_create("PERSON", "") == ""

    def test_lookup_returns_none_for_missing(self, store):
        assert store.lookup("PERSON", "Nobody") is None

    def test_lookup_returns_value_after_create(self, store):
        store.get_or_create("PERSON", "John Doe")
        result = store.lookup("PERSON", "John Doe")
        assert result is not None
        assert result != "John Doe"


class TestIndianPII:
    def test_pan(self, store):
        result = store.get_or_create("IN_PAN", "ABCPD1234E")
        assert result != "ABCPD1234E"
        assert len(result) == 10  # same structure

    def test_aadhaar(self, store):
        result = store.get_or_create("IN_AADHAAR", "2345 6789 0123")
        assert "2345" not in result

    def test_upi_id(self, store):
        result = store.get_or_create("IN_UPI_ID", "john@okaxis")
        assert "john" not in result
        assert "@" in result

    def test_ifsc(self, store):
        result = store.get_or_create("IN_IFSC", "SBIN0001234")
        assert result != "SBIN0001234"

    def test_bank_account(self, store):
        result = store.get_or_create("IN_BANK_ACCOUNT", "1234567890123456")
        assert result != "1234567890123456"

    def test_geo_coordinate(self, store):
        result = store.get_or_create("GEO_COORDINATE", "12.9716,77.5946")
        assert "12.9716" not in result

    def test_medical_license(self, store):
        result = store.get_or_create("MEDICAL_LICENSE", "ML-12345")
        assert "ML-12345" not in result
        assert "[MEDICAL-" in result

    def test_url_domain_mapped(self, store):
        store.add_domain("org_name.com", "example.com")
        result = store.get_or_create("URL", "https://www.org_name.com/doctors")
        assert "org_name.com" not in result
        assert "example.com" in result

    def test_gst_number(self, store):
        result = store.get_or_create("IN_GST", "27AABCU9603R1ZM")
        assert "27AABCU9603R1ZM" not in result

    def test_org_name(self, store):
        result = store.get_or_create("ORG_NAME", "Acme Corp")
        assert result != "Acme Corp"
        assert len(result) > 0


class TestDomainMap:
    def test_add_and_retrieve(self, store):
        store.add_domain("real.com", "fake.com")
        assert store.map_domain("real.com") == "fake.com"

    def test_case_insensitive(self, store):
        store.add_domain("Real.COM", "fake.com")
        assert store.map_domain("real.com") == "fake.com"

    def test_unknown_domain_passthrough(self, store):
        assert store.map_domain("unknown.com") == "unknown.com"

    def test_map_email_domain(self, store):
        store.add_domain("org_name.com", "example.com")
        assert store.map_email_domain("john@org_name.com") == "john@example.com"


class TestImportExport:
    def test_from_json(self, tmp_path):
        roster = {
            "version": 1,
            "domain_map": {"org_name.com": "example.com"},
            "users": [{
                "id": "u1",
                "real": {"email": "john@org_name.com", "name": "John Doe",
                         "github_login": "johndoe"},
                "masked": {"email": "alice@example.com", "name": "Alice Chen",
                           "github_login": "achen"},
            }],
        }
        json_path = tmp_path / "roster.json"
        json_path.write_text(json.dumps(roster))

        store = PIIStore.from_json(str(json_path),
                                   str(tmp_path / "imported.db"))

        # Verify imported entries
        assert store.lookup("EMAIL_ADDRESS", "john@org_name.com") == \
            "alice@example.com"
        assert store.lookup("PERSON", "John Doe") == "Alice Chen"
        assert store.lookup("GITHUB_LOGIN", "johndoe") == "achen"
        assert store.map_domain("org_name.com") == "example.com"

    def test_export_json(self, store, tmp_path):
        store.get_or_create("PERSON", "John Doe")
        store.add_domain("real.com", "fake.com")

        export_path = str(tmp_path / "export.json")
        store.export_json(export_path)

        with open(export_path) as f:
            data = json.load(f)
        assert data["total_entries"] >= 1
        assert "PERSON" in data["entries_by_type"]

    def test_roundtrip(self, tmp_path):
        # Create store, add entries
        store1 = PIIStore(str(tmp_path / "s1.db"))
        store1.add_domain("org.com", "fake.com")
        val1 = store1.get_or_create("PERSON", "John Doe")

        # Export
        store1.export_json(str(tmp_path / "export.json"))

        # Re-open same db — should have same values
        store2 = PIIStore(str(tmp_path / "s1.db"))
        assert store2.lookup("PERSON", "John Doe") == val1
        assert store2.map_domain("org.com") == "fake.com"


class TestStats:
    def test_stats(self, store):
        store.get_or_create("PERSON", "A")
        store.get_or_create("PERSON", "B")
        store.get_or_create("EMAIL_ADDRESS", "a@b.com")
        s = store.stats()
        assert s["PERSON"] == 2
        assert s["EMAIL_ADDRESS"] == 1
