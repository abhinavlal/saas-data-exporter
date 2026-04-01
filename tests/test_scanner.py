"""Tests for scripts.pii_mask.scanner — Aho-Corasick text replacement."""

import pytest

from scripts.pii_mask.roster import Roster
from scripts.pii_mask.scanner import TextScanner


SAMPLE_ROSTER = {
    "version": 1,
    "domain_map": {
        "org_name.com": "example.com",
        "org_name.atlassian.net": "example.atlassian.net",
    },
    "users": [
        {
            "id": "user-001",
            "real": {
                "email": "john.doe@org_name.com",
                "name": "John Doe",
                "first_name": "John",
                "last_name": "Doe",
                "github_login": "johndoe",
                "slack_user_id": "U01ABC123",
                "jira_account_id": "5f7abc12345",
            },
            "masked": {
                "email": "alice.chen@example.com",
                "name": "Alice Chen",
                "first_name": "Alice",
                "last_name": "Chen",
                "github_login": "achen",
                "slack_user_id": "U01MASK01",
                "jira_account_id": "mask-001",
            },
        },
        {
            "id": "user-002",
            "real": {
                "email": "priya.sharma@org_name.com",
                "name": "Priya Sharma",
                "first_name": "Priya",
                "last_name": "Sharma",
                "github_login": "priyasharma",
                "slack_user_id": "U02DEF456",
            },
            "masked": {
                "email": "bob.kumar@example.com",
                "name": "Bob Kumar",
                "first_name": "Bob",
                "last_name": "Kumar",
                "github_login": "bkumar",
                "slack_user_id": "U02MASK02",
            },
        },
    ],
}


@pytest.fixture
def roster():
    return Roster(SAMPLE_ROSTER)


@pytest.fixture
def scanner(roster):
    return TextScanner(roster)


# -- AC replacement -------------------------------------------------------- #

class TestACReplacement:
    def test_replaces_full_name(self, scanner):
        result = scanner.scan("Meeting with John Doe about the project")
        assert "John Doe" not in result
        assert "Alice Chen" in result

    def test_replaces_email_in_text(self, scanner):
        result = scanner.scan("Contact john.doe@org_name.com for details")
        assert "john.doe@org_name.com" not in result
        assert "alice.chen@example.com" in result

    def test_replaces_github_mention(self, scanner):
        result = scanner.scan("CC @johndoe for review")
        assert "@johndoe" not in result
        assert "@achen" in result

    def test_replaces_slack_mention(self, scanner):
        result = scanner.scan("Hey <@U01ABC123> check this")
        assert "<@U01ABC123>" not in result
        assert "<@U01MASK01>" in result

    def test_replaces_multiple_people(self, scanner):
        text = "John Doe and Priya Sharma discussed the feature"
        result = scanner.scan(text)
        assert "John Doe" not in result
        assert "Priya Sharma" not in result
        assert "Alice Chen" in result
        assert "Bob Kumar" in result

    def test_preserves_non_pii_text(self, scanner):
        text = "The server returned a 500 error during deployment"
        result = scanner.scan(text)
        assert "The server returned a 500 error during deployment" == result

    def test_empty_text(self, scanner):
        assert scanner.scan("") == ""
        assert scanner.scan(None) is None

    def test_name_case_sensitive(self, scanner):
        # "john doe" lowercase should NOT match (names are case-sensitive)
        result = scanner.scan("Talked to john doe yesterday")
        assert "Alice Chen" not in result

    def test_email_case_insensitive(self, scanner):
        result = scanner.scan("Email John.Doe@Org_Name.com for info")
        assert "John.Doe@Org_Name.com" not in result

    def test_longest_match_wins(self, scanner):
        # "Priya Sharma" should match as full name, not just "Priya" or "Sharma"
        result = scanner.scan("Ask Priya Sharma about it")
        assert "Bob Kumar" in result
        assert "Priya" not in result


# -- Regex fallback -------------------------------------------------------- #

class TestRegexFallback:
    def test_unknown_email_hashed(self, scanner):
        result = scanner.scan("Contact stranger@org_name.com please")
        assert "stranger@org_name.com" not in result
        assert "@example.com" in result

    def test_external_email_hashed(self, scanner):
        result = scanner.scan("Email someone@gmail.com for help")
        assert "someone@gmail.com" not in result
        assert "@gmail.com" in result

    def test_indian_phone_redacted(self, scanner):
        result = scanner.scan("Call me at 9876543210")
        assert "9876543210" not in result
        assert "[PHONE]" in result

    def test_intl_phone_redacted(self, scanner):
        result = scanner.scan("Reach me at +91 9876543210")
        assert "9876543210" not in result
        assert "[PHONE]" in result

    def test_combined_ac_and_regex(self, scanner):
        text = "John Doe (john.doe@org_name.com, +91 9876543210)"
        result = scanner.scan(text)
        assert "John Doe" not in result
        assert "john.doe@org_name.com" not in result
        assert "9876543210" not in result
        assert "Alice Chen" in result


# -- scan_email ------------------------------------------------------------ #

class TestScanEmail:
    def test_known_email(self, scanner):
        assert scanner.scan_email("john.doe@org_name.com") == \
            "alice.chen@example.com"

    def test_unknown_email(self, scanner):
        result = scanner.scan_email("stranger@org_name.com")
        assert "@example.com" in result
        assert "stranger" not in result

    def test_empty_email(self, scanner):
        assert scanner.scan_email("") == ""


# -- scan_url -------------------------------------------------------------- #

class TestScanURL:
    def test_replaces_domain(self, scanner):
        url = "https://org_name.atlassian.net/rest/api/3/issue/123"
        result = scanner.scan_url(url)
        assert "org_name.atlassian.net" not in result
        assert "example.atlassian.net" in result

    def test_no_match_passthrough(self, scanner):
        url = "https://github.com/repo/pull/1"
        assert scanner.scan_url(url) == url

    def test_empty_url(self, scanner):
        assert scanner.scan_url("") == ""
        assert scanner.scan_url(None) is None


# -- Edge cases ------------------------------------------------------------ #

class TestEdgeCases:
    def test_no_roster_users(self):
        roster = Roster({"version": 1, "users": []})
        scanner = TextScanner(roster)
        result = scanner.scan("Hello world")
        assert result == "Hello world"

    def test_short_first_name_not_indexed(self):
        """First names < 5 chars are not added as standalone terms."""
        roster = Roster({
            "version": 1,
            "users": [{
                "id": "u1",
                "real": {"name": "Al Smith", "first_name": "Al",
                         "last_name": "Smith", "email": "al@test.com"},
                "masked": {"name": "Bo Jones", "first_name": "Bo",
                           "last_name": "Jones", "email": "bo@test.com"},
            }],
        })
        scanner = TextScanner(roster)
        # "Al" alone shouldn't be replaced (too short, false positive risk)
        result = scanner.scan("Al helped with the algorithm")
        # Full name should still be replaced
        result2 = scanner.scan("Al Smith helped")
        assert "Bo Jones" in result2

    def test_deterministic(self, scanner):
        text = "Contact john.doe@org_name.com"
        r1 = scanner.scan(text)
        r2 = scanner.scan(text)
        assert r1 == r2
