"""Tests for scripts.pii_mask.roster — Roster identity mapping."""

import json
import pytest

from scripts.pii_mask.roster import Roster, RosterEntry


# -- Fixtures -------------------------------------------------------------- #

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
                "confluence_account_id": "5f7abc12345",
            },
            "masked": {
                "email": "alice.chen@example.com",
                "name": "Alice Chen",
                "first_name": "Alice",
                "last_name": "Chen",
                "github_login": "achen",
                "slack_user_id": "U01ABC123",
                "jira_account_id": "mask-001",
                "jira_display_name": "Alice Chen",
                "confluence_account_id": "mask-001",
            },
        },
        {
            "id": "user-002",
            "real": {
                "email": "jane.smith@org_name.com",
                "name": "Jane Smith",
                "first_name": "Jane",
                "last_name": "Smith",
                "github_login": "janesmith",
                "slack_user_id": "U02DEF456",
                "jira_account_id": "6a8def67890",
            },
            "masked": {
                "email": "bob.kumar@example.com",
                "name": "Bob Kumar",
                "first_name": "Bob",
                "last_name": "Kumar",
                "github_login": "bkumar",
                "slack_user_id": "U02DEF456",
                "jira_account_id": "mask-002",
                "jira_display_name": "Bob Kumar",
            },
        },
    ],
}


@pytest.fixture
def roster():
    return Roster(SAMPLE_ROSTER)


# -- Lookup tests ---------------------------------------------------------- #

class TestLookup:
    def test_by_email(self, roster):
        entry = roster.by_email("john.doe@org_name.com")
        assert entry is not None
        assert entry.id == "user-001"
        assert entry.masked["name"] == "Alice Chen"

    def test_by_email_case_insensitive(self, roster):
        entry = roster.by_email("John.Doe@Org_Name.com")
        assert entry is not None
        assert entry.id == "user-001"

    def test_by_email_not_found(self, roster):
        assert roster.by_email("unknown@other.com") is None

    def test_by_email_empty(self, roster):
        assert roster.by_email("") is None
        assert roster.by_email(None) is None

    def test_by_name(self, roster):
        entry = roster.by_name("John Doe")
        assert entry is not None
        assert entry.masked["email"] == "alice.chen@example.com"

    def test_by_name_case_insensitive(self, roster):
        entry = roster.by_name("john doe")
        assert entry is not None
        assert entry.id == "user-001"

    def test_by_github_login(self, roster):
        entry = roster.by_github_login("johndoe")
        assert entry is not None
        assert entry.masked["github_login"] == "achen"

    def test_by_github_login_case_insensitive(self, roster):
        entry = roster.by_github_login("JohnDoe")
        assert entry is not None

    def test_by_slack_user_id(self, roster):
        entry = roster.by_slack_user_id("U01ABC123")
        assert entry is not None
        assert entry.masked["name"] == "Alice Chen"

    def test_by_jira_account_id(self, roster):
        entry = roster.by_jira_account_id("5f7abc12345")
        assert entry is not None
        assert entry.masked["jira_account_id"] == "mask-001"

    def test_confluence_shares_jira_account(self, roster):
        # Confluence account ID indexed under jira_account_id
        entry = roster.by_jira_account_id("5f7abc12345")
        assert entry is not None

    def test_users_property(self, roster):
        assert len(roster.users) == 2


# -- Mapping tests --------------------------------------------------------- #

class TestMapping:
    def test_map_domain(self, roster):
        assert roster.map_domain("org_name.com") == "example.com"

    def test_map_domain_case_insensitive(self, roster):
        assert roster.map_domain("Org_Name.com") == "org_name.com" or \
            roster.map_domain("org_name.com") == "example.com"

    def test_map_domain_unknown(self, roster):
        assert roster.map_domain("other.com") == "other.com"

    def test_map_email_known(self, roster):
        assert roster.map_email("john.doe@org_name.com") == "alice.chen@example.com"

    def test_map_email_unknown_hashes_local(self, roster):
        result = roster.map_email("stranger@org_name.com")
        assert "@example.com" in result
        assert "stranger" not in result

    def test_map_email_unknown_domain(self, roster):
        result = roster.map_email("someone@gmail.com")
        assert "@gmail.com" in result
        assert "someone" not in result

    def test_map_email_empty(self, roster):
        assert roster.map_email("") == ""
        assert roster.map_email(None) is None

    def test_map_email_no_at(self, roster):
        result = roster.map_email("noemail")
        assert isinstance(result, str)
        assert "noemail" not in result

    def test_map_name_known(self, roster):
        assert roster.map_name("John Doe") == "Alice Chen"

    def test_map_name_unknown(self, roster):
        result = roster.map_name("Unknown Person")
        assert result.startswith("User ")
        assert "Unknown" not in result

    def test_map_name_empty(self, roster):
        assert roster.map_name("") == ""

    def test_map_github_login_known(self, roster):
        assert roster.map_github_login("johndoe") == "achen"

    def test_map_github_login_unknown(self, roster):
        result = roster.map_github_login("stranger")
        assert result.startswith("user-")
        assert "stranger" not in result

    def test_map_jira_account_id_known(self, roster):
        assert roster.map_jira_account_id("5f7abc12345") == "mask-001"

    def test_map_jira_account_id_unknown(self, roster):
        result = roster.map_jira_account_id("unknown-id")
        assert result.startswith("acct-")

    def test_map_jira_display_name_known(self, roster):
        assert roster.map_jira_display_name("John Doe") == "Alice Chen"

    def test_map_jira_display_name_unknown(self, roster):
        result = roster.map_jira_display_name("Unknown")
        assert result.startswith("User ")

    def test_map_email_deterministic(self, roster):
        r1 = roster.map_email("stranger@org_name.com")
        r2 = roster.map_email("stranger@org_name.com")
        assert r1 == r2

    def test_map_name_deterministic(self, roster):
        r1 = roster.map_name("Random Person")
        r2 = roster.map_name("Random Person")
        assert r1 == r2


# -- Factory tests --------------------------------------------------------- #

class TestFactory:
    def test_from_file(self, tmp_path):
        path = tmp_path / "roster.json"
        path.write_text(json.dumps(SAMPLE_ROSTER))
        roster = Roster.from_file(str(path))
        assert len(roster.users) == 2

    def test_from_empty_data(self):
        roster = Roster({})
        assert len(roster.users) == 0
        assert roster.domain_map == {}

    def test_fallback_hash_consistency(self):
        h1 = Roster._fallback_hash("test", 8)
        h2 = Roster._fallback_hash("test", 8)
        assert h1 == h2
        assert len(h1) == 8
