"""Tests for scripts.pii_mask.roster_builder — roster generation."""

import json
import pytest

from scripts.pii_mask.roster_builder import (
    PersonRecord,
    RosterBuildError,
    merge_persons,
    build_roster,
    generate_fake_identity,
    load_google_users_csv,
)


# -- PersonRecord merging ------------------------------------------------- #

class TestMergePersons:
    def test_merges_by_email(self):
        records = [
            PersonRecord(email="john@org.com", github_login="johndoe",
                         sources=["github"]),
            PersonRecord(email="john@org.com", slack_user_id="U01",
                         name="John Doe", sources=["slack"]),
        ]
        merged = merge_persons(records)
        # Should produce one person with both GitHub and Slack fields
        assert len(merged) == 1
        p = merged[0]
        assert p.email == "john@org.com"
        assert p.github_login == "johndoe"
        assert p.slack_user_id == "U01"
        assert p.name == "John Doe"
        assert "github" in p.sources
        assert "slack" in p.sources

    def test_keeps_separate_if_different_email(self):
        records = [
            PersonRecord(email="john@org.com", sources=["github"]),
            PersonRecord(email="jane@org.com", sources=["slack"]),
        ]
        merged = merge_persons(records)
        assert len(merged) == 2

    def test_case_insensitive_email(self):
        records = [
            PersonRecord(email="John@Org.com", name="John",
                         sources=["github"]),
            PersonRecord(email="john@org.com", slack_user_id="U01",
                         sources=["slack"]),
        ]
        merged = merge_persons(records)
        assert len(merged) == 1
        assert merged[0].slack_user_id == "U01"

    def test_no_email_kept_separate(self):
        records = [
            PersonRecord(github_login="mystery", sources=["github"]),
            PersonRecord(github_login="ghost", sources=["github"]),
        ]
        merged = merge_persons(records)
        assert len(merged) == 2

    def test_fuzzy_match_github_login_to_email_local(self):
        """GitHub user with no email matched to Jira user by login."""
        records = [
            PersonRecord(github_login="anvitakamble", name="anvita.kamble",
                         sources=["github"]),
            PersonRecord(email="anvita.kamble@org.com", name="Anvita Kamble",
                         jira_account_id="jira-1", sources=["jira"]),
        ]
        merged = merge_persons(records)
        assert len(merged) == 1
        p = merged[0]
        assert p.github_login == "anvitakamble"
        assert p.jira_account_id == "jira-1"
        assert p.email == "anvita.kamble@org.com"

    def test_fuzzy_match_by_name(self):
        """GitHub user with name matched to Slack user by exact name."""
        records = [
            PersonRecord(github_login="jdoe", name="John Doe",
                         sources=["github"]),
            PersonRecord(email="john.doe@org.com", name="John Doe",
                         slack_user_id="U01", sources=["slack"]),
        ]
        merged = merge_persons(records)
        assert len(merged) == 1
        assert merged[0].github_login == "jdoe"
        assert merged[0].slack_user_id == "U01"

    def test_three_way_merge(self):
        records = [
            PersonRecord(email="a@org.com", github_login="auser",
                         sources=["github"]),
            PersonRecord(email="a@org.com", jira_account_id="jira-1",
                         jira_display_name="A User", sources=["jira"]),
            PersonRecord(email="a@org.com", slack_user_id="U99",
                         sources=["slack"]),
        ]
        merged = merge_persons(records)
        assert len(merged) == 1
        p = merged[0]
        assert p.github_login == "auser"
        assert p.jira_account_id == "jira-1"
        assert p.slack_user_id == "U99"


# -- Fake identity generation --------------------------------------------- #

class TestFakeIdentity:
    def test_generates_realistic_email(self):
        person = PersonRecord(email="john@org.com", name="John Doe")
        domain_map = {"org.com": "example.com"}
        masked = generate_fake_identity(person, domain_map)
        assert "@example.com" in masked["email"]
        assert masked["name"]
        assert masked["first_name"]
        assert masked["last_name"]

    def test_maps_domain(self):
        person = PersonRecord(email="x@real.co", name="X")
        domain_map = {"real.co": "fake.co"}
        masked = generate_fake_identity(person, domain_map)
        assert "@fake.co" in masked["email"]

    def test_github_login_generated(self):
        person = PersonRecord(email="a@b.com", github_login="auser")
        masked = generate_fake_identity(person, {})
        assert "github_login" in masked
        assert masked["github_login"] != "auser"

    def test_jira_account_id_generated(self):
        person = PersonRecord(email="a@b.com",
                              jira_account_id="real-id")
        masked = generate_fake_identity(person, {})
        assert masked["jira_account_id"].startswith("mask-")
        assert masked["jira_account_id"] != "real-id"

    def test_slack_id_preserved(self):
        person = PersonRecord(email="a@b.com",
                              slack_user_id="U01ABC")
        masked = generate_fake_identity(person, {})
        assert masked["slack_user_id"] == "U01ABC"  # IDs not PII


# -- Roster building ------------------------------------------------------ #

class TestBuildRoster:
    def test_builds_roster_from_persons(self):
        persons = [
            PersonRecord(email="john@org.com", name="John Doe",
                         first_name="John", last_name="Doe",
                         github_login="johndoe"),
            PersonRecord(email="jane@org.com", name="Jane Smith",
                         first_name="Jane", last_name="Smith"),
        ]
        domain_map = {"org.com": "example.com"}
        roster = build_roster(persons, domain_map)

        assert roster["version"] == 1
        assert roster["domain_map"] == domain_map
        assert len(roster["users"]) == 2

        u1 = roster["users"][0]
        assert u1["real"]["email"] == "john@org.com"
        assert u1["real"]["github_login"] == "johndoe"
        assert u1["masked"]["email"].endswith("@example.com")
        assert "github_login" in u1["masked"]

    def test_incremental_preserves_existing(self):
        persons = [
            PersonRecord(email="john@org.com", name="John Doe",
                         first_name="John", last_name="Doe"),
            PersonRecord(email="new@org.com", name="New Person",
                         first_name="New", last_name="Person"),
        ]
        existing = {
            "version": 1,
            "domain_map": {"org.com": "example.com"},
            "users": [{
                "id": "user-0001",
                "real": {"email": "john@org.com", "name": "John Doe"},
                "masked": {"email": "alice@example.com",
                           "name": "Alice Chen"},
            }],
        }
        roster = build_roster(persons, {"org.com": "example.com"},
                              existing)

        assert len(roster["users"]) == 2
        # Existing user preserved
        john = next(u for u in roster["users"]
                    if u["real"]["email"] == "john@org.com")
        assert john["masked"]["email"] == "alice@example.com"
        # New user gets a new fake
        new = next(u for u in roster["users"]
                   if u["real"]["email"] == "new@org.com")
        assert new["masked"]["email"].endswith("@example.com")
        assert new["masked"]["email"] != "alice@example.com"

    def test_roundtrip_json(self, tmp_path):
        persons = [
            PersonRecord(email="a@b.com", name="A B",
                         first_name="A", last_name="B"),
        ]
        roster = build_roster(persons, {"b.com": "fake.com"})
        path = tmp_path / "roster.json"
        with open(path, "w") as f:
            json.dump(roster, f)
        with open(path) as f:
            loaded = json.load(f)
        assert len(loaded["users"]) == 1
        assert loaded["users"][0]["real"]["email"] == "a@b.com"


# -- Google CSV loading --------------------------------------------------- #

class TestGoogleCSV:
    def test_loads_users_from_csv(self, tmp_path):
        csv_path = tmp_path / "users.csv"
        csv_path.write_text("user\njohn.doe@org.com\njane.smith@org.com\n")
        users = load_google_users_csv(str(csv_path))
        assert len(users) == 2
        assert users[0].email == "john.doe@org.com"
        assert users[0].first_name == "John"
        assert users[0].last_name == "Doe"
        assert users[0].name == "John Doe"

    def test_skips_empty_rows(self, tmp_path):
        csv_path = tmp_path / "users.csv"
        csv_path.write_text("user\njohn@org.com\n\n\n")
        users = load_google_users_csv(str(csv_path))
        assert len(users) == 1

    def test_handles_email_column(self, tmp_path):
        csv_path = tmp_path / "users.csv"
        csv_path.write_text("email\nbob@org.com\n")
        users = load_google_users_csv(str(csv_path))
        assert len(users) == 1
        assert users[0].email == "bob@org.com"


# -- Fail-fast behavior --------------------------------------------------- #

class TestFailFast:
    def test_csv_file_not_found_raises(self):
        with pytest.raises(RosterBuildError, match="file not found"):
            load_google_users_csv("/nonexistent/path.csv")

    def test_empty_csv_raises(self, tmp_path):
        csv_path = tmp_path / "empty.csv"
        csv_path.write_text("user\n")
        with pytest.raises(RosterBuildError, match="0 users loaded"):
            load_google_users_csv(str(csv_path))
