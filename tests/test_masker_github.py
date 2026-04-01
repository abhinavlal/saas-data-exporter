"""Tests for scripts.pii_mask.maskers.github — GitHub roster-based masking."""

import pytest
import boto3
from moto import mock_aws

from lib.s3 import S3Store
from lib.checkpoint import CheckpointManager
from scripts.pii_mask.roster import Roster
from scripts.pii_mask.scanner import TextScanner
from scripts.pii_mask.maskers.github import GitHubMasker

SRC_BUCKET = "src-bucket"
DST_BUCKET = "dst-bucket"

SAMPLE_ROSTER = {
    "version": 1,
    "domain_map": {
        "org_name.com": "example.com",
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
            },
            "masked": {
                "email": "alice.chen@example.com",
                "name": "Alice Chen",
                "first_name": "Alice",
                "last_name": "Chen",
                "github_login": "achen",
            },
        },
        {
            "id": "user-002",
            "real": {
                "email": "amit.kumar@org_name.com",
                "name": "Amit Kumar",
                "first_name": "Amit",
                "last_name": "Kumar",
                "github_login": "amitkumar",
            },
            "masked": {
                "email": "carol.li@example.com",
                "name": "Carol Li",
                "first_name": "Carol",
                "last_name": "Kumar",
                "github_login": "cli42",
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


@pytest.fixture
def masker(roster, scanner):
    return GitHubMasker(roster, scanner)


@pytest.fixture
def s3_env():
    with mock_aws():
        conn = boto3.client("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=SRC_BUCKET)
        conn.create_bucket(Bucket=DST_BUCKET)
        src = S3Store(bucket=SRC_BUCKET)
        dst = S3Store(bucket=DST_BUCKET)
        yield src, dst, conn


# -- PR masking ------------------------------------------------------------ #

class TestPRMasking:
    def test_structured_fields_use_roster(self, masker, s3_env):
        src, dst, _ = s3_env
        pr = {
            "number": 42,
            "author": "johndoe",
            "author_id": 12345,
            "assignees": ["amitkumar"],
            "requested_reviewers": ["johndoe"],
            "title": "Fix login bug",
            "body": "This fixes the login for John Doe",
            "html_url": "https://github.com/org_name/repo/pull/42",
            "reviews": [{"reviewer": "amitkumar", "body": "LGTM"}],
            "review_comments": [],
            "comments": [],
            "commits": [{
                "sha": "abc123",
                "author_name": "John Doe",
                "author_email": "john.doe@org_name.com",
                "author_login": "johndoe",
                "message": "fix login for John Doe",
            }],
        }
        src.upload_json(pr, "github/org_name__repo/prs/42.json")
        result = masker.mask_file(src, dst, "github/org_name__repo/prs/42.json")
        assert result == "ok"

        masked = dst.download_json("github/example__repo/prs/42.json")
        assert masked is not None

        # Structured fields: roster lookup
        assert masked["author"] == "achen"
        assert masked["author_id"] == 0
        assert masked["assignees"] == ["cli42"]
        assert masked["requested_reviewers"] == ["achen"]

        # Freeform text: scanned, not destroyed
        assert "John Doe" not in masked["body"]
        assert "Alice Chen" in masked["body"]
        assert "fixes the login" in masked["body"]  # non-PII preserved

        # Commit: roster lookup + scanned message
        commit = masked["commits"][0]
        assert commit["author_name"] == "Alice Chen"
        assert commit["author_email"] == "alice.chen@example.com"
        assert commit["author_login"] == "achen"
        assert "John Doe" not in commit["message"]

    def test_title_preserved_readable(self, masker, s3_env):
        src, dst, _ = s3_env
        pr = {
            "number": 1,
            "author": "",
            "title": "Add retry logic for API calls",
            "body": "",
            "html_url": "",
            "reviews": [], "review_comments": [], "comments": [], "commits": [],
        }
        src.upload_json(pr, "github/org_name__repo/prs/1.json")
        masker.mask_file(src, dst, "github/org_name__repo/prs/1.json")
        masked = dst.download_json("github/example__repo/prs/1.json")
        # No PII in title → should be unchanged
        assert masked["title"] == "Add retry logic for API calls"


# -- Contributors ---------------------------------------------------------- #

class TestContributors:
    def test_masks_contributor_logins(self, masker, s3_env):
        src, dst, _ = s3_env
        contributors = [
            {"login": "johndoe", "id": 123,
             "profile_url": "https://github.com/johndoe"},
            {"login": "amitkumar", "id": 456,
             "profile_url": "https://github.com/amitkumar"},
        ]
        src.upload_json(contributors,
                        "github/org_name__repo/contributors.json")
        masker.mask_file(src, dst, "github/org_name__repo/contributors.json")
        masked = dst.download_json("github/example__repo/contributors.json")
        assert masked[0]["login"] == "achen"
        assert masked[0]["id"] == 0
        assert "achen" in masked[0]["profile_url"]
        assert masked[1]["login"] == "cli42"


# -- Repo metadata -------------------------------------------------------- #

class TestRepoMetadata:
    def test_masks_org_in_full_name(self, masker, s3_env):
        src, dst, _ = s3_env
        meta = {"full_name": "org_name/repo", "description": "A test repo"}
        src.upload_json(meta, "github/org_name__repo/repo_metadata.json")
        masker.mask_file(src, dst, "github/org_name__repo/repo_metadata.json")
        masked = dst.download_json("github/example__repo/repo_metadata.json")
        assert "org_name" not in masked["full_name"]


# -- Key rewriting --------------------------------------------------------- #

class TestKeyRewriting:
    def test_rewrites_org_in_key(self, masker):
        assert masker.rewrite_key("github/org_name__repo/prs/1.json") == \
            "github/example__repo/prs/1.json"

    def test_preserves_non_org_keys(self, masker):
        assert masker.rewrite_key("github/other__repo/prs/1.json") == \
            "github/other__repo/prs/1.json"


# -- File routing ---------------------------------------------------------- #

class TestFileRouting:
    def test_skips_stats(self, masker, s3_env):
        src, dst, _ = s3_env
        src.upload_json({"total": 5}, "github/org_name__repo/_stats.json")
        result = masker.mask_file(src, dst, "github/org_name__repo/_stats.json")
        assert result == "ok"

    def test_skips_unknown_type(self, masker, s3_env):
        src, dst, _ = s3_env
        src.upload_json({}, "github/org_name__repo/unknown.json")
        result = masker.mask_file(src, dst, "github/org_name__repo/unknown.json")
        assert result == "skipped (unknown type)"

    def test_list_keys(self, masker, s3_env):
        src, dst, _ = s3_env
        src.upload_json({}, "github/org_name__repo/prs/1.json")
        src.upload_bytes(b"binary", "github/org_name__repo/file.bin")
        keys = masker.list_keys(src)
        assert "github/org_name__repo/prs/1.json" in keys
        assert "github/org_name__repo/file.bin" not in keys


# -- Cross-service consistency --------------------------------------------- #

class TestCrossServiceConsistency:
    def test_same_person_same_output(self, masker, s3_env):
        """Verify the same person maps to the same fake identity."""
        src, dst, _ = s3_env
        pr1 = {
            "number": 1, "author": "johndoe", "title": "PR by John Doe",
            "body": "john.doe@org_name.com",
            "html_url": "", "reviews": [], "review_comments": [],
            "comments": [], "commits": [],
        }
        pr2 = {
            "number": 2, "author": "johndoe", "title": "Another by John Doe",
            "body": "Contact john.doe@org_name.com",
            "html_url": "", "reviews": [], "review_comments": [],
            "comments": [], "commits": [],
        }
        src.upload_json(pr1, "github/org_name__repo/prs/1.json")
        src.upload_json(pr2, "github/org_name__repo/prs/2.json")
        masker.mask_file(src, dst, "github/org_name__repo/prs/1.json")
        masker.mask_file(src, dst, "github/org_name__repo/prs/2.json")

        m1 = dst.download_json("github/example__repo/prs/1.json")
        m2 = dst.download_json("github/example__repo/prs/2.json")
        # Same login → same masked login
        assert m1["author"] == m2["author"] == "achen"
        # Same email in body → same replacement
        assert "alice.chen@example.com" in m1["body"]
        assert "alice.chen@example.com" in m2["body"]
