"""Tests for scripts.pii_mask_github — GitHub JSON PII masking."""

import json

import boto3
import pytest
from moto import mock_aws

from lib.s3 import S3Store
from scripts.pii_mask_github import (
    _hash, _hash_email, _hash_login, _hash_name, _hash_url,
    _hash_text, _mask_body,
    mask_pr, mask_contributors, mask_repo_metadata,
    mask_github_exports,
)

SRC_BUCKET = "src-bucket"
DST_BUCKET = "dst-bucket"


# ── Fixtures ──────────────────────────────────────────────────────────────

@pytest.fixture
def s3_env():
    with mock_aws():
        conn = boto3.client("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=SRC_BUCKET)
        conn.create_bucket(Bucket=DST_BUCKET)
        src = S3Store(bucket=SRC_BUCKET)
        dst = S3Store(bucket=DST_BUCKET)
        yield src, dst, conn


# ── Hash helpers ──────────────────────────────────────────────────────────

class TestHashHelpers:
    def test_hash_is_deterministic(self):
        assert _hash("foo") == _hash("foo")
        assert _hash("foo") != _hash("bar")

    def test_hash_email_preserves_structure(self):
        result = _hash_email("john@practo.com")
        assert "@" in result
        assert "example-health.com" in result
        assert "john" not in result

    def test_hash_email_deterministic(self):
        assert _hash_email("a@b.com") == _hash_email("a@b.com")

    def test_hash_email_handles_none(self):
        assert _hash_email("") == ""
        assert _hash_email(None) is None

    def test_hash_login(self):
        result = _hash_login("amitchhajer")
        assert result.startswith("user-")
        assert "amitchhajer" not in result

    def test_hash_login_deterministic(self):
        assert _hash_login("foo") == _hash_login("foo")

    def test_hash_name(self):
        result = _hash_name("Amit Chhajer")
        assert result.startswith("User ")
        assert "Amit" not in result

    def test_hash_url(self):
        result = _hash_url("https://github.com/amitchhajer")
        assert "github.com" in result
        assert "amitchhajer" not in result
        assert result.startswith("https://github.com/user-")

    def test_hash_url_non_github(self):
        assert _hash_url("https://example.com/foo") == "https://example.com/foo"

    def test_hash_preserves_none(self):
        assert _hash_login(None) is None
        assert _hash_login("") == ""
        assert _hash_name(None) is None
        assert _hash_url(None) is None
        assert _hash_url("") == ""


# ── Freeform body masking ─────────────────────────────────────────────────

class TestMaskBody:
    def test_masks_at_mentions(self):
        result = _mask_body("cc @johndoe @janedoe please review")
        assert "@johndoe" not in result
        assert "@janedoe" not in result
        assert "cc @user-" in result

    def test_masks_emails_in_text(self):
        result = _mask_body("contact john@practo.com for details")
        assert "john@practo.com" not in result
        assert "@example-health.com" in result

    def test_preserves_none_and_empty(self):
        assert _mask_body(None) is None
        assert _mask_body("") == ""

    def test_leaves_non_pii_text(self):
        assert _mask_body("fixed the bug in parser") == "fixed the bug in parser"


# ── PR masking ────────────────────────────────────────────────────────────

class TestMaskPR:
    def _sample_pr(self):
        return {
            "number": 42,
            "title": "Fix bug reported by @johndoe",
            "state": "merged",
            "author": "amitchhajer",
            "author_id": 427888,
            "body": "See john@practo.com for context",
            "assignees": ["alice", "bob"],
            "requested_reviewers": ["charlie"],
            "html_url": "https://github.com/practo/repo/pull/42",
            "labels": ["bugfix"],
            "reviews": [
                {"reviewer": "alice", "body": "LGTM @amitchhajer",
                 "state": "APPROVED"},
            ],
            "review_comments": [
                {"author": "bob", "body": "nit: rename this var"},
            ],
            "comments": [
                {"author": "charlie", "body": "ping @alice"},
            ],
            "commits": [
                {"sha": "abc123", "message": "fix for @johndoe",
                 "author_name": "Amit Chhajer",
                 "author_email": "amit@practo.com",
                 "author_login": "amitchhajer",
                 "date": "2024-01-01T00:00:00Z"},
            ],
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-01T00:00:00Z",
        }

    def test_masks_author_fields(self):
        pr = mask_pr(self._sample_pr())
        assert "amitchhajer" not in pr["author"]
        assert pr["author_id"] == 0
        assert pr["author"].startswith("user-")

    def test_masks_assignees_and_reviewers(self):
        pr = mask_pr(self._sample_pr())
        assert all(a.startswith("user-") for a in pr["assignees"])
        assert "alice" not in str(pr["assignees"])
        assert all(r.startswith("user-") for r in pr["requested_reviewers"])

    def test_hashes_title_and_body(self):
        pr = mask_pr(self._sample_pr())
        # Title and body are fully hashed (not just @mention replacement)
        assert pr["title"] != "Fix bug reported by @johndoe"
        assert pr["body"] != "See john@practo.com for context"
        assert len(pr["title"]) == 24  # hash length
        assert len(pr["body"]) == 24

    def test_hashes_review_body(self):
        pr = mask_pr(self._sample_pr())
        review = pr["reviews"][0]
        assert "alice" not in review["reviewer"]
        assert review["body"] != "LGTM @amitchhajer"
        assert len(review["body"]) == 24

    def test_hashes_review_comment_body(self):
        pr = mask_pr(self._sample_pr())
        rc = pr["review_comments"][0]
        assert "bob" not in rc["author"]
        assert rc["body"] != "nit: rename this var"
        assert len(rc["body"]) == 24

    def test_hashes_comment_body(self):
        pr = mask_pr(self._sample_pr())
        c = pr["comments"][0]
        assert "charlie" not in c["author"]
        assert c["body"] != "ping @alice"
        assert len(c["body"]) == 24

    def test_hashes_commit_message(self):
        pr = mask_pr(self._sample_pr())
        commit = pr["commits"][0]
        assert "Amit" not in commit["author_name"]
        assert "amit@practo.com" not in commit["author_email"]
        assert "amitchhajer" not in commit["author_login"]
        assert commit["message"] != "fix for @johndoe"
        assert len(commit["message"]) == 24

    def test_preserves_non_pii_fields(self):
        pr = mask_pr(self._sample_pr())
        assert pr["number"] == 42
        assert pr["state"] == "merged"
        assert pr["labels"] == ["bugfix"]
        assert pr["commits"][0]["sha"] == "abc123"
        assert pr["commits"][0]["date"] == "2024-01-01T00:00:00Z"

    def test_deterministic(self):
        pr1 = mask_pr(self._sample_pr())
        pr2 = mask_pr(self._sample_pr())
        assert pr1["author"] == pr2["author"]
        assert pr1["commits"][0]["author_email"] == \
            pr2["commits"][0]["author_email"]


# ── Contributors masking ──────────────────────────────────────────────────

class TestMaskContributors:
    def test_masks_all_fields(self):
        contribs = mask_contributors([
            {"login": "alice", "id": 123, "type": "User",
             "contributions": 50,
             "profile_url": "https://github.com/alice"},
        ])
        c = contribs[0]
        assert "alice" not in c["login"]
        assert c["id"] == 0
        assert "alice" not in c["profile_url"]
        assert c["type"] == "User"
        assert c["contributions"] == 50


# ── Repo metadata masking ─────────────────────────────────────────────────

class TestMaskRepoMetadata:
    def test_masks_org_in_full_name(self):
        meta = mask_repo_metadata({
            "full_name": "practo/MyRepo",
            "description": "Contact admin for help",
        })
        assert "practo" not in meta["full_name"]
        assert "/MyRepo" in meta["full_name"]
        # description is fully hashed
        assert meta["description"] != "Contact admin for help"
        assert len(meta["description"]) == 24


# ── Integration: full pipeline ────────────────────────────────────────────

class TestPipelineEndToEnd:
    def test_masks_all_file_types(self, s3_env):
        src, dst, conn = s3_env

        # Upload sample data
        src.upload_json({
            "number": 1, "title": "test", "state": "open",
            "author": "alice", "author_id": 1, "body": "",
            "assignees": [], "requested_reviewers": [],
            "html_url": "https://github.com/practo/repo/pull/1",
            "labels": [], "reviews": [], "review_comments": [],
            "comments": [], "commits": [
                {"sha": "aaa", "message": "init",
                 "author_name": "Alice", "author_email": "alice@practo.com",
                 "author_login": "alice", "date": "2024-01-01"},
            ],
        }, "github/practo__repo/prs/1.json")

        src.upload_json([
            {"login": "alice", "id": 1, "type": "User",
             "contributions": 10,
             "profile_url": "https://github.com/alice"},
        ], "github/practo__repo/contributors.json")

        src.upload_json({
            "full_name": "practo/repo", "description": "test repo",
        }, "github/practo__repo/repo_metadata.json")

        src.upload_json({"total": 1}, "github/practo__repo/_stats.json")

        # Run
        from lib.checkpoint import CheckpointManager
        cp = CheckpointManager(dst, "pii_mask/github")
        cp.load()
        mask_github_exports(src=src, dst=dst, checkpoint=cp, max_workers=1)

        # Verify PR
        pr = dst.download_json("github/practo__repo/prs/1.json")
        assert "alice" not in pr["author"]
        assert "alice@practo.com" not in pr["commits"][0]["author_email"]

        # Verify contributors
        contribs = dst.download_json("github/practo__repo/contributors.json")
        assert "alice" not in contribs[0]["login"]
        assert contribs[0]["id"] == 0

        # Verify metadata
        meta = dst.download_json("github/practo__repo/repo_metadata.json")
        assert "practo" not in meta["full_name"]

        # Verify stats copied unchanged
        stats = dst.download_json("github/practo__repo/_stats.json")
        assert stats == {"total": 1}

    def test_checkpoint_resume(self, s3_env):
        src, dst, conn = s3_env

        src.upload_json(
            {"number": 1, "title": "t", "state": "open", "author": "a",
             "author_id": 1, "body": "", "assignees": [],
             "requested_reviewers": [], "html_url": "", "labels": [],
             "reviews": [], "review_comments": [], "comments": [],
             "commits": []},
            "github/practo__r/prs/1.json",
        )
        src.upload_json(
            {"number": 2, "title": "t", "state": "open", "author": "bob",
             "author_id": 2, "body": "", "assignees": [],
             "requested_reviewers": [], "html_url": "", "labels": [],
             "reviews": [], "review_comments": [], "comments": [],
             "commits": []},
            "github/practo__r/prs/2.json",
        )

        # Pre-populate: PR 1 already done
        from lib.checkpoint import CheckpointManager
        cp = CheckpointManager(dst, "pii_mask/github")
        cp.load()
        cp.start_phase("mask", total=2)
        cp.mark_item_done("mask", "github/practo__r/prs/1.json")
        cp.save(force=True)
        # Write PR 1 to dst as-is (simulating prior run)
        dst.upload_json({"number": 1, "author": "a"}, "github/practo__r/prs/1.json")

        # Run
        cp2 = CheckpointManager(dst, "pii_mask/github")
        cp2.load()
        mask_github_exports(src=src, dst=dst, checkpoint=cp2, max_workers=1)

        # PR 1 should be untouched (author still "a")
        pr1 = dst.download_json("github/practo__r/prs/1.json")
        assert pr1["author"] == "a"

        # PR 2 should be masked
        pr2 = dst.download_json("github/practo__r/prs/2.json")
        assert "bob" not in pr2["author"]
