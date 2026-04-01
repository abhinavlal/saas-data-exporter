"""Tests for scripts.pii_mask.maskers.github — Presidio-first masking."""

import json
import pytest
import boto3
from moto import mock_aws

from lib.s3 import S3Store
from scripts.pii_mask.pii_store import PIIStore
from scripts.pii_mask.scanner import TextScanner
from scripts.pii_mask.maskers.github import GitHubMasker

SRC_BUCKET = "src-bucket"
DST_BUCKET = "dst-bucket"


@pytest.fixture
def store(tmp_path):
    store = PIIStore(str(tmp_path / "test.db"))
    store.add_domain("org_name.com", "example.com")
    # Pre-populate known identities
    store.get_or_create("GITHUB_LOGIN", "johndoe")
    store.get_or_create("EMAIL_ADDRESS", "john.doe@org_name.com")
    store.get_or_create("PERSON", "John Doe")
    return store


@pytest.fixture(scope="module")
def _analyzer():
    from presidio_analyzer import AnalyzerEngine
    return AnalyzerEngine()


@pytest.fixture
def scanner(store, _analyzer):
    s = TextScanner.__new__(TextScanner)
    s._store = store
    s._threshold = 0.5
    s._analyzer = _analyzer
    return s


@pytest.fixture
def masker(scanner):
    return GitHubMasker(scanner)


@pytest.fixture
def s3_env():
    with mock_aws():
        conn = boto3.client("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=SRC_BUCKET)
        conn.create_bucket(Bucket=DST_BUCKET)
        src = S3Store(bucket=SRC_BUCKET)
        dst = S3Store(bucket=DST_BUCKET)
        yield src, dst, conn


class TestPRMasking:
    def test_structured_fields_replaced(self, masker, s3_env, store):
        src, dst, _ = s3_env
        pr = {
            "number": 42,
            "author": "johndoe",
            "author_id": 12345,
            "assignees": [],
            "requested_reviewers": [],
            "title": "Fix login bug",
            "body": "",
            "html_url": "",
            "reviews": [], "review_comments": [], "comments": [],
            "commits": [{
                "sha": "abc123",
                "author_name": "John Doe",
                "author_email": "john.doe@org_name.com",
                "author_login": "johndoe",
                "message": "fix login",
            }],
        }
        src.upload_json(pr, "github/org_name__repo/prs/42.json")
        masker.mask_file(src, dst, "github/org_name__repo/prs/42.json")

        # Find the masked file (key rewritten by domain map)
        dst_keys = dst.list_keys("github/")
        assert len(dst_keys) >= 1
        masked = dst.download_json(dst_keys[0])

        # Structured fields: replaced via store
        assert masked["author"] != "johndoe"
        assert masked["author_id"] == 0
        assert masked["commits"][0]["author_email"] != "john.doe@org_name.com"

    def test_title_without_pii_preserved(self, masker, s3_env):
        src, dst, _ = s3_env
        pr = {
            "number": 1, "author": "", "title": "Add retry logic",
            "body": "", "html_url": "",
            "reviews": [], "review_comments": [], "comments": [], "commits": [],
        }
        src.upload_json(pr, "github/x__repo/prs/1.json")
        masker.mask_file(src, dst, "github/x__repo/prs/1.json")
        masked = dst.download_json("github/x__repo/prs/1.json")
        assert masked["title"] == "Add retry logic"


class TestFileRouting:
    def test_skips_stats(self, masker, s3_env):
        src, dst, _ = s3_env
        src.upload_json({"total": 5}, "github/x__repo/_stats.json")
        result = masker.mask_file(src, dst, "github/x__repo/_stats.json")
        assert result == "ok"

    def test_unknown_type_still_scanned(self, masker, s3_env):
        src, dst, _ = s3_env
        src.upload_json({"data": "test"}, "github/x__repo/unknown.json")
        result = masker.mask_file(src, dst, "github/x__repo/unknown.json")
        assert result == "ok"  # now scans all strings, doesn't skip
