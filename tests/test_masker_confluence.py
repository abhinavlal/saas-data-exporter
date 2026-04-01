"""Tests for scripts.pii_mask.maskers.confluence — Presidio-first masking."""

import pytest
import boto3
from moto import mock_aws

from lib.s3 import S3Store
from scripts.pii_mask.pii_store import PIIStore
from scripts.pii_mask.scanner import TextScanner
from scripts.pii_mask.maskers.confluence import ConfluenceMasker

SRC_BUCKET = "src-bucket"
DST_BUCKET = "dst-bucket"


@pytest.fixture
def store(tmp_path):
    s = PIIStore(str(tmp_path / "test.db"))
    s.add_domain("org_name.com", "example.com")
    s.get_or_create("EMAIL_ADDRESS", "john@org_name.com")
    s.get_or_create("PERSON", "John Doe")
    s.get_or_create("JIRA_ACCOUNT_ID", "acct-123")
    return s


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
    return ConfluenceMasker(scanner)


@pytest.fixture
def s3_env():
    with mock_aws():
        conn = boto3.client("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=SRC_BUCKET)
        conn.create_bucket(Bucket=DST_BUCKET)
        yield S3Store(bucket=SRC_BUCKET), S3Store(bucket=DST_BUCKET), conn


class TestPageMasking:
    def test_page_fields_masked(self, masker, store, s3_env):
        src, dst, _ = s3_env
        page = {
            "id": "1", "title": "Architecture by John Doe",
            "author_id": "acct-123",
            "body": "<p>Overview written by John Doe</p>",
            "comments": [{"author_id": "acct-123", "body": "Updated"}],
        }
        src.upload_json(page, "confluence/TEAM/pages/1.json")
        masker.mask_file(src, dst, "confluence/TEAM/pages/1.json")
        p = dst.download_json("confluence/TEAM/pages/1.json")

        assert p["author_id"] != "acct-123"
        assert "John Doe" not in p["title"]
        assert "Architecture by" in p["title"]
        assert "John Doe" not in p["body"]
        assert p["comments"][0]["author_id"] != "acct-123"

    def test_skips_attachments(self, masker):
        assert not masker.should_process("confluence/TEAM/attachments/1.json")

    def test_passes_through_stats(self, masker, s3_env):
        src, dst, _ = s3_env
        src.upload_json({"total": 5}, "confluence/TEAM/_stats.json")
        result = masker.mask_file(src, dst, "confluence/TEAM/_stats.json")
        assert result == "ok"
