"""Tests for scripts.pii_mask.maskers.confluence."""

import pytest
import boto3
from moto import mock_aws

from lib.s3 import S3Store
from scripts.pii_mask.roster import Roster
from scripts.pii_mask.scanner import TextScanner
from scripts.pii_mask.maskers.confluence import ConfluenceMasker

SRC_BUCKET = "src-bucket"
DST_BUCKET = "dst-bucket"

SAMPLE_ROSTER = {
    "version": 1,
    "domain_map": {"org_name.com": "example.com"},
    "users": [{
        "id": "user-001",
        "real": {"email": "john@org_name.com", "name": "John Doe",
                 "first_name": "John", "last_name": "Doe",
                 "jira_account_id": "acct-123"},
        "masked": {"email": "alice@example.com", "name": "Alice Chen",
                   "jira_account_id": "mask-001",
                   "jira_display_name": "Alice Chen"},
    }],
}


@pytest.fixture
def s3_env():
    with mock_aws():
        conn = boto3.client("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=SRC_BUCKET)
        conn.create_bucket(Bucket=DST_BUCKET)
        yield S3Store(bucket=SRC_BUCKET), S3Store(bucket=DST_BUCKET), conn


@pytest.fixture
def masker():
    roster = Roster(SAMPLE_ROSTER)
    return ConfluenceMasker(roster, TextScanner(roster))


class TestPageMasking:
    def test_page_fields_masked(self, masker, s3_env):
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

        assert p["author_id"] == "mask-001"
        assert "John Doe" not in p["title"]
        assert "Alice Chen" in p["title"]
        assert "Architecture by" in p["title"]
        assert "John Doe" not in p["body"]
        assert p["comments"][0]["author_id"] == "mask-001"

    def test_skips_attachments(self, masker):
        assert not masker.should_process("confluence/TEAM/attachments/1.json")

    def test_passes_through_stats(self, masker, s3_env):
        src, dst, _ = s3_env
        src.upload_json({"total": 5}, "confluence/TEAM/_stats.json")
        result = masker.mask_file(src, dst, "confluence/TEAM/_stats.json")
        assert result == "ok"
