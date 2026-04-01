"""Tests for scripts.pii_mask_confluence — Confluence JSON PII masking."""

import boto3
import pytest
from moto import mock_aws

from lib.s3 import S3Store
from scripts.deprecated.pii_mask_confluence import (
    _hash_account_id, _hash_text,
    mask_page, mask_confluence_exports,
)

SRC_BUCKET = "src-bucket"
DST_BUCKET = "dst-bucket"


@pytest.fixture
def s3_env():
    with mock_aws():
        conn = boto3.client("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=SRC_BUCKET)
        conn.create_bucket(Bucket=DST_BUCKET)
        src = S3Store(bucket=SRC_BUCKET)
        dst = S3Store(bucket=DST_BUCKET)
        yield src, dst, conn


class TestHashHelpers:
    def test_hash_account_id(self):
        result = _hash_account_id("557058:06e09af3-f47a-4dd3")
        assert result.startswith("acct-")
        assert "557058" not in result

    def test_hash_text(self):
        result = _hash_text("<p>Some HTML content</p>")
        assert len(result) == 24
        assert "<p>" not in result

    def test_deterministic(self):
        assert _hash_account_id("x") == _hash_account_id("x")
        assert _hash_text("y") == _hash_text("y")

    def test_handles_empty(self):
        assert _hash_account_id("") == ""
        assert _hash_text("") == ""
        assert _hash_text(None) is None


class TestMaskPage:
    def _sample_page(self):
        return {
            "id": "55968652",
            "title": "Segment - Diagnostic Home",
            "space_key": "DIAGNOSTIC",
            "space_id": "56426499",
            "status": "current",
            "created_at": "2015-10-06T05:46:12.592Z",
            "author_id": "557058:06e09af3-f47a-4dd3",
            "parent_id": None,
            "version": 1,
            "body_format": "storage",
            "body": "<p>Page content with <a href='https://org_name.com'>link</a></p>",
            "comments": [
                {"id": "13697090", "author_id": "557058:abc123",
                 "created_at": "2020-01-01", "version": 1,
                 "body": "<p>Comment text here</p>"},
                {"id": "13697091", "author_id": None,
                 "created_at": None, "version": 1,
                 "body": "<p>Anonymous comment</p>"},
            ],
        }

    def test_hashes_author_id(self):
        p = mask_page(self._sample_page())
        assert "557058" not in p["author_id"]
        assert p["author_id"].startswith("acct-")

    def test_hashes_title(self):
        p = mask_page(self._sample_page())
        assert p["title"] != "Segment - Diagnostic Home"
        assert len(p["title"]) == 24

    def test_hashes_body(self):
        p = mask_page(self._sample_page())
        assert "<p>" not in p["body"]
        assert len(p["body"]) == 24

    def test_hashes_comments(self):
        p = mask_page(self._sample_page())
        c = p["comments"][0]
        assert "557058" not in c["author_id"]
        assert c["author_id"].startswith("acct-")
        assert c["body"] != "<p>Comment text here</p>"
        assert len(c["body"]) == 24

    def test_handles_null_author_in_comment(self):
        p = mask_page(self._sample_page())
        c = p["comments"][1]
        assert c["author_id"] is None  # stays null

    def test_preserves_non_pii(self):
        p = mask_page(self._sample_page())
        assert p["id"] == "55968652"
        assert p["space_key"] == "DIAGNOSTIC"
        assert p["status"] == "current"
        assert p["version"] == 1

    def test_deterministic(self):
        p1 = mask_page(self._sample_page())
        p2 = mask_page(self._sample_page())
        assert p1["title"] == p2["title"]
        assert p1["author_id"] == p2["author_id"]


class TestPipelineEndToEnd:
    def test_masks_pages_and_skips_attachments(self, s3_env):
        src, dst, conn = s3_env

        src.upload_json({
            "id": "1", "title": "Org_Name Architecture",
            "space_key": "ENG", "space_id": "100",
            "status": "current", "created_at": "2024-01-01",
            "author_id": "user-abc", "parent_id": None,
            "version": 1, "body_format": "storage",
            "body": "<p>Org_Name service overview</p>",
            "comments": [],
        }, "confluence/ENG/pages/1.json")

        src.upload_json([{"id": "1"}],
                        "confluence/ENG/pages/_index.json")

        src.upload_json({"total": 1},
                        "confluence/ENG/_stats.json")

        # Attachment — should be skipped
        src.upload_json({"data": "binary"},
                        "confluence/ENG/attachments/1/file.json")

        from lib.checkpoint import CheckpointManager
        cp = CheckpointManager(dst, "pii_mask/confluence")
        cp.load()
        mask_confluence_exports(src=src, dst=dst, checkpoint=cp,
                                max_workers=1)

        # Page masked
        p = dst.download_json("confluence/ENG/pages/1.json")
        assert p is not None
        assert p["title"] != "Org_Name Architecture"
        assert p["author_id"].startswith("acct-")
        # Org name replaced in remaining strings
        assert "org_name" not in str(p).lower()

        # Stats and index copied
        assert dst.download_json("confluence/ENG/_stats.json") == {"total": 1}
        assert dst.download_json("confluence/ENG/pages/_index.json") == [{"id": "1"}]

        # Attachment NOT copied
        assert dst.download_json("confluence/ENG/attachments/1/file.json") is None

    def test_checkpoint_resume(self, s3_env):
        src, dst, conn = s3_env

        for i in (1, 2):
            src.upload_json({
                "id": str(i), "title": f"Page {i}",
                "space_key": "S", "space_id": "1",
                "status": "current", "created_at": "2024-01-01",
                "author_id": f"u{i}", "parent_id": None,
                "version": 1, "body_format": "storage",
                "body": f"<p>Body {i}</p>", "comments": [],
            }, f"confluence/S/pages/{i}.json")

        from lib.checkpoint import CheckpointManager
        cp = CheckpointManager(dst, "pii_mask/confluence")
        cp.load()
        cp.start_phase("mask", total=2)
        cp.mark_item_done("mask", "confluence/S/pages/1.json")
        cp.save(force=True)
        dst.upload_json({"id": "1", "title": "original"},
                        "confluence/S/pages/1.json")

        cp2 = CheckpointManager(dst, "pii_mask/confluence")
        cp2.load()
        mask_confluence_exports(src=src, dst=dst, checkpoint=cp2,
                                max_workers=1)

        # Page 1 untouched
        p1 = dst.download_json("confluence/S/pages/1.json")
        assert p1["title"] == "original"

        # Page 2 masked
        p2 = dst.download_json("confluence/S/pages/2.json")
        assert p2["title"] != "Page 2"
