"""Tests for exporters.confluence — ConfluenceExporter with mocked API and moto S3."""

import json

import boto3
import pytest
import responses
from moto import mock_aws

from lib.s3 import S3Store
from lib.types import ExportConfig
from exporters.confluence import ConfluenceExporter

DOMAIN = "test.atlassian.net"
BASE_V2 = f"https://{DOMAIN}/wiki/api/v2"
SPACE_KEY = "ENG"
SPACE_ID = "12345"


# ── Fixtures ──────────────────────────────────────────────────────────────

@pytest.fixture
def s3_env():
    with mock_aws():
        conn = boto3.client("s3", region_name="us-east-1")
        conn.create_bucket(Bucket="test-bucket")
        store = S3Store(bucket="test-bucket")
        config = ExportConfig(s3_bucket="test-bucket", max_workers=2)
        yield store, config, conn


def _make_exporter(s3_env, **kwargs):
    store, config, _ = s3_env
    defaults = dict(
        token="fake-token",
        email="test@test.com",
        domain=DOMAIN,
        spaces=[SPACE_KEY],
        s3=store,
        config=config,
        page_limit=10,
        skip_attachments=True,
        skip_comments=True,
    )
    defaults.update(kwargs)
    return ConfluenceExporter(**defaults)


# ── Mock API helpers ──────────────────────────────────────────────────────

def mock_space_api():
    responses.add(
        responses.GET, f"{BASE_V2}/spaces",
        json={
            "results": [{"id": SPACE_ID, "key": SPACE_KEY, "name": "Engineering"}],
            "_links": {},
        },
        status=200,
    )


def mock_pages_api(pages=None):
    if pages is None:
        pages = [_make_page("101", "Getting Started"), _make_page("102", "Architecture")]
    responses.add(
        responses.GET, f"{BASE_V2}/spaces/{SPACE_ID}/pages",
        json={"results": pages, "_links": {}},
        status=200,
    )


def mock_pages_api_paginated(page1, page2):
    """Mock two pages of results with cursor-based pagination."""
    responses.add(
        responses.GET, f"{BASE_V2}/spaces/{SPACE_ID}/pages",
        json={
            "results": page1,
            "_links": {"next": f"/wiki/api/v2/spaces/{SPACE_ID}/pages?cursor=page2"},
        },
        status=200,
    )
    responses.add(
        responses.GET, f"{BASE_V2}/spaces/{SPACE_ID}/pages",
        json={"results": page2, "_links": {}},
        status=200,
    )


def mock_comments_api(page_id, comments=None):
    if comments is None:
        comments = []
    responses.add(
        responses.GET, f"{BASE_V2}/pages/{page_id}/footer-comments",
        json={"results": comments, "_links": {}},
        status=200,
    )


def mock_attachments_api(page_id, attachments=None):
    if attachments is None:
        attachments = []
    responses.add(
        responses.GET, f"{BASE_V2}/pages/{page_id}/attachments",
        json={"results": attachments, "_links": {}},
        status=200,
    )


def _make_page(page_id, title, body="<p>Content</p>"):
    return {
        "id": page_id,
        "title": title,
        "spaceId": SPACE_ID,
        "status": "current",
        "createdAt": "2024-01-01T00:00:00.000Z",
        "authorId": "user1",
        "parentId": None,
        "parentType": "space",
        "position": 0,
        "version": {"number": 1, "createdAt": "2024-01-01T00:00:00.000Z"},
        "body": {"storage": {"value": body}},
    }


def _make_comment(comment_id, body="Nice page!"):
    return {
        "id": comment_id,
        "authorId": "user2",
        "createdAt": "2024-06-01T00:00:00.000Z",
        "version": {"number": 1},
        "body": {"storage": {"value": body}},
    }


def _make_attachment(att_id, filename, media_type="application/pdf", size=1024):
    return {
        "id": att_id,
        "title": filename,
        "mediaType": media_type,
        "fileSize": size,
        "_links": {"download": f"/download/attachments/101/{filename}"},
    }


# ── Tests ────────────────────────────────────────────────────────────────

class TestSpaceResolution:
    @responses.activate
    def test_resolves_space_key_to_id(self, s3_env):
        mock_space_api()
        mock_pages_api(pages=[])
        exporter = _make_exporter(s3_env)
        exporter.run()
        # Should not error — space was resolved


class TestPageExport:
    @responses.activate
    def test_exports_pages_as_individual_files(self, s3_env):
        mock_space_api()
        mock_pages_api()
        exporter = _make_exporter(s3_env)
        exporter.run()

        store, _, _ = s3_env
        page1 = store.download_json(f"confluence/{SPACE_KEY}/pages/101.json")
        assert page1["title"] == "Getting Started"
        assert page1["body"] == "<p>Content</p>"
        assert page1["space_key"] == SPACE_KEY

        page2 = store.download_json(f"confluence/{SPACE_KEY}/pages/102.json")
        assert page2["title"] == "Architecture"

    @responses.activate
    def test_writes_page_index(self, s3_env):
        mock_space_api()
        mock_pages_api()
        exporter = _make_exporter(s3_env)
        exporter.run()

        store, _, _ = s3_env
        index = store.download_json(f"confluence/{SPACE_KEY}/pages/_index.json")
        assert index == ["101", "102"]

    @responses.activate
    def test_paginates_pages(self, s3_env):
        mock_space_api()
        page1 = [_make_page("201", "Page A")]
        page2 = [_make_page("202", "Page B")]
        mock_pages_api_paginated(page1, page2)

        exporter = _make_exporter(s3_env)
        exporter.run()

        store, _, _ = s3_env
        index = store.download_json(f"confluence/{SPACE_KEY}/pages/_index.json")
        assert index == ["201", "202"]

    @responses.activate
    def test_respects_page_limit(self, s3_env):
        mock_space_api()
        pages = [_make_page(str(i), f"Page {i}") for i in range(300, 310)]
        mock_pages_api(pages)

        exporter = _make_exporter(s3_env, page_limit=3)
        exporter.run()

        store, _, _ = s3_env
        index = store.download_json(f"confluence/{SPACE_KEY}/pages/_index.json")
        assert len(index) == 3


class TestCommentExport:
    @responses.activate
    def test_fetches_and_embeds_comments_in_single_pass(self, s3_env):
        """Comments are embedded during page export — no separate phase."""
        mock_space_api()
        mock_pages_api([_make_page("101", "Page")])
        mock_comments_api("101", [_make_comment("c1", "Great!"), _make_comment("c2", "Thanks")])

        exporter = _make_exporter(s3_env, skip_comments=False)
        exporter.run()

        store, _, _ = s3_env
        page = store.download_json(f"confluence/{SPACE_KEY}/pages/101.json")
        assert len(page["comments"]) == 2
        assert page["comments"][0]["id"] == "c1"
        assert page["comments"][0]["body"] == "Great!"

    @responses.activate
    def test_empty_comments(self, s3_env):
        mock_space_api()
        mock_pages_api([_make_page("101", "Page")])
        mock_comments_api("101", [])

        exporter = _make_exporter(s3_env, skip_comments=False)
        exporter.run()

        store, _, _ = s3_env
        page = store.download_json(f"confluence/{SPACE_KEY}/pages/101.json")
        assert page["comments"] == []


class TestAttachmentDownload:
    @responses.activate
    def test_downloads_attachments_to_s3(self, s3_env):
        mock_space_api()
        mock_pages_api([_make_page("101", "Page")])
        mock_attachments_api("101", [
            _make_attachment("att1", "design.pdf", "application/pdf", 2048),
        ])
        responses.add(
            responses.GET, f"https://{DOMAIN}/wiki/download/attachments/101/design.pdf",
            body=b"PDF_CONTENT",
            status=200,
        )

        exporter = _make_exporter(s3_env, skip_attachments=False)
        exporter.run()

        store, _, conn = s3_env
        resp = conn.list_objects_v2(Bucket="test-bucket",
                                    Prefix=f"confluence/{SPACE_KEY}/attachments/")
        keys = [obj["Key"] for obj in resp.get("Contents", [])]
        assert any("design.pdf" in k for k in keys)


class TestCheckpointResume:
    @responses.activate
    def test_resume_skips_completed_pages(self, s3_env):
        """First run completes all pages; second run skips entirely."""
        mock_space_api()
        mock_pages_api([_make_page("101", "Page")])
        exporter = _make_exporter(s3_env)
        exporter.run()

        # Second run — pages phase already done
        mock_space_api()
        exporter2 = _make_exporter(s3_env)
        exporter2.run()

        # Should still have the page from first run
        store, _, _ = s3_env
        page = store.download_json(f"confluence/{SPACE_KEY}/pages/101.json")
        assert page["title"] == "Page"

    @responses.activate
    def test_page_level_checkpoint_within_phase(self, s3_env):
        """Within a single run, already-done pages are skipped on retry."""
        mock_space_api()
        pages = [_make_page("101", "Page A"), _make_page("102", "Page B")]
        mock_pages_api(pages)
        mock_comments_api("101", [_make_comment("c1")])
        mock_comments_api("102", [_make_comment("c2")])

        exporter = _make_exporter(s3_env, skip_comments=False)
        exporter.run()

        store, _, _ = s3_env
        # Both pages exported with comments in a single pass
        page1 = store.download_json(f"confluence/{SPACE_KEY}/pages/101.json")
        page2 = store.download_json(f"confluence/{SPACE_KEY}/pages/102.json")
        assert page1["title"] == "Page A"
        assert len(page1["comments"]) == 1
        assert page2["title"] == "Page B"
        assert len(page2["comments"]) == 1


class TestStats:
    @responses.activate
    def test_stats_written(self, s3_env):
        mock_space_api()
        mock_pages_api()
        exporter = _make_exporter(s3_env)
        exporter.run()

        store, _, _ = s3_env
        stats = store.download_json(f"confluence/{SPACE_KEY}/_stats.json")
        assert stats["exporter"] == "confluence"
        assert stats["target"] == SPACE_KEY
        assert stats["pages"]["total"] == 2
        assert "exported_at" in stats


class TestFullExport:
    @responses.activate
    def test_all_files_created(self, s3_env):
        mock_space_api()
        mock_pages_api([_make_page("101", "Page")])
        mock_comments_api("101", [_make_comment("c1")])
        mock_attachments_api("101", [
            _make_attachment("att1", "file.txt", "text/plain", 100),
        ])
        responses.add(
            responses.GET, f"https://{DOMAIN}/wiki/download/attachments/101/file.txt",
            body=b"hello",
            status=200,
        )

        exporter = _make_exporter(s3_env, skip_comments=False, skip_attachments=False)
        exporter.run()

        store, _, conn = s3_env
        resp = conn.list_objects_v2(Bucket="test-bucket",
                                    Prefix=f"confluence/{SPACE_KEY}/")
        keys = {obj["Key"] for obj in resp.get("Contents", [])}
        expected = {
            f"confluence/{SPACE_KEY}/pages/101.json",
            f"confluence/{SPACE_KEY}/pages/_index.json",
            f"confluence/{SPACE_KEY}/attachments/101/file.txt",
            f"confluence/{SPACE_KEY}/_stats.json",
        }
        assert expected.issubset(keys), f"Missing keys: {expected - keys}"

        # Page has comments embedded in single pass — no re-read from S3
        page = store.download_json(f"confluence/{SPACE_KEY}/pages/101.json")
        assert len(page["comments"]) == 1

        # Stats reflect single-pass totals
        stats = store.download_json(f"confluence/{SPACE_KEY}/_stats.json")
        assert stats["pages"]["total"] == 1
        assert stats["comments"]["total"] == 1
        assert stats["attachments"]["total"] == 1
