"""Tests for exporters.catalog — CatalogGenerator with moto S3."""

import json

import boto3
import pytest
from moto import mock_aws

from lib.s3 import S3Store
from exporters.catalog import CatalogGenerator


@pytest.fixture
def s3_env():
    with mock_aws():
        conn = boto3.client("s3", region_name="us-east-1")
        conn.create_bucket(Bucket="test-bucket")
        s3 = S3Store(bucket="test-bucket")
        yield s3, conn


def _seed_github_stats(s3):
    s3.upload_json({
        "exporter": "github",
        "target": "acme/api",
        "target_slug": "acme__api",
        "exported_at": "2026-03-29T10:00:00Z",
        "repo": {"full_name": "acme/api", "private": False, "stars": 100, "forks": 10,
                 "open_issues": 5, "watchers": 80},
        "languages": {"Python": {"bytes": 50000, "percentage": 80.0},
                      "Go": {"bytes": 12500, "percentage": 20.0}},
        "contributors": {"total": 15, "top": "alice"},
        "commits": {"total": 200, "unique_authors": 10},
        "pull_requests": {"total": 50, "open": 5, "closed": 10, "merged": 35,
                          "total_reviews": 100, "total_review_comments": 50,
                          "total_comments": 30, "total_additions": 10000,
                          "total_deletions": 5000, "total_changed_files": 200},
    }, "github/acme__api/_stats.json")


def _seed_jira_stats(s3):
    s3.upload_json({
        "exporter": "jira",
        "target": "PROJ",
        "exported_at": "2026-03-29T10:00:00Z",
        "tickets": {"total": 300, "by_type": {"Bug": 100, "Story": 150, "Task": 50},
                    "by_status": {"Open": 50, "Done": 250},
                    "by_status_category": {"To Do": 50, "Done": 250},
                    "by_priority": {"High": 30, "Medium": 200, "Low": 70}},
        "comments": {"total": 800, "tickets_with_comments": 200},
        "attachments": {"total": 120, "total_size_bytes": 50000000,
                        "by_mime_type": {"image/png": 60, "application/pdf": 40, "text/plain": 20}},
        "changelog": {"total": 1500},
    }, "jira/PROJ/_stats.json")


def _seed_slack_stats(s3):
    s3.upload_json({
        "exporter": "slack",
        "target": "C0TEST",
        "exported_at": "2026-03-29T10:00:00Z",
        "channel": {"name": "general", "is_private": False, "num_members": 50},
        "messages": {"total": 5000, "thread_parents": 200, "total_thread_replies": 800,
                     "with_reactions": 300, "total_reactions": 1500,
                     "by_subtype": {"user_message": 4500, "bot_message": 500}},
        "files": {"total": 100, "downloaded": 90, "by_extension": {".png": 40, ".pdf": 30, ".xlsx": 20, ".unknown": 10}},
    }, "slack/C0TEST/_stats.json")


def _seed_google_stats(s3):
    s3.upload_json({
        "exporter": "google_workspace",
        "target": "alice@acme.com",
        "target_slug": "alice_at_acme.com",
        "exported_at": "2026-03-29T10:00:00Z",
        "gmail": {"total_messages": 2000, "total_size_bytes": 500000000,
                  "total_attachments": 400, "messages_with_attachments": 150,
                  "attachments_by_extension": {".pdf": 200, ".docx": 100, ".xlsx": 100}},
        "calendar": {"total_events": 500, "with_attendees": 400, "with_location": 100},
        "drive": {"total_files": 80, "downloaded": 70, "skipped": 10,
                  "total_size_bytes": 200000000,
                  "by_mime_type": {"application/pdf": 30, "text/plain": 20, "application/vnd.google-apps.document": 30}},
    }, "google/alice_at_acme.com/_stats.json")


def _parse_jsonl(s3, path):
    """Read a JSON Lines file from S3 (cannot use download_json — that expects a single JSON doc)."""
    resp = s3._client.get_object(Bucket=s3.bucket, Key=s3._key(path))
    body = resp["Body"].read().decode("utf-8")
    return [json.loads(line) for line in body.strip().split("\n") if line.strip()]


class TestGitHubCatalog:
    def test_github_repos_table(self, s3_env):
        s3, _ = s3_env
        _seed_github_stats(s3)
        CatalogGenerator(s3).run()

        rows = _parse_jsonl(s3, "catalog/github_repos.jsonl")
        assert len(rows) == 1
        row = rows[0]
        assert row["target"] == "acme/api"
        assert row["stars"] == 100
        assert row["total_prs"] == 50
        assert row["prs_merged"] == 35
        assert row["total_commits"] == 200

    def test_github_languages_table(self, s3_env):
        s3, _ = s3_env
        _seed_github_stats(s3)
        CatalogGenerator(s3).run()

        rows = _parse_jsonl(s3, "catalog/github_languages.jsonl")
        assert len(rows) == 2
        langs = {r["language"]: r for r in rows}
        assert langs["Python"]["bytes"] == 50000
        assert langs["Go"]["percentage"] == 20.0


class TestJiraCatalog:
    def test_jira_projects_table(self, s3_env):
        s3, _ = s3_env
        _seed_jira_stats(s3)
        CatalogGenerator(s3).run()

        rows = _parse_jsonl(s3, "catalog/jira_projects.jsonl")
        assert len(rows) == 1
        row = rows[0]
        assert row["target"] == "PROJ"
        assert row["total_tickets"] == 300
        assert row["total_comments"] == 800
        assert row["total_attachments"] == 120


class TestSlackCatalog:
    def test_slack_channels_table(self, s3_env):
        s3, _ = s3_env
        _seed_slack_stats(s3)
        CatalogGenerator(s3).run()

        rows = _parse_jsonl(s3, "catalog/slack_channels.jsonl")
        assert len(rows) == 1
        row = rows[0]
        assert row["target"] == "C0TEST"
        assert row["channel_name"] == "general"
        assert row["total_messages"] == 5000
        assert row["total_files"] == 100


class TestGoogleCatalog:
    def test_google_users_table(self, s3_env):
        s3, _ = s3_env
        _seed_google_stats(s3)
        CatalogGenerator(s3).run()

        rows = _parse_jsonl(s3, "catalog/google_users.jsonl")
        assert len(rows) == 1
        row = rows[0]
        assert row["target"] == "alice@acme.com"
        assert row["gmail_messages"] == 2000
        assert row["gmail_attachments"] == 400
        assert row["calendar_events"] == 500
        assert row["drive_files"] == 80


class TestFileTypes:
    def test_cross_exporter_file_types(self, s3_env):
        s3, _ = s3_env
        _seed_google_stats(s3)
        _seed_slack_stats(s3)
        _seed_jira_stats(s3)
        CatalogGenerator(s3).run()

        rows = _parse_jsonl(s3, "catalog/file_types.jsonl")
        assert len(rows) > 0

        # Check we have entries from multiple exporters
        exporters = {r["exporter"] for r in rows}
        assert "google_workspace" in exporters
        assert "slack" in exporters
        assert "jira" in exporters

        # Check a specific entry
        pdf_entries = [r for r in rows if r["file_type"] == ".pdf" and r["exporter"] == "google_workspace"]
        assert len(pdf_entries) == 1
        assert pdf_entries[0]["count"] == 200


class TestSummary:
    def test_full_summary(self, s3_env):
        s3, _ = s3_env
        _seed_github_stats(s3)
        _seed_jira_stats(s3)
        _seed_slack_stats(s3)
        _seed_google_stats(s3)
        CatalogGenerator(s3).run()

        summary = s3.download_json("catalog/summary.json")
        assert summary["github"]["repos"] == 1
        assert summary["github"]["total_prs"] == 50
        assert summary["jira"]["projects"] == 1
        assert summary["jira"]["total_tickets"] == 300
        assert summary["slack"]["channels"] == 1
        assert summary["slack"]["total_messages"] == 5000
        assert summary["google"]["users"] == 1
        assert summary["google"]["total_emails"] == 2000


class TestDryRun:
    def test_dry_run_no_writes(self, s3_env):
        s3, conn = s3_env
        _seed_github_stats(s3)
        CatalogGenerator(s3, dry_run=True).run()

        # Should NOT create any catalog files
        resp = conn.list_objects_v2(Bucket="test-bucket", Prefix="catalog/")
        assert resp.get("KeyCount", 0) == 0


class TestExporterFilter:
    def test_filter_github_only(self, s3_env):
        s3, _ = s3_env
        _seed_github_stats(s3)
        _seed_jira_stats(s3)
        CatalogGenerator(s3, exporter_filter="github").run()

        # Should only write github tables
        rows = _parse_jsonl(s3, "catalog/github_repos.jsonl")
        assert len(rows) == 1

        # Summary should only have github
        summary = s3.download_json("catalog/summary.json")
        assert "github" in summary
        assert "jira" not in summary
