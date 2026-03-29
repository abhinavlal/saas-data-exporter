"""Edge case tests across all exporters."""

import json

import boto3
import pytest
import responses
from moto import mock_aws
from unittest.mock import MagicMock, patch
from botocore.exceptions import ClientError

from lib.s3 import S3Store
from lib.types import ExportConfig
from lib.checkpoint import CheckpointManager


# ── Fixtures ──────────────────────────────────────────────────────────────

@pytest.fixture
def s3_env():
    with mock_aws():
        conn = boto3.client("s3", region_name="us-east-1")
        conn.create_bucket(Bucket="test-bucket")
        store = S3Store(bucket="test-bucket")
        config = ExportConfig(s3_bucket="test-bucket", max_workers=2)
        yield store, config, conn


@pytest.fixture
def mock_google_credentials():
    with patch("exporters.google_workspace.service_account.Credentials") as mc:
        mi = MagicMock()
        mi.with_subject.return_value = mi
        mc.from_service_account_file.return_value = mi
        yield mc


# ── Empty Exports ─────────────────────────────────────────────────────────

class TestEmptyExports:
    """All exporters should handle empty data gracefully (empty JSON arrays, not errors)."""

    @responses.activate
    def test_github_empty_repo(self, s3_env):
        store, config, _ = s3_env
        from exporters.github import GitHubExporter

        # Repo metadata
        responses.add(responses.GET, "https://api.github.com/repos/owner/empty",
                       json={"full_name": "owner/empty", "topics": []}, status=200)
        responses.add(responses.GET, "https://api.github.com/repos/owner/empty/languages",
                       json={}, status=200)
        # Empty contributors
        responses.add(responses.GET, "https://api.github.com/repos/owner/empty/contributors",
                       json=[], status=200)
        # Empty commits
        responses.add(responses.GET, "https://api.github.com/repos/owner/empty/commits",
                       json=[], status=200)
        # Empty PRs
        responses.add(responses.GET, "https://api.github.com/repos/owner/empty/pulls",
                       json=[], status=200)

        exporter = GitHubExporter(
            token="fake", repo="owner/empty", s3=store, config=config,
            commit_limit=10, pr_limit=10, skip_commits=False,
        )
        exporter.run()

        assert store.download_json("github/owner__empty/contributors.json") == []
        # Metadata should still have language_breakdown (empty)
        meta = store.download_json("github/owner__empty/repo_metadata.json")
        assert meta["language_breakdown"] == {}

    @responses.activate
    def test_jira_empty_project(self, s3_env):
        store, config, _ = s3_env
        from exporters.jira import JiraExporter

        responses.add(responses.GET, "https://test.atlassian.net/rest/api/3/field",
                       json=[], status=200)
        responses.add(responses.POST, "https://test.atlassian.net/rest/api/3/search/jql",
                       json={"issues": [], "nextPageToken": None}, status=200)

        exporter = JiraExporter(
            token="fake", email="test@test.com", domain="test.atlassian.net",
            projects=["EMPTY"], s3=store, config=config, limit=10,
        )
        exporter.run()

        index = store.download_json("jira/EMPTY/tickets/_index.json")
        assert index["keys"] == []

    @responses.activate
    def test_slack_empty_channel(self, s3_env):
        store, config, _ = s3_env
        from exporters.slack import SlackExporter

        responses.add(responses.GET, "https://slack.com/api/conversations.info",
                       json={"ok": True, "channel": {"id": "C0EMPTY", "name": "empty"}}, status=200)
        responses.add(responses.GET, "https://slack.com/api/conversations.history",
                       json={"ok": True, "messages": []}, status=200)

        exporter = SlackExporter(
            token="xoxb-fake", channel_ids=["C0EMPTY"], s3=store, config=config,
        )
        exporter.run()

        index = store.download_json("slack/C0EMPTY/messages/_index.json")
        assert index == []

    def test_google_empty_drive(self, s3_env, mock_google_credentials):
        store, config, _ = s3_env
        from exporters.google_workspace import GoogleWorkspaceExporter

        drive_service = MagicMock()
        drive_service.files().list().execute.return_value = {"files": []}

        with patch("exporters.google_workspace.build", return_value=drive_service):
            exporter = GoogleWorkspaceExporter(
                user="empty@test.com", service_account_key="fake.json",
                s3=store, config=config,
                skip_gmail=True, skip_calendar=True, file_limit=10,
            )
            exporter.run()

        index = store.download_json("google/empty_at_test.com/drive/_index.json")
        assert index == []


# ── Unicode Filenames ─────────────────────────────────────────────────────

class TestUnicodeFilenames:
    @responses.activate
    def test_jira_unicode_attachment_name(self, s3_env):
        store, config, _ = s3_env
        from exporters.jira import JiraExporter

        responses.add(responses.GET, "https://test.atlassian.net/rest/api/3/field",
                       json=[], status=200)

        issue = {
            "key": "UNI-1", "id": "1", "self": "...",
            "fields": {
                "summary": "Unicode test",
                "description": None,
                "issuetype": {"name": "Task"},
                "status": {"name": "Open", "statusCategory": {"name": "To Do"}},
                "priority": None, "resolution": None,
                "project": {"key": "UNI", "name": "Unicode"},
                "created": "2024-01-01T00:00:00.000+0000",
                "updated": "2024-01-01T00:00:00.000+0000",
                "resolutiondate": None, "duedate": None,
                "assignee": None, "reporter": None, "creator": None,
                "labels": [], "components": [], "fixVersions": [], "versions": [],
                "timetracking": {}, "votes": {"votes": 0}, "watches": {"watchCount": 0},
                "attachment": [{
                    "id": "att-uni",
                    "filename": "rapport_résumé_日本語.pdf",
                    "size": 100,
                    "mimeType": "application/pdf",
                    "created": "2024-01-01T00:00:00.000+0000",
                    "author": None,
                    "content": "https://test.atlassian.net/rest/api/3/attachment/content/att-uni",
                }],
            },
            "changelog": {"histories": []},
            "renderedFields": {},
        }
        responses.add(responses.POST, "https://test.atlassian.net/rest/api/3/search/jql",
                       json={"issues": [issue], "nextPageToken": None}, status=200)
        responses.add(responses.GET,
                       "https://test.atlassian.net/rest/api/3/attachment/content/att-uni",
                       body=b"PDF", status=200)

        exporter = JiraExporter(
            token="fake", email="test@test.com", domain="test.atlassian.net",
            projects=["UNI"], s3=store, config=config, limit=10,
            skip_comments=True, skip_attachments=False,
        )
        exporter.run()

        assert store.exists("jira/UNI/attachments/UNI-1/rapport_résumé_日本語.pdf")

    @responses.activate
    def test_slack_unicode_filename(self, s3_env):
        store, config, _ = s3_env
        from exporters.slack import SlackExporter

        responses.add(responses.GET, "https://slack.com/api/conversations.info",
                       json={"ok": True, "channel": {"id": "CUNI", "name": "unicode"}}, status=200)
        responses.add(responses.GET, "https://slack.com/api/conversations.history",
                       json={"ok": True, "messages": [{
                           "type": "message", "user": "U01", "text": "File",
                           "ts": "1700000001.000000",
                           "files": [{
                               "id": "FUNI",
                               "name": "données_été.xlsx",
                               "url_private_download": "https://files.slack.com/dl/données.xlsx",
                           }],
                       }]}, status=200)
        responses.add(responses.GET, "https://files.slack.com/dl/données.xlsx",
                       body=b"excel", status=200, headers={"Content-Type": "application/xlsx"})

        exporter = SlackExporter(
            token="xoxb-fake", channel_ids=["CUNI"], s3=store, config=config,
            skip_attachments=False,
        )
        exporter.run()

        assert store.exists("slack/CUNI/attachments/FUNI_données_été.xlsx")


# ── Checkpoint Persistence on Error ───────────────────────────────────────

class TestCheckpointOnError:
    @responses.activate
    def test_github_checkpoint_preserved_on_api_error(self, s3_env):
        """If commit detail fetch fails mid-way, checkpoint should have partial progress."""
        store, config, _ = s3_env
        from exporters.github import GitHubExporter

        def _commit_obj(sha):
            return {
                "sha": sha,
                "commit": {"message": "ok", "author": {"name": "A", "email": "a@b.com", "date": "2024-01-01T00:00:00Z"},
                           "committer": {"name": "A", "email": "a@b.com", "date": "2024-01-01T00:00:00Z"}},
                "author": {"login": "a"}, "committer": {"login": "a"},
                "parents": [], "html_url": "...",
            }

        # Metadata
        responses.add(responses.GET, "https://api.github.com/repos/owner/repo",
                       json={"full_name": "owner/repo", "topics": []}, status=200)
        responses.add(responses.GET, "https://api.github.com/repos/owner/repo/languages",
                       json={}, status=200)
        # Contributors
        responses.add(responses.GET, "https://api.github.com/repos/owner/repo/contributors",
                       json=[], status=200)
        # Commit list
        responses.add(responses.GET, "https://api.github.com/repos/owner/repo/commits",
                       json=[_commit_obj("sha1"), _commit_obj("sha2")], status=200)
        responses.add(responses.GET, "https://api.github.com/repos/owner/repo/commits",
                       json=[], status=200)
        # Detail endpoints (for commit_details=True)
        responses.add(responses.GET, "https://api.github.com/repos/owner/repo/commits/sha1",
                       json={**_commit_obj("sha1"), "stats": {"additions": 1, "deletions": 0, "total": 1}, "files": []},
                       status=200)
        # sha2 fails with 404
        responses.add(responses.GET, "https://api.github.com/repos/owner/repo/commits/sha2",
                       json={"message": "Not Found"}, status=404)
        # Empty PRs
        responses.add(responses.GET, "https://api.github.com/repos/owner/repo/pulls",
                       json=[], status=200)

        exporter = GitHubExporter(
            token="fake", repo="owner/repo", s3=store, config=config,
            commit_limit=10, pr_limit=10, skip_commits=False, commit_details=True,
        )
        exporter.run()

        # Export should complete despite sha2 failing — sha1 written as individual file
        c = store.download_json("github/owner__repo/commits/sha1.json")
        assert c is not None
        assert c["sha"] == "sha1"

        # Checkpoint should be completed
        cp = CheckpointManager(store, "github/owner__repo")
        cp.load()
        assert cp.status == "completed"


# ── S3 Error Handling ─────────────────────────────────────────────────────

class TestS3Errors:
    def test_s3_upload_json_to_nonexistent_bucket(self):
        """S3Store should raise when bucket doesn't exist."""
        with mock_aws():
            boto3.client("s3", region_name="us-east-1")
            # Don't create the bucket
            store = S3Store(bucket="nonexistent-bucket")
            with pytest.raises(ClientError):
                store.upload_json({"test": True}, "test.json")

    def test_s3_download_json_from_nonexistent_bucket(self):
        """S3Store should raise on bucket-level errors (not NoSuchKey)."""
        with mock_aws():
            boto3.client("s3", region_name="us-east-1")
            store = S3Store(bucket="nonexistent-bucket")
            with pytest.raises(ClientError):
                store.download_json("test.json")


# ── CSV Generation Edge Cases ─────────────────────────────────────────────

class TestCheckpointDefensiveness:
    def test_mark_item_done_without_start_phase(self, s3_env):
        """mark_item_done should not crash if start_phase was not called."""
        store, _, _ = s3_env
        cp = CheckpointManager(store, "test/defensive")
        cp.load()
        # Don't call start_phase — mark_item_done should auto-create
        cp.mark_item_done("auto_phase", "item1")
        assert cp.is_item_done("auto_phase", "item1")

    def test_set_cursor_without_start_phase(self, s3_env):
        store, _, _ = s3_env
        cp = CheckpointManager(store, "test/defensive")
        cp.load()
        cp.set_cursor("auto_phase", "cursor1")
        assert cp.get_cursor("auto_phase") == "cursor1"

    def test_complete_phase_without_start_phase(self, s3_env):
        store, _, _ = s3_env
        cp = CheckpointManager(store, "test/defensive")
        cp.load()
        cp.complete_phase("auto_phase")
        assert cp.is_phase_done("auto_phase")


class TestSlackMalformedTimestamp:
    @responses.activate
    def test_bad_ts_does_not_crash_sort(self, s3_env):
        store, config, _ = s3_env
        from exporters.slack import SlackExporter

        responses.add(responses.GET, "https://slack.com/api/conversations.info",
                       json={"ok": True, "channel": {"id": "CTS", "name": "ts-test"}}, status=200)
        responses.add(responses.GET, "https://slack.com/api/conversations.history",
                       json={"ok": True, "messages": [
                           {"type": "message", "text": "good", "ts": "1700000001.000000"},
                           {"type": "message", "text": "bad ts", "ts": "not_a_number"},
                           {"type": "message", "text": "missing ts"},
                       ]}, status=200)

        exporter = SlackExporter(
            token="xoxb-fake", channel_ids=["CTS"], s3=store, config=config,
        )
        exporter.run()  # Should not crash

        index = store.download_json("slack/CTS/messages/_index.json")
        assert len(index) == 3


class TestCsvEdgeCases:
    @responses.activate
    def test_github_empty_pr_csv(self, s3_env):
        """Empty PR list should produce an empty CSV (not error)."""
        store, config, conn = s3_env
        from exporters.github import GitHubExporter

        responses.add(responses.GET, "https://api.github.com/repos/o/r",
                       json={"full_name": "o/r", "topics": []}, status=200)
        responses.add(responses.GET, "https://api.github.com/repos/o/r/languages",
                       json={}, status=200)
        responses.add(responses.GET, "https://api.github.com/repos/o/r/contributors",
                       json=[], status=200)
        responses.add(responses.GET, "https://api.github.com/repos/o/r/pulls",
                       json=[], status=200)

        exporter = GitHubExporter(
            token="fake", repo="o/r", s3=store, config=config,
            skip_commits=True, pr_limit=10,
        )
        exporter.run()

        # CSV should exist but be empty content (just headers or empty)
        resp = conn.get_object(Bucket="test-bucket", Key="github/o__r/pull_requests.csv")
        csv_bytes = resp["Body"].read()
        assert csv_bytes == b""  # empty CSV for no PRs


class TestPerTargetErrorHandling:
    """One bad target should not kill the entire exporter."""

    @responses.activate
    def test_jira_continues_after_project_failure(self, s3_env):
        """If one Jira project fails, the next should still be exported."""
        store, config, _ = s3_env
        from exporters.jira import JiraExporter

        responses.add(responses.GET, "https://test.atlassian.net/rest/api/3/field",
                       json=[], status=200)
        # FAIL project: search returns 500
        responses.add(responses.POST, "https://test.atlassian.net/rest/api/3/search/jql",
                       json={"error": "Internal error"}, status=500)
        # OK project: search returns empty
        responses.add(responses.POST, "https://test.atlassian.net/rest/api/3/search/jql",
                       json={"issues": [], "nextPageToken": None}, status=200)

        exporter = JiraExporter(
            token="fake", email="test@test.com", domain="test.atlassian.net",
            projects=["FAIL", "OK"], s3=store, config=config, limit=10,
        )
        exporter.run()  # Should not raise

        # The OK project should still have been exported
        index = store.download_json("jira/OK/tickets/_index.json")
        assert index["keys"] == []

    @responses.activate
    def test_slack_continues_after_channel_failure(self, s3_env):
        """If one Slack channel fails, the next should still be exported."""
        store, config, _ = s3_env
        from exporters.slack import SlackExporter

        # FAIL channel: info returns error
        responses.add(responses.GET, "https://slack.com/api/conversations.info",
                       json={"ok": False, "error": "channel_not_found"}, status=200)
        responses.add(responses.GET, "https://slack.com/api/conversations.history",
                       body="server error", status=500)
        # OK channel
        responses.add(responses.GET, "https://slack.com/api/conversations.info",
                       json={"ok": True, "channel": {"id": "COK", "name": "ok"}}, status=200)
        responses.add(responses.GET, "https://slack.com/api/conversations.history",
                       json={"ok": True, "messages": []}, status=200)

        exporter = SlackExporter(
            token="xoxb-fake", channel_ids=["CFAIL", "COK"], s3=store, config=config,
        )
        exporter.run()  # Should not raise

        index = store.download_json("slack/COK/messages/_index.json")
        assert index == []
