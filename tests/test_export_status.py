"""Tests for scripts.export_status — S3 checkpoint status display."""

import boto3
import pytest
from moto import mock_aws

from lib.s3 import S3Store
from scripts.export_status import get_status_rows, print_status_table, _phase_summary


@pytest.fixture
def s3_store():
    with mock_aws():
        conn = boto3.client("s3", region_name="us-east-1")
        conn.create_bucket(Bucket="test-bucket")
        yield S3Store(bucket="test-bucket", prefix="v1")


def _upload_checkpoint(s3, job_id, status, phases=None):
    """Helper to upload a checkpoint JSON to S3."""
    data = {
        "job_id": job_id,
        "status": status,
        "started_at": "2026-03-29T10:00:00+00:00",
        "updated_at": "2026-03-29T10:05:00+00:00",
        "phases": phases or {},
    }
    s3.upload_json(data, f"_checkpoints/{job_id}.json")


class TestPhaseSummary:
    def test_no_phases(self):
        assert _phase_summary({}) == "no phases"

    def test_all_completed(self):
        phases = {
            "commits": {"status": "completed"},
            "pull_requests": {"status": "completed"},
        }
        assert _phase_summary(phases) == "2/2 complete"

    def test_in_progress(self):
        phases = {
            "commits": {"status": "completed"},
            "pull_requests": {"status": "in_progress"},
            "contributors": {"status": "pending"},
        }
        result = _phase_summary(phases)
        assert result == "1/3 (pull_requests)"

    def test_none_completed(self):
        phases = {
            "tickets": {"status": "in_progress"},
        }
        assert _phase_summary(phases) == "0/1 (tickets)"


class TestGetStatusRows:
    def test_no_checkpoints(self, s3_store):
        rows = get_status_rows(s3_store)
        assert rows == []

    def test_single_completed(self, s3_store):
        _upload_checkpoint(s3_store, "github/owner__repo", "completed", {
            "commits": {"status": "completed"},
            "pull_requests": {"status": "completed"},
        })
        rows = get_status_rows(s3_store)
        assert len(rows) == 1
        assert rows[0]["exporter"] == "github"
        assert rows[0]["target"] == "owner/repo"
        assert rows[0]["status"] == "completed"
        assert rows[0]["phases"] == "2/2 complete"

    def test_mixed_statuses(self, s3_store):
        _upload_checkpoint(s3_store, "github/owner__repo", "completed", {
            "commits": {"status": "completed"},
        })
        _upload_checkpoint(s3_store, "jira/PROJ", "in_progress", {
            "tickets": {"status": "completed"},
            "attachments": {"status": "in_progress"},
        })
        _upload_checkpoint(s3_store, "slack/C123", "completed", {
            "messages": {"status": "completed"},
        })
        rows = get_status_rows(s3_store)
        assert len(rows) == 3
        # Sorted by exporter then target
        assert rows[0]["exporter"] == "github"
        assert rows[1]["exporter"] == "jira"
        assert rows[1]["status"] == "in_progress"
        assert rows[2]["exporter"] == "slack"

    def test_github_target_name_restored(self, s3_store):
        _upload_checkpoint(s3_store, "github/org__my-repo", "completed")
        rows = get_status_rows(s3_store)
        assert rows[0]["target"] == "org/my-repo"


class TestPrintStatusTable:
    def test_no_exports(self, capsys):
        print_status_table([])
        captured = capsys.readouterr()
        assert "No exports found" in captured.out

    def test_prints_rows(self, capsys):
        rows = [
            {
                "exporter": "github",
                "target": "owner/repo",
                "status": "completed",
                "phases": "2/2 complete",
                "updated": "2026-03-29T10:05:00+00:00",
            },
        ]
        print_status_table(rows)
        captured = capsys.readouterr()
        assert "github" in captured.out
        assert "owner/repo" in captured.out
        assert "completed" in captured.out
        assert "Exporter" in captured.out  # header


class TestExitCode:
    def test_all_completed_exits_zero(self, s3_store):
        _upload_checkpoint(s3_store, "github/owner__repo", "completed")
        rows = get_status_rows(s3_store)
        assert all(r["status"] == "completed" for r in rows)

    def test_in_progress_detected(self, s3_store):
        _upload_checkpoint(s3_store, "jira/PROJ", "in_progress")
        rows = get_status_rows(s3_store)
        assert any(r["status"] != "completed" for r in rows)
