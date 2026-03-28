"""Tests for lib.checkpoint — CheckpointManager with moto S3."""

import boto3
import pytest
from moto import mock_aws

from lib.s3 import S3Store
from lib.checkpoint import CheckpointManager, PhaseState


@pytest.fixture
def s3_store():
    with mock_aws():
        conn = boto3.client("s3", region_name="us-east-1")
        conn.create_bucket(Bucket="test-bucket")
        yield S3Store(bucket="test-bucket")


class TestLoadFresh:
    def test_no_existing_checkpoint(self, s3_store):
        cp = CheckpointManager(s3_store, "github/owner__repo")
        existed = cp.load()
        assert existed is False
        assert cp.status == "in_progress"
        assert cp.started_at is not None
        assert cp.phases == {}


class TestSaveAndLoad:
    def test_round_trip(self, s3_store):
        cp = CheckpointManager(s3_store, "github/owner__repo")
        cp.load()
        cp.start_phase("commits", total=100)
        cp.mark_item_done("commits", "sha1")
        cp.mark_item_done("commits", "sha2")
        cp.set_cursor("commits", "page_2")
        cp.save(force=True)

        cp2 = CheckpointManager(s3_store, "github/owner__repo")
        existed = cp2.load()
        assert existed is True
        assert cp2.status == "in_progress"
        assert "commits" in cp2.phases
        phase = cp2.phases["commits"]
        assert phase.status == "in_progress"
        assert phase.total == 100
        assert phase.completed == 2
        assert phase.cursor == "page_2"
        assert phase.completed_ids == {"sha1", "sha2"}


class TestPhaseTracking:
    def test_is_phase_done(self, s3_store):
        cp = CheckpointManager(s3_store, "test/job")
        cp.load()
        cp.start_phase("phase1")
        assert cp.is_phase_done("phase1") is False
        cp.complete_phase("phase1")
        assert cp.is_phase_done("phase1") is True

    def test_is_phase_done_unknown_phase(self, s3_store):
        cp = CheckpointManager(s3_store, "test/job")
        cp.load()
        assert cp.is_phase_done("nonexistent") is False


class TestItemTracking:
    def test_is_item_done(self, s3_store):
        cp = CheckpointManager(s3_store, "test/job")
        cp.load()
        cp.start_phase("items")
        assert cp.is_item_done("items", 42) is False
        cp.mark_item_done("items", 42)
        assert cp.is_item_done("items", 42) is True
        assert cp.phases["items"].completed == 1

    def test_is_item_done_unknown_phase(self, s3_store):
        cp = CheckpointManager(s3_store, "test/job")
        cp.load()
        assert cp.is_item_done("nope", "id") is False


class TestCursor:
    def test_set_and_get_cursor(self, s3_store):
        cp = CheckpointManager(s3_store, "test/job")
        cp.load()
        cp.start_phase("prs")
        cp.set_cursor("prs", "abc123")
        assert cp.get_cursor("prs") == "abc123"

    def test_get_cursor_unknown_phase(self, s3_store):
        cp = CheckpointManager(s3_store, "test/job")
        cp.load()
        assert cp.get_cursor("unknown") is None


class TestComplete:
    def test_complete_saves(self, s3_store):
        cp = CheckpointManager(s3_store, "test/job")
        cp.load()
        cp.start_phase("only_phase")
        cp.complete_phase("only_phase")
        cp.complete()

        cp2 = CheckpointManager(s3_store, "test/job")
        cp2.load()
        assert cp2.status == "completed"


class TestSaveThrottling:
    def test_throttled_save_skips(self, s3_store):
        cp = CheckpointManager(s3_store, "test/job")
        cp.load()
        cp.start_phase("p")
        cp.save(force=True)
        # Immediately save again (non-forced) — should be throttled
        cp.mark_item_done("p", "id1")
        cp.save(force=False)

        # Load and check — if throttled, the item won't be persisted
        cp2 = CheckpointManager(s3_store, "test/job")
        cp2.load()
        # The non-forced save was within SAVE_INTERVAL, so it was skipped
        assert cp2.phases["p"].completed == 0

    def test_forced_save_always_writes(self, s3_store):
        cp = CheckpointManager(s3_store, "test/job")
        cp.load()
        cp.start_phase("p")
        cp.save(force=True)
        cp.mark_item_done("p", "id1")
        cp.save(force=True)

        cp2 = CheckpointManager(s3_store, "test/job")
        cp2.load()
        assert cp2.phases["p"].completed == 1


class TestResume:
    def test_resume_skips_completed_items(self, s3_store):
        """Simulate export, crash, resume."""
        # First run — process 3 items, save checkpoint
        cp = CheckpointManager(s3_store, "test/resume")
        cp.load()
        cp.start_phase("items", total=5)
        for i in range(3):
            cp.mark_item_done("items", i)
        cp.save(force=True)

        # Second run — resume
        cp2 = CheckpointManager(s3_store, "test/resume")
        cp2.load()
        assert cp2.phases["items"].completed == 3
        remaining = [i for i in range(5) if not cp2.is_item_done("items", i)]
        assert remaining == [3, 4]
