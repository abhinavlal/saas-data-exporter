"""Tests for lib.stats — StatsCollector with moto S3."""

import time

import boto3
import pytest
from moto import mock_aws

from lib.s3 import S3Store
from lib.stats import StatsCollector


@pytest.fixture
def s3_store():
    with mock_aws():
        conn = boto3.client("s3", region_name="us-east-1")
        conn.create_bucket(Bucket="test-bucket")
        yield S3Store(bucket="test-bucket")


class TestBasicOperations:
    def test_set(self, s3_store):
        stats = StatsCollector(s3_store, "test/_stats.json")
        stats.set("exporter", "github")
        assert stats.data["exporter"] == "github"

    def test_increment(self, s3_store):
        stats = StatsCollector(s3_store, "test/_stats.json")
        stats.increment("commits.total")
        stats.increment("commits.total")
        stats.increment("commits.total", by=3)
        assert stats.data["commits"]["total"] == 5

    def test_add_to_map(self, s3_store):
        stats = StatsCollector(s3_store, "test/_stats.json")
        stats.add_to_map("tickets.by_type", "Bug")
        stats.add_to_map("tickets.by_type", "Bug")
        stats.add_to_map("tickets.by_type", "Story", 3)
        assert stats.data["tickets"]["by_type"] == {"Bug": 2, "Story": 3}

    def test_set_nested(self, s3_store):
        stats = StatsCollector(s3_store, "test/_stats.json")
        stats.set_nested("repo.stars", 1234)
        stats.set_nested("repo.forks", 56)
        assert stats.data["repo"]["stars"] == 1234
        assert stats.data["repo"]["forks"] == 56

    def test_get(self, s3_store):
        stats = StatsCollector(s3_store, "test/_stats.json")
        stats.set_nested("commits.total", 500)
        assert stats.get("commits.total") == 500
        assert stats.get("commits.missing", "default") == "default"
        assert stats.get("nonexistent.deep.path") is None

    def test_get_top_level(self, s3_store):
        stats = StatsCollector(s3_store, "test/_stats.json")
        stats.set("exporter", "jira")
        assert stats.get("exporter") == "jira"


class TestSaveAndLoad:
    def test_save_force(self, s3_store):
        stats = StatsCollector(s3_store, "test/_stats.json")
        stats.set("exporter", "github")
        stats.increment("commits.total", 42)
        stats.save(force=True)

        # Verify it was written to S3
        data = s3_store.download_json("test/_stats.json")
        assert data["exporter"] == "github"
        assert data["commits"]["total"] == 42
        assert "updated_at" in data

    def test_load_crash_recovery(self, s3_store):
        # Simulate: first run writes stats, then crash
        stats1 = StatsCollector(s3_store, "test/_stats.json")
        stats1.set("exporter", "jira")
        stats1.increment("tickets.total", 100)
        stats1.add_to_map("tickets.by_type", "Bug", 40)
        stats1.save(force=True)

        # Simulate: second run loads existing stats
        stats2 = StatsCollector(s3_store, "test/_stats.json")
        stats2.load()
        assert stats2.data["exporter"] == "jira"
        assert stats2.get("tickets.total") == 100
        assert stats2.data["tickets"]["by_type"]["Bug"] == 40

        # Continue incrementing
        stats2.increment("tickets.total", 50)
        assert stats2.get("tickets.total") == 150

    def test_load_empty(self, s3_store):
        stats = StatsCollector(s3_store, "nonexistent/_stats.json")
        stats.load()
        assert stats.data == {}

    def test_load_non_dict(self, s3_store):
        """If someone wrote a non-dict, load should not crash."""
        s3_store.upload_json(["not", "a", "dict"], "test/_stats.json")
        stats = StatsCollector(s3_store, "test/_stats.json")
        stats.load()
        assert stats.data == {}


class TestThrottle:
    def test_throttle_skips_save(self, s3_store):
        stats = StatsCollector(s3_store, "test/_stats.json", save_interval=60)
        stats.set("exporter", "test")
        stats.save(force=True)  # first save always works

        stats.set("extra", "data")
        stats.save()  # should be throttled — within 60s

        # Reload from S3 — should NOT have "extra" key
        data = s3_store.download_json("test/_stats.json")
        assert "extra" not in data

    def test_force_bypasses_throttle(self, s3_store):
        stats = StatsCollector(s3_store, "test/_stats.json", save_interval=60)
        stats.set("exporter", "test")
        stats.save(force=True)

        stats.set("extra", "data")
        stats.save(force=True)  # force bypasses throttle

        data = s3_store.download_json("test/_stats.json")
        assert data["extra"] == "data"

    def test_save_after_interval(self, s3_store):
        stats = StatsCollector(s3_store, "test/_stats.json", save_interval=0)
        stats.set("exporter", "test")
        stats.save()  # interval=0 so always saves

        data = s3_store.download_json("test/_stats.json")
        assert data["exporter"] == "test"


class TestDeepNesting:
    def test_increment_creates_nested(self, s3_store):
        stats = StatsCollector(s3_store, "test/_stats.json")
        stats.increment("a.b.c.d", 5)
        assert stats.data["a"]["b"]["c"]["d"] == 5

    def test_add_to_map_creates_nested(self, s3_store):
        stats = StatsCollector(s3_store, "test/_stats.json")
        stats.add_to_map("attachments.by_mime_type", "image/png", 10)
        assert stats.data["attachments"]["by_mime_type"]["image/png"] == 10
