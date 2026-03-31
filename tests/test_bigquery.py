"""Tests for exporters.bigquery — BigQueryExporter with mocked BQ client and moto S3."""

import io
import json
from unittest.mock import MagicMock, patch

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from moto import mock_aws

from lib.s3 import S3Store
from lib.types import ExportConfig

PROJECT = "test-project"
DATASET = "analytics_123"


# ── Fixtures ──────────────────────────────────────────────────────────────

@pytest.fixture
def s3_env():
    with mock_aws():
        conn = boto3.client("s3", region_name="us-east-1")
        conn.create_bucket(Bucket="test-bucket")
        store = S3Store(bucket="test-bucket")
        config = ExportConfig(s3_bucket="test-bucket", max_workers=2)
        yield store, config, conn


# ── Helpers ───────────────────────────────────────────────────────────────

def _make_arrow_table(num_rows=10, date_str="20250401"):
    """Create a minimal Arrow table mimicking GA4 schema."""
    return pa.table({
        "event_date": [date_str] * num_rows,
        "event_timestamp": list(range(num_rows)),
        "event_name": ["page_view"] * num_rows,
        "user_pseudo_id": [f"user_{i}" for i in range(num_rows)],
        "platform": ["WEB"] * num_rows,
    })


def _mock_bq_client(tables: dict[str, pa.Table]):
    """Return a mock BigQuery client that serves the given date->table map.

    *tables*: mapping from date_str (e.g. "20250401") to a pyarrow Table.
    """
    client = MagicMock()
    table_map = {}  # mock_table_obj id -> arrow_table

    def get_table_effect(table_id):
        # table_id = "project.dataset.events_intraday_YYYYMMDD"
        # Extract date from the end after the last underscore
        date_str = table_id.rsplit("_", 1)[-1]
        if date_str not in tables:
            from google.api_core.exceptions import NotFound
            raise NotFound(f"Table {table_id} not found")
        mock_table = MagicMock()
        mock_table.num_rows = tables[date_str].num_rows
        table_map[id(mock_table)] = tables[date_str]
        return mock_table

    def list_rows_effect(table, **kwargs):
        arrow_table = table_map[id(table)]
        row_iter = MagicMock()
        row_iter.to_arrow_iterable.return_value = arrow_table.to_batches()
        return row_iter

    client.get_table.side_effect = get_table_effect
    client.list_rows.side_effect = list_rows_effect
    return client


def _make_exporter(s3_env, mock_client, **kwargs):
    """Create a BigQueryExporter with a pre-built mock client."""
    store, config, _ = s3_env
    with patch("exporters.bigquery.service_account.Credentials") as mock_creds:
        mock_creds.from_service_account_file.return_value = MagicMock()
        with patch("exporters.bigquery.bigquery.Client", return_value=mock_client):
            from exporters.bigquery import BigQueryExporter
            defaults = dict(
                key_path="fake-key.json",
                project=PROJECT,
                dataset=DATASET,
                s3=store,
                config=config,
                days=1,
                parallel=1,
                end_date="20250401",
            )
            defaults.update(kwargs)
            return BigQueryExporter(**defaults)


def _read_parquet(conn, s3_path):
    """Download a Parquet file from mock S3 and return as Arrow table."""
    try:
        resp = conn.get_object(Bucket="test-bucket", Key=s3_path)
        data = resp["Body"].read()
    except conn.exceptions.NoSuchKey:
        return None
    return pq.read_table(io.BytesIO(data))


def _download_raw(conn, s3_path):
    """Download raw bytes from mock S3."""
    try:
        resp = conn.get_object(Bucket="test-bucket", Key=s3_path)
        return resp["Body"].read()
    except conn.exceptions.NoSuchKey:
        return None


# ── Tests ─────────────────────────────────────────────────────────────────

class TestSingleDayExport:
    def test_exports_single_day_to_parquet(self, s3_env):
        tables = {"20250401": _make_arrow_table(num_rows=5, date_str="20250401")}
        client = _mock_bq_client(tables)
        exporter = _make_exporter(s3_env, client, days=1, end_date="20250401")

        exporter.run()

        _, _, conn = s3_env
        result = _read_parquet(conn, f"bigquery/{DATASET}/events/20250401.parquet")
        assert result is not None
        assert result.num_rows == 5
        assert result.column("event_name").to_pylist() == ["page_view"] * 5
        assert result.column("event_date").to_pylist() == ["20250401"] * 5
        assert result.column("platform").to_pylist() == ["WEB"] * 5

    def test_stats_populated_after_export(self, s3_env):
        tables = {"20250401": _make_arrow_table(num_rows=20, date_str="20250401")}
        client = _mock_bq_client(tables)
        exporter = _make_exporter(s3_env, client, days=1, end_date="20250401")

        exporter.run()

        store, _, _ = s3_env
        stats = store.download_json(f"bigquery/{DATASET}/_stats.json")
        assert stats is not None
        assert stats["exporter"] == "bigquery"
        assert stats["target"] == f"{PROJECT}.{DATASET}"
        assert stats["events"]["total_rows"] == 20
        assert stats["events"]["days_exported"] == 1
        assert stats["events"]["total_bytes"] > 0
        assert "exported_at" in stats


class TestMissingTable:
    def test_skips_missing_day_gracefully(self, s3_env):
        # No tables at all — should skip without error
        client = _mock_bq_client({})
        exporter = _make_exporter(s3_env, client, days=1, end_date="20250401")

        exporter.run()

        _, _, conn = s3_env
        # No data file should exist
        assert _download_raw(conn, f"bigquery/{DATASET}/events/20250401.parquet") is None
        # Stats should still exist but with 0 rows
        store, _, _ = s3_env
        stats = store.download_json(f"bigquery/{DATASET}/_stats.json")
        assert stats is not None
        assert stats.get("events", {}).get("total_rows", 0) == 0


class TestCheckpointResume:
    def test_resumes_from_checkpoint(self, s3_env):
        tables = {
            "20250401": _make_arrow_table(num_rows=3, date_str="20250401"),
            "20250402": _make_arrow_table(num_rows=7, date_str="20250402"),
        }
        client = _mock_bq_client(tables)
        store, config, conn = s3_env

        # Pre-populate checkpoint: day 1 already done
        from lib.checkpoint import CheckpointManager
        cp = CheckpointManager(store, f"bigquery/{DATASET}")
        cp.load()
        cp.start_phase("events", total=2)
        cp.mark_item_done("events", "20250401")
        cp.save(force=True)

        exporter = _make_exporter(s3_env, client, days=2, end_date="20250402")
        exporter.run()

        # Day 1 should NOT have been re-exported (get_table not called for it)
        get_table_calls = [str(c) for c in client.get_table.call_args_list]
        table_ids_called = [c for c in get_table_calls if "20250401" in c]
        assert len(table_ids_called) == 0, "Day 20250401 should have been skipped"

        # Day 2 should be exported
        result = _read_parquet(conn, f"bigquery/{DATASET}/events/20250402.parquet")
        assert result is not None
        assert result.num_rows == 7


class TestMultiDayExport:
    def test_exports_multiple_days_in_parallel(self, s3_env):
        tables = {
            "20250401": _make_arrow_table(num_rows=3, date_str="20250401"),
            "20250402": _make_arrow_table(num_rows=5, date_str="20250402"),
            "20250403": _make_arrow_table(num_rows=8, date_str="20250403"),
        }
        client = _mock_bq_client(tables)
        exporter = _make_exporter(
            s3_env, client, days=3, end_date="20250403", parallel=2,
        )

        exporter.run()

        _, _, conn = s3_env
        for date_str, expected_rows in [("20250401", 3), ("20250402", 5), ("20250403", 8)]:
            result = _read_parquet(conn, f"bigquery/{DATASET}/events/{date_str}.parquet")
            assert result is not None, f"Missing data for {date_str}"
            assert result.num_rows == expected_rows, f"Wrong row count for {date_str}"

    def test_stats_accumulated_across_days(self, s3_env):
        tables = {
            "20250401": _make_arrow_table(num_rows=10, date_str="20250401"),
            "20250402": _make_arrow_table(num_rows=20, date_str="20250402"),
        }
        client = _mock_bq_client(tables)
        exporter = _make_exporter(s3_env, client, days=2, end_date="20250402")

        exporter.run()

        store, _, _ = s3_env
        stats = store.download_json(f"bigquery/{DATASET}/_stats.json")
        assert stats["events"]["total_rows"] == 30
        assert stats["events"]["days_exported"] == 2


class TestParquetValidity:
    def test_output_is_valid_parquet_with_correct_schema(self, s3_env):
        tables = {"20250401": _make_arrow_table(num_rows=3, date_str="20250401")}
        client = _mock_bq_client(tables)
        exporter = _make_exporter(s3_env, client, days=1, end_date="20250401")

        exporter.run()

        _, _, conn = s3_env
        result = _read_parquet(conn, f"bigquery/{DATASET}/events/20250401.parquet")
        assert result is not None
        assert result.num_rows == 3

        # Verify schema
        names = result.schema.names
        assert "event_date" in names
        assert "event_name" in names
        assert "user_pseudo_id" in names
        assert "event_timestamp" in names
        assert "platform" in names

    def test_parquet_uses_snappy_compression(self, s3_env):
        tables = {"20250401": _make_arrow_table(num_rows=100, date_str="20250401")}
        client = _mock_bq_client(tables)
        exporter = _make_exporter(s3_env, client, days=1, end_date="20250401")

        exporter.run()

        _, _, conn = s3_env
        raw = _download_raw(conn, f"bigquery/{DATASET}/events/20250401.parquet")
        pf = pq.ParquetFile(io.BytesIO(raw))
        # Check that compression is snappy
        col_meta = pf.metadata.row_group(0).column(0)
        assert col_meta.compression == "SNAPPY"
