"""Tests for scripts.pii_mask_bigquery — DuckDB-based BigQuery Parquet PII masking."""

import io
import os

import boto3
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from moto import mock_aws

from lib.s3 import S3Store
from scripts.pii_mask_bigquery import (
    mask_parquet,
    mask_bigquery_parquet,
    _parse_struct_fields,
)

SRC_BUCKET = "src-bucket"
DST_BUCKET = "dst-bucket"
DATASET = "analytics_123"


# ── Fixtures ──────────────────────────────────────────────────────────────

@pytest.fixture
def con():
    """Fresh DuckDB in-memory connection for each test."""
    c = duckdb.connect()
    yield c
    c.close()


@pytest.fixture
def s3_env():
    with mock_aws():
        conn = boto3.client("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=SRC_BUCKET)
        conn.create_bucket(Bucket=DST_BUCKET)
        src = S3Store(bucket=SRC_BUCKET)
        dst = S3Store(bucket=DST_BUCKET)
        yield src, dst, conn


# ── Helpers ───────────────────────────────────────────────────────────────

def _write_parquet(path, table: pa.Table):
    pq.write_table(table, str(path), compression="snappy")


def _read_parquet(path) -> pa.Table:
    return pq.read_table(str(path))


def _mask(con, tmp_path, table, src_domain="practo.com",
          dst_domain="example-health.com"):
    """Write table → mask → return result table."""
    src = str(tmp_path / "input.parquet")
    dst = str(tmp_path / "output.parquet")
    _write_parquet(src, table)
    mask_parquet(con, src, dst, src_domain, dst_domain)
    return _read_parquet(dst)


def _upload_parquet_s3(conn, bucket, key, table: pa.Table):
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    conn.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())


def _download_parquet_s3(conn, bucket, key) -> pa.Table | None:
    try:
        resp = conn.get_object(Bucket=bucket, Key=key)
        return pq.read_table(io.BytesIO(resp["Body"].read()))
    except conn.exceptions.NoSuchKey:
        return None


# ── Struct type parser ────────────────────────────────────────────────────

class TestParseStructFields:
    def test_simple_struct(self):
        fields = _parse_struct_fields("STRUCT(city VARCHAR, country VARCHAR)")
        assert fields == [("city", "VARCHAR"), ("country", "VARCHAR")]

    def test_nested_struct(self):
        fields = _parse_struct_fields(
            'STRUCT("key" VARCHAR, "value" STRUCT(string_value VARCHAR, int_value BIGINT))')
        assert fields[0] == ("key", "VARCHAR")
        assert fields[1][0] == "value"
        assert fields[1][1].startswith("STRUCT(")

    def test_non_struct(self):
        assert _parse_struct_fields("VARCHAR") == []


# ── Domain regex masking on strings ───────────────────────────────────────

class TestDomainMasking:
    def test_replaces_domain_in_string_column(self, con, tmp_path):
        table = pa.table({"url": [
            "https://www.practo.com/doctors",
            "https://m.practo.com/app",
            "https://example.org",
        ]})
        result = _mask(con, tmp_path, table)
        urls = result.column("url").to_pylist()
        assert urls[0] == "https://www.example-health.com/doctors"
        assert urls[1] == "https://m.example-health.com/app"
        assert urls[2] == "https://example.org"

    def test_handles_nulls(self, con, tmp_path):
        table = pa.table({"url": ["practo.com", None, "other.com"]})
        result = _mask(con, tmp_path, table)
        urls = result.column("url").to_pylist()
        assert urls[0] == "example-health.com"
        assert urls[1] is None
        assert urls[2] == "other.com"

    def test_leaves_non_string_unchanged(self, con, tmp_path):
        table = pa.table({"count": [1, 2, 3]})
        result = _mask(con, tmp_path, table)
        assert result.column("count").to_pylist() == [1, 2, 3]


# ── Struct with string fields ─────────────────────────────────────────────

class TestStructMasking:
    def test_replaces_domain_in_struct_fields(self, con, tmp_path):
        traffic_type = pa.struct([
            pa.field("source", pa.string()),
            pa.field("medium", pa.string()),
        ])
        table = pa.table({
            "traffic_source": pa.array([
                {"source": "www.practo.com/doctors", "medium": "organic"},
                {"source": "other.com", "medium": "referral"},
            ], type=traffic_type),
        })
        result = _mask(con, tmp_path, table)
        rows = result.column("traffic_source").to_pylist()
        assert rows[0]["source"] == "www.example-health.com/doctors"
        assert rows[1]["source"] == "other.com"


# ── List-of-structs columns (e.g. items) ──────────────────────────────────

class TestListOfStructsPassthrough:
    def test_items_list_passes_through(self, con, tmp_path):
        """GA4 'items' column is STRUCT(...)[] — must not attempt field access."""
        item_type = pa.struct([
            pa.field("item_id", pa.string()),
            pa.field("item_name", pa.string()),
            pa.field("price", pa.float64()),
        ])
        table = pa.table({
            "event_name": ["purchase"],
            "items": pa.array([
                [{"item_id": "SKU-001", "item_name": "Widget", "price": 9.99}],
            ], type=pa.list_(item_type)),
        })
        result = _mask(con, tmp_path, table)
        items = result.column("items").to_pylist()[0]
        assert items[0]["item_id"] == "SKU-001"
        assert items[0]["price"] == 9.99


# ── Geo redaction ─────────────────────────────────────────────────────────

class TestGeoRedaction:
    def test_redacts_city_region_metro(self, con, tmp_path):
        geo_type = pa.struct([
            pa.field("city", pa.string()),
            pa.field("country", pa.string()),
            pa.field("region", pa.string()),
            pa.field("metro", pa.string()),
        ])
        table = pa.table({
            "geo": pa.array([
                {"city": "Mumbai", "country": "India",
                 "region": "Maharashtra", "metro": "(not set)"},
                {"city": "New York", "country": "US",
                 "region": "New York", "metro": "(not set)"},
            ], type=geo_type),
        })
        result = _mask(con, tmp_path, table)
        rows = result.column("geo").to_pylist()
        assert rows[0]["city"] == "(redacted)"
        assert rows[0]["country"] == "India"
        assert rows[0]["region"] == "(redacted)"
        assert rows[0]["metro"] == "(redacted)"
        assert rows[1]["country"] == "US"

    def test_preserves_null_geo_fields(self, con, tmp_path):
        geo_type = pa.struct([
            pa.field("city", pa.string()),
            pa.field("country", pa.string()),
        ])
        table = pa.table({
            "geo": pa.array([
                {"city": "Mumbai", "country": "India"},
                {"city": None, "country": "US"},
            ], type=geo_type),
        })
        result = _mask(con, tmp_path, table)
        rows = result.column("geo").to_pylist()
        assert rows[0]["city"] == "(redacted)"
        assert rows[1]["city"] is None


# ── Event params masking ──────────────────────────────────────────────────

class TestEventParams:
    @pytest.fixture
    def _param_types(self):
        inner_value = pa.struct([
            pa.field("string_value", pa.string()),
            pa.field("int_value", pa.int64()),
        ])
        param_type = pa.struct([
            pa.field("key", pa.string()),
            pa.field("value", inner_value),
        ])
        return param_type

    def test_randomizes_gclid(self, con, tmp_path, _param_types):
        table = pa.table({
            "event_params": pa.array([
                [{"key": "gclid",
                  "value": {"string_value": "Cj0KCQjw_ORIGINAL", "int_value": None}},
                 {"key": "page_location",
                  "value": {"string_value": "https://www.practo.com/", "int_value": None}}],
            ], type=pa.list_(_param_types)),
        })
        result = _mask(con, tmp_path, table)
        params = result.column("event_params").to_pylist()[0]
        gclid = next(p for p in params if p["key"] == "gclid")
        assert gclid["value"]["string_value"] != "Cj0KCQjw_ORIGINAL"
        assert len(gclid["value"]["string_value"]) > 10
        # page_location should have domain masked
        loc = next(p for p in params if p["key"] == "page_location")
        assert "example-health.com" in loc["value"]["string_value"]

    def test_redacts_term(self, con, tmp_path, _param_types):
        table = pa.table({
            "event_params": pa.array([
                [{"key": "term",
                  "value": {"string_value": "doctor near me", "int_value": None}}],
            ], type=pa.list_(_param_types)),
        })
        result = _mask(con, tmp_path, table)
        params = result.column("event_params").to_pylist()[0]
        assert params[0]["value"]["string_value"] == "(redacted)"

    def test_randomizes_transaction_id(self, con, tmp_path, _param_types):
        table = pa.table({
            "event_params": pa.array([
                [{"key": "transaction_id",
                  "value": {"string_value": "TXN-12345", "int_value": None}}],
            ], type=pa.list_(_param_types)),
        })
        result = _mask(con, tmp_path, table)
        params = result.column("event_params").to_pylist()[0]
        assert params[0]["value"]["string_value"] != "TXN-12345"

    def test_preserves_null_string_value(self, con, tmp_path, _param_types):
        table = pa.table({
            "event_params": pa.array([
                [{"key": "gclid",
                  "value": {"string_value": None, "int_value": 42}}],
            ], type=pa.list_(_param_types)),
        })
        result = _mask(con, tmp_path, table)
        params = result.column("event_params").to_pylist()[0]
        # int_value preserved
        assert params[0]["value"]["int_value"] == 42


# ── Tracking struct masking ───────────────────────────────────────────────

class TestTrackingStruct:
    def test_randomizes_gclid_dclid_srsltid(self, con, tmp_path):
        ts_type = pa.struct([
            pa.field("manual_source", pa.string()),
            pa.field("gclid", pa.string()),
            pa.field("dclid", pa.string()),
            pa.field("srsltid", pa.string()),
        ])
        table = pa.table({
            "collected_traffic_source": pa.array([
                {"manual_source": "google", "gclid": "abc123",
                 "dclid": "def456", "srsltid": "ghi789"},
                {"manual_source": "bing", "gclid": None,
                 "dclid": None, "srsltid": None},
            ], type=ts_type),
        })
        result = _mask(con, tmp_path, table)
        rows = result.column("collected_traffic_source").to_pylist()
        assert rows[0]["manual_source"] == "google"  # not a tracking ID
        assert rows[0]["gclid"] != "abc123"
        assert rows[0]["dclid"] != "def456"
        assert rows[0]["srsltid"] != "ghi789"
        assert rows[1]["gclid"] is None  # null stays null


# ── User ID randomization ─────────────────────────────────────────────────

class TestUserIdMasking:
    def test_randomizes_user_pseudo_id(self, con, tmp_path):
        table = pa.table({
            "event_name": ["page_view"],
            "user_pseudo_id": ["273770909.1743358206"],
        })
        result = _mask(con, tmp_path, table)
        uid = result.column("user_pseudo_id").to_pylist()[0]
        assert uid != "273770909.1743358206"
        assert len(uid) == 32  # md5 hex

    def test_user_id_null_stays_null(self, con, tmp_path):
        table = pa.table({
            "user_id": pa.array([None, "real-user"], type=pa.string()),
        })
        result = _mask(con, tmp_path, table)
        uids = result.column("user_id").to_pylist()
        assert uids[0] is None
        assert uids[1] != "real-user"


# ── Full table masking (GA4-like schema) ──────────────────────────────────

class TestFullMasking:
    def test_full_ga4_table(self, con, tmp_path):
        inner_value = pa.struct([
            pa.field("string_value", pa.string()),
            pa.field("int_value", pa.int64()),
        ])
        param_type = pa.struct([
            pa.field("key", pa.string()),
            pa.field("value", inner_value),
        ])
        geo_type = pa.struct([
            pa.field("city", pa.string()),
            pa.field("country", pa.string()),
            pa.field("region", pa.string()),
            pa.field("metro", pa.string()),
        ])
        traffic_type = pa.struct([
            pa.field("source", pa.string()),
            pa.field("medium", pa.string()),
        ])

        table = pa.table({
            "event_name": ["page_view"],
            "user_pseudo_id": ["273770909.1743358206"],
            "event_params": pa.array([
                [{"key": "page_location",
                  "value": {"string_value":
                            "https://www.practo.com/doctor/dr-smith-dentist",
                            "int_value": None}},
                 {"key": "gclid",
                  "value": {"string_value": "Cj0_ORIGINAL", "int_value": None}},
                 {"key": "term",
                  "value": {"string_value": "best dentist", "int_value": None}}],
            ], type=pa.list_(param_type)),
            "geo": pa.array([
                {"city": "Mumbai", "country": "India",
                 "region": "Maharashtra", "metro": "(not set)"},
            ], type=geo_type),
            "traffic_source": pa.array([
                {"source": "www.practo.com", "medium": "referral"},
            ], type=traffic_type),
        })

        result = _mask(con, tmp_path, table)

        # user_pseudo_id randomized
        assert result.column("user_pseudo_id").to_pylist()[0] != "273770909.1743358206"

        # geo redacted
        geo = result.column("geo").to_pylist()[0]
        assert geo["city"] == "(redacted)"
        assert geo["country"] == "India"
        assert geo["region"] == "(redacted)"

        # event_params
        params = result.column("event_params").to_pylist()[0]
        gclid_param = next(p for p in params if p["key"] == "gclid")
        assert gclid_param["value"]["string_value"] != "Cj0_ORIGINAL"
        term_param = next(p for p in params if p["key"] == "term")
        assert term_param["value"]["string_value"] == "(redacted)"

        loc_param = next(p for p in params if p["key"] == "page_location")
        loc = loc_param["value"]["string_value"]
        assert "practo.com" not in loc
        assert "example-health.com" in loc
        assert "dr-smith" not in loc
        assert "/doctor/redacted" in loc

        # traffic_source: domain replaced
        ts = result.column("traffic_source").to_pylist()[0]
        assert ts["source"] == "www.example-health.com"

    def test_amp_and_translate_domain_variants(self, con, tmp_path):
        table = pa.table({
            "url": [
                "https://www-practo-com.cdn.ampproject.org/page",
                "https://www-practo-com.translate.goog/consult",
            ],
        })
        result = _mask(con, tmp_path, table)
        urls = result.column("url").to_pylist()
        assert "www-example-health-com" in urls[0]
        assert "www-example-health-com" in urls[1]
        assert "practo" not in urls[0].lower()
        assert "practo" not in urls[1].lower()

    def test_consult_paths_redacted(self, con, tmp_path):
        table = pa.table({
            "url": [
                "https://www.practo.com/consult/sensitive-question/q?param=1",
                "https://www.practo.com/consult/pregnancy-question/q",
            ],
        })
        result = _mask(con, tmp_path, table)
        urls = result.column("url").to_pylist()
        assert "/consult/redacted" in urls[0]
        assert "sensitive" not in urls[0]
        assert "/consult/redacted" in urls[1]
        assert "pregnancy" not in urls[1]

    def test_brand_name_in_page_title(self, con, tmp_path):
        table = pa.table({
            "title": ["Best Doctors | Practo Consult", "Practo Health"],
        })
        result = _mask(con, tmp_path, table)
        titles = result.column("title").to_pylist()
        assert "Practo" not in titles[0]
        assert "ExampleHealth" in titles[0]
        assert "Practo" not in titles[1]

    def test_feedback_upload_ids_redacted(self, con, tmp_path):
        table = pa.table({
            "url": ["https://drive.practo.com/feedback/upload/122989391?up=false"],
        })
        result = _mask(con, tmp_path, table)
        url = result.column("url").to_pylist()[0]
        assert "122989391" not in url
        assert "/feedback/upload/0" in url

    def test_practice_and_session_ids_redacted(self, con, tmp_path):
        table = pa.table({
            "url": [
                "https://www.practo.com/doctor/redacted?practice_id=1277810&c_sid=122145574&f_sid=122145575&gad_source=5",
            ],
        })
        result = _mask(con, tmp_path, table)
        url = result.column("url").to_pylist()[0]
        assert "1277810" not in url
        assert "122145574" not in url
        assert "practice_id=0" in url


# ── Integration: full pipeline with S3 ────────────────────────────────────

class TestPipelineEndToEnd:
    def test_masks_parquet_files_in_s3(self, s3_env):
        src, dst, conn = s3_env

        day1 = pa.table({
            "event_date": ["20250401"] * 2,
            "event_name": ["page_view", "click"],
            "user_pseudo_id": ["abc.123", "def.456"],
            "page_url": [
                "https://www.practo.com/doctor/dr-smith-dentist",
                "https://practo.com/consult/sensitive-question/q",
            ],
        })
        _upload_parquet_s3(conn, SRC_BUCKET,
                           f"bigquery/{DATASET}/events/20250401.parquet", day1)
        src.upload_json({"total": 2}, f"bigquery/{DATASET}/_stats.json")

        from lib.checkpoint import CheckpointManager
        cp = CheckpointManager(dst, f"pii_mask/bigquery/{DATASET}")
        cp.load()

        mask_bigquery_parquet(
            src=src, dst=dst, dataset=DATASET,
            source_domain="practo.com",
            target_domain="example-health.com",
            checkpoint=cp,
            max_workers=1,
            use_httpfs=False,
        )

        result = _download_parquet_s3(conn, DST_BUCKET,
                                      f"bigquery/{DATASET}/events/20250401.parquet")
        assert result is not None

        # user_pseudo_id randomized
        uids = result.column("user_pseudo_id").to_pylist()
        assert uids[0] != "abc.123"
        assert uids[1] != "def.456"

        # URLs masked
        urls = result.column("page_url").to_pylist()
        assert "practo" not in urls[0].lower()
        assert "dr-smith" not in urls[0]
        assert "/doctor/redacted" in urls[0]
        assert "sensitive-question" not in urls[1]
        assert "/consult/redacted" in urls[1]

    def test_checkpoint_resume(self, s3_env):
        src, dst, conn = s3_env

        day1 = pa.table({"event_date": ["20250401"], "val": ["original"]})
        day2 = pa.table({"event_date": ["20250402"],
                         "val": ["https://www.practo.com/"]})
        _upload_parquet_s3(conn, SRC_BUCKET,
                           f"bigquery/{DATASET}/events/20250401.parquet", day1)
        _upload_parquet_s3(conn, SRC_BUCKET,
                           f"bigquery/{DATASET}/events/20250402.parquet", day2)

        # Pre-populate checkpoint and dst for day1
        from lib.checkpoint import CheckpointManager
        cp = CheckpointManager(dst, f"pii_mask/bigquery/{DATASET}")
        cp.load()
        cp.start_phase("mask", total=2)
        cp.mark_item_done("mask",
                          f"bigquery/{DATASET}/events/20250401.parquet")
        cp.save(force=True)
        _upload_parquet_s3(conn, DST_BUCKET,
                           f"bigquery/{DATASET}/events/20250401.parquet", day1)

        # Run
        cp2 = CheckpointManager(dst, f"pii_mask/bigquery/{DATASET}")
        cp2.load()
        mask_bigquery_parquet(
            src=src, dst=dst, dataset=DATASET,
            source_domain="practo.com",
            target_domain="example-health.com",
            checkpoint=cp2,
            max_workers=1,
            use_httpfs=False,
        )

        # Day 1 untouched (still "original")
        r1 = _download_parquet_s3(conn, DST_BUCKET,
                                  f"bigquery/{DATASET}/events/20250401.parquet")
        assert r1.column("val").to_pylist() == ["original"]

        # Day 2 masked
        r2 = _download_parquet_s3(conn, DST_BUCKET,
                                  f"bigquery/{DATASET}/events/20250402.parquet")
        assert "practo" not in r2.column("val").to_pylist()[0].lower()

    def test_stats_copied(self, s3_env):
        src, dst, conn = s3_env

        src.upload_json({"total": 42}, f"bigquery/{DATASET}/_stats.json")

        from lib.checkpoint import CheckpointManager
        cp = CheckpointManager(dst, f"pii_mask/bigquery/{DATASET}")
        cp.load()

        mask_bigquery_parquet(
            src=src, dst=dst, dataset=DATASET,
            source_domain="practo.com",
            target_domain="example-health.com",
            checkpoint=cp,
            max_workers=1,
            use_httpfs=False,
        )

        stats = dst.download_json(f"bigquery/{DATASET}/_stats.json")
        assert stats == {"total": 42}
