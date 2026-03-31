"""Tests for scripts.pii_mask_bigquery — BigQuery Parquet full PII masking."""

import io
import json

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from moto import mock_aws

from lib.s3 import S3Store
from scripts.pii_mask_bigquery import (
    mask_column, mask_table, mask_bigquery_parquet,
    _mask_event_params, _redact_struct_fields, _mask_tracking_struct,
    _GEO_REDACT_FIELDS, _TRACKING_ID_FIELDS,
)

SRC_BUCKET = "src-bucket"
DST_BUCKET = "dst-bucket"
DATASET = "analytics_123"


# ── Fixtures ──────────────────────────────────────────────────────────────

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

def _upload_parquet(conn, bucket, key, table: pa.Table):
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    conn.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())


def _download_parquet(conn, bucket, key) -> pa.Table | None:
    try:
        resp = conn.get_object(Bucket=bucket, Key=key)
        return pq.read_table(io.BytesIO(resp["Body"].read()))
    except conn.exceptions.NoSuchKey:
        return None


# ── mask_column: regex replacement on flat/nested types ───────────────────

class TestMaskColumnFlat:
    def test_replaces_domain_in_string_column(self):
        arr = pa.array(["https://www.org_name.com/doctors",
                        "https://m.org_name.com/app",
                        "https://example.org"])
        result = mask_column(arr, r"org_name\.com", "example-health.com")
        assert result.to_pylist() == [
            "https://www.example-health.com/doctors",
            "https://m.example-health.com/app",
            "https://example.org",
        ]

    def test_handles_nulls(self):
        arr = pa.array(["org_name.com", None, "other.com"])
        result = mask_column(arr, r"org_name\.com", "example-health.com")
        assert result.to_pylist() == ["example-health.com", None, "other.com"]

    def test_leaves_non_string_unchanged(self):
        arr = pa.array([1, 2, 3], type=pa.int64())
        result = mask_column(arr, r"org_name\.com", "example-health.com")
        assert result.to_pylist() == [1, 2, 3]


class TestMaskColumnStruct:
    def test_replaces_in_struct_fields(self):
        arr = pa.StructArray.from_arrays(
            [pa.array(["www.org_name.com/doctors", "other.com"]),
             pa.array(["organic", "referral"])],
            names=["source", "medium"],
        )
        result = mask_column(arr, r"org_name\.com", "example-health.com")
        assert result.to_pylist()[0]["source"] == "www.example-health.com/doctors"
        assert result.to_pylist()[1]["source"] == "other.com"


class TestMaskColumnListOfStructs:
    def test_replaces_in_nested_list_struct(self):
        inner_value = pa.struct([
            pa.field("string_value", pa.string()),
            pa.field("int_value", pa.int64()),
        ])
        param_type = pa.struct([
            pa.field("key", pa.string()),
            pa.field("value", inner_value),
        ])
        params = pa.array([
            [{"key": "page_location",
              "value": {"string_value": "https://www.org_name.com/consult",
                        "int_value": None}}],
        ], type=pa.list_(param_type))

        result = mask_column(params, r"org_name\.com", "example-health.com")
        assert result.to_pylist()[0][0]["value"]["string_value"] == \
            "https://www.example-health.com/consult"


# ── Column-specific masking functions ─────────────────────────────────────

class TestRedactStructFields:
    def test_redacts_geo_fields(self):
        geo = pa.chunked_array([pa.StructArray.from_arrays(
            [pa.array(["Mumbai", "New York"]),
             pa.array(["India", "US"]),
             pa.array(["Maharashtra", "New York"]),
             pa.array(["(not set)", "(not set)"])],
            names=["city", "country", "region", "metro"],
        )])
        result = _redact_struct_fields(geo, _GEO_REDACT_FIELDS)
        rows = result.to_pylist()
        assert rows[0]["city"] == "(redacted)"
        assert rows[0]["country"] == "India"  # not redacted
        assert rows[0]["region"] == "(redacted)"
        assert rows[0]["metro"] == "(redacted)"
        assert rows[1]["country"] == "US"

    def test_preserves_nulls(self):
        geo = pa.chunked_array([pa.StructArray.from_arrays(
            [pa.array(["Mumbai", None]),
             pa.array(["India", "US"])],
            names=["city", "country"],
            mask=pa.array([False, False]),
        )])
        result = _redact_struct_fields(geo, frozenset({"city"}))
        rows = result.to_pylist()
        assert rows[0]["city"] == "(redacted)"
        assert rows[1]["city"] is None  # was null, stays null


class TestMaskEventParams:
    def test_randomizes_gclid(self):
        inner_value = pa.struct([
            pa.field("string_value", pa.string()),
            pa.field("int_value", pa.int64()),
        ])
        param_type = pa.struct([
            pa.field("key", pa.string()),
            pa.field("value", inner_value),
        ])
        params = pa.chunked_array([pa.array([
            [{"key": "gclid",
              "value": {"string_value": "Cj0KCQjw_ORIGINAL_GCLID", "int_value": None}},
             {"key": "page_location",
              "value": {"string_value": "https://www.org_name.com/", "int_value": None}}],
        ], type=pa.list_(param_type))])

        result = _mask_event_params(params)
        rows = result.to_pylist()
        # gclid should be randomized
        assert rows[0][0]["value"]["string_value"] != "Cj0KCQjw_ORIGINAL_GCLID"
        assert len(rows[0][0]["value"]["string_value"]) > 10
        # page_location NOT changed by this function (regex does it later)
        assert rows[0][1]["value"]["string_value"] == "https://www.org_name.com/"

    def test_redacts_term(self):
        inner_value = pa.struct([
            pa.field("string_value", pa.string()),
            pa.field("int_value", pa.int64()),
        ])
        param_type = pa.struct([
            pa.field("key", pa.string()),
            pa.field("value", inner_value),
        ])
        params = pa.chunked_array([pa.array([
            [{"key": "term", "value": {"string_value": "doctor near me", "int_value": None}}],
        ], type=pa.list_(param_type))])

        result = _mask_event_params(params)
        assert result.to_pylist()[0][0]["value"]["string_value"] == "(redacted)"

    def test_randomizes_transaction_id(self):
        inner_value = pa.struct([
            pa.field("string_value", pa.string()),
            pa.field("int_value", pa.int64()),
        ])
        param_type = pa.struct([
            pa.field("key", pa.string()),
            pa.field("value", inner_value),
        ])
        params = pa.chunked_array([pa.array([
            [{"key": "transaction_id",
              "value": {"string_value": "TXN-12345", "int_value": None}}],
        ], type=pa.list_(param_type))])

        result = _mask_event_params(params)
        assert result.to_pylist()[0][0]["value"]["string_value"] != "TXN-12345"

    def test_skips_null_values(self):
        inner_value = pa.struct([
            pa.field("string_value", pa.string()),
            pa.field("int_value", pa.int64()),
        ])
        param_type = pa.struct([
            pa.field("key", pa.string()),
            pa.field("value", inner_value),
        ])
        params = pa.chunked_array([pa.array([
            [{"key": "gclid", "value": {"string_value": None, "int_value": 42}}],
        ], type=pa.list_(param_type))])

        result = _mask_event_params(params)
        assert result.to_pylist()[0][0]["value"]["string_value"] is None
        assert result.to_pylist()[0][0]["value"]["int_value"] == 42


class TestMaskTrackingStruct:
    def test_randomizes_gclid_dclid_srsltid(self):
        ts_type = pa.struct([
            pa.field("manual_source", pa.string()),
            pa.field("gclid", pa.string()),
            pa.field("dclid", pa.string()),
            pa.field("srsltid", pa.string()),
        ])
        col = pa.chunked_array([pa.array([
            {"manual_source": "google", "gclid": "abc123",
             "dclid": "def456", "srsltid": "ghi789"},
            {"manual_source": "bing", "gclid": None,
             "dclid": None, "srsltid": None},
        ], type=ts_type)])

        result = _mask_tracking_struct(col, _TRACKING_ID_FIELDS)
        rows = result.to_pylist()
        assert rows[0]["manual_source"] == "google"  # not tracking, unchanged
        assert rows[0]["gclid"] != "abc123"
        assert rows[0]["dclid"] != "def456"
        assert rows[0]["srsltid"] != "ghi789"
        # Null values stay null
        assert rows[1]["gclid"] is None


# ── mask_table: full orchestration ────────────────────────────────────────

class TestMaskTable:
    def test_full_masking_on_ga4_like_table(self):
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
                            "https://www.org_name.com/doctor/dr-smith-dentist",
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
                {"source": "www.org_name.com", "medium": "referral"},
            ], type=traffic_type),
        })

        result = mask_table(table, "org_name.com", "example-health.com")

        # user_pseudo_id randomized
        assert result.column("user_pseudo_id").to_pylist()[0] != "273770909.1743358206"

        # geo redacted (city/region/metro) but country kept
        geo = result.column("geo").to_pylist()[0]
        assert geo["city"] == "(redacted)"
        assert geo["country"] == "India"
        assert geo["region"] == "(redacted)"

        # event_params: gclid randomized, term redacted
        params = result.column("event_params").to_pylist()[0]
        gclid_param = next(p for p in params if p["key"] == "gclid")
        assert gclid_param["value"]["string_value"] != "Cj0_ORIGINAL"
        term_param = next(p for p in params if p["key"] == "term")
        assert term_param["value"]["string_value"] == "(redacted)"

        # event_params: page_location has domain + doctor name masked
        loc_param = next(p for p in params if p["key"] == "page_location")
        loc = loc_param["value"]["string_value"]
        assert "org_name.com" not in loc
        assert "example-health.com" in loc
        assert "dr-smith" not in loc
        assert "/doctor/redacted" in loc

        # traffic_source: domain replaced
        ts = result.column("traffic_source").to_pylist()[0]
        assert ts["source"] == "www.example-health.com"

    def test_amp_and_translate_domain_variants(self):
        table = pa.table({
            "url": [
                "https://www-org_name-com.cdn.ampproject.org/page",
                "https://www-org_name-com.translate.goog/consult",
            ],
        })
        result = mask_table(table, "org_name.com", "example-health.com")
        urls = result.column("url").to_pylist()
        assert "www-example-health-com" in urls[0]
        assert "www-example-health-com" in urls[1]
        assert "org_name" not in urls[0].lower()
        assert "org_name" not in urls[1].lower()

    def test_consult_paths_redacted(self):
        table = pa.table({
            "url": [
                "https://www.org_name.com/consult/penis-size-question-text/q?param=1",
                "https://www.org_name.com/consult/pregnancy-question/q",
            ],
        })
        result = mask_table(table, "org_name.com", "example-health.com")
        urls = result.column("url").to_pylist()
        assert "/consult/redacted" in urls[0]
        assert "penis" not in urls[0]
        assert "/consult/redacted" in urls[1]
        assert "pregnancy" not in urls[1]

    def test_brand_name_in_page_title(self):
        table = pa.table({
            "title": ["Best Doctors | Org_Name Consult", "Org_Name Health"],
        })
        result = mask_table(table, "org_name.com", "example-health.com")
        titles = result.column("title").to_pylist()
        assert "Org_Name" not in titles[0]
        assert "ExampleHealth" in titles[0]
        assert "Org_Name" not in titles[1]

    def test_feedback_upload_ids_redacted(self):
        table = pa.table({
            "url": ["https://drive.org_name.com/feedback/upload/122989391?up=false"],
        })
        result = mask_table(table, "org_name.com", "example-health.com")
        url = result.column("url").to_pylist()[0]
        assert "122989391" not in url
        assert "/feedback/upload/0" in url

    def test_practice_and_session_ids_redacted(self):
        table = pa.table({
            "url": [
                "https://www.org_name.com/doctor/redacted?practice_id=1277810&c_sid=122145574&f_sid=122145575&gad_source=5",
            ],
        })
        result = mask_table(table, "org_name.com", "example-health.com")
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
                "https://www.org_name.com/doctor/dr-smith-dentist",
                "https://org_name.com/consult/sensitive-question/q",
            ],
        })
        _upload_parquet(conn, SRC_BUCKET,
                        f"bigquery/{DATASET}/events/20250401.parquet", day1)
        src.upload_json({"total": 2}, f"bigquery/{DATASET}/_stats.json")

        from lib.checkpoint import CheckpointManager
        cp = CheckpointManager(dst, f"pii_mask/bigquery/{DATASET}")
        cp.load()

        mask_bigquery_parquet(
            src=src, dst=dst, dataset=DATASET,
            source_domain="org_name.com",
            target_domain="example-health.com",
            checkpoint=cp,
        )

        result = _download_parquet(conn, DST_BUCKET,
                                   f"bigquery/{DATASET}/events/20250401.parquet")
        assert result is not None

        # user_pseudo_id randomized
        uids = result.column("user_pseudo_id").to_pylist()
        assert uids[0] != "abc.123"
        assert uids[1] != "def.456"

        # URLs masked
        urls = result.column("page_url").to_pylist()
        assert "org_name" not in urls[0].lower()
        assert "dr-smith" not in urls[0]
        assert "/doctor/redacted" in urls[0]
        assert "sensitive-question" not in urls[1]
        assert "/consult/redacted" in urls[1]

    def test_checkpoint_resume(self, s3_env):
        src, dst, conn = s3_env

        day1 = pa.table({"event_date": ["20250401"], "val": ["original"]})
        day2 = pa.table({"event_date": ["20250402"],
                         "val": ["https://www.org_name.com/"]})
        _upload_parquet(conn, SRC_BUCKET,
                        f"bigquery/{DATASET}/events/20250401.parquet", day1)
        _upload_parquet(conn, SRC_BUCKET,
                        f"bigquery/{DATASET}/events/20250402.parquet", day2)

        # Pre-populate checkpoint and dst for day1
        from lib.checkpoint import CheckpointManager
        cp = CheckpointManager(dst, f"pii_mask/bigquery/{DATASET}")
        cp.load()
        cp.start_phase("mask", total=2)
        cp.mark_item_done("mask",
                          f"bigquery/{DATASET}/events/20250401.parquet")
        cp.save(force=True)
        _upload_parquet(conn, DST_BUCKET,
                        f"bigquery/{DATASET}/events/20250401.parquet", day1)

        # Run
        cp2 = CheckpointManager(dst, f"pii_mask/bigquery/{DATASET}")
        cp2.load()
        mask_bigquery_parquet(
            src=src, dst=dst, dataset=DATASET,
            source_domain="org_name.com",
            target_domain="example-health.com",
            checkpoint=cp2,
        )

        # Day 1 untouched (still "original")
        r1 = _download_parquet(conn, DST_BUCKET,
                               f"bigquery/{DATASET}/events/20250401.parquet")
        assert r1.column("val").to_pylist() == ["original"]

        # Day 2 masked
        r2 = _download_parquet(conn, DST_BUCKET,
                               f"bigquery/{DATASET}/events/20250402.parquet")
        assert "org_name" not in r2.column("val").to_pylist()[0].lower()
