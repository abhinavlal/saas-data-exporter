"""Tests for scripts.pii_mask.maskers.bigquery — BigQuery pipeline integration."""

import io
import pytest
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from moto import mock_aws

from lib.s3 import S3Store
from scripts.pii_mask.roster import Roster
from scripts.pii_mask.scanner import TextScanner
from scripts.pii_mask.maskers.bigquery import BigQueryMasker

SRC_BUCKET = "src-bucket"
DST_BUCKET = "dst-bucket"

SAMPLE_ROSTER = {
    "version": 1,
    "domain_map": {"org_name.com": "example.com"},
    "users": [],
}


@pytest.fixture
def s3_env():
    with mock_aws():
        conn = boto3.client("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=SRC_BUCKET)
        conn.create_bucket(Bucket=DST_BUCKET)
        src = S3Store(bucket=SRC_BUCKET)
        dst = S3Store(bucket=DST_BUCKET)
        yield src, dst, conn


def _upload_parquet_s3(conn, bucket, key, table: pa.Table):
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    conn.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())


def _download_parquet_s3(conn, bucket, key) -> pa.Table:
    resp = conn.get_object(Bucket=bucket, Key=key)
    buf = io.BytesIO(resp["Body"].read())
    return pq.read_table(buf)


class TestBigQueryMasker:
    def test_masks_parquet_via_pipeline_interface(self, s3_env):
        src, dst, conn = s3_env
        roster = Roster(SAMPLE_ROSTER)
        scanner = TextScanner(roster)
        masker = BigQueryMasker(
            roster, scanner,
            dataset="analytics_123",
            source_domain="org_name.com",
            target_domain="example-health.com",
            use_httpfs=False,
        )

        # Upload a simple parquet file with a URL column
        table = pa.table({
            "event_date": ["20260401"],
            "user_pseudo_id": ["abc123"],
            "page_location": ["https://www.org_name.com/doctors"],
        })
        _upload_parquet_s3(conn, SRC_BUCKET,
                           "bigquery/analytics_123/events/20260401.parquet",
                           table)

        # Mask via the BaseMasker interface
        result = masker.mask_file(src, dst,
                                  "bigquery/analytics_123/events/20260401.parquet")
        assert result.startswith("ok")

        # Verify output
        out = _download_parquet_s3(conn, DST_BUCKET,
                                   "bigquery/analytics_123/events/20260401.parquet")
        urls = out.column("page_location").to_pylist()
        assert "org_name.com" not in urls[0]

        # user_pseudo_id should be randomized (different from input)
        uids = out.column("user_pseudo_id").to_pylist()
        assert uids[0] != "abc123"

    def test_list_keys_filters_parquet(self, s3_env):
        src, dst, conn = s3_env
        roster = Roster(SAMPLE_ROSTER)
        scanner = TextScanner(roster)
        masker = BigQueryMasker(roster, scanner, dataset="ds1",
                                use_httpfs=False)

        table = pa.table({"x": [1]})
        _upload_parquet_s3(conn, SRC_BUCKET,
                           "bigquery/ds1/events/day1.parquet", table)
        src.upload_json({"total": 1}, "bigquery/ds1/_stats.json")

        keys = masker.list_keys(src)
        assert "bigquery/ds1/events/day1.parquet" in keys
        assert "bigquery/ds1/_stats.json" not in keys

    def test_should_process(self):
        roster = Roster(SAMPLE_ROSTER)
        scanner = TextScanner(roster)
        masker = BigQueryMasker(roster, scanner)
        assert masker.should_process("bigquery/ds/events/f.parquet")
        assert not masker.should_process("bigquery/ds/_stats.json")
