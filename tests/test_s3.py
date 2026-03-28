"""Tests for lib.s3 — S3Store with moto mock."""

import io
import json
import tempfile
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

from lib.s3 import S3Store


@pytest.fixture
def s3_bucket():
    with mock_aws():
        conn = boto3.client("s3", region_name="us-east-1")
        conn.create_bucket(Bucket="test-bucket")
        yield "test-bucket"


@pytest.fixture
def store(s3_bucket):
    with mock_aws():
        conn = boto3.client("s3", region_name="us-east-1")
        conn.create_bucket(Bucket="test-bucket")
        s = S3Store(bucket="test-bucket", prefix="exports")
        yield s, conn


class TestUploadBytes:
    def test_upload_and_download(self, store):
        s, conn = store
        s.upload_bytes(b"hello world", "test.txt", content_type="text/plain")

        resp = conn.get_object(Bucket="test-bucket", Key="exports/test.txt")
        assert resp["Body"].read() == b"hello world"
        assert resp["ContentType"] == "text/plain"

    def test_upload_bytes_no_prefix(self):
        with mock_aws():
            conn = boto3.client("s3", region_name="us-east-1")
            conn.create_bucket(Bucket="test-bucket")
            s = S3Store(bucket="test-bucket", prefix="")
            s.upload_bytes(b"data", "bare.txt")

            resp = conn.get_object(Bucket="test-bucket", Key="bare.txt")
            assert resp["Body"].read() == b"data"


class TestUploadJson:
    def test_round_trip(self, store):
        s, _ = store
        data = {"key": "value", "count": 42, "nested": [1, 2, 3]}
        s.upload_json(data, "data.json")

        result = s.download_json("data.json")
        assert result == data

    def test_upload_list(self, store):
        s, _ = store
        data = [{"a": 1}, {"b": 2}]
        s.upload_json(data, "list.json")
        assert s.download_json("list.json") == data


class TestDownloadJson:
    def test_returns_none_for_missing_key(self, store):
        s, _ = store
        assert s.download_json("nonexistent.json") is None


class TestExists:
    def test_exists_true(self, store):
        s, _ = store
        s.upload_bytes(b"x", "exists.txt")
        assert s.exists("exists.txt") is True

    def test_exists_false(self, store):
        s, _ = store
        assert s.exists("nope.txt") is False


class TestUploadFile:
    def test_upload_from_disk(self, store):
        s, conn = store
        with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as tmp:
            tmp.write(b"file content here")
            tmp.flush()
            s.upload_file(tmp.name, "uploaded.txt", content_type="text/plain")

        resp = conn.get_object(Bucket="test-bucket", Key="exports/uploaded.txt")
        assert resp["Body"].read() == b"file content here"


class TestUploadStream:
    def test_upload_stream(self, store):
        s, conn = store
        stream = io.BytesIO(b"streamed data")
        s.upload_stream(stream, "streamed.bin")

        resp = conn.get_object(Bucket="test-bucket", Key="exports/streamed.bin")
        assert resp["Body"].read() == b"streamed data"


class TestPrefix:
    def test_prefix_stripped(self):
        with mock_aws():
            conn = boto3.client("s3", region_name="us-east-1")
            conn.create_bucket(Bucket="test-bucket")
            s = S3Store(bucket="test-bucket", prefix="/leading/trailing/")
            s.upload_bytes(b"x", "file.txt")

            resp = conn.get_object(Bucket="test-bucket", Key="leading/trailing/file.txt")
            assert resp["Body"].read() == b"x"
