"""Tests for scripts.pii_mask.maskers.slack — Presidio-first masking."""

import pytest
import boto3
from moto import mock_aws

from lib.s3 import S3Store
from scripts.pii_mask.pii_store import PIIStore
from scripts.pii_mask.scanner import TextScanner
from scripts.pii_mask.maskers.slack import SlackMasker

SRC_BUCKET = "src-bucket"
DST_BUCKET = "dst-bucket"


@pytest.fixture
def store(tmp_path):
    s = PIIStore(str(tmp_path / "test.db"))
    s.add_domain("org_name.com", "example.com")
    s.get_or_create("EMAIL_ADDRESS", "john.doe@org_name.com")
    s.get_or_create("PERSON", "John Doe")
    s.get_or_create("SLACK_USER_ID", "U01ABC123")
    return s


@pytest.fixture(scope="module")
def _analyzer():
    from presidio_analyzer import AnalyzerEngine
    return AnalyzerEngine()


@pytest.fixture
def scanner(store, _analyzer):
    s = TextScanner.__new__(TextScanner)
    s._store = store
    s._threshold = 0.5
    s._analyzer = _analyzer
    return s


@pytest.fixture
def masker(scanner):
    return SlackMasker(scanner)


@pytest.fixture
def s3_env():
    with mock_aws():
        conn = boto3.client("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=SRC_BUCKET)
        conn.create_bucket(Bucket=DST_BUCKET)
        yield S3Store(bucket=SRC_BUCKET), S3Store(bucket=DST_BUCKET), conn


class TestMessageMasking:
    def test_user_id_mapped(self, masker, store, s3_env):
        src, dst, _ = s3_env
        messages = [
            {"user": "U01ABC123", "text": "Hello team",
             "reactions": [], "files": []},
        ]
        src.upload_json(messages, "slack/C090/messages.json")
        masker.mask_file(src, dst, "slack/C090/messages.json")
        m = dst.download_json("slack/C090/messages.json")
        assert m[0]["user"] != "U01ABC123"

    def test_name_mention_in_text_scanned(self, masker, store, s3_env):
        """Presidio detects person names in message text, even in mentions."""
        src, dst, _ = s3_env
        messages = [
            {"user": "U99",
             "text": "Hey John Doe please check the deployment status",
             "reactions": [], "files": []},
        ]
        src.upload_json(messages, "slack/C090/messages.json")
        masker.mask_file(src, dst, "slack/C090/messages.json")
        m = dst.download_json("slack/C090/messages.json")
        assert "John Doe" not in m[0]["text"]
        assert "check the deployment status" in m[0]["text"]

    def test_name_in_text_scanned(self, masker, s3_env):
        src, dst, _ = s3_env
        messages = [
            {"user": "U99", "text": "John Doe will handle this",
             "reactions": [], "files": []},
        ]
        src.upload_json(messages, "slack/C090/messages.json")
        masker.mask_file(src, dst, "slack/C090/messages.json")
        m = dst.download_json("slack/C090/messages.json")
        assert "John Doe" not in m[0]["text"]
        assert "will handle this" in m[0]["text"]

    def test_reaction_users_mapped(self, masker, store, s3_env):
        src, dst, _ = s3_env
        masked_uid = store.lookup("SLACK_USER_ID", "U01ABC123")
        messages = [
            {"user": "U99", "text": "hi",
             "reactions": [{"name": "thumbsup",
                            "users": ["U01ABC123"]}],
             "files": []},
        ]
        src.upload_json(messages, "slack/C090/messages.json")
        masker.mask_file(src, dst, "slack/C090/messages.json")
        m = dst.download_json("slack/C090/messages.json")
        assert m[0]["reactions"][0]["users"] == [masked_uid]


class TestChannelInfo:
    def test_channel_info_names_masked(self, masker, store, s3_env):
        src, dst, _ = s3_env
        info = {
            "creator": "U01ABC123",
            "topic": {"value": "John Doe's project", "creator": "U01ABC123"},
            "purpose": {"value": "Discuss features", "creator": "U01ABC123"},
        }
        src.upload_json(info, "slack/C090/channel_info.json")
        masker.mask_file(src, dst, "slack/C090/channel_info.json")
        ci = dst.download_json("slack/C090/channel_info.json")
        # _scan_obj processes every string via Presidio — person names detected
        assert "John Doe" not in ci["topic"]["value"]
        # Non-PII preserved
        assert "Discuss features" in ci["purpose"]["value"]


class TestFileRouting:
    def test_skips_attachments(self, masker):
        assert not masker.should_process("slack/C090/attachments/file.json")
