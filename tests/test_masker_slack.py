"""Tests for scripts.pii_mask.maskers.slack."""

import pytest
import boto3
from moto import mock_aws

from lib.s3 import S3Store
from scripts.pii_mask.roster import Roster
from scripts.pii_mask.scanner import TextScanner
from scripts.pii_mask.maskers.slack import SlackMasker

SRC_BUCKET = "src-bucket"
DST_BUCKET = "dst-bucket"

SAMPLE_ROSTER = {
    "version": 1,
    "domain_map": {"org_name.com": "example.com"},
    "users": [{
        "id": "user-001",
        "real": {"email": "john.doe@org_name.com", "name": "John Doe",
                 "first_name": "John", "last_name": "Doe",
                 "slack_user_id": "U01ABC123"},
        "masked": {"email": "alice@example.com", "name": "Alice Chen",
                   "slack_user_id": "U01MASK01"},
    }],
}


@pytest.fixture
def s3_env():
    with mock_aws():
        conn = boto3.client("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=SRC_BUCKET)
        conn.create_bucket(Bucket=DST_BUCKET)
        yield S3Store(bucket=SRC_BUCKET), S3Store(bucket=DST_BUCKET), conn


@pytest.fixture
def masker():
    roster = Roster(SAMPLE_ROSTER)
    return SlackMasker(roster, TextScanner(roster))


class TestMessageMasking:
    def test_user_id_mapped(self, masker, s3_env):
        src, dst, _ = s3_env
        messages = [
            {"user": "U01ABC123", "text": "Hello team",
             "reactions": [], "files": []},
        ]
        src.upload_json(messages, "slack/C090/messages.json")
        masker.mask_file(src, dst, "slack/C090/messages.json")
        m = dst.download_json("slack/C090/messages.json")
        assert m[0]["user"] == "U01MASK01"

    def test_mention_replaced_in_text(self, masker, s3_env):
        src, dst, _ = s3_env
        messages = [
            {"user": "U99", "text": "Hey <@U01ABC123> check this",
             "reactions": [], "files": []},
        ]
        src.upload_json(messages, "slack/C090/messages.json")
        masker.mask_file(src, dst, "slack/C090/messages.json")
        m = dst.download_json("slack/C090/messages.json")
        assert "<@U01ABC123>" not in m[0]["text"]
        assert "<@U01MASK01>" in m[0]["text"]

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
        assert "Alice Chen" in m[0]["text"]
        assert "will handle this" in m[0]["text"]

    def test_reaction_users_mapped(self, masker, s3_env):
        src, dst, _ = s3_env
        messages = [
            {"user": "U99", "text": "hi",
             "reactions": [{"name": "thumbsup",
                            "users": ["U01ABC123"]}],
             "files": []},
        ]
        src.upload_json(messages, "slack/C090/messages.json")
        masker.mask_file(src, dst, "slack/C090/messages.json")
        m = dst.download_json("slack/C090/messages.json")
        assert m[0]["reactions"][0]["users"] == ["U01MASK01"]


class TestChannelInfo:
    def test_channel_info_masked(self, masker, s3_env):
        src, dst, _ = s3_env
        info = {
            "creator": "U01ABC123",
            "topic": {"value": "John Doe's project", "creator": "U01ABC123"},
            "purpose": {"value": "Discuss features", "creator": "U01ABC123"},
        }
        src.upload_json(info, "slack/C090/channel_info.json")
        masker.mask_file(src, dst, "slack/C090/channel_info.json")
        ci = dst.download_json("slack/C090/channel_info.json")
        assert ci["creator"] == "U01MASK01"
        assert "John Doe" not in ci["topic"]["value"]
        assert ci["topic"]["creator"] == "U01MASK01"


class TestFileRouting:
    def test_skips_attachments(self, masker):
        assert not masker.should_process("slack/C090/attachments/file.json")
