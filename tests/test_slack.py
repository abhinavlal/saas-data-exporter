"""Tests for exporters.slack — SlackExporter with mocked API and moto S3."""

import json

import boto3
import pytest
import responses
from moto import mock_aws

from lib.s3 import S3Store
from lib.types import ExportConfig
from exporters.slack import SlackExporter, _is_skippable_file

SLACK_API = "https://slack.com/api"
CHANNEL = "C0TEST123"


# ── Fixtures ──────────────────────────────────────────────────────────────

@pytest.fixture
def s3_env():
    with mock_aws():
        conn = boto3.client("s3", region_name="us-east-1")
        conn.create_bucket(Bucket="test-bucket")
        store = S3Store(bucket="test-bucket")
        config = ExportConfig(s3_bucket="test-bucket", max_workers=2)
        yield store, config, conn


def _make_exporter(s3_env, **kwargs):
    store, config, _ = s3_env
    defaults = dict(
        token="xoxb-fake",
        channel_ids=[CHANNEL],
        s3=store,
        config=config,
        include_threads=False,
        skip_attachments=True,
    )
    defaults.update(kwargs)
    return SlackExporter(**defaults)


# ── Mock API helpers ──────────────────────────────────────────────────────

def mock_channel_info():
    responses.add(
        responses.GET, f"{SLACK_API}/conversations.info",
        json={
            "ok": True,
            "channel": {
                "id": CHANNEL,
                "name": "test-channel",
                "is_private": False,
                "num_members": 10,
                "topic": {"value": "Testing"},
                "purpose": {"value": "For tests"},
                "creator": "U0CREATOR",
            },
        },
        status=200,
    )


def mock_messages(messages=None, has_next=False):
    if messages is None:
        messages = [
            {"type": "message", "user": "U01", "text": "Hello!", "ts": "1700000001.000000"},
            {"type": "message", "user": "U02", "text": "Hi there", "ts": "1700000002.000000"},
        ]
    meta = {}
    if has_next:
        meta = {"response_metadata": {"next_cursor": "page2"}}
    responses.add(
        responses.GET, f"{SLACK_API}/conversations.history",
        json={"ok": True, "messages": messages, **meta},
        status=200,
    )


def mock_messages_page2():
    responses.add(
        responses.GET, f"{SLACK_API}/conversations.history",
        json={"ok": True, "messages": [
            {"type": "message", "user": "U03", "text": "Page 2", "ts": "1700000003.000000"},
        ]},
        status=200,
    )


def mock_thread_replies(thread_ts, replies):
    responses.add(
        responses.GET, f"{SLACK_API}/conversations.replies",
        json={"ok": True, "messages": replies},
        status=200,
    )


# ── Skip File Tests ───────────────────────────────────────────────────────

class TestIsSkippableFile:
    def test_skips_video(self):
        assert _is_skippable_file({"name": "video.mp4"}) is True
        assert _is_skippable_file({"name": "clip.mov"}) is True

    def test_skips_apk(self):
        assert _is_skippable_file({"name": "app.apk"}) is True

    def test_skips_tombstoned(self):
        assert _is_skippable_file({"name": "file.pdf", "mode": "tombstone"}) is True

    def test_skips_external(self):
        assert _is_skippable_file({"name": "file.pdf", "mode": "external"}) is True

    def test_allows_pdf(self):
        assert _is_skippable_file({"name": "report.pdf"}) is False

    def test_allows_png(self):
        assert _is_skippable_file({"name": "screenshot.png"}) is False

    def test_allows_no_extension(self):
        assert _is_skippable_file({"name": "README"}) is False


# ── Channel Info Tests ────────────────────────────────────────────────────

class TestChannelInfoExport:
    @responses.activate
    def test_exports_channel_info(self, s3_env):
        mock_channel_info()
        mock_messages()
        exporter = _make_exporter(s3_env)
        exporter.run()

        store, _, _ = s3_env
        info = store.download_json(f"slack/{CHANNEL}/channel_info.json")
        assert info["id"] == CHANNEL
        assert info["name"] == "test-channel"
        assert info["creator"] == "U0CREATOR"


# ── Messages Tests ────────────────────────────────────────────────────────

class TestMessagesExport:
    @responses.activate
    def test_exports_messages_as_individual_files(self, s3_env):
        mock_channel_info()
        mock_messages()
        exporter = _make_exporter(s3_env)
        exporter.run()

        store, _, _ = s3_env
        index = store.download_json(f"slack/{CHANNEL}/messages/_index.json")
        assert len(index) == 2
        # Each message has its own file
        msg1 = store.download_json(f"slack/{CHANNEL}/messages/1700000001.000000.json")
        assert msg1["text"] == "Hello!"
        msg2 = store.download_json(f"slack/{CHANNEL}/messages/1700000002.000000.json")
        assert msg2["text"] == "Hi there"

    @responses.activate
    def test_paginates_messages(self, s3_env):
        mock_channel_info()
        mock_messages(has_next=True)
        mock_messages_page2()
        exporter = _make_exporter(s3_env)
        exporter.run()

        store, _, _ = s3_env
        index = store.download_json(f"slack/{CHANNEL}/messages/_index.json")
        assert len(index) == 3


# ── Thread Reply Tests ────────────────────────────────────────────────────

class TestThreadReplies:
    @responses.activate
    def test_fetches_thread_replies(self, s3_env):
        mock_channel_info()
        # Parent message with replies
        parent_msg = {
            "type": "message", "user": "U01", "text": "Thread parent",
            "ts": "1700000001.000000", "thread_ts": "1700000001.000000",
            "reply_count": 2,
        }
        mock_messages(messages=[parent_msg])
        # Thread replies (first is the parent, rest are replies)
        mock_thread_replies("1700000001.000000", [
            {"type": "message", "user": "U01", "text": "Thread parent",
             "ts": "1700000001.000000", "thread_ts": "1700000001.000000"},
            {"type": "message", "user": "U02", "text": "Reply 1",
             "ts": "1700000001.000100", "thread_ts": "1700000001.000000"},
            {"type": "message", "user": "U03", "text": "Reply 2",
             "ts": "1700000001.000200", "thread_ts": "1700000001.000000"},
        ])

        exporter = _make_exporter(s3_env, include_threads=True)
        exporter.run()

        store, _, _ = s3_env
        index = store.download_json(f"slack/{CHANNEL}/messages/_index.json")
        # Only the parent message in index (replies embedded inside it)
        assert len(index) == 1
        # Check parent has _replies embedded
        parent = store.download_json(f"slack/{CHANNEL}/messages/1700000001.000000.json")
        assert len(parent["_replies"]) == 2
        assert parent["_replies"][0]["text"] == "Reply 1"
        assert parent["_replies"][1]["text"] == "Reply 2"


# ── Attachment Tests ──────────────────────────────────────────────────────

class TestAttachmentDownload:
    @responses.activate
    def test_downloads_attachments_to_s3(self, s3_env):
        mock_channel_info()
        msg_with_file = {
            "type": "message", "user": "U01", "text": "See attached",
            "ts": "1700000001.000000",
            "files": [
                {
                    "id": "F0FILE1",
                    "name": "report.pdf",
                    "url_private_download": "https://files.slack.com/download/report.pdf",
                },
            ],
        }
        mock_messages(messages=[msg_with_file])
        responses.add(
            responses.GET, "https://files.slack.com/download/report.pdf",
            body=b"PDF content",
            status=200,
            headers={"Content-Type": "application/pdf"},
        )

        exporter = _make_exporter(s3_env, skip_attachments=False)
        exporter.run()

        store, _, conn = s3_env
        resp = conn.get_object(Bucket="test-bucket", Key=f"slack/{CHANNEL}/attachments/F0FILE1_report.pdf")
        assert resp["Body"].read() == b"PDF content"

    @responses.activate
    def test_skips_video_files(self, s3_env):
        mock_channel_info()
        msg_with_video = {
            "type": "message", "user": "U01", "text": "Video",
            "ts": "1700000001.000000",
            "files": [
                {
                    "id": "F0VIDEO",
                    "name": "demo.mp4",
                    "url_private_download": "https://files.slack.com/download/demo.mp4",
                },
            ],
        }
        mock_messages(messages=[msg_with_video])

        exporter = _make_exporter(s3_env, skip_attachments=False)
        exporter.run()

        store, _, conn = s3_env
        # Video should not be in S3
        resp = conn.list_objects_v2(Bucket="test-bucket", Prefix=f"slack/{CHANNEL}/attachments/")
        assert resp.get("KeyCount", 0) == 0

    @responses.activate
    def test_detects_html_auth_page(self, s3_env):
        mock_channel_info()
        msg_with_file = {
            "type": "message", "user": "U01", "text": "File",
            "ts": "1700000001.000000",
            "files": [
                {
                    "id": "F0AUTH",
                    "name": "doc.pdf",
                    "url_private_download": "https://files.slack.com/download/doc.pdf",
                },
            ],
        }
        mock_messages(messages=[msg_with_file])
        responses.add(
            responses.GET, "https://files.slack.com/download/doc.pdf",
            body=b"<html>Sign in</html>",
            status=200,
            headers={"Content-Type": "text/html"},
        )

        exporter = _make_exporter(s3_env, skip_attachments=False)
        exporter.run()

        store, _, conn = s3_env
        # HTML auth page should not be uploaded
        resp = conn.list_objects_v2(Bucket="test-bucket", Prefix=f"slack/{CHANNEL}/attachments/")
        assert resp.get("KeyCount", 0) == 0

    @responses.activate
    def test_adds_local_file_reference(self, s3_env):
        mock_channel_info()
        msg_with_file = {
            "type": "message", "user": "U01", "text": "File",
            "ts": "1700000001.000000",
            "files": [
                {
                    "id": "F0REF",
                    "name": "data.csv",
                    "url_private_download": "https://files.slack.com/download/data.csv",
                },
            ],
        }
        mock_messages(messages=[msg_with_file])
        responses.add(
            responses.GET, "https://files.slack.com/download/data.csv",
            body=b"a,b\n1,2",
            status=200,
            headers={"Content-Type": "text/csv"},
        )

        exporter = _make_exporter(s3_env, skip_attachments=False)
        exporter.run()

        store, _, _ = s3_env
        msg = store.download_json(f"slack/{CHANNEL}/messages/1700000001.000000.json")
        assert msg["files"][0]["_local_file"] == "attachments/F0REF_data.csv"


# ── Checkpoint Tests ──────────────────────────────────────────────────────

class TestCheckpointResume:
    @responses.activate
    def test_resume_skips_completed(self, s3_env):
        mock_channel_info()
        mock_messages()
        exporter = _make_exporter(s3_env)
        exporter.run()

        call_count_before = len(responses.calls)
        exporter2 = _make_exporter(s3_env)
        exporter2.run()
        # No new API calls — everything checkpointed
        assert len(responses.calls) == call_count_before


# ── No SIGALRM Test ──────────────────────────────────────────────────────

class TestNoSigalrm:
    def test_no_signal_import(self):
        """Verify the slack exporter does not use SIGALRM."""
        import importlib
        import inspect
        from exporters import slack as slack_module
        source = inspect.getsource(slack_module)
        assert "SIGALRM" not in source
        assert "signal.alarm" not in source


# ── Full Export Test ──────────────────────────────────────────────────────

class TestFullExport:
    @responses.activate
    def test_all_files_created(self, s3_env):
        mock_channel_info()
        msg_with_file = {
            "type": "message", "user": "U01", "text": "File here",
            "ts": "1700000001.000000",
            "files": [
                {
                    "id": "F0FULL",
                    "name": "doc.txt",
                    "url_private_download": "https://files.slack.com/download/doc.txt",
                },
            ],
        }
        mock_messages(messages=[msg_with_file])
        responses.add(
            responses.GET, "https://files.slack.com/download/doc.txt",
            body=b"content",
            status=200,
            headers={"Content-Type": "text/plain"},
        )

        exporter = _make_exporter(s3_env, skip_attachments=False)
        exporter.run()

        store, _, conn = s3_env
        resp = conn.list_objects_v2(Bucket="test-bucket", Prefix=f"slack/{CHANNEL}/")
        keys = {obj["Key"] for obj in resp.get("Contents", [])}
        expected = {
            f"slack/{CHANNEL}/channel_info.json",
            f"slack/{CHANNEL}/messages/1700000001.000000.json",
            f"slack/{CHANNEL}/messages/_index.json",
            f"slack/{CHANNEL}/attachments/F0FULL_doc.txt",
        }
        assert expected.issubset(keys)
