"""Tests for exporters.google_workspace — GoogleWorkspaceExporter with mocked Google API and moto S3."""

import base64
import email.mime.multipart
import email.mime.text
import email.mime.base
import io
import json
from unittest.mock import MagicMock, patch, PropertyMock

import boto3
import pytest
from moto import mock_aws

from lib.s3 import S3Store
from lib.types import ExportConfig


USER = "test@example.com"
USER_SLUG = "test_at_example.com"


# ── Fixtures ──────────────────────────────────────────────────────────────

@pytest.fixture
def s3_env():
    with mock_aws():
        conn = boto3.client("s3", region_name="us-east-1")
        conn.create_bucket(Bucket="test-bucket")
        store = S3Store(bucket="test-bucket")
        config = ExportConfig(s3_bucket="test-bucket", max_workers=2)
        yield store, config, conn


def _make_raw_email(subject="Test Email", body="Hello world", attachment_name=None, attachment_content=None):
    """Build a raw RFC 2822 email and return base64url-encoded string."""
    if attachment_name:
        msg = email.mime.multipart.MIMEMultipart()
        msg["Subject"] = subject
        msg["From"] = "sender@test.com"
        msg["To"] = "recipient@test.com"
        msg.attach(email.mime.text.MIMEText(body))
        att = email.mime.base.MIMEBase("application", "octet-stream")
        att.set_payload(attachment_content or b"attachment data")
        att.add_header("Content-Disposition", "attachment", filename=attachment_name)
        msg.attach(att)
    else:
        msg = email.mime.text.MIMEText(body)
        msg["Subject"] = subject
        msg["From"] = "sender@test.com"
        msg["To"] = "recipient@test.com"

    raw_bytes = msg.as_bytes()
    return base64.urlsafe_b64encode(raw_bytes).decode("ascii")


def _mock_gmail_service(message_ids, raw_messages):
    """Create a mock Gmail service that returns given messages."""
    service = MagicMock()

    # messages().list()
    list_resp = {"messages": [{"id": mid} for mid in message_ids]}
    service.users().messages().list().execute.return_value = list_resp

    # messages().get() — return different messages based on id
    def get_side_effect(*args, **kwargs):
        mock_req = MagicMock()
        msg_id = kwargs.get("id") or args[0] if args else None
        # Handle the chained call pattern
        mock_req.execute.return_value = raw_messages.get(msg_id, {})
        return mock_req

    service.users().messages().get.side_effect = get_side_effect

    return service


def _mock_calendar_service(events):
    """Create a mock Calendar service."""
    service = MagicMock()
    service.events().list().execute.return_value = {
        "items": events,
    }
    return service


def _mock_drive_service(files, file_contents=None):
    """Create a mock Drive service."""
    service = MagicMock()
    service.files().list().execute.return_value = {
        "files": files,
    }

    if file_contents:
        def get_media_side_effect(*args, **kwargs):
            file_id = kwargs.get("fileId")
            content = file_contents.get(file_id, b"file content")
            # Return a mock request that MediaIoBaseDownload can use
            mock_req = MagicMock()
            mock_req.execute.return_value = (MagicMock(status=200), content)
            mock_req.uri = f"https://drive.google.com/download/{file_id}"
            mock_req.headers = {}
            # We'll handle this in the _download_drive_file mock
            return mock_req

        service.files().get_media.side_effect = get_media_side_effect

        def export_media_side_effect(*args, **kwargs):
            file_id = kwargs.get("fileId")
            content = file_contents.get(file_id, b"exported content")
            mock_req = MagicMock()
            mock_req.execute.return_value = (MagicMock(status=200), content)
            mock_req.uri = f"https://drive.google.com/export/{file_id}"
            mock_req.headers = {}
            return mock_req

        service.files().export_media.side_effect = export_media_side_effect

    return service


@pytest.fixture
def mock_credentials():
    """Patch service account credentials to avoid needing a real key file."""
    with patch("exporters.google_workspace.service_account.Credentials") as mock_creds:
        mock_instance = MagicMock()
        mock_instance.with_subject.return_value = mock_instance
        mock_creds.from_service_account_file.return_value = mock_instance
        yield mock_creds


# ── Gmail Tests ───────────────────────────────────────────────────────────

class TestGmailExport:
    def test_exports_eml_files_to_s3(self, s3_env, mock_credentials):
        store, config, conn = s3_env
        raw_email = _make_raw_email(subject="Test Subject", body="Test body")
        msg1_data = {
            "id": "msg1", "threadId": "thread1", "labelIds": ["INBOX"],
            "snippet": "Test body", "internalDate": "1700000000000",
            "sizeEstimate": 500, "raw": raw_email,
        }

        with patch("exporters.google_workspace.build") as mock_build:
            mock_build.return_value = MagicMock()

            from exporters.google_workspace import GoogleWorkspaceExporter
            exporter = GoogleWorkspaceExporter(
                user=USER, service_account_key="fake.json",
                s3=store, config=config,
                email_limit=10, skip_calendar=True, skip_drive=True,
            )
            # Mock internal methods directly for reliable testing
            exporter._list_gmail_ids = MagicMock(return_value=["msg1"])
            exporter._batch_fetch_raw = MagicMock(return_value={"msg1": msg1_data})
            exporter.run()

        # Check .eml file exists
        assert store.exists(f"google/{USER_SLUG}/gmail/msg1.eml")

        # Check index
        index = store.download_json(f"google/{USER_SLUG}/gmail/_index.json")
        assert len(index) == 1
        assert index[0]["id"] == "msg1"
        assert index[0]["threadId"] == "thread1"
        assert index[0]["snippet"] == "Test body"

    def test_extracts_attachments(self, s3_env, mock_credentials):
        store, config, _ = s3_env
        raw_email = _make_raw_email(
            subject="With Attachment",
            body="See attached",
            attachment_name="report.pdf",
            attachment_content=b"PDF bytes here",
        )
        msg2_data = {
            "id": "msg2", "threadId": "t2", "labelIds": [], "snippet": "",
            "internalDate": "1700000000000", "sizeEstimate": 1000,
            "raw": raw_email,
        }

        with patch("exporters.google_workspace.build") as mock_build:
            mock_build.return_value = MagicMock()

            from exporters.google_workspace import GoogleWorkspaceExporter
            exporter = GoogleWorkspaceExporter(
                user=USER, service_account_key="fake.json",
                s3=store, config=config,
                email_limit=10, skip_calendar=True, skip_drive=True,
            )
            exporter._list_gmail_ids = MagicMock(return_value=["msg2"])
            exporter._batch_fetch_raw = MagicMock(return_value={"msg2": msg2_data})
            exporter.run()

        # Check attachment uploaded
        assert store.exists(f"google/{USER_SLUG}/gmail/attachments/msg2/report.pdf")

        # Check index lists the attachment
        index = store.download_json(f"google/{USER_SLUG}/gmail/_index.json")
        assert "report.pdf" in index[0]["attachments"]


# ── Calendar Tests ────────────────────────────────────────────────────────

class TestCalendarExport:
    def test_exports_events_and_summary(self, s3_env, mock_credentials):
        store, config, _ = s3_env
        events = [
            {
                "id": "evt1",
                "summary": "Team Standup",
                "start": {"dateTime": "2024-06-01T09:00:00Z"},
                "status": "confirmed",
                "organizer": {"email": "alice@test.com"},
                "attendees": [
                    {"email": "alice@test.com", "displayName": "Alice"},
                    {"email": "bob@test.com", "displayName": "Bob"},
                ],
                "location": "Room 101",
                "hangoutLink": "https://meet.google.com/abc",
            },
            {
                "id": "evt2",
                "summary": "1:1",
                "start": {"date": "2024-06-02"},
                "status": "confirmed",
                "organizer": {"email": "bob@test.com"},
                "attendees": [],
            },
        ]

        cal_service = _mock_calendar_service(events)

        with patch("exporters.google_workspace.build") as mock_build:
            mock_build.return_value = cal_service

            from exporters.google_workspace import GoogleWorkspaceExporter
            exporter = GoogleWorkspaceExporter(
                user=USER, service_account_key="fake.json",
                s3=store, config=config,
                skip_gmail=True, skip_drive=True,
            )
            exporter.run()

        # Check per-event files
        evt1 = store.download_json(f"google/{USER_SLUG}/calendar/events/evt1.json")
        assert evt1["summary"] == "Team Standup"
        assert evt1["location"] == "Room 101"
        evt2 = store.download_json(f"google/{USER_SLUG}/calendar/events/evt2.json")
        assert evt2["summary"] == "1:1"

        # Check _index.json
        index = store.download_json(f"google/{USER_SLUG}/calendar/_index.json")
        assert index == ["evt1", "evt2"]


# ── Drive Tests ───────────────────────────────────────────────────────────

class TestDriveExport:
    def test_exports_drive_index(self, s3_env, mock_credentials):
        store, config, _ = s3_env
        files = [
            {
                "id": "f1", "name": "Folder",
                "mimeType": "application/vnd.google-apps.folder",
                "owners": [{"displayName": "Alice", "emailAddress": "alice@test.com"}],
            },
            {
                "id": "f2", "name": "Report",
                "mimeType": "application/vnd.google-apps.document",
                "size": "5000",
                "modifiedTime": "2024-06-01T00:00:00Z",
                "owners": [{"displayName": "Alice", "emailAddress": "alice@test.com"}],
            },
        ]

        drive_service = _mock_drive_service(files)

        # Mock the export for the Google Doc
        with patch("exporters.google_workspace.build") as mock_build, \
             patch("exporters.google_workspace.MediaIoBaseDownload") as mock_dl:
            mock_build.return_value = drive_service
            # Make MediaIoBaseDownload write content and return done=True
            mock_dl_instance = MagicMock()
            mock_dl_instance.next_chunk.return_value = (None, True)
            mock_dl.side_effect = lambda fh, request: (fh.write(b"exported docx"), mock_dl_instance)[1]

            from exporters.google_workspace import GoogleWorkspaceExporter
            exporter = GoogleWorkspaceExporter(
                user=USER, service_account_key="fake.json",
                s3=store, config=config,
                skip_gmail=True, skip_calendar=True, file_limit=10,
            )
            exporter.run()

        # Check index
        index = store.download_json(f"google/{USER_SLUG}/drive/_index.json")
        assert len(index) == 2

        # Folder should be skipped
        folder_entry = next(e for e in index if e["id"] == "f1")
        assert folder_entry["downloaded"] is False
        assert folder_entry["skip_reason"] == "skipped_type"

        # Doc should be downloaded
        doc_entry = next(e for e in index if e["id"] == "f2")
        assert doc_entry["downloaded"] is True

    def test_skips_images_and_videos(self, s3_env, mock_credentials):
        store, config, _ = s3_env
        files = [
            {"id": "img1", "name": "photo.jpg", "mimeType": "image/jpeg", "owners": []},
            {"id": "vid1", "name": "movie.mp4", "mimeType": "video/mp4", "owners": []},
        ]

        drive_service = _mock_drive_service(files)

        with patch("exporters.google_workspace.build") as mock_build:
            mock_build.return_value = drive_service

            from exporters.google_workspace import GoogleWorkspaceExporter
            exporter = GoogleWorkspaceExporter(
                user=USER, service_account_key="fake.json",
                s3=store, config=config,
                skip_gmail=True, skip_calendar=True, file_limit=10,
            )
            exporter.run()

        index = store.download_json(f"google/{USER_SLUG}/drive/_index.json")
        for entry in index:
            assert entry["downloaded"] is False
            assert entry["skip_reason"] == "skipped_type"


# ── Checkpoint Tests ──────────────────────────────────────────────────────

class TestCheckpointResume:
    def test_resume_skips_completed_phases(self, s3_env, mock_credentials):
        store, config, _ = s3_env

        # Pre-populate checkpoint as fully complete
        from lib.checkpoint import CheckpointManager
        cp = CheckpointManager(store, f"google/{USER_SLUG}")
        cp.load()
        cp.start_phase("gmail")
        cp.complete_phase("gmail")
        cp.start_phase("calendar")
        cp.complete_phase("calendar")
        cp.start_phase("drive")
        cp.complete_phase("drive")
        cp.save(force=True)

        with patch("exporters.google_workspace.build") as mock_build:
            from exporters.google_workspace import GoogleWorkspaceExporter
            exporter = GoogleWorkspaceExporter(
                user=USER, service_account_key="fake.json",
                s3=store, config=config,
            )
            exporter.run()

            # build() should never be called — everything was checkpointed
            mock_build.assert_not_called()


# ── Full Export Test ──────────────────────────────────────────────────────

class TestFullExport:
    def test_all_s3_keys_created(self, s3_env, mock_credentials):
        store, config, conn = s3_env

        # Gmail
        raw_email = _make_raw_email()
        msg_data = {
            "m1": {
                "id": "m1", "threadId": "t1", "labelIds": ["INBOX"],
                "snippet": "Hi", "internalDate": "1700000000000",
                "sizeEstimate": 100, "raw": raw_email,
            },
        }
        gmail_service = _mock_gmail_service(["m1"], msg_data)

        # Calendar
        cal_events = [{"id": "e1", "summary": "Mtg", "start": {"dateTime": "2024-06-01T09:00:00Z"}, "status": "confirmed"}]
        cal_service = _mock_calendar_service(cal_events)

        # Drive — empty
        drive_service = _mock_drive_service([])

        call_count = [0]

        def build_side_effect(api, version, **kwargs):
            if api == "gmail":
                return gmail_service
            elif api == "calendar":
                return cal_service
            elif api == "drive":
                return drive_service

        with patch("exporters.google_workspace.build", side_effect=build_side_effect):
            from exporters.google_workspace import GoogleWorkspaceExporter
            exporter = GoogleWorkspaceExporter(
                user=USER, service_account_key="fake.json",
                s3=store, config=config,
                email_limit=5, event_limit=5, file_limit=5,
            )
            exporter.run()

        resp = conn.list_objects_v2(Bucket="test-bucket", Prefix=f"google/{USER_SLUG}/")
        keys = {obj["Key"] for obj in resp.get("Contents", [])}

        expected = {
            f"google/{USER_SLUG}/gmail/m1.eml",
            f"google/{USER_SLUG}/gmail/_index.json",
            f"google/{USER_SLUG}/calendar/events/e1.json",
            f"google/{USER_SLUG}/calendar/_index.json",
            f"google/{USER_SLUG}/drive/_index.json",
            f"google/{USER_SLUG}/_stats.json",
        }
        assert expected.issubset(keys), f"Missing keys: {expected - keys}"

        # Verify stats content
        stats = store.download_json(f"google/{USER_SLUG}/_stats.json")
        assert stats["exporter"] == "google_workspace"
        assert stats["target"] == USER
        assert stats["gmail"]["total_messages"] == 1
        assert stats["calendar"]["total_events"] == 1
