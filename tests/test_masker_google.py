"""Tests for scripts.pii_mask.maskers.google — Presidio-first masking."""

import pytest
import boto3
from moto import mock_aws

from lib.s3 import S3Store
from scripts.pii_mask.pii_store import PIIStore
from scripts.pii_mask.scanner import TextScanner
from scripts.pii_mask.maskers.google import GoogleMasker

SRC_BUCKET = "src-bucket"
DST_BUCKET = "dst-bucket"


@pytest.fixture
def store(tmp_path):
    s = PIIStore(str(tmp_path / "test.db"))
    s.add_domain("org_name.com", "example.com")
    s.get_or_create("EMAIL_ADDRESS", "john.doe@org_name.com")
    s.get_or_create("PERSON", "John Doe")
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
    return GoogleMasker(scanner)


@pytest.fixture
def s3_env():
    with mock_aws():
        conn = boto3.client("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=SRC_BUCKET)
        conn.create_bucket(Bucket=DST_BUCKET)
        yield S3Store(bucket=SRC_BUCKET), S3Store(bucket=DST_BUCKET), conn


class TestCalendarMasking:
    def test_event_attendees_masked(self, masker, store, s3_env):
        src, dst, _ = s3_env
        masked_email = store.lookup("EMAIL_ADDRESS", "john.doe@org_name.com")
        masked_slug = masked_email.replace("@", "_at_")

        event = {
            "id": "evt1",
            "summary": "Meeting with John Doe",
            "description": "Discuss Q4 roadmap",
            "organizer": {"email": "john.doe@org_name.com",
                          "displayName": "John Doe"},
            "attendees": [
                {"email": "john.doe@org_name.com",
                 "displayName": "John Doe"},
            ],
        }
        src.upload_json(event,
                        "google/john.doe_at_org_name.com/calendar/events/evt1.json")
        masker.mask_file(src, dst,
                         "google/john.doe_at_org_name.com/calendar/events/evt1.json")
        dst_key = f"google/{masked_slug}/calendar/events/evt1.json"
        e = dst.download_json(dst_key)

        assert e["organizer"]["email"] != "john.doe@org_name.com"
        assert e["organizer"]["displayName"] != "John Doe"
        assert e["attendees"][0]["email"] != "john.doe@org_name.com"
        assert "John Doe" not in e["summary"]
        assert "Meeting with" in e["summary"]

    def test_event_no_pii_preserved(self, masker, store, s3_env):
        src, dst, _ = s3_env
        masked_email = store.lookup("EMAIL_ADDRESS", "john.doe@org_name.com")
        masked_slug = masked_email.replace("@", "_at_")

        event = {
            "id": "evt2",
            "summary": "Team standup",
            "organizer": {"email": "john.doe@org_name.com"},
            "attendees": [],
        }
        src.upload_json(event,
                        "google/john.doe_at_org_name.com/calendar/events/evt2.json")
        masker.mask_file(src, dst,
                         "google/john.doe_at_org_name.com/calendar/events/evt2.json")
        dst_key = f"google/{masked_slug}/calendar/events/evt2.json"
        e = dst.download_json(dst_key)
        assert e["summary"] == "Team standup"


class TestDriveMasking:
    def test_drive_file_masked(self, masker, store, s3_env):
        src, dst, _ = s3_env
        masked_email = store.lookup("EMAIL_ADDRESS", "john.doe@org_name.com")
        masked_slug = masked_email.replace("@", "_at_")

        file_meta = {
            "id": "f1",
            "title": "Q4 Report by John Doe",
            "owner_email": "john.doe@org_name.com",
            "owner_name": "John Doe",
        }
        src.upload_json(file_meta,
                        "google/john.doe_at_org_name.com/drive/f1.json")
        masker.mask_file(src, dst,
                         "google/john.doe_at_org_name.com/drive/f1.json")
        dst_key = f"google/{masked_slug}/drive/f1.json"
        f = dst.download_json(dst_key)
        assert f["owner_email"] != "john.doe@org_name.com"
        assert f["owner_name"] != "John Doe"
        assert "John Doe" not in f["title"]


class TestGmailIndex:
    def test_gmail_index_masked(self, masker, store, s3_env):
        src, dst, _ = s3_env
        masked_email = store.lookup("EMAIL_ADDRESS", "john.doe@org_name.com")
        masked_slug = masked_email.replace("@", "_at_")

        index = [
            {"id": "msg1", "from": "john.doe@org_name.com",
             "to": ["other@org_name.com"],
             "snippet": "Hi John Doe, regarding the meeting..."},
        ]
        src.upload_json(index,
                        "google/john.doe_at_org_name.com/gmail/_index.json")
        masker.mask_file(src, dst,
                         "google/john.doe_at_org_name.com/gmail/_index.json")
        dst_key = f"google/{masked_slug}/gmail/_index.json"
        idx = dst.download_json(dst_key)
        assert idx[0]["from"] != "john.doe@org_name.com"
        assert "john.doe@org_name.com" not in str(idx[0]["to"])
        assert "John Doe" not in idx[0]["snippet"]


class TestKeyRewriting:
    def test_rewrites_user_slug(self, masker, store):
        key = "google/john.doe_at_org_name.com/calendar/events/1.json"
        result = masker.rewrite_key(key)
        assert "john.doe_at_org_name.com" not in result

    def test_preserves_key_without_at(self, masker):
        key = "google/shared/calendar/events.json"
        assert masker.rewrite_key(key) == key
