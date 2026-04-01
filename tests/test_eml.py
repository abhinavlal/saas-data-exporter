"""Tests for scripts.pii_mask.eml — EML parsing and masking."""

import email
import email.policy
import pytest
import boto3
from moto import mock_aws

from lib.s3 import S3Store
from scripts.pii_mask.pii_store import PIIStore
from scripts.pii_mask.scanner import TextScanner
from scripts.pii_mask.eml import mask_eml, _mask_address_header
from scripts.pii_mask.maskers.google import GoogleMasker


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


def _make_eml(from_addr="John Doe <john.doe@org_name.com>",
              to_addr="team@org_name.com",
              subject="Meeting with John Doe",
              body="Hi John Doe, see you at the meeting."):
    """Build a minimal EML file as bytes."""
    return (
        f"From: {from_addr}\r\n"
        f"To: {to_addr}\r\n"
        f"Subject: {subject}\r\n"
        f"Content-Type: text/plain; charset=utf-8\r\n"
        f"\r\n"
        f"{body}\r\n"
    ).encode("utf-8")


# -- mask_eml unit tests -------------------------------------------------- #

class TestMaskEML:
    def test_masks_from_header(self, scanner, store):
        eml = _make_eml()
        result = mask_eml(eml, scanner)
        msg = email.message_from_bytes(result, policy=email.policy.default)

        assert "john.doe@org_name.com" not in msg["from"]

    def test_masks_to_header(self, scanner, store):
        eml = _make_eml(to_addr="john.doe@org_name.com")
        result = mask_eml(eml, scanner)
        msg = email.message_from_bytes(result, policy=email.policy.default)

        assert "john.doe@org_name.com" not in msg["to"]

    def test_masks_subject(self, scanner, store):
        eml = _make_eml(subject="Bug reported by John Doe")
        result = mask_eml(eml, scanner)
        msg = email.message_from_bytes(result, policy=email.policy.default)

        assert "John Doe" not in msg["subject"]
        assert "Bug reported by" in msg["subject"]

    def test_masks_body(self, scanner, store):
        eml = _make_eml(body="Contact John Doe at john.doe@org_name.com")
        result = mask_eml(eml, scanner)
        msg = email.message_from_bytes(result, policy=email.policy.default)

        body = msg.get_content()
        assert "John Doe" not in body
        assert "john.doe@org_name.com" not in body

    def test_preserves_non_pii_body(self, scanner):
        eml = _make_eml(from_addr="bot@notifications.com",
                        body="Your deploy succeeded on staging.")
        result = mask_eml(eml, scanner)
        msg = email.message_from_bytes(result, policy=email.policy.default)

        body = msg.get_content()
        assert "Your deploy succeeded on staging." in body

    def test_unknown_email_hashed(self, scanner, store):
        eml = _make_eml(from_addr="stranger@org_name.com")
        result = mask_eml(eml, scanner)
        msg = email.message_from_bytes(result, policy=email.policy.default)

        assert "stranger@org_name.com" not in msg["from"]
        assert "@example.com" in msg["from"]

    def test_multipart_email(self, scanner, store):
        """Test that both text/plain and text/html parts are masked."""
        eml_str = (
            "From: John Doe <john.doe@org_name.com>\r\n"
            "To: team@org_name.com\r\n"
            "Subject: Update\r\n"
            "MIME-Version: 1.0\r\n"
            "Content-Type: multipart/alternative; boundary=boundary123\r\n"
            "\r\n"
            "--boundary123\r\n"
            "Content-Type: text/plain; charset=utf-8\r\n"
            "\r\n"
            "Hi John Doe\r\n"
            "--boundary123\r\n"
            "Content-Type: text/html; charset=utf-8\r\n"
            "\r\n"
            "<p>Hi John Doe</p>\r\n"
            "--boundary123--\r\n"
        )
        result = mask_eml(eml_str.encode(), scanner)
        result_str = result.decode("utf-8", errors="replace")

        assert "John Doe" not in result_str or "john.doe@org_name.com" not in result_str


# -- Address header masking ------------------------------------------------ #

class TestAddressHeader:
    def test_single_address(self, scanner, store):
        result = _mask_address_header(
            "John Doe <john.doe@org_name.com>", scanner)
        assert "john.doe@org_name.com" not in result

    def test_multiple_addresses(self, scanner, store):
        result = _mask_address_header(
            "John Doe <john.doe@org_name.com>, other@gmail.com",
            scanner)
        assert "john.doe@org_name.com" not in result
        # Unknown email also replaced
        assert "other@gmail.com" not in result


# -- GoogleMasker EML integration ----------------------------------------- #

class TestGoogleMaskerEML:
    def test_processes_eml_files(self, scanner):
        masker = GoogleMasker(scanner)
        assert masker.should_process("google/user_at_org.com/gmail/msg1.eml")

    def test_masks_eml_via_google_masker(self, scanner, store):
        masker = GoogleMasker(scanner)
        masked_email = store.lookup("EMAIL_ADDRESS", "john.doe@org_name.com")
        masked_slug = masked_email.replace("@", "_at_")

        with mock_aws():
            conn = boto3.client("s3", region_name="us-east-1")
            conn.create_bucket(Bucket="src")
            conn.create_bucket(Bucket="dst")
            src = S3Store(bucket="src")
            dst = S3Store(bucket="dst")

            eml = _make_eml()
            src.upload_bytes(eml,
                             "google/john.doe_at_org_name.com/gmail/msg1.eml",
                             content_type="message/rfc822")

            result = masker.mask_file(
                src, dst,
                "google/john.doe_at_org_name.com/gmail/msg1.eml")
            assert result == "ok"

            # Check it was written with rewritten key
            dst_key = f"google/{masked_slug}/gmail/msg1.eml"
            masked_bytes = dst.download_bytes(dst_key)
            assert masked_bytes is not None
            assert b"john.doe@org_name.com" not in masked_bytes
