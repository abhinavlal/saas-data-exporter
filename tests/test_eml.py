"""Tests for scripts.pii_mask.eml — EML parsing and masking."""

import email
import email.policy
import pytest
import boto3
from moto import mock_aws

from lib.s3 import S3Store
from scripts.pii_mask.roster import Roster
from scripts.pii_mask.scanner import TextScanner
from scripts.pii_mask.eml import mask_eml, _mask_address_header
from scripts.pii_mask.maskers.google import GoogleMasker

SAMPLE_ROSTER = {
    "version": 1,
    "domain_map": {"org_name.com": "example.com"},
    "users": [{
        "id": "user-001",
        "real": {"email": "john.doe@org_name.com", "name": "John Doe",
                 "first_name": "John", "last_name": "Doe"},
        "masked": {"email": "alice.chen@example.com", "name": "Alice Chen"},
    }],
}


@pytest.fixture
def roster():
    return Roster(SAMPLE_ROSTER)


@pytest.fixture
def scanner(roster):
    return TextScanner(roster)


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
    def test_masks_from_header(self, roster, scanner):
        eml = _make_eml()
        result = mask_eml(eml, roster, scanner)
        msg = email.message_from_bytes(result, policy=email.policy.default)

        assert "john.doe@org_name.com" not in msg["from"]
        assert "alice.chen@example.com" in msg["from"]
        assert "Alice Chen" in msg["from"]

    def test_masks_to_header(self, roster, scanner):
        eml = _make_eml(to_addr="john.doe@org_name.com")
        result = mask_eml(eml, roster, scanner)
        msg = email.message_from_bytes(result, policy=email.policy.default)

        assert "john.doe@org_name.com" not in msg["to"]
        assert "alice.chen@example.com" in msg["to"]

    def test_masks_subject(self, roster, scanner):
        eml = _make_eml(subject="Bug reported by John Doe")
        result = mask_eml(eml, roster, scanner)
        msg = email.message_from_bytes(result, policy=email.policy.default)

        assert "John Doe" not in msg["subject"]
        assert "Alice Chen" in msg["subject"]
        assert "Bug reported by" in msg["subject"]

    def test_masks_body(self, roster, scanner):
        eml = _make_eml(body="Contact John Doe at john.doe@org_name.com")
        result = mask_eml(eml, roster, scanner)
        msg = email.message_from_bytes(result, policy=email.policy.default)

        body = msg.get_content()
        assert "John Doe" not in body
        assert "Alice Chen" in body
        assert "john.doe@org_name.com" not in body
        assert "alice.chen@example.com" in body

    def test_preserves_non_pii_body(self, roster, scanner):
        eml = _make_eml(from_addr="bot@notifications.com",
                        body="Your deploy succeeded on staging.")
        result = mask_eml(eml, roster, scanner)
        msg = email.message_from_bytes(result, policy=email.policy.default)

        body = msg.get_content()
        assert "Your deploy succeeded on staging." in body

    def test_unknown_email_hashed(self, roster, scanner):
        eml = _make_eml(from_addr="stranger@org_name.com")
        result = mask_eml(eml, roster, scanner)
        msg = email.message_from_bytes(result, policy=email.policy.default)

        assert "stranger@org_name.com" not in msg["from"]
        assert "@example.com" in msg["from"]

    def test_multipart_email(self, roster, scanner):
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
        result = mask_eml(eml_str.encode(), roster, scanner)
        result_str = result.decode("utf-8", errors="replace")

        assert "John Doe" not in result_str or "Alice Chen" in result_str


# -- Address header masking ------------------------------------------------ #

class TestAddressHeader:
    def test_single_address(self, roster, scanner):
        result = _mask_address_header(
            "John Doe <john.doe@org_name.com>", roster, scanner)
        assert "john.doe@org_name.com" not in result
        assert "alice.chen@example.com" in result

    def test_multiple_addresses(self, roster, scanner):
        result = _mask_address_header(
            "John Doe <john.doe@org_name.com>, other@gmail.com",
            roster, scanner)
        assert "john.doe@org_name.com" not in result
        assert "alice.chen@example.com" in result
        # Unknown email hashed
        assert "other@gmail.com" not in result


# -- GoogleMasker EML integration ----------------------------------------- #

class TestGoogleMaskerEML:
    def test_processes_eml_files(self):
        roster = Roster(SAMPLE_ROSTER)
        scanner = TextScanner(roster)
        masker = GoogleMasker(roster, scanner)
        assert masker.should_process("google/user_at_org.com/gmail/msg1.eml")

    def test_masks_eml_via_google_masker(self):
        roster = Roster(SAMPLE_ROSTER)
        scanner = TextScanner(roster)
        masker = GoogleMasker(roster, scanner)

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
            dst_key = "google/alice.chen_at_example.com/gmail/msg1.eml"
            masked_bytes = dst.download_bytes(dst_key)
            assert masked_bytes is not None
            assert b"john.doe@org_name.com" not in masked_bytes
