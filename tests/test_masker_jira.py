"""Tests for scripts.pii_mask.maskers.jira — Presidio-first Jira masking."""

import pytest
import boto3
from moto import mock_aws

from lib.s3 import S3Store
from scripts.pii_mask.pii_store import PIIStore
from scripts.pii_mask.scanner import TextScanner
from scripts.pii_mask.maskers.jira import JiraMasker

SRC_BUCKET = "src-bucket"
DST_BUCKET = "dst-bucket"


@pytest.fixture
def store(tmp_path):
    s = PIIStore(str(tmp_path / "test.db"))
    s.add_domain("org_name.com", "example.com")
    s.add_domain("org_name.atlassian.net", "example.atlassian.net")
    # Pre-seed known identities so masking is deterministic for tests
    s.get_or_create("EMAIL_ADDRESS", "mukesh.p@org_name.com")
    s.get_or_create("PERSON", "Mukesh P")
    s.get_or_create("JIRA_ACCOUNT_ID", "5f7abc12345")
    s.get_or_create("EMAIL_ADDRESS", "aiswarya@org_name.com")
    s.get_or_create("PERSON", "Aiswarya R")
    s.get_or_create("JIRA_ACCOUNT_ID", "6a8def67890")
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
    return JiraMasker(scanner)


@pytest.fixture
def s3_env():
    with mock_aws():
        conn = boto3.client("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=SRC_BUCKET)
        conn.create_bucket(Bucket=DST_BUCKET)
        src = S3Store(bucket=SRC_BUCKET)
        dst = S3Store(bucket=DST_BUCKET)
        yield src, dst, conn


# -- Ticket masking -------------------------------------------------------- #

class TestTicketMasking:
    def test_person_fields(self, masker, scanner, store, s3_env):
        src, dst, _ = s3_env
        ticket = {
            "key": "IES-100",
            "self": "https://org_name.atlassian.net/rest/api/3/issue/326840",
            "summary": "Fix login bug",
            "description_text": "The login page is broken",
            "parent_summary": "",
            "assignee": "Mukesh P",
            "assignee_email": "mukesh.p@org_name.com",
            "assignee_account_id": "5f7abc12345",
            "reporter": "Aiswarya R",
            "reporter_email": "aiswarya@org_name.com",
            "reporter_account_id": "6a8def67890",
            "creator": "Aiswarya R",
            "creator_email": "aiswarya@org_name.com",
            "creator_account_id": "6a8def67890",
            "comments": [],
            "attachments": [],
            "changelog": [],
        }
        src.upload_json(ticket, "jira/IES/tickets/IES-100.json")
        masker.mask_file(src, dst, "jira/IES/tickets/IES-100.json")
        t = dst.download_json("jira/IES/tickets/IES-100.json")

        # Assignee fields should be replaced (not the originals)
        assert t["assignee"] != "Mukesh P"
        assert t["assignee_email"] != "mukesh.p@org_name.com"
        assert t["assignee_account_id"] != "5f7abc12345"
        # Reporter fields should be replaced
        assert t["reporter"] != "Aiswarya R"
        assert t["reporter_email"] != "aiswarya@org_name.com"
        # Domain in self URL should be remapped
        assert "org_name.atlassian.net" not in t["self"]
        assert "example.atlassian.net" in t["self"]

    def test_freeform_text_scanned_not_destroyed(self, masker, scanner, store, s3_env):
        src, dst, _ = s3_env
        ticket = {
            "key": "IES-101",
            "summary": "Mukesh P reported a login issue",
            "description_text": "As discussed with Aiswarya R, the login page crashes",
            "parent_summary": "",
            "comments": [],
            "attachments": [],
            "changelog": [],
        }
        src.upload_json(ticket, "jira/IES/tickets/IES-101.json")
        masker.mask_file(src, dst, "jira/IES/tickets/IES-101.json")
        t = dst.download_json("jira/IES/tickets/IES-101.json")

        # Names replaced but text is readable
        assert "Mukesh P" not in t["summary"]
        assert "reported a login issue" in t["summary"]

        assert "Aiswarya R" not in t["description_text"]
        assert "login page crashes" in t["description_text"]


# -- ADF ------------------------------------------------------------------- #

class TestADFMasking:
    def test_text_nodes_scanned(self, masker, s3_env):
        src, dst, _ = s3_env
        ticket = {
            "key": "IES-102",
            "summary": "Test",
            "description_text": "",
            "description_adf": {
                "type": "doc",
                "content": [
                    {"type": "paragraph", "content": [
                        {"type": "text", "text": "Please review with Mukesh P"},
                    ]},
                ],
            },
            "parent_summary": "",
            "comments": [],
            "attachments": [],
            "changelog": [],
        }
        src.upload_json(ticket, "jira/IES/tickets/IES-102.json")
        masker.mask_file(src, dst, "jira/IES/tickets/IES-102.json")
        t = dst.download_json("jira/IES/tickets/IES-102.json")

        text_node = t["description_adf"]["content"][0]["content"][0]
        assert "Mukesh P" not in text_node["text"]
        # Non-PII preserved
        assert "Please review with" in text_node["text"]

    def test_mention_nodes_text_masked(self, masker, store, s3_env):
        src, dst, _ = s3_env
        ticket = {
            "key": "IES-103",
            "summary": "Test",
            "description_text": "",
            "description_adf": {
                "type": "doc",
                "content": [
                    {"type": "paragraph", "content": [
                        {"type": "mention", "attrs": {
                            "id": "5f7abc12345",
                            "text": "Mukesh P",
                        }},
                    ]},
                ],
            },
            "parent_summary": "",
            "comments": [],
            "attachments": [],
            "changelog": [],
        }
        src.upload_json(ticket, "jira/IES/tickets/IES-103.json")
        masker.mask_file(src, dst, "jira/IES/tickets/IES-103.json")
        t = dst.download_json("jira/IES/tickets/IES-103.json")

        mention = t["description_adf"]["content"][0]["content"][0]
        # The text value is scanned by _scan_obj (Presidio detects "Mukesh P")
        assert mention["attrs"]["text"] != "Mukesh P"


# -- Comments -------------------------------------------------------------- #

class TestCommentMasking:
    def test_comment_fields_masked(self, masker, store, s3_env):
        src, dst, _ = s3_env
        ticket = {
            "key": "IES-104",
            "summary": "Test",
            "description_text": "",
            "parent_summary": "",
            "comments": [{
                "author": "Mukesh P",
                "author_email": "mukesh.p@org_name.com",
                "author_account_id": "5f7abc12345",
                "body_text": "Discussed with Aiswarya R about this",
            }],
            "attachments": [],
            "changelog": [],
        }
        src.upload_json(ticket, "jira/IES/tickets/IES-104.json")
        masker.mask_file(src, dst, "jira/IES/tickets/IES-104.json")
        t = dst.download_json("jira/IES/tickets/IES-104.json")

        c = t["comments"][0]
        assert c["author"] != "Mukesh P"
        assert c["author_email"] != "mukesh.p@org_name.com"
        assert c["author_account_id"] != "5f7abc12345"
        assert "Aiswarya R" not in c["body_text"]
        assert "Discussed with" in c["body_text"]


# -- Changelog ------------------------------------------------------------- #

class TestChangelogMasking:
    def test_assignment_changelog_masked(self, masker, store, s3_env):
        src, dst, _ = s3_env
        ticket = {
            "key": "IES-105",
            "summary": "Test",
            "description_text": "",
            "parent_summary": "",
            "comments": [],
            "attachments": [],
            "changelog": [{
                "field": "assignee",
                "author": "Aiswarya R",
                "from": "Mukesh P",
                "to": "Aiswarya R",
            }],
        }
        src.upload_json(ticket, "jira/IES/tickets/IES-105.json")
        masker.mask_file(src, dst, "jira/IES/tickets/IES-105.json")
        t = dst.download_json("jira/IES/tickets/IES-105.json")

        cl = t["changelog"][0]
        assert cl["author"] != "Aiswarya R"
        assert cl["from"] != "Mukesh P"
        assert cl["to"] != "Aiswarya R"


# -- Custom fields --------------------------------------------------------- #

class TestCustomFields:
    def test_custom_field_with_person_name_scanned(self, masker, s3_env):
        src, dst, _ = s3_env
        ticket = {
            "key": "IES-106",
            "summary": "Test",
            "description_text": "",
            "parent_summary": "",
            "Custom field (Reviewer)": "Mukesh P reviewed this",
            "comments": [],
            "attachments": [],
            "changelog": [],
        }
        src.upload_json(ticket, "jira/IES/tickets/IES-106.json")
        masker.mask_file(src, dst, "jira/IES/tickets/IES-106.json")
        t = dst.download_json("jira/IES/tickets/IES-106.json")
        # Person name in custom field should be scanned
        assert "Mukesh P" not in t["Custom field (Reviewer)"]
        assert "reviewed this" in t["Custom field (Reviewer)"]


# -- File routing ---------------------------------------------------------- #

class TestFileRouting:
    def test_skips_attachments(self, masker):
        assert not masker.should_process(
            "jira/IES/attachments/IES-100/file.json")

    def test_processes_tickets(self, masker):
        assert masker.should_process("jira/IES/tickets/IES-100.json")

    def test_skips_non_json(self, masker):
        assert not masker.should_process("jira/IES/tickets/file.txt")
