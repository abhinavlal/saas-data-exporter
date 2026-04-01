"""Tests for scripts.pii_mask_jira — Jira JSON PII masking."""

import boto3
import pytest
from moto import mock_aws

from lib.s3 import S3Store
from scripts.deprecated.pii_mask_jira import (
    _hash_email, _hash_name, _hash_account_id, _hash_text,
    _mask_adf, _replace_org_in_obj, _rewrite_key,
    mask_ticket, mask_jira_exports,
)

SRC_BUCKET = "src-bucket"
DST_BUCKET = "dst-bucket"


@pytest.fixture
def s3_env():
    with mock_aws():
        conn = boto3.client("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=SRC_BUCKET)
        conn.create_bucket(Bucket=DST_BUCKET)
        src = S3Store(bucket=SRC_BUCKET)
        dst = S3Store(bucket=DST_BUCKET)
        yield src, dst, conn


# ── Hash helpers ──────────────────────────────────────────────────────────

class TestHashHelpers:
    def test_hash_email(self):
        result = _hash_email("teja@org_name.com")
        assert "@example-health.com" in result
        assert "teja" not in result

    def test_hash_name(self):
        result = _hash_name("Mukesh Phadtare")
        assert result.startswith("User ")
        assert "Mukesh" not in result

    def test_hash_account_id(self):
        result = _hash_account_id("5cff4c6bf6d6e70bc5b555f9")
        assert result.startswith("acct-")
        assert "5cff4c" not in result

    def test_hash_text(self):
        result = _hash_text("customer asking for an update repeatedly")
        assert len(result) == 24
        assert "customer" not in result

    def test_deterministic(self):
        assert _hash_email("a@b.com") == _hash_email("a@b.com")
        assert _hash_name("Foo") == _hash_name("Foo")

    def test_handles_empty(self):
        assert _hash_email("") == ""
        assert _hash_name("") == ""
        assert _hash_text("") == ""
        assert _hash_account_id("") == ""


# ── Ticket masking ────────────────────────────────────────────────────────

class TestMaskTicket:
    def _sample_ticket(self):
        return {
            "key": "IES-10",
            "id": "326840",
            "self": "https://org_name.atlassian.net/rest/api/3/issue/326840",
            "summary": "Refund not processed for customer",
            "description_text": "Customer reported issue with payment",
            "description_adf": {
                "type": "doc", "version": 1,
                "content": [
                    {"type": "paragraph", "content": [
                        {"type": "text", "text": "Hi team"},
                        {"type": "mention",
                         "attrs": {"id": "5cff4c", "text": "@Mukesh"}},
                    ]},
                ],
            },
            "issue_type": "Bug",
            "status": "Done",
            "status_category": "Done",
            "priority": "High",
            "resolution": None,
            "project_key": "IES",
            "project_name": "IES Support",
            "created": "2020-02-26T17:26:07.823+0530",
            "updated": "2020-04-07T11:38:12.956+0530",
            "resolved": None,
            "due_date": None,
            "assignee": "Mukesh Phadtare",
            "assignee_email": "mukesh.p@org_name.com",
            "assignee_account_id": "5cff4c6bf6d6e70bc5b555f9",
            "reporter": "Aiswarya S",
            "reporter_email": "aiswarya@org_name.com",
            "reporter_account_id": "abc123",
            "creator": "Aiswarya S",
            "creator_email": "aiswarya@org_name.com",
            "creator_account_id": "abc123",
            "labels": [],
            "components": [],
            "fix_versions": [],
            "affected_versions": [],
            "sprint": None,
            "parent_key": "IES-1",
            "parent_summary": "Support backlog triage",
            "votes": 0,
            "watchers": 1,
            "comments": [
                {
                    "id": "161730",
                    "author": "Mukesh Phadtare",
                    "author_email": "mukesh.p@org_name.com",
                    "author_account_id": "5cff4c6bf6d6e70bc5b555f9",
                    "created": "2020-04-07T11:38:12.956+0530",
                    "updated": "2020-04-07T11:38:12.956+0530",
                    "body_text": "customer asking for an update repeatedly",
                    "body_adf": None,
                    "rendered_body": "<p>customer asking for an update</p>",
                },
            ],
            "attachments": [
                {
                    "id": "72802",
                    "filename": "screenshot.png",
                    "size": 72230,
                    "mime_type": "image/png",
                    "created": "2020-02-26T17:26:07.823+0530",
                    "author": "Mukesh Phadtare",
                    "author_email": "mukesh.p@org_name.com",
                    "content_url": "https://org_name.atlassian.net/rest/api/3/attachment/content/72802",
                },
            ],
            "changelog": [
                {"date": "2020-03-01", "author": "Aiswarya S",
                 "field": "status", "from": "Open", "to": "In Progress"},
                {"date": "2020-03-02", "author": "Aiswarya S",
                 "field": "assignee", "from": "Unassigned",
                 "to": "Mukesh Phadtare"},
            ],
            "Custom field (Organizations)": "Org_Name Technologies",
            "Custom field (Story Points)": 2.0,
        }

    def test_masks_person_fields(self):
        t = mask_ticket(self._sample_ticket())
        assert "Mukesh" not in t["assignee"]
        assert "mukesh" not in t["assignee_email"]
        assert "5cff4c" not in t["assignee_account_id"]
        assert "Aiswarya" not in t["reporter"]

    def test_hashes_text_content(self):
        t = mask_ticket(self._sample_ticket())
        assert t["summary"] != "Refund not processed for customer"
        assert len(t["summary"]) == 24
        assert t["description_text"] != "Customer reported issue with payment"
        assert t["parent_summary"] != "Support backlog triage"

    def test_masks_comments(self):
        t = mask_ticket(self._sample_ticket())
        c = t["comments"][0]
        assert "Mukesh" not in c["author"]
        assert "mukesh" not in c["author_email"]
        assert c["body_text"] != "customer asking for an update repeatedly"
        assert len(c["body_text"]) == 24
        assert c["rendered_body"] != "<p>customer asking for an update</p>"

    def test_masks_attachment_metadata(self):
        t = mask_ticket(self._sample_ticket())
        a = t["attachments"][0]
        assert "Mukesh" not in a["author"]
        assert "mukesh" not in a["author_email"]

    def test_masks_changelog(self):
        t = mask_ticket(self._sample_ticket())
        # Status change: from/to NOT hashed (not person names)
        assert t["changelog"][0]["from"] == "Open"
        assert t["changelog"][0]["to"] == "In Progress"
        # Author hashed
        assert "Aiswarya" not in t["changelog"][0]["author"]
        # Assignee change: from/to ARE hashed (person names)
        assert "Mukesh" not in t["changelog"][1]["to"]

    def test_masks_adf(self):
        t = mask_ticket(self._sample_ticket())
        adf = t["description_adf"]
        content = adf["content"][0]["content"]
        # Text node hashed
        text_node = content[0]
        assert text_node["text"] != "Hi team"
        # Mention node: id and text hashed
        mention = content[1]
        assert "5cff4c" not in mention["attrs"]["id"]
        assert "Mukesh" not in mention["attrs"]["text"]

    def test_masks_custom_fields(self):
        t = mask_ticket(self._sample_ticket())
        assert t["Custom field (Organizations)"] != "Org_Name Technologies"
        assert len(t["Custom field (Organizations)"]) == 24
        # Non-string custom fields unchanged
        assert t["Custom field (Story Points)"] == 2.0

    def test_preserves_non_pii_fields(self):
        t = mask_ticket(self._sample_ticket())
        assert t["key"] == "IES-10"
        assert t["id"] == "326840"
        assert t["issue_type"] == "Bug"
        assert t["status"] == "Done"
        assert t["priority"] == "High"
        assert t["project_key"] == "IES"
        assert t["votes"] == 0

    def test_masks_self_url(self):
        t = mask_ticket(self._sample_ticket())
        assert "org_name.atlassian.net" not in t["self"]
        assert "medica.atlassian.net" in t["self"]

    def test_deterministic(self):
        t1 = mask_ticket(self._sample_ticket())
        t2 = mask_ticket(self._sample_ticket())
        assert t1["assignee"] == t2["assignee"]
        assert t1["summary"] == t2["summary"]


# ── Org replacement ───────────────────────────────────────────────────────

class TestOrgReplacement:
    def test_replaces_in_strings(self):
        result = _replace_org_in_obj({"url": "https://org_name.atlassian.net"})
        assert result["url"] == "https://medica.atlassian.net"

    def test_case_preserving(self):
        result = _replace_org_in_obj({"name": "Org_name Technologies"})
        assert result["name"] == "Medica Technologies"

    def test_rewrite_key(self):
        assert _rewrite_key("jira/IES/tickets/IES-10.json") == \
            "jira/IES/tickets/IES-10.json"  # no org_name in key


# ── Integration ───────────────────────────────────────────────────────────

class TestPipelineEndToEnd:
    def test_masks_tickets_and_skips_attachments(self, s3_env):
        src, dst, conn = s3_env

        src.upload_json({
            "key": "IES-1", "id": "1",
            "self": "https://org_name.atlassian.net/rest/api/3/issue/1",
            "summary": "Bug report",
            "description_text": "Details here",
            "description_adf": None,
            "issue_type": "Bug", "status": "Open",
            "status_category": "To Do", "priority": "High",
            "resolution": None, "project_key": "IES",
            "project_name": "IES", "created": "2024-01-01",
            "updated": "2024-01-01", "resolved": None,
            "due_date": None,
            "assignee": "Alice", "assignee_email": "alice@org_name.com",
            "assignee_account_id": "abc",
            "reporter": "Bob", "reporter_email": "bob@org_name.com",
            "reporter_account_id": "def",
            "creator": "Bob", "creator_email": "bob@org_name.com",
            "creator_account_id": "def",
            "labels": [], "components": [],
            "fix_versions": [], "affected_versions": [],
            "sprint": None, "parent_key": None,
            "parent_summary": None, "votes": 0, "watchers": 0,
            "comments": [], "attachments": [], "changelog": [],
        }, "jira/IES/tickets/IES-1.json")

        # This should be listed but skipped
        src.upload_json({"data": "binary placeholder"},
                        "jira/IES/attachments/IES-1/file.json")

        src.upload_json({"total": 1}, "jira/IES/_stats.json")

        from lib.checkpoint import CheckpointManager
        cp = CheckpointManager(dst, "pii_mask/jira")
        cp.load()
        mask_jira_exports(src=src, dst=dst, checkpoint=cp, max_workers=1)

        # Ticket masked
        t = dst.download_json("jira/IES/tickets/IES-1.json")
        assert t is not None
        assert "Alice" not in t["assignee"]
        assert "alice@org_name.com" not in t["assignee_email"]
        assert "medica.atlassian.net" in t["self"]

        # Stats copied
        stats = dst.download_json("jira/IES/_stats.json")
        assert stats == {"total": 1}

        # Attachment NOT copied
        att = dst.download_json("jira/IES/attachments/IES-1/file.json")
        assert att is None

    def test_checkpoint_resume(self, s3_env):
        src, dst, conn = s3_env

        src.upload_json({
            "key": "P-1", "id": "1",
            "self": "", "summary": "t",
            "description_text": "", "description_adf": None,
            "issue_type": "Task", "status": "Open",
            "status_category": "To Do", "priority": "Normal",
            "resolution": None, "project_key": "P",
            "project_name": "P", "created": "2024-01-01",
            "updated": "2024-01-01", "resolved": None,
            "due_date": None,
            "assignee": "a", "assignee_email": "a@x.com",
            "assignee_account_id": "a1",
            "reporter": "", "reporter_email": "",
            "reporter_account_id": "",
            "creator": "", "creator_email": "",
            "creator_account_id": "",
            "labels": [], "components": [],
            "fix_versions": [], "affected_versions": [],
            "sprint": None, "parent_key": None,
            "parent_summary": None, "votes": 0, "watchers": 0,
            "comments": [], "attachments": [], "changelog": [],
        }, "jira/P/tickets/P-1.json")

        src.upload_json({
            "key": "P-2", "id": "2",
            "self": "", "summary": "fix",
            "description_text": "", "description_adf": None,
            "issue_type": "Bug", "status": "Open",
            "status_category": "To Do", "priority": "Normal",
            "resolution": None, "project_key": "P",
            "project_name": "P", "created": "2024-01-01",
            "updated": "2024-01-01", "resolved": None,
            "due_date": None,
            "assignee": "Bob", "assignee_email": "bob@x.com",
            "assignee_account_id": "b1",
            "reporter": "", "reporter_email": "",
            "reporter_account_id": "",
            "creator": "", "creator_email": "",
            "creator_account_id": "",
            "labels": [], "components": [],
            "fix_versions": [], "affected_versions": [],
            "sprint": None, "parent_key": None,
            "parent_summary": None, "votes": 0, "watchers": 0,
            "comments": [], "attachments": [], "changelog": [],
        }, "jira/P/tickets/P-2.json")

        from lib.checkpoint import CheckpointManager
        cp = CheckpointManager(dst, "pii_mask/jira")
        cp.load()
        cp.start_phase("mask", total=2)
        cp.mark_item_done("mask", "jira/P/tickets/P-1.json")
        cp.save(force=True)
        dst.upload_json({"key": "P-1", "assignee": "a"},
                        "jira/P/tickets/P-1.json")

        cp2 = CheckpointManager(dst, "pii_mask/jira")
        cp2.load()
        mask_jira_exports(src=src, dst=dst, checkpoint=cp2, max_workers=1)

        # P-1 untouched
        t1 = dst.download_json("jira/P/tickets/P-1.json")
        assert t1["assignee"] == "a"

        # P-2 masked
        t2 = dst.download_json("jira/P/tickets/P-2.json")
        assert "Bob" not in t2["assignee"]
