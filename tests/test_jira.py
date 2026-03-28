"""Tests for exporters.jira — JiraExporter with mocked API and moto S3."""

import json

import boto3
import pytest
import responses
from moto import mock_aws

from lib.s3 import S3Store
from lib.types import ExportConfig
from exporters.jira import JiraExporter, extract_text_from_adf

DOMAIN = "test.atlassian.net"
BASE = f"https://{DOMAIN}/rest/api/3"
PROJECT = "TEST"


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
        token="fake-token",
        email="test@test.com",
        domain=DOMAIN,
        projects=[PROJECT],
        s3=store,
        config=config,
        limit=5,
        skip_attachments=True,
        skip_comments=True,
    )
    defaults.update(kwargs)
    return JiraExporter(**defaults)


# ── Mock API helpers ──────────────────────────────────────────────────────

def _make_issue(key, summary="Test ticket", has_attachment=False):
    issue = {
        "key": key,
        "id": key.split("-")[1],
        "self": f"{BASE}/issue/{key}",
        "fields": {
            "summary": summary,
            "description": {
                "type": "doc",
                "version": 1,
                "content": [
                    {"type": "paragraph", "content": [
                        {"type": "text", "text": "Description text here"},
                    ]},
                ],
            },
            "issuetype": {"name": "Bug"},
            "status": {"name": "Open", "statusCategory": {"name": "To Do"}},
            "priority": {"name": "High"},
            "resolution": None,
            "project": {"key": PROJECT, "name": "Test Project"},
            "created": "2024-01-01T00:00:00.000+0000",
            "updated": "2024-06-01T00:00:00.000+0000",
            "resolutiondate": None,
            "duedate": "2024-12-31",
            "assignee": {"displayName": "Alice", "emailAddress": "alice@test.com", "accountId": "acc1"},
            "reporter": {"displayName": "Bob", "emailAddress": "bob@test.com", "accountId": "acc2"},
            "creator": {"displayName": "Bob", "emailAddress": "bob@test.com", "accountId": "acc2"},
            "labels": ["bug", "urgent"],
            "components": [{"name": "Backend"}],
            "fixVersions": [{"name": "1.0"}],
            "versions": [{"name": "0.9"}],
            "sprint": {"name": "Sprint 5"},
            "parent": {"key": "TEST-1", "fields": {"summary": "Epic"}},
            "timetracking": {"originalEstimate": "2h", "remainingEstimate": "1h", "timeSpent": "1h"},
            "votes": {"votes": 3},
            "watches": {"watchCount": 5},
            "attachment": [],
            "customfield_10001": {"value": "Custom Value"},
            "customfield_10002": None,
        },
        "changelog": {
            "histories": [
                {
                    "author": {"displayName": "Alice"},
                    "created": "2024-03-01T00:00:00.000+0000",
                    "items": [
                        {"field": "status", "fromString": "Open", "toString": "In Progress"},
                    ],
                },
            ],
        },
        "renderedFields": {},
    }
    if has_attachment:
        issue["fields"]["attachment"] = [
            {
                "id": "att1",
                "filename": "report.pdf",
                "size": 1024,
                "mimeType": "application/pdf",
                "created": "2024-01-15T00:00:00.000+0000",
                "author": {"displayName": "Alice", "emailAddress": "alice@test.com"},
                "content": f"{BASE}/attachment/content/att1",
            },
        ]
    return issue


def mock_field_api():
    responses.add(
        responses.GET, f"{BASE}/field",
        json=[
            {"id": "customfield_10001", "name": "CC"},
            {"id": "customfield_10002", "name": "L2 Assignee"},
            {"id": "summary", "name": "Summary"},
        ],
        status=200,
    )


def mock_search_api(issues):
    responses.add(
        responses.POST, f"{BASE}/search/jql",
        json={"issues": issues, "nextPageToken": None},
        status=200,
    )


def mock_comments_api(ticket_key, comments=None):
    if comments is None:
        comments = [
            {
                "id": "c1",
                "author": {"displayName": "Alice", "emailAddress": "alice@test.com", "accountId": "acc1"},
                "created": "2024-02-01T00:00:00.000+0000",
                "updated": "2024-02-01T00:00:00.000+0000",
                "body": {
                    "type": "doc",
                    "version": 1,
                    "content": [{"type": "paragraph", "content": [{"type": "text", "text": "A comment"}]}],
                },
                "renderedBody": "<p>A comment</p>",
            },
        ]
    responses.add(
        responses.GET, f"{BASE}/issue/{ticket_key}/comment",
        json={"comments": comments, "total": len(comments)},
        status=200,
    )


# ── ADF Tests ─────────────────────────────────────────────────────────────

class TestExtractTextFromAdf:
    def test_simple_text(self):
        adf = {
            "type": "doc",
            "content": [
                {"type": "paragraph", "content": [
                    {"type": "text", "text": "Hello world"},
                ]},
            ],
        }
        assert extract_text_from_adf(adf) == "Hello world"

    def test_nested_paragraphs(self):
        adf = {
            "type": "doc",
            "content": [
                {"type": "paragraph", "content": [
                    {"type": "text", "text": "First. "},
                ]},
                {"type": "paragraph", "content": [
                    {"type": "text", "text": "Second."},
                ]},
            ],
        }
        assert extract_text_from_adf(adf) == "First. Second."

    def test_mention_node(self):
        adf = {
            "type": "doc",
            "content": [
                {"type": "paragraph", "content": [
                    {"type": "text", "text": "Assigned to "},
                    {"type": "mention", "attrs": {"id": "acc1", "text": "@Alice"}},
                ]},
            ],
        }
        assert extract_text_from_adf(adf) == "Assigned to @Alice"

    def test_hard_break(self):
        adf = {
            "type": "doc",
            "content": [
                {"type": "paragraph", "content": [
                    {"type": "text", "text": "Line 1"},
                    {"type": "hardBreak"},
                    {"type": "text", "text": "Line 2"},
                ]},
            ],
        }
        assert extract_text_from_adf(adf) == "Line 1\nLine 2"

    def test_none_input(self):
        assert extract_text_from_adf(None) == ""

    def test_deeply_nested(self):
        adf = {
            "type": "doc",
            "content": [
                {"type": "bulletList", "content": [
                    {"type": "listItem", "content": [
                        {"type": "paragraph", "content": [
                            {"type": "text", "text": "Item 1"},
                        ]},
                    ]},
                    {"type": "listItem", "content": [
                        {"type": "paragraph", "content": [
                            {"type": "text", "text": "Item 2"},
                        ]},
                    ]},
                ]},
            ],
        }
        assert extract_text_from_adf(adf) == "Item 1Item 2"


# ── Exporter Tests ────────────────────────────────────────────────────────

class TestTicketExport:
    @responses.activate
    def test_exports_tickets_to_s3(self, s3_env):
        mock_field_api()
        issues = [_make_issue("TEST-1"), _make_issue("TEST-2", summary="Second ticket")]
        mock_search_api(issues)

        exporter = _make_exporter(s3_env)
        exporter.run()

        store, _, _ = s3_env
        tickets = store.download_json(f"jira/{PROJECT}/tickets.json")
        assert len(tickets) == 2
        t = tickets[0]
        assert t["key"] == "TEST-1"
        assert t["summary"] == "Test ticket"
        assert t["description_text"] == "Description text here"
        assert t["issue_type"] == "Bug"
        assert t["status"] == "Open"
        assert t["status_category"] == "To Do"
        assert t["assignee"] == "Alice"
        assert t["assignee_email"] == "alice@test.com"
        assert t["reporter"] == "Bob"
        assert t["labels"] == ["bug", "urgent"]
        assert t["components"] == ["Backend"]
        assert t["fix_versions"] == ["1.0"]
        assert t["sprint"] == "Sprint 5"
        assert t["parent_key"] == "TEST-1"
        assert t["votes"] == 3
        assert t["watchers"] == 5

    @responses.activate
    def test_custom_fields_renamed(self, s3_env):
        mock_field_api()
        mock_search_api([_make_issue("TEST-1")])

        exporter = _make_exporter(s3_env)
        exporter.run()

        store, _, _ = s3_env
        tickets = store.download_json(f"jira/{PROJECT}/tickets.json")
        t = tickets[0]
        assert t["Custom field (CC)"] == "Custom Value"
        assert "customfield_10001" not in t

    @responses.activate
    def test_changelog_parsed(self, s3_env):
        mock_field_api()
        mock_search_api([_make_issue("TEST-1")])

        exporter = _make_exporter(s3_env)
        exporter.run()

        store, _, _ = s3_env
        tickets = store.download_json(f"jira/{PROJECT}/tickets.json")
        changelog = tickets[0]["changelog"]
        assert len(changelog) == 1
        assert changelog[0]["field"] == "status"
        assert changelog[0]["from"] == "Open"
        assert changelog[0]["to"] == "In Progress"
        assert changelog[0]["author"] == "Alice"


class TestCommentsExport:
    @responses.activate
    def test_fetches_comments(self, s3_env):
        mock_field_api()
        mock_search_api([_make_issue("TEST-1")])
        mock_comments_api("TEST-1")

        exporter = _make_exporter(s3_env, skip_comments=False)
        exporter.run()

        store, _, _ = s3_env
        tickets = store.download_json(f"jira/{PROJECT}/tickets.json")
        comments = tickets[0]["comments"]
        assert len(comments) == 1
        assert comments[0]["author"] == "Alice"
        assert comments[0]["body_text"] == "A comment"
        assert comments[0]["rendered_body"] == "<p>A comment</p>"


class TestAttachmentExport:
    @responses.activate
    def test_downloads_attachments_to_s3(self, s3_env):
        mock_field_api()
        mock_search_api([_make_issue("TEST-1", has_attachment=True)])
        # Mock attachment download
        responses.add(
            responses.GET, f"{BASE}/attachment/content/att1",
            body=b"PDF content here",
            status=200,
        )

        exporter = _make_exporter(s3_env, skip_attachments=False)
        exporter.run()

        store, _, conn = s3_env
        resp = conn.get_object(Bucket="test-bucket", Key=f"jira/{PROJECT}/attachments/TEST-1/report.pdf")
        assert resp["Body"].read() == b"PDF content here"


class TestCsvExport:
    @responses.activate
    def test_generates_csv(self, s3_env):
        mock_field_api()
        mock_search_api([_make_issue("TEST-1")])

        exporter = _make_exporter(s3_env)
        exporter.run()

        store, _, conn = s3_env
        resp = conn.get_object(Bucket="test-bucket", Key=f"jira/{PROJECT}/tickets.csv")
        csv_content = resp["Body"].read().decode()
        assert "key" in csv_content
        assert "TEST-1" in csv_content
        assert "Custom field (CC)" in csv_content


class TestCheckpointResume:
    @responses.activate
    def test_resume_skips_completed(self, s3_env):
        mock_field_api()
        mock_search_api([_make_issue("TEST-1")])

        exporter = _make_exporter(s3_env)
        exporter.run()

        # Second run — should not make any API calls
        call_count_before = len(responses.calls)
        mock_field_api()
        exporter2 = _make_exporter(s3_env)
        exporter2.run()
        # Only field API call expected (for custom field resolution), no search/comment calls
        new_calls = len(responses.calls) - call_count_before
        assert new_calls <= 1  # at most the field resolution call


class TestFullExport:
    @responses.activate
    def test_all_files_created(self, s3_env):
        mock_field_api()
        mock_search_api([_make_issue("TEST-1", has_attachment=True)])
        mock_comments_api("TEST-1")
        responses.add(
            responses.GET, f"{BASE}/attachment/content/att1",
            body=b"PDF",
            status=200,
        )

        exporter = _make_exporter(s3_env, skip_comments=False, skip_attachments=False)
        exporter.run()

        store, _, conn = s3_env
        resp = conn.list_objects_v2(Bucket="test-bucket", Prefix=f"jira/{PROJECT}/")
        keys = {obj["Key"] for obj in resp.get("Contents", [])}
        expected = {
            f"jira/{PROJECT}/tickets.json",
            f"jira/{PROJECT}/tickets.csv",
            f"jira/{PROJECT}/attachments/TEST-1/report.pdf",
        }
        assert expected.issubset(keys)
