"""Integration tests for scripts.pii_mask.pipeline — Presidio-first."""

import pytest
import boto3
from moto import mock_aws

from lib.s3 import S3Store
from lib.checkpoint import CheckpointManager
from scripts.pii_mask.pii_store import PIIStore
from scripts.pii_mask.scanner import TextScanner
from scripts.pii_mask.manifest import Manifest
from scripts.pii_mask.pipeline import run_pipeline
from scripts.pii_mask.maskers.github import GitHubMasker
from scripts.pii_mask.maskers.jira import JiraMasker
from scripts.pii_mask.maskers.slack import SlackMasker

SRC_BUCKET = "src-bucket"
DST_BUCKET = "dst-bucket"


@pytest.fixture
def store(tmp_path):
    store = PIIStore(str(tmp_path / "test.db"))
    store.add_domain("org_name.com", "example.com")
    store.add_domain("org_name.atlassian.net", "example.atlassian.net")
    # Pre-populate known identities for consistency verification
    store.get_or_create("PERSON", "John Doe", source="test")
    store.get_or_create("EMAIL_ADDRESS", "john.doe@org_name.com", source="test")
    store.get_or_create("GITHUB_LOGIN", "johndoe", source="test")
    store.get_or_create("JIRA_ACCOUNT_ID", "5f7abc12345", source="test")
    store.get_or_create("SLACK_USER_ID", "U01ABC123", source="test")
    return store


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
def s3_env():
    with mock_aws():
        conn = boto3.client("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=SRC_BUCKET)
        conn.create_bucket(Bucket=DST_BUCKET)
        src = S3Store(bucket=SRC_BUCKET)
        dst = S3Store(bucket=DST_BUCKET)
        yield src, dst, conn


class TestEndToEnd:
    def test_multi_exporter_pipeline(self, scanner, s3_env):
        src, dst, _ = s3_env

        # Upload GitHub data
        pr = {
            "number": 1, "author": "johndoe",
            "title": "Fix bug", "body": "",
            "html_url": "", "reviews": [], "review_comments": [],
            "comments": [],
            "commits": [{
                "sha": "abc", "author_name": "John Doe",
                "author_email": "john.doe@org_name.com",
                "author_login": "johndoe", "message": "fix",
            }],
        }
        src.upload_json(pr, "github/org_name__repo/prs/1.json")

        # Upload Jira data
        ticket = {
            "key": "IES-1",
            "self": "https://org_name.atlassian.net/rest/api/3/issue/1",
            "summary": "Bug report",
            "description_text": "",
            "parent_summary": "",
            "assignee": "John Doe",
            "assignee_email": "john.doe@org_name.com",
            "assignee_account_id": "5f7abc12345",
            "comments": [], "attachments": [], "changelog": [],
        }
        src.upload_json(ticket, "jira/IES/tickets/IES-1.json")

        # Upload Slack data
        messages = [
            {"user": "U01ABC123", "text": "Hello team",
             "reactions": [], "files": []},
        ]
        src.upload_json(messages, "slack/C090/messages.json")

        maskers = [
            GitHubMasker(scanner),
            JiraMasker(scanner),
            SlackMasker(scanner),
        ]
        checkpoint = CheckpointManager(dst, "pii_mask/pipeline")
        checkpoint.load()
        manifest = Manifest(SRC_BUCKET, DST_BUCKET)

        run_pipeline(src=src, dst=dst, maskers=maskers,
                     checkpoint=checkpoint, manifest=manifest,
                     max_workers=1)

        # Verify files were written
        all_keys = dst.list_keys("")
        github_keys = [k for k in all_keys if k.startswith("github/")]
        jira_keys = [k for k in all_keys if k.startswith("jira/")]
        slack_keys = [k for k in all_keys if k.startswith("slack/")]

        assert len(github_keys) >= 1
        assert len(jira_keys) >= 1
        assert len(slack_keys) >= 1

        # Verify Jira: structured fields replaced
        masked_ticket = dst.download_json(jira_keys[0])
        assert masked_ticket["assignee"] != "John Doe"
        assert masked_ticket["assignee_email"] != "john.doe@org_name.com"
        assert "org_name.atlassian.net" not in masked_ticket.get("self", "")

        # Verify manifest written
        manifest_data = dst.download_json("_manifest/pii_mask.json")
        assert manifest_data is not None
        assert manifest_data["total_files"] >= 3

    def test_checkpoint_resume(self, scanner, s3_env):
        src, dst, _ = s3_env

        for i in (1, 2):
            src.upload_json({
                "number": i, "author": "johndoe",
                "title": f"PR {i}", "body": "", "html_url": "",
                "reviews": [], "review_comments": [],
                "comments": [], "commits": [],
            }, f"github/x__repo/prs/{i}.json")

        maskers = [GitHubMasker(scanner)]

        # Simulate partial run
        cp = CheckpointManager(dst, "pii_mask/pipeline")
        cp.load()
        cp.start_phase("mask/github", total=2)
        cp.mark_item_done("mask/github", "github/x__repo/prs/1.json")
        cp.save(force=True)
        dst.upload_json({"number": 1, "author": "sentinel"},
                        "github/x__repo/prs/1.json")

        # Resume
        cp2 = CheckpointManager(dst, "pii_mask/pipeline")
        cp2.load()
        manifest = Manifest(SRC_BUCKET, DST_BUCKET)
        run_pipeline(src=src, dst=dst, maskers=maskers,
                     checkpoint=cp2, manifest=manifest, max_workers=1)

        pr1 = dst.download_json("github/x__repo/prs/1.json")
        assert pr1["author"] == "sentinel"  # not re-processed


class TestManifest:
    def test_manifest_stats(self):
        m = Manifest("src", "dst")
        m.record("github", "ok")
        m.record("github", "ok")
        m.record("jira", "ok")
        d = m.to_dict()
        assert d["total_files"] == 3
        assert d["masked_files"] == 3
