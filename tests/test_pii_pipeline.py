"""Integration tests for scripts.pii_mask.pipeline."""

import pytest
import boto3
from moto import mock_aws

from lib.s3 import S3Store
from lib.checkpoint import CheckpointManager
from scripts.pii_mask.roster import Roster
from scripts.pii_mask.scanner import TextScanner
from scripts.pii_mask.manifest import Manifest
from scripts.pii_mask.pipeline import run_pipeline
from scripts.pii_mask.maskers.github import GitHubMasker
from scripts.pii_mask.maskers.jira import JiraMasker
from scripts.pii_mask.maskers.slack import SlackMasker

SRC_BUCKET = "src-bucket"
DST_BUCKET = "dst-bucket"

SAMPLE_ROSTER = {
    "version": 1,
    "domain_map": {
        "org_name.com": "example.com",
        "org_name.atlassian.net": "example.atlassian.net",
    },
    "users": [
        {
            "id": "user-001",
            "real": {
                "email": "john.doe@org_name.com",
                "name": "John Doe",
                "first_name": "John",
                "last_name": "Doe",
                "github_login": "johndoe",
                "slack_user_id": "U01ABC123",
                "jira_account_id": "5f7abc12345",
            },
            "masked": {
                "email": "alice.chen@example.com",
                "name": "Alice Chen",
                "first_name": "Alice",
                "last_name": "Chen",
                "github_login": "achen",
                "slack_user_id": "U01MASK01",
                "jira_account_id": "mask-001",
                "jira_display_name": "Alice Chen",
            },
        },
    ],
}


@pytest.fixture
def roster():
    return Roster(SAMPLE_ROSTER)


@pytest.fixture
def scanner(roster):
    return TextScanner(roster)


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
    def test_multi_exporter_pipeline(self, roster, scanner, s3_env):
        src, dst, _ = s3_env

        # Upload GitHub data
        pr = {
            "number": 1, "author": "johndoe",
            "title": "Fix by John Doe", "body": "john.doe@org_name.com",
            "html_url": "https://github.com/org_name/repo/pull/1",
            "reviews": [], "review_comments": [], "comments": [],
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
            "summary": "Bug found by John Doe",
            "description_text": "Login broken",
            "parent_summary": "",
            "assignee": "John Doe",
            "assignee_email": "john.doe@org_name.com",
            "assignee_account_id": "5f7abc12345",
            "comments": [], "attachments": [], "changelog": [],
        }
        src.upload_json(ticket, "jira/IES/tickets/IES-1.json")

        # Upload Slack data
        messages = [
            {"user": "U01ABC123", "text": "Hey John Doe, check the PR",
             "reactions": [], "files": []},
        ]
        src.upload_json(messages, "slack/C090/messages.json")

        # Run pipeline
        maskers = [
            GitHubMasker(roster, scanner),
            JiraMasker(roster, scanner),
            SlackMasker(roster, scanner),
        ]
        checkpoint = CheckpointManager(dst, "pii_mask/pipeline")
        checkpoint.load()
        manifest = Manifest(SRC_BUCKET, DST_BUCKET)

        run_pipeline(src=src, dst=dst, maskers=maskers,
                     checkpoint=checkpoint, manifest=manifest,
                     max_workers=1)

        # Verify GitHub
        masked_pr = dst.download_json("github/example__repo/prs/1.json")
        assert masked_pr is not None
        assert masked_pr["author"] == "achen"
        assert "John Doe" not in masked_pr["title"]
        assert "Alice Chen" in masked_pr["title"]

        # Verify Jira
        masked_ticket = dst.download_json("jira/IES/tickets/IES-1.json")
        assert masked_ticket is not None
        assert masked_ticket["assignee"] == "Alice Chen"
        assert masked_ticket["assignee_email"] == "alice.chen@example.com"
        assert "org_name.atlassian.net" not in masked_ticket["self"]

        # Verify Slack
        masked_msgs = dst.download_json("slack/C090/messages.json")
        assert masked_msgs is not None
        assert masked_msgs[0]["user"] == "U01MASK01"
        assert "John Doe" not in masked_msgs[0]["text"]
        assert "Alice Chen" in masked_msgs[0]["text"]

        # Verify cross-service consistency: same person → same fake identity
        assert masked_pr["author"] == "achen"
        assert masked_ticket["assignee"] == "Alice Chen"
        assert masked_pr["commits"][0]["author_email"] == "alice.chen@example.com"
        assert masked_ticket["assignee_email"] == "alice.chen@example.com"

        # Verify manifest
        m = dst.download_json("_manifest/pii_mask.json")
        assert m is not None
        assert m["total_files"] == 3
        assert m["masked_files"] == 3

    def test_checkpoint_resume(self, roster, scanner, s3_env):
        src, dst, _ = s3_env

        # Upload two PRs
        for i in (1, 2):
            src.upload_json({
                "number": i, "author": "johndoe",
                "title": f"PR {i}", "body": "",
                "html_url": "", "reviews": [],
                "review_comments": [], "comments": [], "commits": [],
            }, f"github/org_name__repo/prs/{i}.json")

        maskers = [GitHubMasker(roster, scanner)]

        # Simulate a partial run: mark file 1 as done
        cp = CheckpointManager(dst, "pii_mask/pipeline")
        cp.load()
        cp.start_phase("mask/github", total=2)
        cp.mark_item_done("mask/github",
                          "github/org_name__repo/prs/1.json")
        cp.save(force=True)
        # Write a sentinel for file 1
        dst.upload_json({"number": 1, "author": "sentinel"},
                        "github/example__repo/prs/1.json")

        # Resume
        cp2 = CheckpointManager(dst, "pii_mask/pipeline")
        cp2.load()
        manifest = Manifest(SRC_BUCKET, DST_BUCKET)
        run_pipeline(src=src, dst=dst, maskers=maskers,
                     checkpoint=cp2, manifest=manifest, max_workers=1)

        # File 1: sentinel preserved (not re-processed)
        pr1 = dst.download_json("github/example__repo/prs/1.json")
        assert pr1["author"] == "sentinel"

        # File 2: actually masked
        pr2 = dst.download_json("github/example__repo/prs/2.json")
        assert pr2["author"] == "achen"


class TestManifest:
    def test_manifest_stats(self):
        m = Manifest("src", "dst")
        m.record("github", "ok")
        m.record("github", "ok")
        m.record("github", "skipped (unknown type)")
        m.record("jira", "ok")

        d = m.to_dict()
        assert d["total_files"] == 4
        assert d["masked_files"] == 3
        assert d["skipped_files"] == 1
        assert d["stats_by_exporter"]["github"]["masked"] == 2
        assert d["stats_by_exporter"]["github"]["skipped"] == 1
        assert d["stats_by_exporter"]["jira"]["masked"] == 1
