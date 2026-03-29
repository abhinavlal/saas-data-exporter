"""Tests for exporters.github — GitHubExporter with mocked API and moto S3."""

import json

import boto3
import pytest
import responses
from moto import mock_aws

from lib.s3 import S3Store
from lib.types import ExportConfig
from exporters.github import GitHubExporter

API = "https://api.github.com"
REPO = "testowner/testrepo"
SLUG = "testowner__testrepo"


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
        repo=REPO,
        s3=store,
        config=config,
        pr_limit=5,
        commit_limit=5,
        skip_commits=True,
        skip_prs=True,
    )
    defaults.update(kwargs)
    return GitHubExporter(**defaults)


# ── Mock API helpers ──────────────────────────────────────────────────────

def mock_repo_api():
    responses.add(
        responses.GET, f"{API}/repos/{REPO}",
        json={
            "full_name": REPO,
            "description": "A test repo",
            "private": False,
            "default_branch": "main",
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-06-01T00:00:00Z",
            "pushed_at": "2024-06-01T00:00:00Z",
            "stargazers_count": 10,
            "forks_count": 2,
            "open_issues_count": 3,
            "watchers_count": 10,
            "topics": ["python", "testing"],
            "license": {"name": "MIT"},
        },
        status=200,
    )
    responses.add(
        responses.GET, f"{API}/repos/{REPO}/languages",
        json={"Python": 10000, "Shell": 500},
        status=200,
    )


def mock_contributors_api():
    responses.add(
        responses.GET, f"{API}/repos/{REPO}/contributors",
        json=[
            {"login": "alice", "id": 1, "type": "User", "contributions": 50, "html_url": "https://github.com/alice"},
            {"login": "bob", "id": 2, "type": "User", "contributions": 30, "html_url": "https://github.com/bob"},
        ],
        status=200,
    )
    # Empty second page
    responses.add(
        responses.GET, f"{API}/repos/{REPO}/contributors",
        json=[],
        status=200,
    )


def _commit_list_item(i):
    """A commit as returned by the list API (no stats/files)."""
    return {
        "sha": f"sha{i}",
        "commit": {
            "message": f"Commit message {i}",
            "author": {"name": "Alice", "email": "alice@test.com", "date": f"2024-06-0{i+1}T00:00:00Z"},
            "committer": {"name": "Alice", "email": "alice@test.com", "date": f"2024-06-0{i+1}T00:00:00Z"},
        },
        "author": {"login": "alice"},
        "committer": {"login": "alice"},
        "parents": [{"sha": f"parent{i}"}],
        "html_url": f"https://github.com/{REPO}/commit/sha{i}",
    }


def mock_commits_api(count=3):
    """Mock commit list + individual detail endpoints."""
    commit_list = [_commit_list_item(i) for i in range(count)]
    responses.add(
        responses.GET, f"{API}/repos/{REPO}/commits",
        json=commit_list,
        status=200,
    )
    # Empty second page
    responses.add(
        responses.GET, f"{API}/repos/{REPO}/commits",
        json=[],
        status=200,
    )
    # Individual commit details (only used when commit_details=True)
    for i in range(count):
        responses.add(
            responses.GET, f"{API}/repos/{REPO}/commits/sha{i}",
            json={
                **_commit_list_item(i),
                "stats": {"additions": 10, "deletions": 2, "total": 12},
                "files": [
                    {"filename": f"file{i}.py", "status": "modified", "additions": 10, "deletions": 2, "patch": "@@ -1 +1 @@"},
                ],
            },
            status=200,
        )


def mock_prs_api(count=2):
    """Mock PR list + detail + sub-resource endpoints."""
    pr_list = [{"number": i + 1} for i in range(count)]
    responses.add(
        responses.GET, f"{API}/repos/{REPO}/pulls",
        json=pr_list,
        status=200,
    )
    # Empty second page
    responses.add(
        responses.GET, f"{API}/repos/{REPO}/pulls",
        json=[],
        status=200,
    )
    for i in range(count):
        n = i + 1
        # PR detail
        responses.add(
            responses.GET, f"{API}/repos/{REPO}/pulls/{n}",
            json={
                "number": n,
                "title": f"PR {n}",
                "state": "closed",
                "user": {"login": "alice", "id": 1},
                "created_at": "2024-01-01T00:00:00Z",
                "updated_at": "2024-06-01T00:00:00Z",
                "closed_at": "2024-06-01T00:00:00Z",
                "merged_at": "2024-06-01T00:00:00Z",
                "merge_commit_sha": f"merge_sha_{n}",
                "draft": False,
                "body": f"PR body {n}",
                "head": {"ref": "feature"},
                "base": {"ref": "main"},
                "labels": [{"name": "bug"}],
                "assignees": [{"login": "bob"}],
                "requested_reviewers": [{"login": "charlie"}],
                "additions": 100,
                "deletions": 20,
                "changed_files": 5,
                "html_url": f"https://github.com/{REPO}/pull/{n}",
            },
            status=200,
        )
        # Reviews
        responses.add(
            responses.GET, f"{API}/repos/{REPO}/pulls/{n}/reviews",
            json=[{"user": {"login": "bob"}, "state": "APPROVED", "body": "LGTM", "submitted_at": "2024-06-01T00:00:00Z"}],
            status=200,
        )
        responses.add(responses.GET, f"{API}/repos/{REPO}/pulls/{n}/reviews", json=[], status=200)
        # Review comments
        responses.add(
            responses.GET, f"{API}/repos/{REPO}/pulls/{n}/comments",
            json=[{"user": {"login": "bob"}, "body": "Inline note", "path": "file.py", "diff_hunk": "@@ -1 +1 @@", "created_at": "2024-06-01T00:00:00Z"}],
            status=200,
        )
        responses.add(responses.GET, f"{API}/repos/{REPO}/pulls/{n}/comments", json=[], status=200)
        # Issue comments
        responses.add(
            responses.GET, f"{API}/repos/{REPO}/issues/{n}/comments",
            json=[{"user": {"login": "alice"}, "body": "Thanks!", "created_at": "2024-06-01T00:00:00Z"}],
            status=200,
        )
        responses.add(responses.GET, f"{API}/repos/{REPO}/issues/{n}/comments", json=[], status=200)
        # PR commits
        responses.add(
            responses.GET, f"{API}/repos/{REPO}/pulls/{n}/commits",
            json=[{
                "sha": f"pr_sha_{n}",
                "commit": {"message": "feat: thing", "author": {"name": "Alice", "email": "alice@test.com", "date": "2024-06-01T00:00:00Z"}},
                "author": {"login": "alice"},
            }],
            status=200,
        )
        responses.add(responses.GET, f"{API}/repos/{REPO}/pulls/{n}/commits", json=[], status=200)


# ── Tests ─────────────────────────────────────────────────────────────────

class TestMetadataExport:
    @responses.activate
    def test_exports_metadata_to_s3(self, s3_env):
        mock_repo_api()
        mock_contributors_api()
        exporter = _make_exporter(s3_env)
        exporter.run()

        store, _, _ = s3_env
        metadata = store.download_json(f"github/{SLUG}/repo_metadata.json")
        assert metadata["full_name"] == REPO
        assert metadata["language_breakdown"]["Python"]["bytes"] == 10000
        assert metadata["language_breakdown"]["Python"]["percentage"] == pytest.approx(95.24, abs=0.01)


class TestContributorsExport:
    @responses.activate
    def test_exports_contributors_sorted(self, s3_env):
        mock_repo_api()
        mock_contributors_api()
        exporter = _make_exporter(s3_env)
        exporter.run()

        store, _, _ = s3_env
        contributors = store.download_json(f"github/{SLUG}/contributors.json")
        assert len(contributors) == 2
        assert contributors[0]["login"] == "alice"
        assert contributors[0]["contributions"] == 50
        assert contributors[1]["login"] == "bob"


class TestCommitsExport:
    @responses.activate
    def test_exports_commits_from_list_api(self, s3_env):
        mock_repo_api()
        mock_contributors_api()
        mock_commits_api(count=3)
        exporter = _make_exporter(s3_env, skip_commits=False, commit_limit=5)
        exporter.run()

        store, _, _ = s3_env
        # Each commit is its own file
        c = store.download_json(f"github/{SLUG}/commits/sha0.json")
        assert c is not None
        assert c["sha"] == "sha0"
        assert c["author_name"] == "Alice"
        assert c["author_login"] == "alice"
        # No stats/files from list API (only with --commit-details)
        assert "stats" not in c

    @responses.activate
    def test_exports_commits_with_details_flag(self, s3_env):
        mock_repo_api()
        mock_contributors_api()
        mock_commits_api(count=3)
        exporter = _make_exporter(s3_env, skip_commits=False, commit_limit=5, commit_details=True)
        exporter.run()

        store, _, _ = s3_env
        # Detail mode writes per-commit files
        c = store.download_json(f"github/{SLUG}/commits/sha0.json")
        assert c["stats"]["additions"] == 10
        assert len(c["files"]) == 1


class TestPullRequestsExport:
    @responses.activate
    def test_exports_prs_with_sub_resources(self, s3_env):
        mock_repo_api()
        mock_contributors_api()
        mock_prs_api(count=2)
        exporter = _make_exporter(s3_env, skip_prs=False, pr_limit=5)
        exporter.run()

        store, _, _ = s3_env
        # PRs are now individual files
        pr = store.download_json(f"github/{SLUG}/prs/1.json")
        assert pr["author"] == "alice"
        assert pr["state"] == "closed"
        assert len(pr["reviews"]) == 1
        assert pr["reviews"][0]["state"] == "APPROVED"
        assert len(pr["review_comments"]) == 1
        assert len(pr["comments"]) == 1
        assert len(pr["commits"]) == 1

    @responses.activate
    def test_exports_pr_csv(self, s3_env):
        mock_repo_api()
        mock_contributors_api()
        mock_prs_api(count=1)
        exporter = _make_exporter(s3_env, skip_prs=False, pr_limit=5)
        exporter.run()

        store, _, conn = s3_env
        resp = conn.get_object(Bucket="test-bucket", Key=f"github/{SLUG}/pull_requests.csv")
        csv_content = resp["Body"].read().decode()
        assert "number" in csv_content
        assert "alice" in csv_content


class TestCheckpointResume:
    @responses.activate
    def test_resume_skips_completed_phases(self, s3_env):
        """Run once to completion, then run again — should skip all phases."""
        mock_repo_api()
        mock_contributors_api()
        exporter = _make_exporter(s3_env)
        exporter.run()

        # Second run — checkpoint says everything is done
        call_count_before = len(responses.calls)
        mock_repo_api()
        mock_contributors_api()
        exporter2 = _make_exporter(s3_env)
        exporter2.run()
        # No new API calls should be made (everything was checkpointed)
        assert len(responses.calls) == call_count_before

    @responses.activate
    def test_resume_commit_details_after_partial(self, s3_env):
        """Simulate partial commit detail fetch, then resume."""
        store, config, _ = s3_env

        # Pre-populate checkpoint: metadata+contributors done,
        # commits listed, sha0 detail already fetched
        from lib.checkpoint import CheckpointManager
        cp = CheckpointManager(store, f"github/{SLUG}")
        cp.load()
        cp.start_phase("metadata")
        cp.complete_phase("metadata")
        cp.start_phase("contributors")
        cp.complete_phase("contributors")
        cp.start_phase("commits", total=3)
        cp.mark_item_done("commits", "sha0")
        cp.mark_item_done("commit_details", "sha0")
        cp.save(force=True)

        # Mock APIs — metadata/contributors not needed (checkpointed)
        mock_commits_api(count=3)

        exporter = _make_exporter(s3_env, skip_commits=False, commit_limit=5, commit_details=True)
        exporter.run()

        # All 3 commits have files; sha0 list-mode only (detail was checkpointed),
        # sha1 and sha2 have full details
        c1 = store.download_json(f"github/{SLUG}/commits/sha1.json")
        c2 = store.download_json(f"github/{SLUG}/commits/sha2.json")
        assert c1 is not None
        assert c2 is not None
        assert "stats" in c1  # detail-fetched
        assert "stats" in c2


class TestFullExport:
    @responses.activate
    def test_full_export_all_files(self, s3_env):
        """End-to-end: export everything and verify all expected S3 keys exist."""
        mock_repo_api()
        mock_contributors_api()
        mock_commits_api(count=2)
        mock_prs_api(count=1)

        exporter = _make_exporter(s3_env, skip_commits=False, skip_prs=False, commit_limit=5, pr_limit=5)
        exporter.run()

        store, _, conn = s3_env
        resp = conn.list_objects_v2(Bucket="test-bucket", Prefix=f"github/{SLUG}/")
        keys = {obj["Key"] for obj in resp.get("Contents", [])}

        expected = {
            f"github/{SLUG}/repo_metadata.json",
            f"github/{SLUG}/contributors.json",
            f"github/{SLUG}/commits/sha0.json",
            f"github/{SLUG}/commits/sha1.json",
            f"github/{SLUG}/prs/1.json",
            f"github/{SLUG}/pull_requests.csv",
        }
        assert expected.issubset(keys)
