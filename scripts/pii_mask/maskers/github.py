"""GitHub masker — Presidio-first PII replacement for GitHub exports."""

import logging

from lib.s3 import S3Store
from scripts.pii_mask.maskers.base import BaseMasker

log = logging.getLogger(__name__)


class GitHubMasker(BaseMasker):
    prefix = "github/"

    def mask_file(self, src: S3Store, dst: S3Store, key: str) -> str:
        data = src.download_json(key)
        if data is None:
            return "skipped (not found)"

        filename = key.rsplit("/", 1)[-1]

        if "/prs/" in key:
            data = self._mask_pr(data)
        elif filename == "contributors.json":
            data = self._mask_contributors(data)
        elif filename == "repo_metadata.json":
            data = self._mask_repo_metadata(data)
        else:
            data = self._scan_obj(data)

        dst_key = self.rewrite_key(key)
        dst.upload_json(data, dst_key)
        return "ok"

    def _mask_pr(self, pr: dict) -> dict:
        # Structured fields: direct type-aware lookup (fast)
        pr["author"] = self.scanner.scan_structured(
            "GITHUB_LOGIN", pr.get("author", ""))
        if pr.get("author_id"):
            pr["author_id"] = 0
        pr["assignees"] = [self.scanner.scan_structured("GITHUB_LOGIN", a)
                           for a in pr.get("assignees", [])]
        pr["requested_reviewers"] = [
            self.scanner.scan_structured("GITHUB_LOGIN", r)
            for r in pr.get("requested_reviewers", [])]

        # All other fields: universal scan (Presidio on every string)
        for field in ("title", "body"):
            pr[field] = self.scanner.scan(pr.get(field, ""))
        pr["html_url"] = self.scanner.scan_url(pr.get("html_url", ""))

        for review in pr.get("reviews", []):
            review["reviewer"] = self.scanner.scan_structured(
                "GITHUB_LOGIN", review.get("reviewer", ""))
            review["body"] = self.scanner.scan(review.get("body", ""))

        for rc in pr.get("review_comments", []):
            rc["author"] = self.scanner.scan_structured(
                "GITHUB_LOGIN", rc.get("author", ""))
            rc["body"] = self.scanner.scan(rc.get("body", ""))

        for comment in pr.get("comments", []):
            comment["author"] = self.scanner.scan_structured(
                "GITHUB_LOGIN", comment.get("author", ""))
            comment["body"] = self.scanner.scan(comment.get("body", ""))

        for commit in pr.get("commits", []):
            self._mask_commit(commit)

        return pr

    def _mask_commit(self, commit: dict) -> None:
        commit["author_name"] = self.scanner.scan_structured(
            "PERSON", commit.get("author_name", ""))
        commit["author_email"] = self.scanner.scan_structured(
            "EMAIL_ADDRESS", commit.get("author_email", ""))
        commit["author_login"] = self.scanner.scan_structured(
            "GITHUB_LOGIN", commit.get("author_login", ""))
        if "committer_name" in commit:
            commit["committer_name"] = self.scanner.scan_structured(
                "PERSON", commit.get("committer_name", ""))
        if "committer_email" in commit:
            commit["committer_email"] = self.scanner.scan_structured(
                "EMAIL_ADDRESS", commit.get("committer_email", ""))
        if "committer_login" in commit:
            commit["committer_login"] = self.scanner.scan_structured(
                "GITHUB_LOGIN", commit.get("committer_login", ""))
        commit["message"] = self.scanner.scan(commit.get("message", ""))

    def _mask_contributors(self, contributors: list) -> list:
        for c in contributors:
            c["login"] = self.scanner.scan_structured(
                "GITHUB_LOGIN", c.get("login", ""))
            c["id"] = 0
            c["profile_url"] = self.scanner.scan_url(
                c.get("profile_url", ""))
        return contributors

    def _mask_repo_metadata(self, meta: dict) -> dict:
        # Scan everything — Presidio handles names in description etc.
        return self._scan_obj(meta)
