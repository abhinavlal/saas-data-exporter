"""GitHub masker — roster-based PII replacement for GitHub exports.

Handles PR JSON, contributor lists, repo metadata, and commit data.
Structured fields are replaced via roster lookup; freeform text fields
(titles, bodies, messages) are scanned with Aho-Corasick to preserve
readable content while replacing only PII.
"""

import logging
import re

from lib.s3 import S3Store
from scripts.pii_mask.maskers.base import BaseMasker

log = logging.getLogger(__name__)


class GitHubMasker(BaseMasker):
    prefix = "github/"

    def should_process(self, key: str) -> bool:
        return key.endswith(".json")

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
        elif filename == "_stats.json":
            pass  # no PII
        else:
            return "skipped (unknown type)"

        data = self._replace_domains_in_obj(data)
        dst_key = self.rewrite_key(key)
        dst.upload_json(data, dst_key)
        return "ok"

    def rewrite_key(self, key: str) -> str:
        """github/{org}__repo → github/{masked_org}__repo."""
        result = key
        for real, fake in self.roster.domain_map.items():
            # Extract org name from domain (before the first dot)
            real_org = real.split(".")[0]
            fake_org = fake.split(".")[0]
            result = result.replace(f"{real_org}__", f"{fake_org}__")
        return result

    # -- PR masking -------------------------------------------------------- #

    def _mask_pr(self, pr: dict) -> dict:
        pr["author"] = self.roster.map_github_login(pr.get("author", ""))
        if pr.get("author_id"):
            pr["author_id"] = 0

        pr["assignees"] = [self.roster.map_github_login(a)
                           for a in pr.get("assignees", [])]
        pr["requested_reviewers"] = [self.roster.map_github_login(r)
                                     for r in pr.get("requested_reviewers", [])]

        # Freeform text: scan, don't destroy
        pr["title"] = self.scanner.scan(pr.get("title", ""))
        pr["body"] = self.scanner.scan(pr.get("body", ""))
        pr["html_url"] = self.scanner.scan_url(pr.get("html_url", ""))

        for review in pr.get("reviews", []):
            review["reviewer"] = self.roster.map_github_login(
                review.get("reviewer", ""))
            review["body"] = self.scanner.scan(review.get("body", ""))

        for rc in pr.get("review_comments", []):
            rc["author"] = self.roster.map_github_login(rc.get("author", ""))
            rc["body"] = self.scanner.scan(rc.get("body", ""))

        for comment in pr.get("comments", []):
            comment["author"] = self.roster.map_github_login(
                comment.get("author", ""))
            comment["body"] = self.scanner.scan(comment.get("body", ""))

        for commit in pr.get("commits", []):
            self._mask_commit(commit)

        return pr

    def _mask_commit(self, commit: dict) -> None:
        commit["author_name"] = self.roster.map_name(
            commit.get("author_name", ""))
        commit["author_email"] = self.scanner.scan_email(
            commit.get("author_email", ""))
        commit["author_login"] = self.roster.map_github_login(
            commit.get("author_login", ""))

        if "committer_name" in commit:
            commit["committer_name"] = self.roster.map_name(
                commit.get("committer_name", ""))
        if "committer_email" in commit:
            commit["committer_email"] = self.scanner.scan_email(
                commit.get("committer_email", ""))
        if "committer_login" in commit:
            commit["committer_login"] = self.roster.map_github_login(
                commit.get("committer_login", ""))

        # Scan commit message — preserves readable text
        commit["message"] = self.scanner.scan(commit.get("message", ""))

    # -- Contributors ------------------------------------------------------ #

    def _mask_contributors(self, contributors: list) -> list:
        for c in contributors:
            c["login"] = self.roster.map_github_login(c.get("login", ""))
            c["id"] = 0
            c["profile_url"] = self._mask_github_url(
                c.get("profile_url", ""))
        return contributors

    def _mask_github_url(self, url: str) -> str:
        if not url:
            return url
        m = re.match(r"(https?://github\.com/)([^/]+)(.*)", url)
        if m:
            masked_login = self.roster.map_github_login(m.group(2))
            return f"{m.group(1)}{masked_login}{m.group(3)}"
        return url

    # -- Repo metadata ----------------------------------------------------- #

    def _mask_repo_metadata(self, meta: dict) -> dict:
        if meta.get("full_name"):
            parts = meta["full_name"].split("/", 1)
            if len(parts) == 2:
                masked_org = self.roster.map_github_login(parts[0])
                meta["full_name"] = f"{masked_org}/{parts[1]}"
        meta["description"] = self.scanner.scan(meta.get("description", ""))
        return meta
