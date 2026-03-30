"""GitHub Repository Exporter — exports repo metadata, contributors, commits, and PRs to S3."""

import argparse
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

from lib.s3 import S3Store
from lib.checkpoint import CheckpointManager
from lib.stats import StatsCollector
from lib.session import make_session
from lib.logging import setup_logging
from lib.types import ExportConfig

log = logging.getLogger(__name__)

API_BASE = "https://api.github.com"


class GitHubExporter:
    def __init__(
        self,
        token: str,
        repo: str,
        s3: S3Store,
        config: ExportConfig,
        pr_limit: int = 0,
        commit_limit: int = 0,
        pr_state: str = "all",
        skip_commits: bool = True,
        skip_prs: bool = False,
        commit_details: bool = False,
        app_pool=None,
    ):
        self.repo = repo
        self.s3 = s3
        self.config = config
        self.pr_limit = pr_limit
        self.commit_limit = commit_limit
        self.pr_state = pr_state
        self.skip_commits = skip_commits
        self.skip_prs = skip_prs
        self.commit_details = commit_details
        self._app_pool = app_pool
        self._pat_token = token

        self.session, self.rate_state = make_session(
            requests_per_second=10,
            burst=20,
            # Pool mode: disable session-level preemptive wait (-1) — the pool
            # manages rate limits across multiple apps with different budgets.
            min_remaining=-1 if app_pool else 50,
        )
        if app_pool:
            # Token set per-request in _api_get()
            pass
        else:
            self.session.headers["Authorization"] = f"Bearer {token}"
        self.session.headers["Accept"] = "application/vnd.github+json"

        self.repo_slug = repo.replace("/", "__")
        self.s3_base = f"github/{self.repo_slug}"
        self.checkpoint = CheckpointManager(s3, f"github/{self.repo_slug}")
        self.stats = StatsCollector(s3, f"{self.s3_base}/_stats.json")

    def _refresh_token_if_needed(self) -> None:
        """No-op for pool mode (token set per-request). For PAT mode, nothing to refresh."""
        pass

    def run(self):
        self.checkpoint.load()
        self.stats.load()
        self.stats.set("exporter", "github")
        self.stats.set("target", self.repo)
        self.stats.set("target_slug", self.repo_slug)
        log.info("Starting GitHub export for %s", self.repo)

        if not self.checkpoint.is_phase_done("metadata"):
            self._refresh_token_if_needed()
            self._export_metadata()

        if not self.checkpoint.is_phase_done("contributors"):
            self._refresh_token_if_needed()
            self._export_contributors()

        if not self.skip_commits and not self.checkpoint.is_phase_done("commits"):
            self._refresh_token_if_needed()
            self._export_commits()

        if not self.skip_prs and not self.checkpoint.is_phase_done("pull_requests"):
            self._refresh_token_if_needed()
            self._export_pull_requests()

        self.checkpoint.complete()
        from datetime import datetime, timezone
        self.stats.set("exported_at", datetime.now(timezone.utc).isoformat())
        self.stats.save(force=True)
        log.info("GitHub export complete for %s", self.repo)

    # ── Metadata ──────────────────────────────────────────────────────────

    def _export_metadata(self):
        log.info("Exporting repository metadata")
        self.checkpoint.start_phase("metadata")

        resp = self._api_get(f"{API_BASE}/repos/{self.repo}")
        resp.raise_for_status()
        repo_data = resp.json()

        # Language breakdown
        lang_resp = self._api_get(f"{API_BASE}/repos/{self.repo}/languages")
        lang_resp.raise_for_status()
        languages = lang_resp.json()

        total_bytes = sum(languages.values()) or 1
        language_breakdown = {
            lang: {"bytes": b, "percentage": round(b / total_bytes * 100, 2)}
            for lang, b in languages.items()
        }

        metadata = {
            "full_name": repo_data.get("full_name"),
            "description": repo_data.get("description"),
            "private": repo_data.get("private"),
            "default_branch": repo_data.get("default_branch"),
            "created_at": repo_data.get("created_at"),
            "updated_at": repo_data.get("updated_at"),
            "pushed_at": repo_data.get("pushed_at"),
            "stargazers_count": repo_data.get("stargazers_count"),
            "forks_count": repo_data.get("forks_count"),
            "open_issues_count": repo_data.get("open_issues_count"),
            "watchers_count": repo_data.get("watchers_count"),
            "topics": repo_data.get("topics", []),
            "license": repo_data.get("license"),
            "language_breakdown": language_breakdown,
        }

        self.s3.upload_json(metadata, f"{self.s3_base}/repo_metadata.json")

        self.stats.set("repo", {
            "full_name": metadata.get("full_name"),
            "private": metadata.get("private"),
            "default_branch": metadata.get("default_branch"),
            "stars": metadata.get("stargazers_count", 0),
            "forks": metadata.get("forks_count", 0),
            "open_issues": metadata.get("open_issues_count", 0),
            "watchers": metadata.get("watchers_count", 0),
        })
        self.stats.set("languages", language_breakdown)
        self.stats.save(force=True)

        self.checkpoint.complete_phase("metadata")
        self.checkpoint.save(force=True)
        log.info("Metadata exported")

    # ── Contributors ──────────────────────────────────────────────────────

    def _export_contributors(self):
        log.info("Exporting contributors")
        self.checkpoint.start_phase("contributors")

        contributors = []
        page = 1
        while True:
            resp = self._api_get(
                f"{API_BASE}/repos/{self.repo}/contributors",
                params={"per_page": 100, "page": page},
            )
            resp.raise_for_status()
            batch = resp.json()
            if not batch:
                break
            for c in batch:
                contributors.append({
                    "login": c.get("login"),
                    "id": c.get("id"),
                    "type": c.get("type"),
                    "contributions": c.get("contributions"),
                    "profile_url": c.get("html_url"),
                })
            page += 1

        contributors.sort(key=lambda c: c["contributions"], reverse=True)
        self.s3.upload_json(contributors, f"{self.s3_base}/contributors.json")

        self.stats.set_nested("contributors.total", len(contributors))
        if contributors:
            self.stats.set_nested("contributors.top", contributors[0].get("login"))
        self.stats.save(force=True)

        self.checkpoint.complete_phase("contributors")
        self.checkpoint.save(force=True)
        log.info("Exported %d contributors", len(contributors))

    # ── Commits ───────────────────────────────────────────────────────────

    def _export_commits(self):
        log.info("Exporting commits (limit=%s, details=%s)",
                 self.commit_limit or "all", self.commit_details)
        self.checkpoint.start_phase("commits", total=self.commit_limit or None)

        # Step 1: paginate commit list — write each commit to its own S3 file
        commits = self._list_commits()
        log.info("Fetched %d commits from list API", len(commits))

        for c in commits:
            sha = c["sha"]
            if not self.checkpoint.is_item_done("commits", sha):
                self.s3.upload_json(c, f"{self.s3_base}/commits/{sha}.json")
                self.checkpoint.mark_item_done("commits", sha)
        self.checkpoint.save()

        # Step 2: optionally overwrite with full details (stats, files, patches)
        if self.commit_details:
            to_fetch = [c["sha"] for c in commits
                        if not self.checkpoint.is_item_done("commit_details", c["sha"])]
            log.info("Fetching full details for %d commits (%d already done)",
                     len(to_fetch), len(commits) - len(to_fetch))

            with ThreadPoolExecutor(max_workers=self.config.max_workers) as pool:
                futures = {pool.submit(self._fetch_commit_detail, sha): sha for sha in to_fetch}
                for future in as_completed(futures):
                    sha = futures[future]
                    try:
                        commit = future.result()
                        if commit:
                            self.s3.upload_json(commit, f"{self.s3_base}/commits/{sha}.json")
                        self.checkpoint.mark_item_done("commit_details", sha)
                        self.checkpoint.save()
                    except Exception:
                        log.error("Failed to fetch commit %s", sha, exc_info=True)

        # Commit stats — computed from full list (always re-fetched)
        authors = set()
        dates = []
        for c in commits:
            author = c.get("author_login") or c.get("author_email")
            if author:
                authors.add(author)
            if c.get("author_date"):
                dates.append(c["author_date"])
        self.stats.set_nested("commits.total", len(commits))
        self.stats.set_nested("commits.unique_authors", len(authors))
        if dates:
            self.stats.set_nested("commits.earliest", min(dates))
            self.stats.set_nested("commits.latest", max(dates))
        self.stats.save(force=True)

        self.checkpoint.complete_phase("commits")
        self.checkpoint.save(force=True)
        log.info("Exported %d commits for %s", len(commits), self.repo)

    def _list_commits(self) -> list[dict]:
        """Paginate the commit list API. Returns basic commit data (no file stats)."""
        commits = []
        page = 1
        while True:
            resp = self._api_get(
                f"{API_BASE}/repos/{self.repo}/commits",
                params={"per_page": 100, "page": page},
            )
            resp.raise_for_status()
            batch = resp.json()
            if not batch:
                break
            for c in batch:
                commit_obj = c.get("commit", {})
                author = commit_obj.get("author", {})
                committer = commit_obj.get("committer", {})
                commits.append({
                    "sha": c.get("sha"),
                    "message": commit_obj.get("message"),
                    "author_name": author.get("name"),
                    "author_email": author.get("email"),
                    "author_login": (c.get("author") or {}).get("login"),
                    "author_date": author.get("date"),
                    "committer_name": committer.get("name"),
                    "committer_email": committer.get("email"),
                    "committer_login": (c.get("committer") or {}).get("login"),
                    "committer_date": committer.get("date"),
                    "parents": [p["sha"] for p in c.get("parents", [])],
                    "html_url": c.get("html_url"),
                })
                if self.commit_limit and len(commits) >= self.commit_limit:
                    return commits
            if len(commits) % 1000 == 0:
                log.info("Listed %d commits so far...", len(commits))
            page += 1
        return commits

    def _fetch_commit_detail(self, sha: str) -> dict | None:
        resp = self._api_get(f"{API_BASE}/repos/{self.repo}/commits/{sha}")
        if resp.status_code == 404:
            log.warning("Commit %s not found (404)", sha)
            return None
        if resp.status_code == 403:
            self._log_403(resp, f"Commit {sha}")
            return None
        resp.raise_for_status()
        c = resp.json()

        commit_obj = c.get("commit", {})
        author = commit_obj.get("author", {})
        committer = commit_obj.get("committer", {})
        stats = c.get("stats", {})

        files = []
        for f in c.get("files", []):
            files.append({
                "filename": f.get("filename"),
                "status": f.get("status"),
                "additions": f.get("additions"),
                "deletions": f.get("deletions"),
                "patch": f.get("patch"),
            })

        return {
            "sha": c.get("sha"),
            "message": commit_obj.get("message"),
            "author_name": author.get("name"),
            "author_email": author.get("email"),
            "author_login": (c.get("author") or {}).get("login"),
            "author_date": author.get("date"),
            "committer_name": committer.get("name"),
            "committer_email": committer.get("email"),
            "committer_login": (c.get("committer") or {}).get("login"),
            "committer_date": committer.get("date"),
            "parents": [p["sha"] for p in c.get("parents", [])],
            "stats": {
                "additions": stats.get("additions"),
                "deletions": stats.get("deletions"),
                "total": stats.get("total"),
            },
            "files": files,
            "html_url": c.get("html_url"),
        }

    # ── Helpers ───────────────────────────────────────────────────────────

    def _api_get(self, url: str, **kwargs) -> requests.Response:
        """GET with per-request token selection and rate-limit 403 retry.

        In pool mode: picks the app with the most remaining budget before
        each request, and feeds rate limit headers back to the pool.
        On rate limit 403: switches to next best app and retries immediately.
        """
        token = self._pick_token()
        self.session.headers["Authorization"] = f"Bearer {token}"
        resp = self.session.get(url, **kwargs)
        self._feed_back_rate_limit(token, resp)

        if resp.status_code == 403 and self._is_rate_limited(resp):
            if self._app_pool:
                # Try another app immediately
                token = self._pick_token()
                self.session.headers["Authorization"] = f"Bearer {token}"
                resp = self.session.get(url, **kwargs)
                self._feed_back_rate_limit(token, resp)

            # If still rate limited (all apps exhausted), wait for reset
            if resp.status_code == 403 and self._is_rate_limited(resp):
                wait = self._rate_limit_wait(resp)
                log.warning("All tokens exhausted, waiting %ds for reset", wait)
                time.sleep(wait)
                token = self._pick_token()
                self.session.headers["Authorization"] = f"Bearer {token}"
                resp = self.session.get(url, **kwargs)
                self._feed_back_rate_limit(token, resp)

        return resp

    def _pick_token(self) -> str:
        """Pick the best available token (pool mode) or return PAT."""
        if self._app_pool:
            return self._app_pool.get_best_token()
        return self._pat_token

    def _feed_back_rate_limit(self, token: str, resp: requests.Response) -> None:
        """Update the pool's budget tracking from response headers."""
        if not self._app_pool:
            return
        remaining = resp.headers.get("X-RateLimit-Remaining")
        reset = resp.headers.get("X-RateLimit-Reset")
        if remaining is not None:
            self._app_pool.update_remaining(
                token,
                int(remaining),
                float(reset) if reset else None,
            )

    @staticmethod
    def _is_rate_limited(resp) -> bool:
        try:
            return "rate limit" in resp.json().get("message", "").lower()
        except Exception:
            return False

    @staticmethod
    def _rate_limit_wait(resp) -> int:
        """Compute seconds to wait from X-RateLimit-Reset header."""
        reset = resp.headers.get("X-RateLimit-Reset")
        if reset:
            wait = int(reset) - int(time.time()) + 5  # 5s buffer
            return max(wait, 60)
        return 300  # default 5 minutes if no header

    @staticmethod
    def _log_403(resp, context: str) -> None:
        """Log a 403 with the correct reason — rate limit vs permission."""
        try:
            msg = resp.json().get("message", "")
        except Exception:
            msg = ""
        if "rate limit" in msg.lower():
            log.warning("%s — rate limit exceeded (403), will retry after reset", context)
        else:
            log.warning("%s — forbidden (403), token may lack access", context)

    # ── Pull Requests ─────────────────────────────────────────────────────

    def _export_pull_requests(self):
        log.info("Exporting pull requests (limit=%s, state=%s)", self.pr_limit or "all", self.pr_state)
        self.checkpoint.start_phase("pull_requests", total=self.pr_limit or None)

        # Step 1: paginate PR list
        pr_numbers = self._list_pr_numbers()
        log.info("Found %d PRs", len(pr_numbers))

        # Step 2: fetch full details in parallel — each PR written to its own S3 file
        to_fetch = [n for n in pr_numbers if not self.checkpoint.is_item_done("pull_requests", n)]
        log.info("Fetching details for %d PRs (%d already done)",
                 len(to_fetch), len(pr_numbers) - len(to_fetch))

        pr_count = 0
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as pool:
            futures = {pool.submit(self._fetch_pr_detail, n): n for n in to_fetch}
            for future in as_completed(futures):
                number = futures[future]
                try:
                    pr = future.result()
                    if pr:
                        self.s3.upload_json(pr, f"{self.s3_base}/prs/{number}.json")
                        self._accumulate_pr_stats(pr)
                        self.stats.save()
                        pr_count += 1
                    self.checkpoint.mark_item_done("pull_requests", number)
                    self.checkpoint.save()
                except Exception:
                    log.error("Failed to fetch PR #%d", number, exc_info=True)

        self.stats.set_nested("pull_requests.total", len(pr_numbers))
        self.stats.save(force=True)
        self.checkpoint.complete_phase("pull_requests")
        self.checkpoint.save(force=True)
        log.info("Exported %d pull requests", pr_count)

    def _list_pr_numbers(self) -> list[int]:
        numbers = []
        page = 1
        while True:
            resp = self._api_get(
                f"{API_BASE}/repos/{self.repo}/pulls",
                params={
                    "state": self.pr_state,
                    "per_page": 100,
                    "page": page,
                    "sort": "updated",
                    "direction": "desc",
                },
            )
            resp.raise_for_status()
            batch = resp.json()
            if not batch:
                break
            for pr in batch:
                numbers.append(pr["number"])
                if self.pr_limit and len(numbers) >= self.pr_limit:
                    return numbers
            page += 1
        return numbers

    def _fetch_pr_detail(self, number: int) -> dict | None:
        resp = self._api_get(f"{API_BASE}/repos/{self.repo}/pulls/{number}")
        if resp.status_code == 404:
            log.warning("PR #%d not found (404)", number)
            return None
        if resp.status_code == 403:
            self._log_403(resp, f"PR #{number}")
            return None
        resp.raise_for_status()
        pr = resp.json()

        # Fetch sub-resources
        reviews = self._fetch_pr_reviews(number)
        review_comments = self._fetch_pr_review_comments(number)
        comments = self._fetch_pr_comments(number)
        commits = self._fetch_pr_commits(number)

        return {
            "number": pr.get("number"),
            "title": pr.get("title"),
            "state": pr.get("state"),
            "author": (pr.get("user") or {}).get("login"),
            "author_id": (pr.get("user") or {}).get("id"),
            "created_at": pr.get("created_at"),
            "updated_at": pr.get("updated_at"),
            "closed_at": pr.get("closed_at"),
            "merged_at": pr.get("merged_at"),
            "merge_commit_sha": pr.get("merge_commit_sha"),
            "draft": pr.get("draft"),
            "body": pr.get("body"),
            "head_ref": (pr.get("head") or {}).get("ref"),
            "base_ref": (pr.get("base") or {}).get("ref"),
            "labels": [l.get("name") for l in pr.get("labels", [])],
            "assignees": [a.get("login") for a in pr.get("assignees", [])],
            "requested_reviewers": [r.get("login") for r in pr.get("requested_reviewers", [])],
            "additions": pr.get("additions"),
            "deletions": pr.get("deletions"),
            "changed_files": pr.get("changed_files"),
            "reviews": reviews,
            "review_comments": review_comments,
            "comments": comments,
            "commits": commits,
            "html_url": pr.get("html_url"),
        }

    def _fetch_pr_reviews(self, number: int) -> list[dict]:
        items = []
        page = 1
        while True:
            resp = self._api_get(
                f"{API_BASE}/repos/{self.repo}/pulls/{number}/reviews",
                params={"per_page": 100, "page": page},
            )
            if resp.status_code != 200:
                break
            batch = resp.json()
            if not batch:
                break
            for r in batch:
                items.append({
                    "reviewer": (r.get("user") or {}).get("login"),
                    "state": r.get("state"),
                    "body": r.get("body"),
                    "submitted_at": r.get("submitted_at"),
                })
            page += 1
        return items

    def _fetch_pr_review_comments(self, number: int) -> list[dict]:
        items = []
        page = 1
        while True:
            resp = self._api_get(
                f"{API_BASE}/repos/{self.repo}/pulls/{number}/comments",
                params={"per_page": 100, "page": page},
            )
            if resp.status_code != 200:
                break
            batch = resp.json()
            if not batch:
                break
            for c in batch:
                items.append({
                    "author": (c.get("user") or {}).get("login"),
                    "body": c.get("body"),
                    "path": c.get("path"),
                    "diff_hunk": c.get("diff_hunk"),
                    "created_at": c.get("created_at"),
                })
            page += 1
        return items

    def _fetch_pr_comments(self, number: int) -> list[dict]:
        items = []
        page = 1
        while True:
            resp = self._api_get(
                f"{API_BASE}/repos/{self.repo}/issues/{number}/comments",
                params={"per_page": 100, "page": page},
            )
            if resp.status_code != 200:
                break
            batch = resp.json()
            if not batch:
                break
            for c in batch:
                items.append({
                    "author": (c.get("user") or {}).get("login"),
                    "body": c.get("body"),
                    "created_at": c.get("created_at"),
                })
            page += 1
        return items

    def _fetch_pr_commits(self, number: int) -> list[dict]:
        items = []
        page = 1
        while True:
            resp = self._api_get(
                f"{API_BASE}/repos/{self.repo}/pulls/{number}/commits",
                params={"per_page": 100, "page": page},
            )
            if resp.status_code != 200:
                break
            batch = resp.json()
            if not batch:
                break
            for c in batch:
                commit_obj = c.get("commit", {})
                author = commit_obj.get("author", {})
                items.append({
                    "sha": c.get("sha"),
                    "message": commit_obj.get("message"),
                    "author_name": author.get("name"),
                    "author_email": author.get("email"),
                    "author_login": (c.get("author") or {}).get("login"),
                    "date": author.get("date"),
                })
            page += 1
        return items

    # ── Stats ────────────────────────────────────────────────────────────

    def _accumulate_pr_stats(self, pr: dict) -> None:
        """Accumulate per-PR statistics into the stats collector."""
        state = pr.get("state", "unknown")
        merged = pr.get("merged_at") is not None
        if merged:
            self.stats.increment("pull_requests.merged")
        elif state == "open":
            self.stats.increment("pull_requests.open")
        elif state == "closed":
            self.stats.increment("pull_requests.closed")

        self.stats.increment("pull_requests.total_reviews",
                             len(pr.get("reviews", [])))
        self.stats.increment("pull_requests.total_review_comments",
                             len(pr.get("review_comments", [])))
        self.stats.increment("pull_requests.total_comments",
                             len(pr.get("comments", [])))
        self.stats.increment("pull_requests.total_additions",
                             pr.get("additions") or 0)
        self.stats.increment("pull_requests.total_deletions",
                             pr.get("deletions") or 0)
        self.stats.increment("pull_requests.total_changed_files",
                             pr.get("changed_files") or 0)
        for label in pr.get("labels", []):
            self.stats.add_to_map("pull_requests.labels", label)



def main():
    from lib.config import load_dotenv, env, env_int, env_bool, env_list
    from lib.input import read_csv_column

    load_dotenv()

    parser = argparse.ArgumentParser(description="Export GitHub repository data to S3")
    parser.add_argument("--token", default=env("GITHUB_TOKEN"),
                        help="GitHub PAT (or comma-separated list for round-robin)")
    parser.add_argument("--repo", nargs="+", help="Repository(s) in owner/repo format")
    parser.add_argument("--input-csv", default=env("GITHUB_INPUT_CSV"),
                        help="CSV file with 'repo' column")
    parser.add_argument("--s3-bucket", default=env("S3_BUCKET"))
    parser.add_argument("--s3-prefix", default=env("S3_PREFIX", ""))
    parser.add_argument("--pr-limit", type=int, default=env_int("GITHUB_PR_LIMIT", 0),
                        help="Max PRs (0=all)")
    parser.add_argument("--pr-state", default=env("GITHUB_PR_STATE", "all"),
                        choices=["open", "closed", "all"])
    parser.add_argument("--commit-limit", type=int, default=env_int("GITHUB_COMMIT_LIMIT", 0),
                        help="Max commits (0=all)")
    parser.add_argument("--skip-commits", action="store_true",
                        default=env_bool("GITHUB_SKIP_COMMITS", True))
    parser.add_argument("--include-commits", action="store_true",
                        default=env_bool("GITHUB_INCLUDE_COMMITS"),
                        help="Include commit export (off by default)")
    parser.add_argument("--skip-prs", action="store_true",
                        default=env_bool("GITHUB_SKIP_PRS"))
    parser.add_argument("--commit-details", action="store_true",
                        default=env_bool("GITHUB_COMMIT_DETAILS"),
                        help="Fetch full commit details (stats, files, patches)")
    # GitHub App auth (higher rate limits)
    parser.add_argument("--app-id", default=env("GITHUB_APP_ID"),
                        help="GitHub App ID (comma-separated for multiple apps)")
    parser.add_argument("--app-key", default=env("GITHUB_APP_PRIVATE_KEY"),
                        help="Path to private key .pem (comma-separated for multiple apps)")
    parser.add_argument("--app-installation-id", default=env("GITHUB_APP_INSTALLATION_ID"),
                        help="Installation ID (comma-separated for multiple apps)")
    parser.add_argument("--parallel", type=int, default=env_int("GITHUB_PARALLEL", 4),
                        help="Repos to export in parallel")
    parser.add_argument("--max-workers", type=int, default=env_int("MAX_WORKERS", 10),
                        help="Parallel PR/commit detail fetches per repo")
    parser.add_argument("--log-level", default=env("LOG_LEVEL", "INFO"))
    parser.add_argument("--no-json-logs", action="store_true",
                        default=not env_bool("JSON_LOGS", True))
    parser.add_argument("--log-dir", default=env("LOG_DIR", "logs"),
                        help="Directory for log files")
    args = parser.parse_args()

    if not args.s3_bucket:
        parser.error("--s3-bucket is required (or set S3_BUCKET)")

    # --include-commits overrides --skip-commits
    if args.include_commits:
        args.skip_commits = False

    # Resolve repo list: CLI > CSV > env var
    repos = args.repo
    if not repos and args.input_csv:
        repos = read_csv_column(args.input_csv, "repo")
    if not repos:
        repos = env_list("GITHUB_REPOS") or ([env("GITHUB_REPO")] if env("GITHUB_REPO") else [])
    if not repos:
        parser.error("--repo or --input-csv is required (or set GITHUB_REPOS)")

    # Determine auth method: GitHub App (preferred) or PAT (fallback)
    app_pool = None
    if args.app_id and args.app_key and args.app_installation_id:
        from lib.github_auth import GitHubAppAuth, GitHubAppPool
        app_ids = [x.strip() for x in args.app_id.split(",")]
        app_keys = [x.strip() for x in args.app_key.split(",")]
        app_installs = [x.strip() for x in args.app_installation_id.split(",")]
        if len(app_ids) != len(app_keys) or len(app_ids) != len(app_installs):
            parser.error("--app-id, --app-key, and --app-installation-id must have the same number of comma-separated values")
        apps = [GitHubAppAuth(app_id=aid, private_key_path=key, installation_id=iid)
                for aid, key, iid in zip(app_ids, app_keys, app_installs)]
        app_pool = GitHubAppPool(apps)
    elif not args.token:
        parser.error("--token or GitHub App config (--app-id, --app-key, --app-installation-id) is required")

    # PAT tokens (fallback)
    tokens = []
    if args.token:
        tokens = [t.strip() for t in args.token.split(",") if t.strip()]

    log_file = os.path.join(args.log_dir, "github.log")
    setup_logging(level=args.log_level, json_output=not args.no_json_logs, log_file=log_file)
    s3 = S3Store(bucket=args.s3_bucket, prefix=args.s3_prefix)
    config = ExportConfig(
        s3_bucket=args.s3_bucket,
        s3_prefix=args.s3_prefix,
        max_workers=args.max_workers,
        log_level=args.log_level,
    )

    def _export_one_repo(repo: str, index: int) -> None:
        log.info("Exporting repo %s", repo)
        repo_token = tokens[index % len(tokens)] if tokens and not app_pool else ""
        exporter = GitHubExporter(
            token=repo_token,
            repo=repo,
            s3=s3,
            config=config,
            pr_limit=args.pr_limit,
            commit_limit=args.commit_limit,
            pr_state=args.pr_state,
            skip_commits=args.skip_commits,
            skip_prs=args.skip_prs,
            commit_details=args.commit_details,
            app_pool=app_pool,
        )
        exporter.run()

    auth_desc = f"{len(app_pool)} GitHub Apps" if app_pool else f"{len(tokens)} PATs"
    failed = []
    log.info("Exporting %d repos (%d in parallel, %s)", len(repos), args.parallel, auth_desc)
    with ThreadPoolExecutor(max_workers=args.parallel) as pool:
        futures = {pool.submit(_export_one_repo, r, i): r
                   for i, r in enumerate(repos)}
        for future in as_completed(futures):
            repo = futures[future]
            try:
                future.result()
            except Exception:
                log.error("Export failed for repo %s, continuing with next", repo, exc_info=True)
                failed.append(repo)
    if failed:
        log.error("Failed repos (%d/%d): %s", len(failed), len(repos), ", ".join(failed))


if __name__ == "__main__":
    main()
