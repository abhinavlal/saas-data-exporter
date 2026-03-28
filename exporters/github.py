"""GitHub Repository Exporter — exports repo metadata, contributors, commits, and PRs to S3."""

import argparse
import csv
import io
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from lib.s3 import S3Store
from lib.checkpoint import CheckpointManager
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
        pr_limit: int = 500,
        commit_limit: int = 1000,
        pr_state: str = "all",
        skip_commits: bool = False,
        skip_prs: bool = False,
    ):
        self.repo = repo
        self.s3 = s3
        self.config = config
        self.pr_limit = pr_limit
        self.commit_limit = commit_limit
        self.pr_state = pr_state
        self.skip_commits = skip_commits
        self.skip_prs = skip_prs

        self.session, self.rate_state = make_session(
            requests_per_second=10,
            burst=20,
            min_remaining=50,
        )
        self.session.headers["Authorization"] = f"Bearer {token}"
        self.session.headers["Accept"] = "application/vnd.github+json"

        self.repo_slug = repo.replace("/", "__")
        self.s3_base = f"github/{self.repo_slug}"
        self.checkpoint = CheckpointManager(s3, f"github/{self.repo_slug}")

    def run(self):
        self.checkpoint.load()
        log.info("Starting GitHub export for %s", self.repo)

        if not self.checkpoint.is_phase_done("metadata"):
            self._export_metadata()

        if not self.checkpoint.is_phase_done("contributors"):
            self._export_contributors()

        if not self.skip_commits and not self.checkpoint.is_phase_done("commits"):
            self._export_commits()

        if not self.skip_prs and not self.checkpoint.is_phase_done("pull_requests"):
            self._export_pull_requests()

        self.checkpoint.complete()
        log.info("GitHub export complete for %s", self.repo)

    # ── Metadata ──────────────────────────────────────────────────────────

    def _export_metadata(self):
        log.info("Exporting repository metadata")
        self.checkpoint.start_phase("metadata")

        resp = self.session.get(f"{API_BASE}/repos/{self.repo}")
        resp.raise_for_status()
        repo_data = resp.json()

        # Language breakdown
        lang_resp = self.session.get(f"{API_BASE}/repos/{self.repo}/languages")
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
            resp = self.session.get(
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
        self.checkpoint.complete_phase("contributors")
        self.checkpoint.save(force=True)
        log.info("Exported %d contributors", len(contributors))

    # ── Commits ───────────────────────────────────────────────────────────

    def _export_commits(self):
        log.info("Exporting commits (limit=%d)", self.commit_limit)
        self.checkpoint.start_phase("commits", total=self.commit_limit)

        # Step 1: paginate commit list to get SHAs
        shas = self._list_commit_shas()
        log.info("Found %d commit SHAs", len(shas))

        # Step 2: fetch full details in parallel
        commits = []
        to_fetch = [sha for sha in shas if not self.checkpoint.is_item_done("commits", sha)]
        log.info("Fetching details for %d commits (%d already done)",
                 len(to_fetch), len(shas) - len(to_fetch))

        with ThreadPoolExecutor(max_workers=self.config.max_workers) as pool:
            futures = {pool.submit(self._fetch_commit_detail, sha): sha for sha in to_fetch}
            for future in as_completed(futures):
                sha = futures[future]
                try:
                    commit = future.result()
                    if commit:
                        commits.append(commit)
                    self.checkpoint.mark_item_done("commits", sha)
                    self.checkpoint.save()
                except Exception:
                    log.error("Failed to fetch commit %s", sha, exc_info=True)

        # Also include previously completed commits if resuming
        # (they were already uploaded in prior run — re-fetch for final JSON)
        # For simplicity, we re-fetch all. Checkpoint prevents duplicate API calls.
        # Sort by date descending
        commits.sort(key=lambda c: c.get("author_date", ""), reverse=True)

        self.s3.upload_json(commits, f"{self.s3_base}/commits.json")
        self.checkpoint.complete_phase("commits")
        self.checkpoint.save(force=True)
        log.info("Exported %d commits", len(commits))

    def _list_commit_shas(self) -> list[str]:
        shas = []
        page = 1
        while len(shas) < self.commit_limit:
            resp = self.session.get(
                f"{API_BASE}/repos/{self.repo}/commits",
                params={"per_page": 100, "page": page},
            )
            resp.raise_for_status()
            batch = resp.json()
            if not batch:
                break
            for c in batch:
                shas.append(c["sha"])
                if len(shas) >= self.commit_limit:
                    break
            page += 1
        return shas

    def _fetch_commit_detail(self, sha: str) -> dict | None:
        resp = self.session.get(f"{API_BASE}/repos/{self.repo}/commits/{sha}")
        if resp.status_code == 404:
            log.warning("Commit %s not found (404)", sha)
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

    # ── Pull Requests ─────────────────────────────────────────────────────

    def _export_pull_requests(self):
        log.info("Exporting pull requests (limit=%d, state=%s)", self.pr_limit, self.pr_state)
        self.checkpoint.start_phase("pull_requests", total=self.pr_limit)

        # Step 1: paginate PR list
        pr_numbers = self._list_pr_numbers()
        log.info("Found %d PRs", len(pr_numbers))

        # Step 2: fetch full details in parallel
        prs = []
        to_fetch = [n for n in pr_numbers if not self.checkpoint.is_item_done("pull_requests", n)]
        log.info("Fetching details for %d PRs (%d already done)",
                 len(to_fetch), len(pr_numbers) - len(to_fetch))

        with ThreadPoolExecutor(max_workers=self.config.max_workers) as pool:
            futures = {pool.submit(self._fetch_pr_detail, n): n for n in to_fetch}
            for future in as_completed(futures):
                number = futures[future]
                try:
                    pr = future.result()
                    if pr:
                        prs.append(pr)
                    self.checkpoint.mark_item_done("pull_requests", number)
                    self.checkpoint.save()
                except Exception:
                    log.error("Failed to fetch PR #%d", number, exc_info=True)

        prs.sort(key=lambda p: p.get("number", 0), reverse=True)

        self.s3.upload_json(prs, f"{self.s3_base}/pull_requests.json")
        self._upload_pr_csv(prs)
        self.checkpoint.complete_phase("pull_requests")
        self.checkpoint.save(force=True)
        log.info("Exported %d pull requests", len(prs))

    def _list_pr_numbers(self) -> list[int]:
        numbers = []
        page = 1
        while len(numbers) < self.pr_limit:
            resp = self.session.get(
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
                if len(numbers) >= self.pr_limit:
                    break
            page += 1
        return numbers

    def _fetch_pr_detail(self, number: int) -> dict | None:
        resp = self.session.get(f"{API_BASE}/repos/{self.repo}/pulls/{number}")
        if resp.status_code == 404:
            log.warning("PR #%d not found (404)", number)
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
            resp = self.session.get(
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
            resp = self.session.get(
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
            resp = self.session.get(
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
            resp = self.session.get(
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

    # ── CSV ───────────────────────────────────────────────────────────────

    def _upload_pr_csv(self, prs: list[dict]) -> None:
        if not prs:
            self.s3.upload_bytes(b"", f"{self.s3_base}/pull_requests.csv", "text/csv")
            return

        output = io.StringIO()
        fieldnames = [
            "number", "title", "state", "author", "created_at", "updated_at",
            "closed_at", "merged_at", "draft", "body", "head_ref", "base_ref",
            "labels", "assignees", "requested_reviewers", "additions", "deletions",
            "changed_files", "review_count", "comment_count", "html_url",
        ]
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()

        for pr in prs:
            body = pr.get("body") or ""
            writer.writerow({
                "number": pr.get("number"),
                "title": pr.get("title"),
                "state": pr.get("state"),
                "author": pr.get("author"),
                "created_at": pr.get("created_at"),
                "updated_at": pr.get("updated_at"),
                "closed_at": pr.get("closed_at"),
                "merged_at": pr.get("merged_at"),
                "draft": pr.get("draft"),
                "body": body[:1000],
                "head_ref": pr.get("head_ref"),
                "base_ref": pr.get("base_ref"),
                "labels": "|".join(pr.get("labels", [])),
                "assignees": "|".join(pr.get("assignees", [])),
                "requested_reviewers": "|".join(pr.get("requested_reviewers", [])),
                "additions": pr.get("additions"),
                "deletions": pr.get("deletions"),
                "changed_files": pr.get("changed_files"),
                "review_count": len(pr.get("reviews", [])),
                "comment_count": len(pr.get("comments", [])),
                "html_url": pr.get("html_url"),
            })

        self.s3.upload_bytes(
            output.getvalue().encode("utf-8"),
            f"{self.s3_base}/pull_requests.csv",
            content_type="text/csv",
        )


def main():
    from lib.config import load_dotenv, env, env_int, env_bool, env_list
    from lib.input import read_csv_column

    load_dotenv()

    parser = argparse.ArgumentParser(description="Export GitHub repository data to S3")
    parser.add_argument("--token", default=env("GITHUB_TOKEN"), help="GitHub personal access token")
    parser.add_argument("--repo", nargs="+", help="Repository(s) in owner/repo format")
    parser.add_argument("--input-csv", default=env("GITHUB_INPUT_CSV"), help="CSV file with 'repo' column")
    parser.add_argument("--s3-bucket", default=env("S3_BUCKET"))
    parser.add_argument("--s3-prefix", default=env("S3_PREFIX", ""))
    parser.add_argument("--pr-limit", type=int, default=env_int("GITHUB_PR_LIMIT", 500))
    parser.add_argument("--pr-state", default=env("GITHUB_PR_STATE", "all"), choices=["open", "closed", "all"])
    parser.add_argument("--commit-limit", type=int, default=env_int("GITHUB_COMMIT_LIMIT", 1000))
    parser.add_argument("--skip-commits", action="store_true", default=env_bool("GITHUB_SKIP_COMMITS"))
    parser.add_argument("--skip-prs", action="store_true", default=env_bool("GITHUB_SKIP_PRS"))
    parser.add_argument("--max-workers", type=int, default=env_int("MAX_WORKERS", 5))
    parser.add_argument("--log-level", default=env("LOG_LEVEL", "INFO"))
    parser.add_argument("--no-json-logs", action="store_true", default=not env_bool("JSON_LOGS", True))
    args = parser.parse_args()

    if not args.token:
        parser.error("--token is required (or set GITHUB_TOKEN)")
    if not args.s3_bucket:
        parser.error("--s3-bucket is required (or set S3_BUCKET)")

    # Resolve repo list: CLI > CSV > env var
    repos = args.repo
    if not repos and args.input_csv:
        repos = read_csv_column(args.input_csv, "repo")
    if not repos:
        repos = env_list("GITHUB_REPOS") or ([env("GITHUB_REPO")] if env("GITHUB_REPO") else [])
    if not repos:
        parser.error("--repo or --input-csv is required (or set GITHUB_REPOS)")

    setup_logging(level=args.log_level, json_output=not args.no_json_logs)
    s3 = S3Store(bucket=args.s3_bucket, prefix=args.s3_prefix)
    config = ExportConfig(
        s3_bucket=args.s3_bucket,
        s3_prefix=args.s3_prefix,
        max_workers=args.max_workers,
        log_level=args.log_level,
    )
    for repo in repos:
        log.info("Exporting repo %s (%d of %d)", repo, repos.index(repo) + 1, len(repos))
        exporter = GitHubExporter(
            token=args.token,
            repo=repo,
            s3=s3,
            config=config,
            pr_limit=args.pr_limit,
            commit_limit=args.commit_limit,
            pr_state=args.pr_state,
            skip_commits=args.skip_commits,
            skip_prs=args.skip_prs,
        )
        exporter.run()


if __name__ == "__main__":
    main()
