"""Athena Catalog Generator — aggregates _stats.json files into queryable JSON Lines tables."""

import argparse
import json
import logging
import os
from datetime import datetime, timezone

from lib.s3 import S3Store
from lib.logging import setup_logging

log = logging.getLogger(__name__)

EXPORTERS = ("github", "jira", "slack", "google")


class CatalogGenerator:
    def __init__(self, s3: S3Store, dry_run: bool = False,
                 exporter_filter: str | None = None):
        self.s3 = s3
        self.dry_run = dry_run
        self.exporter_filter = exporter_filter

    def run(self):
        stats_files = self._discover_stats_files()
        log.info("Found %d _stats.json files", len(stats_files))

        if not stats_files:
            log.warning("No _stats.json files found — run exporters first")
            return

        # Group by exporter
        by_exporter: dict[str, list[dict]] = {}
        for path, data in stats_files:
            exporter = data.get("exporter", "unknown")
            by_exporter.setdefault(exporter, []).append(data)

        # Generate per-exporter tables
        if "github" in by_exporter:
            self._write_github_tables(by_exporter["github"])
        if "jira" in by_exporter:
            self._write_jira_table(by_exporter["jira"])
        if "slack" in by_exporter:
            self._write_slack_table(by_exporter["slack"])
        if "google_workspace" in by_exporter:
            self._write_google_table(by_exporter["google_workspace"])

        # Cross-exporter file types table
        self._write_file_types_table(by_exporter)

        # Summary
        self._write_summary(by_exporter)

        log.info("Catalog generation complete")

    # ── Discovery ─────────────────────────────────────────────────────────

    def _discover_stats_files(self) -> list[tuple[str, dict]]:
        """Find and download all _stats.json files by checking known exporter prefixes."""
        results = []
        prefix = self.s3.prefix

        for exporter_prefix in EXPORTERS:
            if self.exporter_filter and exporter_prefix != self.exporter_filter:
                continue
            # List objects under {prefix}/{exporter_prefix}/ and find _stats.json
            search_prefix = f"{prefix}/{exporter_prefix}/" if prefix else f"{exporter_prefix}/"
            try:
                paginator = self.s3._client.get_paginator("list_objects_v2")
                for page in paginator.paginate(Bucket=self.s3.bucket, Prefix=search_prefix):
                    for obj in page.get("Contents", []):
                        key = obj["Key"]
                        if key.endswith("/_stats.json"):
                            # Strip prefix to get the relative path for download
                            rel_path = key[len(prefix) + 1:] if prefix else key
                            data = self.s3.download_json(rel_path)
                            if data and isinstance(data, dict):
                                results.append((rel_path, data))
            except Exception:
                log.error("Failed to list stats for %s", exporter_prefix, exc_info=True)

        return results

    # ── Upload helper ─────────────────────────────────────────────────────

    def _upload_jsonl(self, rows: list[dict], path: str) -> None:
        """Write rows as JSON Lines to S3."""
        if self.dry_run:
            log.info("[DRY RUN] Would write %d rows to %s", len(rows), path)
            for row in rows[:3]:
                log.info("  %s", json.dumps(row, default=str)[:200])
            return
        lines = "\n".join(json.dumps(row, default=str) for row in rows)
        self.s3.upload_bytes(
            lines.encode("utf-8"),
            path,
            content_type="application/jsonlines",
        )
        log.info("Wrote %d rows to %s", len(rows), path)

    # ── GitHub ────────────────────────────────────────────────────────────

    def _write_github_tables(self, stats_list: list[dict]) -> None:
        repo_rows = []
        lang_rows = []

        for s in stats_list:
            target = s.get("target", "")
            repo = s.get("repo", {})
            commits = s.get("commits", {})
            prs = s.get("pull_requests", {})
            contribs = s.get("contributors", {})

            repo_rows.append({
                "target": target,
                "target_slug": s.get("target_slug", ""),
                "exported_at": s.get("exported_at", ""),
                "private": repo.get("private"),
                "stars": repo.get("stars", 0),
                "forks": repo.get("forks", 0),
                "open_issues": repo.get("open_issues", 0),
                "watchers": repo.get("watchers", 0),
                "total_contributors": contribs.get("total", 0),
                "total_commits": commits.get("total", 0),
                "commit_unique_authors": commits.get("unique_authors", 0),
                "total_prs": prs.get("total", 0),
                "prs_open": prs.get("open", 0),
                "prs_closed": prs.get("closed", 0),
                "prs_merged": prs.get("merged", 0),
                "total_reviews": prs.get("total_reviews", 0),
                "total_review_comments": prs.get("total_review_comments", 0),
                "total_comments": prs.get("total_comments", 0),
                "total_additions": prs.get("total_additions", 0),
                "total_deletions": prs.get("total_deletions", 0),
                "total_changed_files": prs.get("total_changed_files", 0),
            })

            for lang, info in s.get("languages", {}).items():
                lang_rows.append({
                    "target": target,
                    "language": lang,
                    "bytes": info.get("bytes", 0) if isinstance(info, dict) else info,
                    "percentage": info.get("percentage", 0) if isinstance(info, dict) else 0,
                })

        self._upload_jsonl(repo_rows, "catalog/github_repos.jsonl")
        if lang_rows:
            self._upload_jsonl(lang_rows, "catalog/github_languages.jsonl")

    # ── Jira ──────────────────────────────────────────────────────────────

    def _write_jira_table(self, stats_list: list[dict]) -> None:
        rows = []
        for s in stats_list:
            tickets = s.get("tickets", {})
            comments = s.get("comments", {})
            attachments = s.get("attachments", {})

            rows.append({
                "target": s.get("target", ""),
                "exported_at": s.get("exported_at", ""),
                "total_tickets": tickets.get("total", 0),
                "by_type": tickets.get("by_type", {}),
                "by_status": tickets.get("by_status", {}),
                "by_status_category": tickets.get("by_status_category", {}),
                "by_priority": tickets.get("by_priority", {}),
                "total_comments": comments.get("total", 0),
                "tickets_with_comments": comments.get("tickets_with_comments", 0),
                "total_attachments": attachments.get("total", 0),
                "total_attachment_size_bytes": attachments.get("total_size_bytes", 0),
                "attachments_by_mime_type": attachments.get("by_mime_type", {}),
                "total_changelog_entries": s.get("changelog", {}).get("total", 0),
            })

        self._upload_jsonl(rows, "catalog/jira_projects.jsonl")

    # ── Slack ─────────────────────────────────────────────────────────────

    def _write_slack_table(self, stats_list: list[dict]) -> None:
        rows = []
        for s in stats_list:
            channel = s.get("channel", {})
            messages = s.get("messages", {})
            files = s.get("files", {})

            rows.append({
                "target": s.get("target", ""),
                "channel_name": channel.get("name"),
                "is_private": channel.get("is_private"),
                "num_members": channel.get("num_members"),
                "exported_at": s.get("exported_at", ""),
                "total_messages": messages.get("total", 0),
                "thread_parents": messages.get("thread_parents", 0),
                "total_thread_replies": messages.get("total_thread_replies", 0),
                "with_reactions": messages.get("with_reactions", 0),
                "total_reactions": messages.get("total_reactions", 0),
                "by_subtype": messages.get("by_subtype", {}),
                "total_files": files.get("total", 0),
                "files_downloaded": files.get("downloaded", 0),
                "files_by_extension": files.get("by_extension", {}),
            })

        self._upload_jsonl(rows, "catalog/slack_channels.jsonl")

    # ── Google ────────────────────────────────────────────────────────────

    def _write_google_table(self, stats_list: list[dict]) -> None:
        rows = []
        for s in stats_list:
            gmail = s.get("gmail", {})
            calendar = s.get("calendar", {})
            drive = s.get("drive", {})

            rows.append({
                "target": s.get("target", ""),
                "target_slug": s.get("target_slug", ""),
                "exported_at": s.get("exported_at", ""),
                "gmail_messages": gmail.get("total_messages", 0),
                "gmail_size_bytes": gmail.get("total_size_bytes", 0),
                "gmail_attachments": gmail.get("total_attachments", 0),
                "gmail_messages_with_attachments": gmail.get("messages_with_attachments", 0),
                "gmail_attachments_by_extension": gmail.get("attachments_by_extension", {}),
                "calendar_events": calendar.get("total_events", 0),
                "calendar_with_attendees": calendar.get("with_attendees", 0),
                "calendar_with_location": calendar.get("with_location", 0),
                "drive_files": drive.get("total_files", 0),
                "drive_downloaded": drive.get("downloaded", 0),
                "drive_skipped": drive.get("skipped", 0),
                "drive_size_bytes": drive.get("total_size_bytes", 0),
                "drive_by_mime_type": drive.get("by_mime_type", {}),
            })

        self._upload_jsonl(rows, "catalog/google_users.jsonl")

    # ── File Types (cross-exporter) ───────────────────────────────────────

    def _write_file_types_table(self, by_exporter: dict[str, list[dict]]) -> None:
        rows = []

        for s in by_exporter.get("google_workspace", []):
            target = s.get("target", "")
            for ext, count in s.get("gmail", {}).get("attachments_by_extension", {}).items():
                rows.append({"exporter": "google_workspace", "target": target,
                             "category": "email_attachment", "file_type": ext, "count": count})
            for mime, count in s.get("drive", {}).get("by_mime_type", {}).items():
                rows.append({"exporter": "google_workspace", "target": target,
                             "category": "drive_file", "file_type": mime, "count": count})

        for s in by_exporter.get("slack", []):
            target = s.get("target", "")
            for ext, count in s.get("files", {}).get("by_extension", {}).items():
                rows.append({"exporter": "slack", "target": target,
                             "category": "channel_file", "file_type": ext, "count": count})

        for s in by_exporter.get("jira", []):
            target = s.get("target", "")
            for mime, count in s.get("attachments", {}).get("by_mime_type", {}).items():
                rows.append({"exporter": "jira", "target": target,
                             "category": "ticket_attachment", "file_type": mime, "count": count})

        if rows:
            self._upload_jsonl(rows, "catalog/file_types.jsonl")

    # ── Summary ───────────────────────────────────────────────────────────

    def _write_summary(self, by_exporter: dict[str, list[dict]]) -> None:
        summary = {"generated_at": datetime.now(timezone.utc).isoformat()}

        gh = by_exporter.get("github", [])
        if gh:
            summary["github"] = {
                "repos": len(gh),
                "total_commits": sum(s.get("commits", {}).get("total", 0) for s in gh),
                "total_prs": sum(s.get("pull_requests", {}).get("total", 0) for s in gh),
                "total_contributors": sum(s.get("contributors", {}).get("total", 0) for s in gh),
            }

        jira = by_exporter.get("jira", [])
        if jira:
            summary["jira"] = {
                "projects": len(jira),
                "total_tickets": sum(s.get("tickets", {}).get("total", 0) for s in jira),
                "total_comments": sum(s.get("comments", {}).get("total", 0) for s in jira),
                "total_attachments": sum(s.get("attachments", {}).get("total", 0) for s in jira),
            }

        slack = by_exporter.get("slack", [])
        if slack:
            summary["slack"] = {
                "channels": len(slack),
                "total_messages": sum(s.get("messages", {}).get("total", 0) for s in slack),
                "total_files": sum(s.get("files", {}).get("total", 0) for s in slack),
            }

        google = by_exporter.get("google_workspace", [])
        if google:
            summary["google"] = {
                "users": len(google),
                "total_emails": sum(s.get("gmail", {}).get("total_messages", 0) for s in google),
                "total_email_attachments": sum(s.get("gmail", {}).get("total_attachments", 0) for s in google),
                "total_calendar_events": sum(s.get("calendar", {}).get("total_events", 0) for s in google),
                "total_drive_files": sum(s.get("drive", {}).get("total_files", 0) for s in google),
            }

        if self.dry_run:
            log.info("[DRY RUN] Summary: %s", json.dumps(summary, indent=2))
            return

        self.s3.upload_json(summary, "catalog/summary.json")
        log.info("Summary: %s", json.dumps(summary, indent=2))


def main():
    from lib.config import load_dotenv, env

    load_dotenv()

    parser = argparse.ArgumentParser(description="Generate Athena-queryable catalog from export stats")
    parser.add_argument("--s3-bucket", default=env("S3_BUCKET"))
    parser.add_argument("--s3-prefix", default=env("S3_PREFIX", ""))
    parser.add_argument("--exporter", choices=["github", "jira", "slack", "google"],
                        help="Only process a specific exporter")
    parser.add_argument("--dry-run", action="store_true", help="Print summary without writing to S3")
    parser.add_argument("--log-level", default=env("LOG_LEVEL", "INFO"))
    parser.add_argument("--no-json-logs", action="store_true", default=True)
    parser.add_argument("--log-dir", default=env("LOG_DIR", "logs"), help="Directory for log files")
    args = parser.parse_args()

    if not args.s3_bucket:
        parser.error("--s3-bucket is required (or set S3_BUCKET)")

    log_file = os.path.join(args.log_dir, "catalog.log")
    setup_logging(level=args.log_level, json_output=not args.no_json_logs, log_file=log_file)
    s3 = S3Store(bucket=args.s3_bucket, prefix=args.s3_prefix)
    generator = CatalogGenerator(s3, dry_run=args.dry_run, exporter_filter=args.exporter)
    generator.run()


if __name__ == "__main__":
    main()
