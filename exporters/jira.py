"""Jira Project Exporter — exports tickets, comments, attachments, and changelogs to S3."""

import argparse
import csv
import io
import logging
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed

from lib.s3 import S3Store
from lib.checkpoint import CheckpointManager
from lib.session import make_session
from lib.logging import setup_logging
from lib.types import ExportConfig

log = logging.getLogger(__name__)


def extract_text_from_adf(node: dict | None) -> str:
    """Recursively extract plain text from Atlassian Document Format."""
    if node is None:
        return ""
    if isinstance(node, str):
        return node

    parts = []
    node_type = node.get("type", "")

    if node_type == "text":
        parts.append(node.get("text", ""))
    elif node_type == "mention":
        attrs = node.get("attrs", {})
        parts.append(attrs.get("text", attrs.get("id", "")))
    elif node_type == "hardBreak":
        parts.append("\n")

    for child in node.get("content", []):
        parts.append(extract_text_from_adf(child))

    return "".join(parts)


def _person_fields(person: dict | None) -> tuple[str | None, str | None, str | None]:
    """Extract (displayName, email, accountId) from a Jira person object."""
    if not person:
        return None, None, None
    return (
        person.get("displayName"),
        person.get("emailAddress"),
        person.get("accountId"),
    )


class JiraExporter:
    def __init__(
        self,
        token: str,
        email: str,
        domain: str,
        projects: list[str],
        s3: S3Store,
        config: ExportConfig,
        limit: int = 100,
        skip_attachments: bool = False,
        skip_comments: bool = False,
    ):
        self.projects = projects
        self.s3 = s3
        self.config = config
        self.limit = limit
        self.skip_attachments = skip_attachments
        self.skip_comments = skip_comments
        self.base_url = f"https://{domain}/rest/api/3"

        self.session, self.rate_state = make_session(
            requests_per_second=5,
            burst=10,
            min_remaining=50,
        )
        self.session.auth = (email, token)
        self.session.headers["Accept"] = "application/json"

        self._field_map: dict[str, str] | None = None

    def run(self):
        for project in self.projects:
            self._export_project(project)

    def _export_project(self, project_key: str):
        log.info("Starting Jira export for project %s", project_key)
        checkpoint = CheckpointManager(self.s3, f"jira/{project_key}")
        checkpoint.load()
        s3_base = f"jira/{project_key}"

        # Resolve custom field names
        if self._field_map is None:
            self._field_map = self._resolve_custom_fields()

        # Step 1: search tickets
        if not checkpoint.is_phase_done("tickets"):
            tickets = self._search_tickets(project_key, checkpoint)
            checkpoint.complete_phase("tickets")
            checkpoint.save(force=True)
        else:
            # Load tickets from S3 for comment/attachment phases
            tickets = self.s3.download_json(f"{s3_base}/tickets.json") or []

        # Step 2: fetch comments
        if not self.skip_comments and not checkpoint.is_phase_done("comments"):
            checkpoint.start_phase("comments", total=len(tickets))
            for ticket in tickets:
                key = ticket["key"]
                if checkpoint.is_item_done("comments", key):
                    continue
                ticket["comments"] = self._fetch_comments(key)
                checkpoint.mark_item_done("comments", key)
                checkpoint.save()
            checkpoint.complete_phase("comments")
            checkpoint.save(force=True)

        # Step 3: download attachments
        if not self.skip_attachments and not checkpoint.is_phase_done("attachments"):
            checkpoint.start_phase("attachments")
            self._download_all_attachments(tickets, project_key, s3_base, checkpoint)
            checkpoint.complete_phase("attachments")
            checkpoint.save(force=True)

        # Step 4: upload final results
        self.s3.upload_json(tickets, f"{s3_base}/tickets.json")
        self._upload_csv(tickets, s3_base)
        checkpoint.complete()
        log.info("Jira export complete for %s (%d tickets)", project_key, len(tickets))

    # ── Ticket Search ─────────────────────────────────────────────────────

    def _search_tickets(self, project_key: str, checkpoint: CheckpointManager) -> list[dict]:
        checkpoint.start_phase("tickets", total=self.limit)
        tickets = []
        jql = f"project = {project_key} ORDER BY created DESC"
        next_page_token = checkpoint.get_cursor("tickets")

        while len(tickets) < self.limit:
            body = {
                "jql": jql,
                "maxResults": min(100, self.limit - len(tickets)),
                "fields": ["*navigable", "attachment", "comment"],
                "expand": "changelog,renderedFields",
            }
            if next_page_token:
                body["nextPageToken"] = next_page_token

            resp = self.session.post(f"{self.base_url}/search/jql", json=body)
            resp.raise_for_status()
            data = resp.json()

            log.debug("Search response keys: %s, issues count: %d",
                      list(data.keys()), len(data.get("issues", [])))

            for issue in data.get("issues", []):
                ticket = self._parse_ticket(issue)
                tickets.append(ticket)
                checkpoint.mark_item_done("tickets", ticket["key"])

            next_page_token = data.get("nextPageToken")
            checkpoint.set_cursor("tickets", next_page_token)
            checkpoint.save()

            if not next_page_token or not data.get("issues"):
                break

        # Save intermediate results
        s3_base = f"jira/{project_key}"
        self.s3.upload_json(tickets, f"{s3_base}/tickets.json")
        log.info("Fetched %d tickets for %s", len(tickets), project_key)
        return tickets

    def _parse_ticket(self, issue: dict) -> dict:
        fields = issue.get("fields", {})
        rendered = issue.get("renderedFields", {})

        assignee_name, assignee_email, assignee_id = _person_fields(fields.get("assignee"))
        reporter_name, reporter_email, reporter_id = _person_fields(fields.get("reporter"))
        creator_name, creator_email, creator_id = _person_fields(fields.get("creator"))

        # Sprint (usually customfield, but may be in fields directly)
        sprint = fields.get("sprint")
        if sprint and isinstance(sprint, dict):
            sprint = sprint.get("name")

        # Parent
        parent = fields.get("parent")
        parent_key = parent.get("key") if parent else None
        parent_summary = (parent.get("fields", {}).get("summary") if parent else None)

        ticket = {
            "key": issue.get("key"),
            "id": issue.get("id"),
            "self": issue.get("self"),
            "summary": fields.get("summary"),
            "description_text": extract_text_from_adf(fields.get("description")),
            "description_adf": fields.get("description"),
            "issue_type": (fields.get("issuetype") or {}).get("name"),
            "status": (fields.get("status") or {}).get("name"),
            "status_category": (fields.get("status") or {}).get("statusCategory", {}).get("name"),
            "priority": (fields.get("priority") or {}).get("name"),
            "resolution": (fields.get("resolution") or {}).get("name"),
            "project_key": (fields.get("project") or {}).get("key"),
            "project_name": (fields.get("project") or {}).get("name"),
            "created": fields.get("created"),
            "updated": fields.get("updated"),
            "resolved": fields.get("resolutiondate"),
            "due_date": fields.get("duedate"),
            "assignee": assignee_name,
            "assignee_email": assignee_email,
            "assignee_account_id": assignee_id,
            "reporter": reporter_name,
            "reporter_email": reporter_email,
            "reporter_account_id": reporter_id,
            "creator": creator_name,
            "creator_email": creator_email,
            "creator_account_id": creator_id,
            "labels": fields.get("labels", []),
            "components": [c.get("name") for c in fields.get("components", [])],
            "fix_versions": [v.get("name") for v in fields.get("fixVersions", [])],
            "affected_versions": [v.get("name") for v in fields.get("versions", [])],
            "sprint": sprint,
            "parent_key": parent_key,
            "parent_summary": parent_summary,
            "original_estimate": fields.get("timeoriginalestimate", ""),
            "remaining_estimate": fields.get("timeestimate", ""),
            "time_spent": fields.get("timespent", ""),
            "votes": (fields.get("votes") or {}).get("votes", 0),
            "watchers": (fields.get("watches") or {}).get("watchCount", 0),
            "comments": [],
            "attachments": self._parse_attachments(fields.get("attachment", [])),
            "changelog": self._parse_changelog(issue.get("changelog", {})),
        }

        # Custom fields
        for field_id, value in fields.items():
            if field_id.startswith("customfield_") and value is not None:
                human_name = self._field_map.get(field_id, field_id)
                display_name = f"Custom field ({human_name})"
                # Extract display value from complex custom field types
                if isinstance(value, dict):
                    value = value.get("value") or value.get("name") or value.get("displayName") or str(value)
                elif isinstance(value, list):
                    value = ", ".join(
                        (item.get("value") or item.get("name") or item.get("displayName") or str(item))
                        if isinstance(item, dict) else str(item)
                        for item in value
                    )
                ticket[display_name] = value

        return ticket

    def _parse_attachments(self, attachments: list) -> list[dict]:
        result = []
        for a in attachments:
            author_name, author_email, _ = _person_fields(a.get("author"))
            result.append({
                "id": a.get("id"),
                "filename": a.get("filename"),
                "size": a.get("size"),
                "mime_type": a.get("mimeType"),
                "created": a.get("created"),
                "author": author_name,
                "author_email": author_email,
                "content_url": a.get("content"),
            })
        return result

    def _parse_changelog(self, changelog: dict) -> list[dict]:
        entries = []
        for history in changelog.get("histories", []):
            author = (history.get("author") or {}).get("displayName")
            date = history.get("created")
            for item in history.get("items", []):
                entries.append({
                    "date": date,
                    "author": author,
                    "field": item.get("field"),
                    "from": item.get("fromString"),
                    "to": item.get("toString"),
                })
        return entries

    # ── Comments ──────────────────────────────────────────────────────────

    def _fetch_comments(self, ticket_key: str) -> list[dict]:
        comments = []
        start_at = 0
        while True:
            resp = self.session.get(
                f"{self.base_url}/issue/{ticket_key}/comment",
                params={"startAt": start_at, "maxResults": 100, "expand": "renderedBody"},
            )
            if resp.status_code != 200:
                log.warning("Failed to fetch comments for %s: %d", ticket_key, resp.status_code)
                break
            data = resp.json()
            for c in data.get("comments", []):
                author_name, author_email, author_id = _person_fields(c.get("author"))
                comments.append({
                    "id": c.get("id"),
                    "author": author_name,
                    "author_email": author_email,
                    "author_account_id": author_id,
                    "created": c.get("created"),
                    "updated": c.get("updated"),
                    "body_text": extract_text_from_adf(c.get("body")),
                    "body_adf": c.get("body"),
                    "rendered_body": c.get("renderedBody"),
                })
                start_at += 1
            if start_at >= data.get("total", 0):
                break
        return comments

    # ── Custom Fields ─────────────────────────────────────────────────────

    def _resolve_custom_fields(self) -> dict[str, str]:
        resp = self.session.get(f"{self.base_url}/field")
        if resp.status_code != 200:
            log.warning("Failed to fetch field definitions: %d, body: %s",
                        resp.status_code, resp.text[:500])
            return {}
        fields_data = resp.json()
        log.debug("Field API returned %d fields (type: %s)",
                  len(fields_data) if isinstance(fields_data, list) else 0,
                  type(fields_data).__name__)
        field_map = {}
        for f in fields_data:
            if f.get("id", "").startswith("customfield_"):
                field_map[f["id"]] = f.get("name", f["id"])
        log.info("Resolved %d custom field names", len(field_map))
        return field_map

    # ── Attachments ───────────────────────────────────────────────────────

    def _download_all_attachments(self, tickets: list[dict], project_key: str,
                                  s3_base: str, checkpoint: CheckpointManager) -> None:
        all_downloads = []
        for ticket in tickets:
            for att in ticket.get("attachments", []):
                url = att.get("content_url")
                if not url:
                    continue
                att_id = att.get("id", "unknown")
                filename = att.get("filename", "unknown")
                s3_path = f"{s3_base}/attachments/{ticket['key']}/{filename}"
                all_downloads.append((att_id, url, s3_path, filename, ticket["key"]))

        log.info("Downloading %d attachments for %s", len(all_downloads), project_key)

        with ThreadPoolExecutor(max_workers=self.config.max_workers) as pool:
            futures = {}
            for att_id, url, s3_path, filename, ticket_key in all_downloads:
                ck_id = f"{ticket_key}/{att_id}"
                if checkpoint.is_item_done("attachments", ck_id):
                    continue
                futures[pool.submit(self._stream_attachment_to_s3, url, s3_path, filename)] = ck_id

            for future in as_completed(futures):
                ck_id = futures[future]
                try:
                    future.result()
                    checkpoint.mark_item_done("attachments", ck_id)
                    checkpoint.save()
                except Exception:
                    log.error("Failed to download attachment %s", ck_id, exc_info=True)

    def _stream_attachment_to_s3(self, url: str, s3_path: str, filename: str) -> None:
        with tempfile.NamedTemporaryFile(suffix=f"_{filename}", delete=True) as tmp:
            resp = self.session.get(url, stream=True, timeout=(10, 300))
            resp.raise_for_status()
            for chunk in resp.iter_content(chunk_size=8192):
                tmp.write(chunk)
            tmp.flush()
            self.s3.upload_file(tmp.name, s3_path)

    # ── CSV ───────────────────────────────────────────────────────────────

    def _upload_csv(self, tickets: list[dict], s3_base: str) -> None:
        if not tickets:
            self.s3.upload_bytes(b"", f"{s3_base}/tickets.csv", "text/csv")
            return

        # Collect all field names (including custom fields)
        base_fields = [
            "key", "summary", "issue_type", "status", "status_category", "priority",
            "resolution", "project_key", "created", "updated", "resolved", "due_date",
            "assignee", "assignee_email", "reporter", "reporter_email", "creator",
            "labels", "components", "fix_versions", "sprint", "parent_key",
            "original_estimate", "remaining_estimate", "time_spent",
            "votes", "watchers", "comments_text", "attachment_filenames", "changelog_count",
        ]
        custom_fields = sorted({
            k for ticket in tickets for k in ticket
            if k.startswith("Custom field")
        })
        fieldnames = base_fields + custom_fields

        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()

        for ticket in tickets:
            # Join comments as text
            comment_parts = []
            for c in ticket.get("comments", []):
                comment_parts.append(f"[{c.get('created', '')}] {c.get('author', '')}: {c.get('body_text', '')}")
            comments_text = "\n---\n".join(comment_parts)

            attachment_filenames = "|".join(a.get("filename", "") for a in ticket.get("attachments", []))
            changelog_count = len(ticket.get("changelog", []))

            row = {
                **{k: ticket.get(k) for k in base_fields if k not in ("comments_text", "attachment_filenames", "changelog_count", "labels", "components", "fix_versions")},
                "labels": "|".join(ticket.get("labels", [])),
                "components": "|".join(ticket.get("components", [])),
                "fix_versions": "|".join(ticket.get("fix_versions", [])),
                "comments_text": comments_text,
                "attachment_filenames": attachment_filenames,
                "changelog_count": changelog_count,
            }
            # Add custom fields
            for cf in custom_fields:
                row[cf] = ticket.get(cf, "")
            writer.writerow(row)

        self.s3.upload_bytes(
            output.getvalue().encode("utf-8"),
            f"{s3_base}/tickets.csv",
            content_type="text/csv",
        )


def main():
    from lib.config import load_dotenv, env, env_int, env_bool, env_list
    from lib.input import read_csv_column

    load_dotenv()

    parser = argparse.ArgumentParser(description="Export Jira project data to S3")
    parser.add_argument("--token", default=env("JIRA_TOKEN"), help="Jira API token")
    parser.add_argument("--email", default=env("JIRA_EMAIL"), help="Jira account email")
    parser.add_argument("--domain", default=env("JIRA_DOMAIN", "org_name.atlassian.net"), help="Jira domain")
    parser.add_argument("--project", action="append", dest="projects", help="Project key(s) to export")
    parser.add_argument("--input-csv", default=env("JIRA_INPUT_CSV"), help="CSV file with 'project' column")
    parser.add_argument("--s3-bucket", default=env("S3_BUCKET"))
    parser.add_argument("--s3-prefix", default=env("S3_PREFIX", ""))
    parser.add_argument("--limit", type=int, default=env_int("JIRA_LIMIT", 100), help="Max tickets per project")
    parser.add_argument("--skip-attachments", action="store_true", default=env_bool("JIRA_SKIP_ATTACHMENTS"))
    parser.add_argument("--skip-comments", action="store_true", default=env_bool("JIRA_SKIP_COMMENTS"))
    parser.add_argument("--max-workers", type=int, default=env_int("MAX_WORKERS", 5))
    parser.add_argument("--log-level", default=env("LOG_LEVEL", "INFO"))
    parser.add_argument("--no-json-logs", action="store_true", default=not env_bool("JSON_LOGS", True))
    args = parser.parse_args()

    # Resolve project list: CLI > CSV > env var
    if not args.projects and args.input_csv:
        args.projects = read_csv_column(args.input_csv, "project")
    if not args.projects:
        args.projects = env_list("JIRA_PROJECTS")
    if not args.projects:
        parser.error("At least one --project is required (or set JIRA_PROJECTS or use --input-csv)")
    if not args.token:
        parser.error("--token is required (or set JIRA_TOKEN)")
    if not args.email:
        parser.error("--email is required (or set JIRA_EMAIL)")
    if not args.s3_bucket:
        parser.error("--s3-bucket is required (or set S3_BUCKET)")

    setup_logging(level=args.log_level, json_output=not args.no_json_logs)
    s3 = S3Store(bucket=args.s3_bucket, prefix=args.s3_prefix)
    config = ExportConfig(
        s3_bucket=args.s3_bucket,
        s3_prefix=args.s3_prefix,
        max_workers=args.max_workers,
        log_level=args.log_level,
    )
    exporter = JiraExporter(
        token=args.token,
        email=args.email,
        domain=args.domain,
        projects=args.projects,
        s3=s3,
        config=config,
        limit=args.limit,
        skip_attachments=args.skip_attachments,
        skip_comments=args.skip_comments,
    )
    exporter.run()


if __name__ == "__main__":
    main()
