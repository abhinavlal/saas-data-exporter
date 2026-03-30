"""Confluence Space Exporter — exports pages, comments, and attachments to S3."""

import argparse
import logging
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed

from lib.s3 import S3Store, sanitize_filename
from lib.checkpoint import CheckpointManager
from lib.stats import StatsCollector
from lib.session import make_session
from lib.logging import setup_logging
from lib.types import ExportConfig

log = logging.getLogger(__name__)


class ConfluenceExporter:
    def __init__(
        self,
        token: str,
        email: str,
        domain: str,
        spaces: list[str],
        s3: S3Store,
        config: ExportConfig,
        page_limit: int = 0,
        skip_attachments: bool = False,
        skip_comments: bool = False,
        body_format: str = "storage",
        parallel: int = 1,
    ):
        self.spaces = spaces
        self.parallel = parallel
        self.s3 = s3
        self.config = config
        self.page_limit = page_limit
        self.skip_attachments = skip_attachments
        self.skip_comments = skip_comments
        self.body_format = body_format
        self.domain = domain
        self.base_url = f"https://{domain}/wiki/api/v2"

        # Confluence Cloud — no published hard limit for Basic Auth,
        # but be conservative to avoid 429s.
        self.session, self.rate_state = make_session(
            requests_per_second=10,
            burst=20,
            min_remaining=50,
        )
        self.session.auth = (email, token)
        self.session.headers["Accept"] = "application/json"

        # Cache: space key -> space id (v2 uses numeric IDs)
        self._space_ids: dict[str, str] = {}

    def run(self):
        failed = []
        log.info("Exporting %d spaces (%d in parallel)", len(self.spaces), self.parallel)
        with ThreadPoolExecutor(max_workers=self.parallel) as pool:
            futures = {pool.submit(self._export_space, key): key for key in self.spaces}
            for future in as_completed(futures):
                space_key = futures[future]
                try:
                    future.result()
                except Exception:
                    log.error("Export failed for space %s, continuing with next",
                              space_key, exc_info=True)
                    failed.append(space_key)
        if failed:
            log.error("Failed spaces (%d/%d): %s",
                      len(failed), len(self.spaces), ", ".join(failed))

    # ── Space Export ─────────────────────────────────────────────────────

    def _export_space(self, space_key: str):
        log.info("Starting Confluence export for space %s", space_key)
        checkpoint = CheckpointManager(self.s3, f"confluence/{space_key}")
        checkpoint.load()
        s3_base = f"confluence/{space_key}"
        stats = StatsCollector(self.s3, f"{s3_base}/_stats.json")
        stats.load()
        stats.set("exporter", "confluence")
        stats.set("target", space_key)

        # Resolve space key -> numeric ID (v2 requires ID for sub-endpoints)
        space_id = self._resolve_space_id(space_key)
        if not space_id:
            log.error("Space %s not found, skipping", space_key)
            return

        stats.set("space_id", space_id)

        # Single phase: list pages, then fully process each page
        # (content + comments + attachments) before moving to the next.
        if not checkpoint.is_phase_done("pages"):
            self._export_all_pages(space_key, space_id, s3_base, checkpoint, stats)
            checkpoint.complete_phase("pages")
            checkpoint.save(force=True)

        index = self.s3.download_json(f"{s3_base}/pages/_index.json") or []

        from datetime import datetime, timezone
        stats.set("exported_at", datetime.now(timezone.utc).isoformat())
        stats.save(force=True)
        checkpoint.complete()
        log.info("Confluence export complete for %s (%d pages)", space_key, len(index))

    def _resolve_space_id(self, space_key: str) -> str | None:
        """Resolve a space key to its numeric v2 ID."""
        if space_key in self._space_ids:
            return self._space_ids[space_key]

        resp = self.session.get(
            f"{self.base_url}/spaces",
            params={"keys": space_key, "limit": 1},
        )
        resp.raise_for_status()
        results = resp.json().get("results", [])
        if not results:
            return None

        space = results[0]
        space_id = space.get("id")
        self._space_ids[space_key] = space_id
        return space_id

    # ── Pages ────────────────────────────────────────────────────────────

    def _export_all_pages(self, space_key: str, space_id: str, s3_base: str,
                          checkpoint: CheckpointManager,
                          stats: StatsCollector) -> None:
        """Paginate pages and fully export each (content + comments + attachments)
        before fetching the next batch.

        Crash recovery: the pagination cursor is saved after each batch so
        restarts resume where they left off.  The page index is rebuilt from
        ``completed_ids`` (already-done pages) plus newly processed pages.
        """
        checkpoint.start_phase("pages", total=self.page_limit or None)

        # Restore page_ids from previous run's completed_ids
        phase = checkpoint.phases.get("pages")
        already_done = list(phase.completed_ids) if phase else []
        page_ids: list[str] = list(already_done)
        page_count = len(already_done)

        if already_done:
            log.info("Resuming: %d pages already done, continuing from saved cursor",
                     len(already_done))

        # Resume pagination from saved cursor (if any)
        saved_cursor = checkpoint.get_cursor("pages")
        if saved_cursor:
            url = f"https://{self.domain}{saved_cursor}"
            params = {}
        else:
            url = f"{self.base_url}/spaces/{space_id}/pages"
            params = {
                "limit": 250,
                "depth": "all",
                "sort": "created-date",
                "body-format": self.body_format,
            }

        while url:
            if self.page_limit and page_count >= self.page_limit:
                break

            resp = self.session.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()

            for page in data.get("results", []):
                if self.page_limit and page_count >= self.page_limit:
                    break

                page_id = page.get("id")
                if not page_id:
                    continue

                if checkpoint.is_item_done("pages", page_id):
                    continue

                try:
                    self._export_one_page(page, space_key, s3_base, checkpoint, stats)
                    page_ids.append(page_id)
                    page_count += 1
                except Exception:
                    log.error("Failed to export page %s (%s), continuing",
                              page_id, page.get("title", "?"), exc_info=True)

            # Save cursor so restarts skip already-paginated batches
            next_path = data.get("_links", {}).get("next")
            checkpoint.set_cursor("pages", next_path)
            checkpoint.save()

            if next_path:
                url = f"https://{self.domain}{next_path}"
                params = {}
            else:
                url = None

        # Write index — all done page IDs (restored + newly processed)
        self.s3.upload_json(page_ids, f"{s3_base}/pages/_index.json")
        stats.save(force=True)
        log.info("Exported %d pages for space %s (%d already done)",
                 page_count, space_key, len(already_done))

    def _export_one_page(self, page: dict, space_key: str, s3_base: str,
                         checkpoint: CheckpointManager,
                         stats: StatsCollector) -> None:
        """Fully export a single page: content + comments + attachments."""
        page_id = page["id"]

        # Build page record with body content
        page_data = self._build_page_record(page, space_key)

        # Fetch and embed comments
        if not self.skip_comments:
            comments = self._fetch_page_comments(page_id)
            page_data["comments"] = comments
            comment_count = len(comments)
            stats.increment("comments.total", comment_count)
            if comment_count:
                stats.increment("comments.pages_with_comments")

        # Write the complete page file (content + comments) in one shot
        self.s3.upload_json(page_data, f"{s3_base}/pages/{page_id}.json")

        # Download attachments
        if not self.skip_attachments:
            self._download_page_attachments(page_id, s3_base, stats)

        # Page stats
        stats.increment("pages.total")
        stats.add_to_map("pages.by_status", page.get("status", "unknown"))

        # Mark page as fully done (content + comments + attachments)
        checkpoint.mark_item_done("pages", page_id)
        checkpoint.save()
        stats.save()

    def _build_page_record(self, page: dict, space_key: str) -> dict:
        """Build a normalized page record from the v2 API response."""
        body = page.get("body", {})
        body_content = None
        for fmt in ("storage", "atlas_doc_format", "view"):
            if fmt in body:
                body_content = body[fmt].get("value")
                break

        return {
            "id": page.get("id"),
            "title": page.get("title"),
            "space_key": space_key,
            "space_id": page.get("spaceId"),
            "status": page.get("status"),
            "created_at": page.get("createdAt"),
            "author_id": page.get("authorId"),
            "parent_id": page.get("parentId"),
            "parent_type": page.get("parentType"),
            "position": page.get("position"),
            "version": page.get("version", {}).get("number") if isinstance(page.get("version"), dict) else page.get("version"),
            "body_format": self.body_format,
            "body": body_content,
        }

    # ── Comments ─────────────────────────────────────────────────────────

    def _fetch_page_comments(self, page_id: str) -> list[dict]:
        """Fetch footer comments for a page."""
        comments = []

        url = f"{self.base_url}/pages/{page_id}/footer-comments"
        params = {"limit": 250, "body-format": "storage"}

        while url:
            resp = self.session.get(url, params=params)
            if resp.status_code != 200:
                log.warning("Failed to fetch comments for page %s: %d",
                            page_id, resp.status_code)
                break
            data = resp.json()

            for comment in data.get("results", []):
                body = comment.get("body", {})
                body_content = None
                for fmt in ("storage", "atlas_doc_format", "view"):
                    if fmt in body:
                        body_content = body[fmt].get("value")
                        break

                comments.append({
                    "id": comment.get("id"),
                    "author_id": comment.get("authorId"),
                    "created_at": comment.get("createdAt"),
                    "version": comment.get("version", {}).get("number") if isinstance(comment.get("version"), dict) else None,
                    "body": body_content,
                })

            next_path = data.get("_links", {}).get("next")
            if next_path:
                url = f"https://{self.domain}{next_path}"
                params = {}
            else:
                url = None

        return comments

    # ── Attachments ──────────────────────────────────────────────────────

    def _download_page_attachments(self, page_id: str, s3_base: str,
                                   stats: StatsCollector) -> None:
        """List and download all attachments for a single page."""
        attachments = self._list_page_attachments(page_id)
        if not attachments:
            return

        for att in attachments:
            att_id = att.get("id", "unknown")
            filename = sanitize_filename(att.get("title", "unnamed"))
            download_url = att.get("_links", {}).get("download")
            if not download_url:
                continue

            s3_path = f"{s3_base}/attachments/{page_id}/{filename}"
            full_url = f"https://{self.domain}{download_url}"

            try:
                self._download_attachment(full_url, s3_path, filename)
                stats.increment("attachments.total")
                media_type = att.get("mediaType", "unknown")
                stats.add_to_map("attachments.by_media_type", media_type)
                stats.increment("attachments.total_size_bytes", att.get("fileSize", 0))
            except Exception as exc:
                # 404 is common for old/deleted attachments — warn, don't log full traceback
                status = getattr(getattr(exc, "response", None), "status_code", None)
                if status == 404:
                    log.warning("Attachment not found (404): %s on page %s", filename, page_id)
                else:
                    log.error("Failed to download attachment %s for page %s",
                              att_id, page_id, exc_info=True)
                stats.increment("attachments.failed")

    def _list_page_attachments(self, page_id: str) -> list[dict]:
        """List attachments for a page via v2 API."""
        attachments = []

        url = f"{self.base_url}/pages/{page_id}/attachments"
        params = {"limit": 250}

        while url:
            resp = self.session.get(url, params=params)
            if resp.status_code != 200:
                log.warning("Failed to list attachments for page %s: %d",
                            page_id, resp.status_code)
                break
            data = resp.json()
            attachments.extend(data.get("results", []))

            next_path = data.get("_links", {}).get("next")
            if next_path:
                url = f"https://{self.domain}{next_path}"
                params = {}
            else:
                url = None

        return attachments

    def _download_attachment(self, url: str, s3_path: str, filename: str) -> None:
        """Download an attachment binary and upload to S3."""
        with tempfile.NamedTemporaryFile(suffix=f"_{filename}", delete=True) as tmp:
            resp = self.session.get(url, stream=True, timeout=(10, 300))
            resp.raise_for_status()
            for chunk in resp.iter_content(chunk_size=8192):
                tmp.write(chunk)
            tmp.flush()
            self.s3.upload_file(tmp.name, s3_path)


def main():
    from lib.config import load_dotenv, env, env_int, env_bool, env_list
    from lib.input import read_csv_column

    load_dotenv()

    parser = argparse.ArgumentParser(description="Export Confluence space data to S3")
    parser.add_argument("--token", default=env("CONFLUENCE_TOKEN", env("JIRA_TOKEN")),
                        help="Confluence/Atlassian API token (defaults to JIRA_TOKEN)")
    parser.add_argument("--email", default=env("CONFLUENCE_EMAIL", env("JIRA_EMAIL")),
                        help="Atlassian account email (defaults to JIRA_EMAIL)")
    parser.add_argument("--domain", default=env("CONFLUENCE_DOMAIN", env("JIRA_DOMAIN", "practo.atlassian.net")),
                        help="Atlassian domain")
    parser.add_argument("--space", action="append", dest="spaces",
                        help="Space key(s) to export")
    parser.add_argument("--input-csv", default=env("CONFLUENCE_INPUT_CSV"),
                        help="CSV file with 'space' column")
    parser.add_argument("--s3-bucket", default=env("S3_BUCKET"))
    parser.add_argument("--s3-prefix", default=env("S3_PREFIX", ""))
    parser.add_argument("--page-limit", type=int,
                        default=env_int("CONFLUENCE_PAGE_LIMIT", 0),
                        help="Max pages per space (0=all)")
    parser.add_argument("--skip-attachments", action="store_true",
                        default=env_bool("CONFLUENCE_SKIP_ATTACHMENTS"))
    parser.add_argument("--skip-comments", action="store_true",
                        default=env_bool("CONFLUENCE_SKIP_COMMENTS"))
    parser.add_argument("--body-format", default=env("CONFLUENCE_BODY_FORMAT", "storage"),
                        choices=["storage", "atlas_doc_format"],
                        help="Page body format (default: storage)")
    parser.add_argument("--parallel", type=int,
                        default=env_int("CONFLUENCE_PARALLEL", 1),
                        help="Spaces to export in parallel")
    parser.add_argument("--max-workers", type=int,
                        default=env_int("MAX_WORKERS", 5))
    parser.add_argument("--log-level", default=env("LOG_LEVEL", "INFO"))
    parser.add_argument("--no-json-logs", action="store_true",
                        default=not env_bool("JSON_LOGS", True))
    args = parser.parse_args()

    # Resolve space list: CLI > CSV > env var
    if not args.spaces and args.input_csv:
        args.spaces = read_csv_column(args.input_csv, "space")
    if not args.spaces:
        args.spaces = env_list("CONFLUENCE_SPACES")
    if not args.spaces:
        parser.error("At least one --space is required (or set CONFLUENCE_SPACES or use --input-csv)")
    if not args.token:
        parser.error("--token is required (or set CONFLUENCE_TOKEN / JIRA_TOKEN)")
    if not args.email:
        parser.error("--email is required (or set CONFLUENCE_EMAIL / JIRA_EMAIL)")
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
    exporter = ConfluenceExporter(
        token=args.token,
        email=args.email,
        domain=args.domain,
        spaces=args.spaces,
        s3=s3,
        config=config,
        page_limit=args.page_limit,
        skip_attachments=args.skip_attachments,
        skip_comments=args.skip_comments,
        body_format=args.body_format,
        parallel=args.parallel,
    )
    exporter.run()


if __name__ == "__main__":
    main()
