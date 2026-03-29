"""Google Workspace Exporter — exports Gmail, Calendar, and Drive data to S3."""

import argparse
import base64
import email
import io
import logging
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

from lib.s3 import S3Store
from lib.checkpoint import CheckpointManager
from lib.retry import retry
from lib.logging import setup_logging
from lib.types import ExportConfig

log = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/calendar.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]

# Google-native MIME types and their export formats
GOOGLE_EXPORT_MAP = {
    "application/vnd.google-apps.document": (
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document", ".docx"
    ),
    "application/vnd.google-apps.spreadsheet": (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", ".xlsx"
    ),
    "application/vnd.google-apps.presentation": (
        "application/vnd.openxmlformats-officedocument.presentationml.presentation", ".pptx"
    ),
    "application/vnd.google-apps.drawing": ("application/pdf", ".pdf"),
    "application/vnd.google-apps.form": ("application/pdf", ".pdf"),
}

# MIME types to skip in Drive
SKIP_DRIVE_TYPES = {
    "application/vnd.google-apps.folder",
    "application/vnd.google-apps.shortcut",
    "application/vnd.google-apps.map",
    "application/vnd.google-apps.site",
    "application/vnd.google-apps.fusiontable",
}


def _user_slug(email_addr: str) -> str:
    return email_addr.replace("@", "_at_")


class GoogleWorkspaceExporter:
    def __init__(
        self,
        user: str,
        service_account_key: str,
        s3: S3Store,
        config: ExportConfig,
        email_limit: int = 0,
        event_limit: int = 0,
        file_limit: int = 0,
        skip_gmail: bool = False,
        skip_calendar: bool = False,
        skip_drive: bool = False,
    ):
        self.user = user
        self.s3 = s3
        self.config = config
        self.email_limit = email_limit
        self.event_limit = event_limit
        self.file_limit = file_limit
        self.skip_gmail = skip_gmail
        self.skip_calendar = skip_calendar
        self.skip_drive = skip_drive

        self.user_slug = _user_slug(user)
        self.s3_base = f"google/{self.user_slug}"

        credentials = service_account.Credentials.from_service_account_file(
            service_account_key, scopes=SCOPES,
        )
        self.credentials = credentials.with_subject(user)

        self.checkpoint = CheckpointManager(s3, f"google/{self.user_slug}")

    def _build_service(self, api: str, version: str):
        return build(api, version, credentials=self.credentials, cache_discovery=False)

    def run(self):
        self.checkpoint.load()
        log.info("Starting Google Workspace export for %s", self.user)

        if not self.skip_gmail and not self.checkpoint.is_phase_done("gmail"):
            self._export_gmail()

        if not self.skip_calendar and not self.checkpoint.is_phase_done("calendar"):
            self._export_calendar()

        if not self.skip_drive and not self.checkpoint.is_phase_done("drive"):
            self._export_drive()

        self.checkpoint.complete()
        log.info("Google Workspace export complete for %s", self.user)

    # ── Gmail ─────────────────────────────────────────────────────────────

    def _export_gmail(self):
        log.info("Exporting Gmail for %s (limit=%s)", self.user, self.email_limit or "all")
        self.checkpoint.start_phase("gmail", total=self.email_limit or None)
        service = self._build_service("gmail", "v1")

        # Step 1: list message IDs
        message_ids = self._list_gmail_ids(service)
        log.info("Found %d Gmail message IDs", len(message_ids))

        # Step 2: batch-fetch and upload
        index_entries = []
        for batch_start in range(0, len(message_ids), 10):
            batch_ids = message_ids[batch_start:batch_start + 10]
            batch_ids = [mid for mid in batch_ids
                         if not self.checkpoint.is_item_done("gmail", mid)]
            if not batch_ids:
                continue

            raw_messages = self._batch_fetch_raw(service, batch_ids)

            # Parallel upload of .eml files and attachment extraction
            with ThreadPoolExecutor(max_workers=self.config.max_workers) as pool:
                futures = []
                for msg_id, msg_data in raw_messages.items():
                    futures.append(pool.submit(self._save_eml_to_s3, msg_id, msg_data))
                    futures.append(pool.submit(self._extract_and_upload_attachments, msg_id, msg_data))
                for f in as_completed(futures):
                    try:
                        f.result()
                    except Exception:
                        log.error("Gmail upload error", exc_info=True)

            # Build index entries and mark done
            for msg_id, msg_data in raw_messages.items():
                try:
                    entry = self._build_index_entry(msg_id, msg_data)
                    index_entries.append(entry)
                except Exception:
                    log.error("Failed to build index for message %s", msg_id, exc_info=True)
                self.checkpoint.mark_item_done("gmail", msg_id)

            self.checkpoint.save()
            time.sleep(2)  # 2s between batches

        # Upload index
        self.s3.upload_json(index_entries, f"{self.s3_base}/gmail/_index.json")
        self.checkpoint.complete_phase("gmail")
        self.checkpoint.save(force=True)
        log.info("Exported %d Gmail messages", len(index_entries))

    def _list_gmail_ids(self, service) -> list[str]:
        ids = []
        page_token = None
        while True:
            batch_size = 100
            if self.email_limit:
                batch_size = min(100, self.email_limit - len(ids))
                if batch_size <= 0:
                    break
            resp = service.users().messages().list(
                userId="me",
                maxResults=batch_size,
                pageToken=page_token,
            ).execute()
            for msg in resp.get("messages", []):
                ids.append(msg["id"])
                if self.email_limit and len(ids) >= self.email_limit:
                    return ids
            page_token = resp.get("nextPageToken")
            if not page_token:
                break
        return ids

    @retry(max_attempts=5, backoff_base=2.0, exceptions=(HttpError,))
    def _batch_fetch_raw(self, service, msg_ids: list[str]) -> dict[str, dict]:
        """Fetch raw messages via individual API calls (batch API is complex with googleapiclient)."""
        results = {}
        for msg_id in msg_ids:
            try:
                msg = service.users().messages().get(
                    userId="me", id=msg_id, format="raw",
                ).execute()
                results[msg_id] = msg
            except HttpError as e:
                if e.resp.status in (429, 500, 503):
                    raise  # retry decorator handles these
                log.warning("Failed to fetch message %s: %s", msg_id, e)
        return results

    def _save_eml_to_s3(self, msg_id: str, msg_data: dict) -> None:
        raw = msg_data.get("raw", "")
        eml_bytes = base64.urlsafe_b64decode(raw)
        self.s3.upload_bytes(
            eml_bytes,
            f"{self.s3_base}/gmail/{msg_id}.eml",
            content_type="message/rfc822",
        )

    def _extract_and_upload_attachments(self, msg_id: str, msg_data: dict) -> None:
        raw = msg_data.get("raw", "")
        eml_bytes = base64.urlsafe_b64decode(raw)
        msg = email.message_from_bytes(eml_bytes)

        for part in msg.walk():
            content_disposition = str(part.get("Content-Disposition", ""))
            if "attachment" not in content_disposition and "inline" not in content_disposition:
                continue
            filename = part.get_filename()
            if not filename:
                continue
            payload = part.get_payload(decode=True)
            if not payload:
                continue
            self.s3.upload_bytes(
                payload,
                f"{self.s3_base}/gmail/attachments/{msg_id}/{filename}",
            )

    def _build_index_entry(self, msg_id: str, msg_data: dict) -> dict:
        raw = msg_data.get("raw", "")
        eml_bytes = base64.urlsafe_b64decode(raw)
        msg = email.message_from_bytes(eml_bytes)

        attachments = []
        for part in msg.walk():
            filename = part.get_filename()
            if filename:
                attachments.append(filename)

        return {
            "id": msg_id,
            "threadId": msg_data.get("threadId"),
            "labelIds": msg_data.get("labelIds", []),
            "snippet": msg_data.get("snippet", ""),
            "internalDate": msg_data.get("internalDate"),
            "sizeEstimate": msg_data.get("sizeEstimate"),
            "attachments": attachments,
        }

    # ── Calendar ──────────────────────────────────────────────────────────

    def _export_calendar(self):
        log.info("Exporting Calendar for %s (limit=%s)", self.user, self.event_limit or "all")
        self.checkpoint.start_phase("calendar", total=self.event_limit or None)
        service = self._build_service("calendar", "v3")

        time_min = (datetime.now(timezone.utc) - timedelta(days=730)).isoformat()
        events = []
        page_token = None

        while True:
            batch_size = 250
            if self.event_limit:
                batch_size = min(250, self.event_limit - len(events))
                if batch_size <= 0:
                    break
            try:
                resp = service.events().list(
                    calendarId="primary",
                    timeMin=time_min,
                    maxResults=batch_size,
                    singleEvents=True,
                    orderBy="startTime",
                    pageToken=page_token,
                ).execute()
            except HttpError as e:
                log.error("Calendar API error: %s", e)
                break

            for event in resp.get("items", []):
                events.append(event)
                if self.event_limit and len(events) >= self.event_limit:
                    break

            page_token = resp.get("nextPageToken")
            if not page_token:
                break

        # Full events
        self.s3.upload_json(events, f"{self.s3_base}/calendar/events.json")

        # Lightweight summary
        summary = []
        for e in events:
            start = e.get("start", {})
            start_time = start.get("dateTime") or start.get("date")
            summary.append({
                "id": e.get("id"),
                "title": e.get("summary"),
                "start": start_time,
                "status": e.get("status"),
                "organizer": (e.get("organizer") or {}).get("email"),
                "attendee_count": len(e.get("attendees", [])),
                "location": e.get("location"),
                "hangout_link": e.get("hangoutLink"),
            })
        self.s3.upload_json(summary, f"{self.s3_base}/calendar/_summary.json")

        self.checkpoint.complete_phase("calendar")
        self.checkpoint.save(force=True)
        log.info("Exported %d calendar events", len(events))

    # ── Drive ─────────────────────────────────────────────────────────────

    def _export_drive(self):
        log.info("Exporting Drive for %s (limit=%s)", self.user, self.file_limit or "all")
        self.checkpoint.start_phase("drive", total=self.file_limit or None)
        service = self._build_service("drive", "v3")

        # List files
        files = self._list_drive_files(service)
        log.info("Found %d Drive files", len(files))

        index = []
        for file_meta in files:
            file_id = file_meta["id"]
            if self.checkpoint.is_item_done("drive", file_id):
                index.append(self._drive_index_entry(file_meta, downloaded=True))
                continue

            mime = file_meta.get("mimeType", "")

            # Skip unwanted types
            if mime in SKIP_DRIVE_TYPES or mime.startswith("image/") or mime.startswith("video/"):
                index.append(self._drive_index_entry(file_meta, downloaded=False, reason="skipped_type"))
                self.checkpoint.mark_item_done("drive", file_id)
                self.checkpoint.save()
                continue

            try:
                if mime in GOOGLE_EXPORT_MAP:
                    self._export_google_doc(service, file_meta)
                else:
                    self._download_drive_file(service, file_meta)
                index.append(self._drive_index_entry(file_meta, downloaded=True))
            except Exception:
                log.error("Failed to download Drive file %s", file_meta.get("name"), exc_info=True)
                index.append(self._drive_index_entry(file_meta, downloaded=False, reason="error"))

            self.checkpoint.mark_item_done("drive", file_id)
            self.checkpoint.save()
            time.sleep(0.3)

        self.s3.upload_json(index, f"{self.s3_base}/drive/_index.json")
        self.checkpoint.complete_phase("drive")
        self.checkpoint.save(force=True)
        log.info("Exported %d Drive files", len(index))

    def _list_drive_files(self, service) -> list[dict]:
        files = []
        page_token = None
        while True:
            batch_size = 100
            if self.file_limit:
                batch_size = min(100, self.file_limit - len(files))
                if batch_size <= 0:
                    break
            try:
                resp = service.files().list(
                    q=f"'{self.user}' in owners",
                    pageSize=batch_size,
                    orderBy="modifiedTime desc",
                    fields="nextPageToken, files(id, name, mimeType, size, owners, modifiedTime)",
                    pageToken=page_token,
                ).execute()
            except HttpError as e:
                log.error("Drive list error: %s", e)
                break

            for f in resp.get("files", []):
                files.append(f)
                if self.file_limit and len(files) >= self.file_limit:
                    return files

            page_token = resp.get("nextPageToken")
            if not page_token:
                break
        return files

    @retry(max_attempts=3, backoff_base=2.0, exceptions=(HttpError, IOError))
    def _export_google_doc(self, service, file_meta: dict) -> None:
        """Export a Google-native doc to its converted format and upload to S3."""
        mime = file_meta["mimeType"]
        export_mime, ext = GOOGLE_EXPORT_MAP[mime]
        name = file_meta.get("name", "untitled") + ext

        request = service.files().export_media(fileId=file_meta["id"], mimeType=export_mime)
        with tempfile.NamedTemporaryFile(delete=True) as tmp:
            downloader = MediaIoBaseDownload(tmp, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()
            tmp.flush()
            self.s3.upload_file(tmp.name, f"{self.s3_base}/drive/{name}")

    @retry(max_attempts=3, backoff_base=2.0, exceptions=(HttpError, IOError))
    def _download_drive_file(self, service, file_meta: dict) -> None:
        """Download a regular (non-Google) file and upload to S3."""
        name = file_meta.get("name", "untitled")
        if "." not in name:
            name += ".bin"

        request = service.files().get_media(fileId=file_meta["id"])
        with tempfile.NamedTemporaryFile(delete=True) as tmp:
            downloader = MediaIoBaseDownload(tmp, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()
            tmp.flush()
            self.s3.upload_file(tmp.name, f"{self.s3_base}/drive/{name}")

    def _drive_index_entry(self, file_meta: dict, downloaded: bool,
                           reason: str | None = None) -> dict:
        entry = {
            "id": file_meta.get("id"),
            "name": file_meta.get("name"),
            "mimeType": file_meta.get("mimeType"),
            "size": file_meta.get("size"),
            "modifiedTime": file_meta.get("modifiedTime"),
            "owners": [
                {"displayName": o.get("displayName"), "emailAddress": o.get("emailAddress")}
                for o in file_meta.get("owners", [])
            ],
            "downloaded": downloaded,
        }
        if reason:
            entry["skip_reason"] = reason
        return entry


def main():
    from lib.config import load_dotenv, env, env_int, env_bool, env_list
    from lib.input import read_csv_column

    load_dotenv()

    parser = argparse.ArgumentParser(description="Export Google Workspace data to S3")
    parser.add_argument("--user", nargs="+", help="Target user email(s)")
    parser.add_argument("--input-csv", default=env("GOOGLE_INPUT_CSV"), help="CSV file with 'user' column")
    parser.add_argument("--key", default=env("GOOGLE_SERVICE_ACCOUNT_KEY"), help="Service account JSON key file")
    parser.add_argument("--s3-bucket", default=env("S3_BUCKET"))
    parser.add_argument("--s3-prefix", default=env("S3_PREFIX", ""))
    parser.add_argument("--emails", type=int, default=env_int("GOOGLE_EMAIL_LIMIT", 0), help="Max emails (0=all)")
    parser.add_argument("--events", type=int, default=env_int("GOOGLE_EVENT_LIMIT", 0), help="Max events (0=all)")
    parser.add_argument("--files", type=int, default=env_int("GOOGLE_FILE_LIMIT", 0), help="Max Drive files (0=all)")
    parser.add_argument("--skip-gmail", action="store_true", default=env_bool("GOOGLE_SKIP_GMAIL"))
    parser.add_argument("--skip-calendar", action="store_true", default=env_bool("GOOGLE_SKIP_CALENDAR"))
    parser.add_argument("--skip-drive", action="store_true", default=env_bool("GOOGLE_SKIP_DRIVE"))
    parser.add_argument("--max-workers", type=int, default=env_int("MAX_WORKERS", 5))
    parser.add_argument("--log-level", default=env("LOG_LEVEL", "INFO"))
    parser.add_argument("--no-json-logs", action="store_true", default=not env_bool("JSON_LOGS", True))
    args = parser.parse_args()

    if not args.key:
        parser.error("--key is required (or set GOOGLE_SERVICE_ACCOUNT_KEY)")
    if not args.s3_bucket:
        parser.error("--s3-bucket is required (or set S3_BUCKET)")

    # Resolve user list: CLI > CSV > env var
    users = args.user
    if not users and args.input_csv:
        users = read_csv_column(args.input_csv, "user")
    if not users:
        users = env_list("GOOGLE_USERS") or ([env("GOOGLE_USER")] if env("GOOGLE_USER") else [])
    if not users:
        parser.error("--user or --input-csv is required (or set GOOGLE_USERS)")

    setup_logging(level=args.log_level, json_output=not args.no_json_logs)
    s3 = S3Store(bucket=args.s3_bucket, prefix=args.s3_prefix)
    config = ExportConfig(
        s3_bucket=args.s3_bucket,
        s3_prefix=args.s3_prefix,
        max_workers=args.max_workers,
        log_level=args.log_level,
    )
    failed = []
    for i, user in enumerate(users, 1):
        log.info("Exporting user %s (%d of %d)", user, i, len(users))
        try:
            exporter = GoogleWorkspaceExporter(
                user=user,
                service_account_key=args.key,
                s3=s3,
                config=config,
                email_limit=args.emails,
                event_limit=args.events,
                file_limit=args.files,
                skip_gmail=args.skip_gmail,
                skip_calendar=args.skip_calendar,
                skip_drive=args.skip_drive,
            )
            exporter.run()
        except Exception:
            log.error("Export failed for user %s, continuing with next", user, exc_info=True)
            failed.append(user)
    if failed:
        log.error("Failed users (%d/%d): %s", len(failed), len(users), ", ".join(failed))


if __name__ == "__main__":
    main()
