"""Google Workspace masker — roster-based PII replacement.

Handles calendar events and drive file metadata. Replaces attendee/
organizer emails and names, scans event descriptions and file titles.

S3 key rewriting: google/{user_at_domain}/ → google/{masked_email_slug}/
"""

import logging

from lib.s3 import S3Store
from scripts.pii_mask.eml import mask_eml
from scripts.pii_mask.maskers.base import BaseMasker

log = logging.getLogger(__name__)


class GoogleMasker(BaseMasker):
    prefix = "google/"

    def should_process(self, key: str) -> bool:
        return key.endswith(".json") or key.endswith(".eml")

    def mask_file(self, src: S3Store, dst: S3Store, key: str) -> str:
        if key.endswith(".eml"):
            return self._mask_eml_file(src, dst, key)
        return self._mask_json_file(src, dst, key)

    def _mask_eml_file(self, src: S3Store, dst: S3Store,
                       key: str) -> str:
        """Download, parse, mask, and re-upload an EML file."""
        eml_bytes = src.download_bytes(key)
        if eml_bytes is None:
            return "skipped (not found)"

        masked_bytes = mask_eml(eml_bytes, self.roster, self.scanner)
        dst_key = self.rewrite_key(key)
        dst.upload_bytes(masked_bytes, dst_key,
                         content_type="message/rfc822")
        return "ok"

    def _mask_json_file(self, src: S3Store, dst: S3Store,
                        key: str) -> str:
        data = src.download_json(key)
        if data is None:
            return "skipped (not found)"

        filename = key.rsplit("/", 1)[-1]

        if "/calendar/" in key and filename != "_index.json":
            if isinstance(data, list):
                data = [self._mask_event(e) for e in data]
            elif isinstance(data, dict):
                data = self._mask_event(data)
        elif "/drive/" in key and filename != "_index.json":
            if isinstance(data, dict):
                data = self._mask_drive_file(data)
        elif "/gmail/" in key and filename == "_index.json":
            if isinstance(data, list):
                data = [self._mask_gmail_index_entry(e) for e in data]
        elif filename in ("_stats.json", "_index.json"):
            pass
        else:
            return "skipped (unknown type)"

        data = self._replace_domains_in_obj(data)
        dst_key = self.rewrite_key(key)
        dst.upload_json(data, dst_key)
        return "ok"

    def rewrite_key(self, key: str) -> str:
        """Rewrite google/{user_at_domain}/ prefix.

        The user slug in S3 keys is the email with @ replaced by _at_.
        Map it using the roster's email lookup.
        """
        # Extract user slug: google/{slug}/...
        parts = key.split("/", 2)
        if len(parts) < 3:
            return key

        slug = parts[1]
        if "_at_" not in slug:
            return key

        # Reconstruct email from slug
        email = slug.replace("_at_", "@")
        mapped_email = self.roster.map_email(email)
        mapped_slug = mapped_email.replace("@", "_at_")

        return f"{parts[0]}/{mapped_slug}/{parts[2]}"

    # -- Calendar ---------------------------------------------------------- #

    def _mask_event(self, event: dict) -> dict:
        # Organizer
        if event.get("organizer"):
            org = event["organizer"]
            if org.get("email"):
                org["email"] = self.scanner.scan_email(org["email"])
            if org.get("displayName"):
                org["displayName"] = self.roster.map_name(
                    org["displayName"])

        # Attendees
        for att in event.get("attendees", []):
            if att.get("email"):
                att["email"] = self.scanner.scan_email(att["email"])
            if att.get("displayName"):
                att["displayName"] = self.roster.map_name(
                    att["displayName"])

        # Freeform text
        event["summary"] = self.scanner.scan(event.get("summary", ""))
        if event.get("description"):
            event["description"] = self.scanner.scan(event["description"])
        if event.get("location"):
            event["location"] = self.scanner.scan(event["location"])

        return event

    # -- Drive ------------------------------------------------------------- #

    def _mask_drive_file(self, file_meta: dict) -> dict:
        if file_meta.get("owner_email"):
            file_meta["owner_email"] = self.scanner.scan_email(
                file_meta["owner_email"])
        if file_meta.get("owner_name"):
            file_meta["owner_name"] = self.roster.map_name(
                file_meta["owner_name"])
        if file_meta.get("title"):
            file_meta["title"] = self.scanner.scan(file_meta["title"])
        return file_meta

    # -- Gmail index ------------------------------------------------------- #

    def _mask_gmail_index_entry(self, entry: dict) -> dict:
        if entry.get("from"):
            entry["from"] = self.scanner.scan_email(entry["from"])
        if entry.get("to"):
            if isinstance(entry["to"], list):
                entry["to"] = [self.scanner.scan_email(e)
                               for e in entry["to"]]
            else:
                entry["to"] = self.scanner.scan_email(entry["to"])
        if entry.get("snippet"):
            entry["snippet"] = self.scanner.scan(entry["snippet"])
        return entry
