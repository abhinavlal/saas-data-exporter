"""Google Workspace masker — Presidio-first PII replacement.

Handles calendar events, drive file metadata, Gmail index, EML files,
and Office documents (Drive exports + Gmail attachments).
"""

import logging

from lib.s3 import S3Store
from scripts.pii_mask.documents import is_office_doc
from scripts.pii_mask.eml import mask_eml
from scripts.pii_mask.maskers.base import BaseMasker

log = logging.getLogger(__name__)


class GoogleMasker(BaseMasker):
    prefix = "google/"

    def __init__(self, scanner, users: set[str] | None = None):
        super().__init__(scanner)
        # Optional user filter: set of emails like {"john@practo.com"}
        # Converted to S3 slug format: {"john_at_practo.com"}
        self._user_slugs = None
        if users:
            self._user_slugs = {
                u.replace("@", "_at_") for u in users
            }

    def list_keys(self, src: S3Store) -> list[str]:
        if self._user_slugs is None:
            return super().list_keys(src)
        # Build key list from index files — no S3 listing needed.
        # Structure: google/{slug}/gmail/{id}.eml
        #            google/{slug}/gmail/_index.json
        #            google/{slug}/calendar/events/{event_id}.json
        #            google/{slug}/calendar/_index.json
        #            google/{slug}/drive/_index.json
        keys = []
        for i, slug in enumerate(sorted(self._user_slugs), 1):
            base = f"{self.prefix}{slug}"
            user_keys = self._keys_from_indexes(src, base)
            keys.extend(user_keys)
            if i % 50 == 0 or i == len(self._user_slugs):
                log.info("google: enumerated %d/%d users (%d files)",
                         i, len(self._user_slugs), len(keys))
        return keys

    def _keys_from_indexes(self, src: S3Store, base: str) -> list[str]:
        """Derive all file keys from a user's index files."""
        keys = []

        # Gmail: _index.json has [{id, ...}, ...] → gmail/{id}.eml
        gmail_idx = src.download_json(f"{base}/gmail/_index.json")
        if gmail_idx:
            keys.append(f"{base}/gmail/_index.json")
            for entry in gmail_idx:
                msg_id = entry.get("id") if isinstance(entry, dict) else None
                if msg_id:
                    keys.append(f"{base}/gmail/{msg_id}.eml")

            # Gmail attachments: list Office docs per message
            for entry in gmail_idx:
                msg_id = entry.get("id") if isinstance(entry, dict) else None
                if not msg_id:
                    continue
                att_keys = src.list_keys(
                    f"{base}/gmail/attachments/{msg_id}/")
                keys.extend(k for k in att_keys if is_office_doc(k))

        # Calendar: _index.json has [event_id, ...] → calendar/events/{id}.json
        cal_idx = src.download_json(f"{base}/calendar/_index.json")
        if cal_idx:
            keys.append(f"{base}/calendar/_index.json")
            for event_id in cal_idx:
                if isinstance(event_id, str):
                    keys.append(f"{base}/calendar/events/{event_id}.json")

        # Drive: index + Office doc files
        drive_idx = src.download_json(f"{base}/drive/_index.json")
        if drive_idx is not None:
            keys.append(f"{base}/drive/_index.json")
            if isinstance(drive_idx, list):
                for entry in drive_idx:
                    if not isinstance(entry, dict):
                        continue
                    if not entry.get("downloaded"):
                        continue
                    file_id = entry.get("id", "")
                    name = entry.get("name", "")
                    if not file_id or not name:
                        continue
                    # Reconstruct S3 key: {base}/drive/{id}_{sanitized_name}
                    # The exporter appends extensions for Google-native docs
                    # so name already has .docx/.xlsx/.pptx if applicable.
                    from lib.s3 import sanitize_filename
                    s3_name = f"{file_id}_{sanitize_filename(name)}"
                    if is_office_doc(s3_name):
                        keys.append(f"{base}/drive/{s3_name}")

        return keys

    def mask_file(self, src: S3Store, dst: S3Store, key: str) -> str:
        if key.endswith(".eml"):
            return self._mask_eml_file(src, dst, key)
        if key.endswith((".docx", ".xlsx", ".pptx")):
            return self._mask_document_file(src, dst, key)
        return self._mask_json_file(src, dst, key)

    def rewrite_key(self, key: str) -> str:
        """Rewrite google/{user_at_domain}/ prefix."""
        parts = key.split("/", 2)
        if len(parts) < 3:
            return self.scanner.scan_url(key)

        slug = parts[1]
        if "_at_" not in slug:
            return self.scanner.scan_url(key)

        email = slug.replace("_at_", "@")
        mapped_email = self.scanner.scan_structured("EMAIL_ADDRESS", email)
        mapped_slug = mapped_email.replace("@", "_at_")

        return f"{parts[0]}/{mapped_slug}/{parts[2]}"

    def _mask_eml_file(self, src: S3Store, dst: S3Store,
                       key: str) -> str:
        eml_bytes = src.download_bytes(key)
        if eml_bytes is None:
            return "skipped (not found)"

        masked_bytes = mask_eml(eml_bytes, self.scanner)
        dst_key = self.rewrite_key(key)
        dst.upload_bytes(masked_bytes, dst_key,
                         content_type="message/rfc822")
        return "ok"

    def _mask_json_file(self, src: S3Store, dst: S3Store,
                        key: str) -> str:
        data = src.download_json(key)
        if data is None:
            return "skipped (not found)"

        # Universal scan — Presidio on every string value
        data = self._scan_obj(data)

        dst_key = self.rewrite_key(key)
        dst.upload_json(data, dst_key)
        return "ok"
