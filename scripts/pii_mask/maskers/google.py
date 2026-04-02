"""Google Workspace masker — Presidio-first PII replacement.

Handles calendar events, drive file metadata, Gmail index, and EML files.
"""

import logging

from lib.s3 import S3Store
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
        keys = super().list_keys(src)
        if self._user_slugs is None:
            return keys
        return [k for k in keys
                if self._user_slug_from_key(k) in self._user_slugs]

    @staticmethod
    def _user_slug_from_key(key: str) -> str:
        """Extract user slug from google/{slug}/..."""
        parts = key.split("/", 2)
        return parts[1] if len(parts) >= 2 else ""

    def mask_file(self, src: S3Store, dst: S3Store, key: str) -> str:
        if key.endswith(".eml"):
            return self._mask_eml_file(src, dst, key)
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
