"""Confluence masker — Presidio-first PII replacement."""

import logging

from lib.s3 import S3Store
from scripts.pii_mask.maskers.base import BaseMasker

log = logging.getLogger(__name__)


class ConfluenceMasker(BaseMasker):
    prefix = "confluence/"

    def should_process(self, key: str) -> bool:
        return key.endswith(".json") and "/attachments/" not in key

    def mask_file(self, src: S3Store, dst: S3Store, key: str) -> str:
        data = src.download_json(key)
        if data is None:
            return "skipped (not found)"

        filename = key.rsplit("/", 1)[-1]

        if "/pages/" in key and filename != "_index.json":
            data = self._mask_page(data)
        elif filename in ("_stats.json", "_index.json"):
            pass
        else:
            data = self._scan_obj(data)

        dst.upload_json(data, key)
        return "ok"

    def _mask_page(self, page: dict) -> dict:
        if page.get("author_id"):
            page["author_id"] = self.scanner.scan_structured(
                "JIRA_ACCOUNT_ID", page["author_id"])

        # Title and body: full Presidio scan
        page["title"] = self.scanner.scan(page.get("title", ""))
        page["body"] = self.scanner.scan(page.get("body", ""))

        for comment in page.get("comments", []):
            if comment.get("author_id"):
                comment["author_id"] = self.scanner.scan_structured(
                    "JIRA_ACCOUNT_ID", comment["author_id"])
            comment["body"] = self.scanner.scan(comment.get("body", ""))

        return page
