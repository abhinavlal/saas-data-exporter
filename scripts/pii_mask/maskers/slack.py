"""Slack masker — Presidio-first PII replacement."""

import logging

from lib.s3 import S3Store
from scripts.pii_mask.maskers.base import BaseMasker

log = logging.getLogger(__name__)


class SlackMasker(BaseMasker):
    prefix = "slack/"

    def should_process(self, key: str) -> bool:
        return super().should_process(key) and "/attachments/" not in key

    def mask_file(self, src: S3Store, dst: S3Store, key: str) -> str:
        data = src.download_json(key)
        if data is None:
            return "skipped (not found)"

        filename = key.rsplit("/", 1)[-1]

        if filename == "messages.json" and isinstance(data, list):
            data = [self._mask_message(m) for m in data]
        elif filename == "channel_info.json":
            data = self._mask_channel_info(data)
        else:
            data = self._scan_obj(data)

        dst.upload_json(data, key)
        return "ok"

    def _mask_message(self, msg: dict) -> dict:
        if msg.get("user"):
            msg["user"] = self.scanner.scan_structured(
                "SLACK_USER_ID", msg["user"])

        # Message text: full Presidio scan
        if msg.get("text"):
            msg["text"] = self.scanner.scan(msg["text"])

        for reaction in msg.get("reactions", []):
            reaction["users"] = [
                self.scanner.scan_structured("SLACK_USER_ID", u)
                for u in reaction.get("users", [])]

        for reply in msg.get("replies", []):
            if reply.get("user"):
                reply["user"] = self.scanner.scan_structured(
                    "SLACK_USER_ID", reply["user"])

        for f in msg.get("files", []):
            if f.get("user"):
                f["user"] = self.scanner.scan_structured(
                    "SLACK_USER_ID", f["user"])

        return msg

    def _mask_channel_info(self, info: dict) -> dict:
        # Scan everything — catches names in topic/purpose
        return self._scan_obj(info)
