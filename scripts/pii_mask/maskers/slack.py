"""Slack masker — roster-based PII replacement for Slack exports.

Handles message JSON and channel info. Replaces user IDs, mention
syntax (<@U01ABC123>), reaction user IDs, and scans freeform message
text for PII.
"""

import logging
import re

from lib.s3 import S3Store
from scripts.pii_mask.maskers.base import BaseMasker

log = logging.getLogger(__name__)

# Slack mention pattern: <@U01ABC123>
_SLACK_MENTION_RE = re.compile(r"<@([A-Z0-9]+)>")


class SlackMasker(BaseMasker):
    prefix = "slack/"

    def should_process(self, key: str) -> bool:
        return key.endswith(".json") and "/attachments/" not in key

    def mask_file(self, src: S3Store, dst: S3Store, key: str) -> str:
        data = src.download_json(key)
        if data is None:
            return "skipped (not found)"

        filename = key.rsplit("/", 1)[-1]

        if filename == "messages.json" and isinstance(data, list):
            data = [self._mask_message(m) for m in data]
        elif filename == "channel_info.json":
            data = self._mask_channel_info(data)
        elif filename in ("_stats.json", "_index.json"):
            pass
        else:
            return "skipped (unknown type)"

        data = self._replace_domains_in_obj(data)
        dst.upload_json(data, key)
        return "ok"

    def _mask_message(self, msg: dict) -> dict:
        # User ID
        if msg.get("user"):
            msg["user"] = self.roster.map_slack_user_id(msg["user"])

        # Message text: scan for PII + replace <@UID> mentions
        if msg.get("text"):
            msg["text"] = self.scanner.scan(msg["text"])

        # Reactions: replace user IDs
        for reaction in msg.get("reactions", []):
            reaction["users"] = [
                self.roster.map_slack_user_id(u)
                for u in reaction.get("users", [])
            ]

        # Thread replies
        for reply in msg.get("replies", []):
            if reply.get("user"):
                reply["user"] = self.roster.map_slack_user_id(reply["user"])

        # File attachments metadata
        for f in msg.get("files", []):
            if f.get("user"):
                f["user"] = self.roster.map_slack_user_id(f["user"])

        return msg

    def _mask_channel_info(self, info: dict) -> dict:
        if info.get("creator"):
            info["creator"] = self.roster.map_slack_user_id(info["creator"])
        if info.get("topic", {}).get("creator"):
            info["topic"]["creator"] = self.roster.map_slack_user_id(
                info["topic"]["creator"])
        if info.get("purpose", {}).get("creator"):
            info["purpose"]["creator"] = self.roster.map_slack_user_id(
                info["purpose"]["creator"])
        # Scan topic/purpose text
        if info.get("topic", {}).get("value"):
            info["topic"]["value"] = self.scanner.scan(
                info["topic"]["value"])
        if info.get("purpose", {}).get("value"):
            info["purpose"]["value"] = self.scanner.scan(
                info["purpose"]["value"])
        return info
