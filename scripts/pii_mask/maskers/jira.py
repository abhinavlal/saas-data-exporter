"""Jira masker — Presidio-first PII replacement for Jira exports."""

import logging

from lib.s3 import S3Store
from scripts.pii_mask.maskers.base import BaseMasker

log = logging.getLogger(__name__)


class JiraMasker(BaseMasker):
    prefix = "jira/"

    def should_process(self, key: str) -> bool:
        return key.endswith(".json") and "/attachments/" not in key

    def mask_file(self, src: S3Store, dst: S3Store, key: str) -> str:
        data = src.download_json(key)
        if data is None:
            return "skipped (not found)"

        filename = key.rsplit("/", 1)[-1]

        if "/tickets/" in key and filename != "_index.json":
            data = self._mask_ticket(data)
        elif filename in ("_stats.json", "_index.json"):
            pass
        else:
            data = self._scan_obj(data)

        dst_key = self.rewrite_key(key)
        dst.upload_json(data, dst_key)
        return "ok"

    def _mask_ticket(self, ticket: dict) -> dict:
        # Structured person fields: direct type-aware lookup
        for prefix in ("assignee", "reporter", "creator"):
            if ticket.get(prefix):
                ticket[prefix] = self.scanner.scan_structured(
                    "PERSON", ticket[prefix])
            email_key = f"{prefix}_email"
            if ticket.get(email_key):
                ticket[email_key] = self.scanner.scan_structured(
                    "EMAIL_ADDRESS", ticket[email_key])
            acct_key = f"{prefix}_account_id"
            if ticket.get(acct_key):
                ticket[acct_key] = self.scanner.scan_structured(
                    "JIRA_ACCOUNT_ID", ticket[acct_key])

        # URL
        if ticket.get("self"):
            ticket["self"] = self.scanner.scan_url(ticket["self"])

        # All remaining fields: universal Presidio scan
        for field in ("summary", "description_text", "parent_summary"):
            if ticket.get(field):
                ticket[field] = self.scanner.scan(ticket[field])

        if ticket.get("description_adf"):
            ticket["description_adf"] = self._scan_obj(
                ticket["description_adf"])

        for comment in ticket.get("comments", []):
            self._mask_comment(comment)
        for att in ticket.get("attachments", []):
            self._mask_attachment_meta(att)
        for entry in ticket.get("changelog", []):
            self._mask_changelog_entry(entry)

        # Custom fields and anything else: universal scan
        for key in list(ticket.keys()):
            if key.startswith("Custom field ("):
                val = ticket[key]
                if isinstance(val, str) and val:
                    ticket[key] = self.scanner.scan(val)

        return ticket

    def _mask_comment(self, comment: dict) -> None:
        comment["author"] = self.scanner.scan_structured(
            "PERSON", comment.get("author", ""))
        if comment.get("author_email"):
            comment["author_email"] = self.scanner.scan_structured(
                "EMAIL_ADDRESS", comment["author_email"])
        if comment.get("author_account_id"):
            comment["author_account_id"] = self.scanner.scan_structured(
                "JIRA_ACCOUNT_ID", comment["author_account_id"])
        for field in ("body_text", "rendered_body"):
            if comment.get(field):
                comment[field] = self.scanner.scan(comment[field])
        if comment.get("body_adf"):
            comment["body_adf"] = self._scan_obj(comment["body_adf"])

    def _mask_attachment_meta(self, att: dict) -> None:
        att["author"] = self.scanner.scan_structured(
            "PERSON", att.get("author", ""))
        if att.get("author_email"):
            att["author_email"] = self.scanner.scan_structured(
                "EMAIL_ADDRESS", att["author_email"])
        if att.get("content_url"):
            att["content_url"] = self.scanner.scan_url(att["content_url"])

    def _mask_changelog_entry(self, entry: dict) -> None:
        entry["author"] = self.scanner.scan_structured(
            "PERSON", entry.get("author", ""))
        if entry.get("field") in ("assignee", "reporter", "creator",
                                   "Reviewer", "Approver"):
            entry["from"] = self.scanner.scan_structured(
                "PERSON", entry.get("from", ""))
            entry["to"] = self.scanner.scan_structured(
                "PERSON", entry.get("to", ""))
