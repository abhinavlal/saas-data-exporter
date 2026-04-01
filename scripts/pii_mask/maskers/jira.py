"""Jira masker — roster-based PII replacement for Jira exports.

Handles ticket JSON: person fields, ADF description/comments,
changelog entries, custom fields, and attachment metadata.
Skips binary attachment files.
"""

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
            pass  # no PII
        else:
            return "skipped (unknown type)"

        data = self._replace_domains_in_obj(data)
        dst_key = self.rewrite_key(key)
        dst.upload_json(data, dst_key)
        return "ok"

    # -- Ticket masking ---------------------------------------------------- #

    def _mask_ticket(self, ticket: dict) -> dict:
        # Person fields: assignee, reporter, creator
        for prefix in ("assignee", "reporter", "creator"):
            if ticket.get(prefix):
                ticket[prefix] = self.roster.map_jira_display_name(
                    ticket[prefix])
            email_key = f"{prefix}_email"
            if ticket.get(email_key):
                ticket[email_key] = self.scanner.scan_email(
                    ticket[email_key])
            acct_key = f"{prefix}_account_id"
            if ticket.get(acct_key):
                ticket[acct_key] = self.roster.map_jira_account_id(
                    ticket[acct_key])

        # Freeform text: scan, preserving readability
        ticket["summary"] = self.scanner.scan(ticket.get("summary", ""))
        ticket["description_text"] = self.scanner.scan(
            ticket.get("description_text", ""))
        if ticket.get("description_adf"):
            ticket["description_adf"] = self._mask_adf(
                ticket["description_adf"])
        ticket["parent_summary"] = self.scanner.scan(
            ticket.get("parent_summary", ""))

        # URL with domain
        if ticket.get("self"):
            ticket["self"] = self.scanner.scan_url(ticket["self"])

        # Comments, attachments, changelog, custom fields
        for comment in ticket.get("comments", []):
            self._mask_comment(comment)
        for att in ticket.get("attachments", []):
            self._mask_attachment_meta(att)
        for entry in ticket.get("changelog", []):
            self._mask_changelog_entry(entry)
        self._mask_custom_fields(ticket)

        return ticket

    def _mask_comment(self, comment: dict) -> None:
        comment["author"] = self.roster.map_jira_display_name(
            comment.get("author", ""))
        if comment.get("author_email"):
            comment["author_email"] = self.scanner.scan_email(
                comment["author_email"])
        if comment.get("author_account_id"):
            comment["author_account_id"] = self.roster.map_jira_account_id(
                comment["author_account_id"])
        comment["body_text"] = self.scanner.scan(
            comment.get("body_text", ""))
        if comment.get("body_adf"):
            comment["body_adf"] = self._mask_adf(comment["body_adf"])
        if comment.get("rendered_body"):
            comment["rendered_body"] = self.scanner.scan(
                comment["rendered_body"])

    def _mask_attachment_meta(self, att: dict) -> None:
        att["author"] = self.roster.map_jira_display_name(
            att.get("author", ""))
        if att.get("author_email"):
            att["author_email"] = self.scanner.scan_email(
                att["author_email"])
        if att.get("content_url"):
            att["content_url"] = self.scanner.scan_url(att["content_url"])

    def _mask_changelog_entry(self, entry: dict) -> None:
        entry["author"] = self.roster.map_jira_display_name(
            entry.get("author", ""))
        if entry.get("field") in ("assignee", "reporter", "creator",
                                   "Reviewer", "Approver"):
            entry["from"] = self.roster.map_jira_display_name(
                entry.get("from", ""))
            entry["to"] = self.roster.map_jira_display_name(
                entry.get("to", ""))

    def _mask_custom_fields(self, ticket: dict) -> None:
        for key in list(ticket.keys()):
            if not key.startswith("Custom field ("):
                continue
            val = ticket[key]
            if isinstance(val, str) and val:
                ticket[key] = self.scanner.scan(val)

    # -- ADF (Atlassian Document Format) ----------------------------------- #

    def _mask_adf(self, adf: dict) -> dict:
        """Walk the ADF tree and mask PII in text and mention nodes."""
        if not isinstance(adf, dict):
            return adf

        if adf.get("type") == "text" and "text" in adf:
            adf["text"] = self.scanner.scan(adf["text"])

        if adf.get("type") == "mention":
            attrs = adf.get("attrs", {})
            if attrs.get("id"):
                attrs["id"] = self.roster.map_jira_account_id(attrs["id"])
            if attrs.get("text"):
                attrs["text"] = self.roster.map_jira_display_name(
                    attrs["text"])

        for child in adf.get("content", []):
            self._mask_adf(child)

        return adf
