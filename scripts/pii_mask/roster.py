"""Roster — identity mapping for PII masking.

The roster is a JSON file that maps every known person to a fake identity
across all services.  It is the single source of truth for the masking
pipeline: every masker looks up real values here and replaces them with
the corresponding fake value.

Roster JSON schema:
{
  "version": 1,
  "domain_map": {"real.com": "fake.com"},
  "users": [
    {
      "id": "user-001",
      "real":   {"email": "...", "name": "...", ...},
      "masked": {"email": "...", "name": "...", ...}
    }
  ]
}
"""

import hashlib
import json
import logging
from dataclasses import dataclass, field

log = logging.getLogger(__name__)

# Fallback hash salt for emails/names not in the roster
_FALLBACK_SALT = "pii-mask-fallback-v1:"


@dataclass
class RosterEntry:
    """One person's real ↔ masked identity mapping."""
    id: str
    real: dict = field(default_factory=dict)
    masked: dict = field(default_factory=dict)


class Roster:
    """Identity lookup table built from a roster JSON file.

    Builds indices for O(1) lookup by email, name, GitHub login,
    Slack user ID, and Jira/Confluence account ID.
    """

    def __init__(self, data: dict):
        self.version = data.get("version", 1)
        self.domain_map: dict[str, str] = data.get("domain_map", {})
        self._entries: list[RosterEntry] = []

        # Lookup indices (all keys lowercased for case-insensitive matching)
        self._by_email: dict[str, RosterEntry] = {}
        self._by_name: dict[str, RosterEntry] = {}
        self._by_first_last: dict[str, RosterEntry] = {}
        self._by_github_login: dict[str, RosterEntry] = {}
        self._by_slack_user_id: dict[str, RosterEntry] = {}
        self._by_jira_account_id: dict[str, RosterEntry] = {}

        for user_data in data.get("users", []):
            entry = RosterEntry(
                id=user_data["id"],
                real=user_data.get("real", {}),
                masked=user_data.get("masked", {}),
            )
            self._entries.append(entry)
            self._index_entry(entry)

        log.info("Loaded roster: %d users, %d domain mappings",
                 len(self._entries), len(self.domain_map))

    def _index_entry(self, entry: RosterEntry) -> None:
        """Build all lookup indices for one entry."""
        real = entry.real

        if real.get("email"):
            self._by_email[real["email"].lower()] = entry
        if real.get("name"):
            self._by_name[real["name"].lower()] = entry
        if real.get("first_name") and real.get("last_name"):
            key = f"{real['first_name']} {real['last_name']}".lower()
            self._by_first_last[key] = entry
        if real.get("github_login"):
            self._by_github_login[real["github_login"].lower()] = entry
        if real.get("slack_user_id"):
            self._by_slack_user_id[real["slack_user_id"]] = entry
        if real.get("jira_account_id"):
            self._by_jira_account_id[real["jira_account_id"]] = entry
        # Confluence shares Jira account IDs
        if real.get("confluence_account_id"):
            self._by_jira_account_id[real["confluence_account_id"]] = entry

    # -- Factory methods --------------------------------------------------- #

    @classmethod
    def from_file(cls, path: str) -> "Roster":
        with open(path) as f:
            return cls(json.load(f))

    @classmethod
    def from_s3(cls, store, key: str) -> "Roster":
        """Load roster from an S3Store."""
        data = store.download_json(key)
        if data is None:
            raise FileNotFoundError(f"Roster not found at s3://{store.bucket}/{key}")
        return cls(data)

    # -- Lookup methods ---------------------------------------------------- #

    def by_email(self, email: str) -> RosterEntry | None:
        if not email:
            return None
        return self._by_email.get(email.lower())

    def by_name(self, name: str) -> RosterEntry | None:
        if not name:
            return None
        lower = name.lower()
        return self._by_name.get(lower) or self._by_first_last.get(lower)

    def by_github_login(self, login: str) -> RosterEntry | None:
        if not login:
            return None
        return self._by_github_login.get(login.lower())

    def by_slack_user_id(self, uid: str) -> RosterEntry | None:
        if not uid:
            return None
        return self._by_slack_user_id.get(uid)

    def by_jira_account_id(self, aid: str) -> RosterEntry | None:
        if not aid:
            return None
        return self._by_jira_account_id.get(aid)

    # -- Mapping helpers --------------------------------------------------- #

    def map_domain(self, domain: str) -> str:
        """Map a real domain to its fake counterpart, or return as-is."""
        return self.domain_map.get(domain.lower(), domain)

    def map_email(self, email: str) -> str:
        """Map a real email to its fake counterpart.

        1. Roster lookup by full email
        2. Roster lookup + domain mapping
        3. Fallback: deterministic hash of local part + mapped domain
        """
        if not email or "@" not in email:
            return self._fallback_hash_email(email) if email else email

        entry = self.by_email(email)
        if entry and entry.masked.get("email"):
            return entry.masked["email"]

        # Unknown email — hash local part, map domain
        local, domain = email.rsplit("@", 1)
        mapped_domain = self.map_domain(domain)
        hashed_local = self._fallback_hash(local, 8)
        return f"{hashed_local}@{mapped_domain}"

    def map_name(self, name: str) -> str:
        """Map a real name to its fake counterpart, or hash."""
        if not name:
            return name
        entry = self.by_name(name)
        if entry and entry.masked.get("name"):
            return entry.masked["name"]
        return f"User {self._fallback_hash(name, 8)}"

    def map_github_login(self, login: str) -> str:
        if not login:
            return login
        entry = self.by_github_login(login)
        if entry and entry.masked.get("github_login"):
            return entry.masked["github_login"]
        return f"user-{self._fallback_hash(login, 10)}"

    def map_jira_account_id(self, aid: str) -> str:
        if not aid:
            return aid
        entry = self.by_jira_account_id(aid)
        if entry and entry.masked.get("jira_account_id"):
            return entry.masked["jira_account_id"]
        return f"acct-{self._fallback_hash(aid, 16)}"

    def map_slack_user_id(self, uid: str) -> str:
        if not uid:
            return uid
        entry = self.by_slack_user_id(uid)
        if entry and entry.masked.get("slack_user_id"):
            return entry.masked["slack_user_id"]
        return uid  # Slack user IDs aren't PII themselves

    def map_jira_display_name(self, name: str) -> str:
        """Map a Jira display name — try name index, then fallback."""
        if not name:
            return name
        entry = self.by_name(name)
        if entry:
            return entry.masked.get("jira_display_name") or entry.masked.get("name", name)
        return f"User {self._fallback_hash(name, 8)}"

    @property
    def users(self) -> list[RosterEntry]:
        return list(self._entries)

    # -- Fallback hashing -------------------------------------------------- #

    @staticmethod
    def _fallback_hash(value: str, length: int = 12) -> str:
        """Deterministic hash for values not in the roster."""
        digest = hashlib.sha256((_FALLBACK_SALT + value).encode()).hexdigest()
        return digest[:length]

    @staticmethod
    def _fallback_hash_email(email: str) -> str:
        digest = hashlib.sha256((_FALLBACK_SALT + email).encode()).hexdigest()
        return digest[:12]
