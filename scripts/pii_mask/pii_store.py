"""PIIStore — SQLite-backed PII identity store for consistent masking.

Maps real PII values to fake replacements. Thread-safe via WAL mode.
Auto-generates fakes for new PII not yet in the store.

Usage:
    store = PIIStore("pii_store.db")
    store.add_domain("org_name.com", "example.com")

    # Known type — direct lookup, no NER
    fake_email = store.get_or_create("EMAIL_ADDRESS", "john@org_name.com")

    # Import existing roster.json
    store = PIIStore.from_json("roster.json", "pii_store.db")
"""

import hashlib
import json
import logging
import sqlite3
import threading

from faker import Faker

log = logging.getLogger(__name__)

fake = Faker()
Faker.seed(0)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS roster_entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type TEXT NOT NULL,
    real_value TEXT NOT NULL,
    masked_value TEXT NOT NULL,
    source TEXT DEFAULT '',
    created_at TEXT DEFAULT (datetime('now')),
    UNIQUE(entity_type, real_value)
);

CREATE TABLE IF NOT EXISTS domain_map (
    real_domain TEXT PRIMARY KEY,
    masked_domain TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_roster_lookup
    ON roster_entries(entity_type, real_value);
"""


class PIIStore:
    """Thread-safe SQLite PII store with in-memory cache.

    WAL mode allows concurrent reads from multiple threads.
    Writes are serialized via SQLite's internal locking.
    An in-memory cache avoids repeated SQLite round-trips for
    the same values (common — the same person appears many times).
    """

    def __init__(self, db_path: str):
        self._db_path = db_path
        self._local = threading.local()
        self._cache: dict[tuple[str, str], str] = {}
        self._cache_lock = threading.Lock()
        self._domain_map: dict[str, str] = {}

        # Initialize schema on the main connection
        con = self._get_connection()
        con.executescript(_SCHEMA)
        con.commit()

        # Load domain map into memory
        rows = con.execute("SELECT real_domain, masked_domain FROM domain_map").fetchall()
        for real, masked in rows:
            self._domain_map[real.lower()] = masked

        # Pre-warm cache from existing entries
        rows = con.execute(
            "SELECT entity_type, real_value, masked_value FROM roster_entries"
        ).fetchall()
        for etype, real, masked in rows:
            self._cache[(etype, real.lower())] = masked

        log.info("PIIStore loaded: %d entries, %d domains from %s",
                 len(self._cache), len(self._domain_map), db_path)

    def _get_connection(self) -> sqlite3.Connection:
        """Get a thread-local SQLite connection."""
        if not hasattr(self._local, "con"):
            con = sqlite3.connect(self._db_path, timeout=30)
            con.execute("PRAGMA journal_mode=WAL")
            con.execute("PRAGMA busy_timeout=5000")
            self._local.con = con
        return self._local.con

    # -- Lookup / Create --------------------------------------------------- #

    def lookup(self, entity_type: str, real_value: str) -> str | None:
        """Look up a replacement. Returns None if not found."""
        key = (entity_type, real_value.lower())
        with self._cache_lock:
            if key in self._cache:
                return self._cache[key]
        return None

    def get_or_create(self, entity_type: str, real_value: str,
                      source: str = "") -> str:
        """Look up replacement, or generate + store a new one.

        Thread-safe: uses INSERT OR IGNORE + SELECT to handle races.
        """
        if not real_value:
            return real_value

        key = (entity_type, real_value.lower())

        # Fast path: cache hit
        with self._cache_lock:
            if key in self._cache:
                return self._cache[key]

        # Generate fake
        masked_value = self._generate_fake(entity_type, real_value)

        # Insert (or get existing if another thread beat us)
        con = self._get_connection()
        try:
            con.execute(
                "INSERT OR IGNORE INTO roster_entries "
                "(entity_type, real_value, masked_value, source) "
                "VALUES (?, ?, ?, ?)",
                (entity_type, real_value, masked_value, source),
            )
            con.commit()
        except sqlite3.OperationalError:
            # Database locked — another thread inserting. That's fine.
            pass

        # Read back (in case another thread inserted a different value)
        row = con.execute(
            "SELECT masked_value FROM roster_entries "
            "WHERE entity_type = ? AND real_value = ?",
            (entity_type, real_value),
        ).fetchone()

        result = row[0] if row else masked_value

        # Update cache
        with self._cache_lock:
            self._cache[key] = result

        return result

    # -- Domain map -------------------------------------------------------- #

    @property
    def domain_map(self) -> dict[str, str]:
        return dict(self._domain_map)

    def add_domain(self, real: str, masked: str) -> None:
        con = self._get_connection()
        con.execute(
            "INSERT OR REPLACE INTO domain_map (real_domain, masked_domain) "
            "VALUES (?, ?)", (real.lower(), masked),
        )
        con.commit()
        self._domain_map[real.lower()] = masked

    def map_domain(self, domain: str) -> str:
        return self._domain_map.get(domain.lower(), domain)

    def map_email_domain(self, email: str) -> str:
        """Replace the domain part of an email using domain_map."""
        if not email or "@" not in email:
            return email
        local, domain = email.rsplit("@", 1)
        return f"{local}@{self.map_domain(domain)}"

    # -- Fake generation --------------------------------------------------- #

    def _generate_fake(self, entity_type: str, real_value: str) -> str:
        """Generate a fake replacement for a PII entity."""
        if entity_type == "PERSON":
            return fake.name()

        if entity_type == "EMAIL_ADDRESS":
            if "@" in real_value:
                _, domain = real_value.rsplit("@", 1)
                mapped_domain = self.map_domain(domain)
                fake_local = f"{fake.first_name().lower()}.{fake.last_name().lower()}"
                return f"{fake_local}@{mapped_domain}"
            return fake.email()

        if entity_type == "PHONE_NUMBER":
            h = self._hash(real_value, 6)
            return f"[PHONE-{h}]"

        if entity_type == "LOCATION":
            return fake.city()

        if entity_type == "IP_ADDRESS":
            h = self._hash(real_value, 4)
            return f"10.0.{int(h[:2], 16) % 256}.{int(h[2:], 16) % 256}"

        if entity_type == "CREDIT_CARD":
            return "[REDACTED]"

        if entity_type == "IBAN_CODE":
            return "[REDACTED-IBAN]"

        if entity_type == "US_SSN":
            return "[REDACTED-SSN]"

        if entity_type == "URL":
            # Replace domains in URLs
            result = real_value
            for real_d, fake_d in self._domain_map.items():
                result = result.replace(real_d, fake_d)
            return result

        if entity_type == "MEDICAL_LICENSE":
            return f"[MEDICAL-{self._hash(real_value, 6)}]"

        # -- Indian PII types --
        if entity_type == "IN_PAN":
            # Structurally valid fake PAN
            h = self._hash(real_value, 8)
            return f"ZZZZZ{h[:4].upper()}Z"

        if entity_type == "IN_AADHAAR":
            h = self._hash(real_value, 12)
            digits = "".join(str(int(c, 16) % 10) for c in h)
            return f"{digits[:4]} {digits[4:8]} {digits[8:12]}"

        if entity_type == "IN_UPI_ID":
            h = self._hash(real_value, 8)
            return f"user.{h}@redacted"

        if entity_type == "IN_IFSC":
            return f"XXXX0{self._hash(real_value, 6).upper()}"

        if entity_type == "IN_BANK_ACCOUNT":
            h = self._hash(real_value, 14)
            return "".join(str(int(c, 16) % 10) for c in h)

        if entity_type == "GEO_COORDINATE":
            # Replace with a generic location (0°N, 0°E)
            h = self._hash(real_value, 8)
            lat = int(h[:4], 16) % 180 - 90
            lon = int(h[4:], 16) % 360 - 180
            return f"{lat}.0000,{lon}.0000"

        # -- Service-specific types --
        if entity_type == "GITHUB_LOGIN":
            f_first = fake.first_name()
            f_last = fake.last_name()
            return f"{f_first[0].lower()}{f_last.lower()}"

        if entity_type == "JIRA_ACCOUNT_ID":
            return f"mask-{self._hash(real_value, 8)}"

        # Generic fallback
        h = self._hash(real_value, 8)
        return f"[{entity_type}-{h}]"

    @staticmethod
    def _hash(value: str, length: int = 8) -> str:
        return hashlib.sha256(
            f"pii-store:{value}".encode()).hexdigest()[:length]

    # -- Import / Export --------------------------------------------------- #

    @classmethod
    def from_json(cls, json_path: str, db_path: str) -> "PIIStore":
        """Import a roster.json into a new SQLite store."""
        with open(json_path) as f:
            data = json.load(f)

        store = cls(db_path)

        # Import domain map
        for real, masked in data.get("domain_map", {}).items():
            store.add_domain(real, masked)

        # Import users
        con = store._get_connection()
        count = 0
        for user in data.get("users", []):
            real = user.get("real", {})
            masked = user.get("masked", {})

            # Map each real field to its masked counterpart
            field_type_map = {
                "email": "EMAIL_ADDRESS",
                "name": "PERSON",
                "github_login": "GITHUB_LOGIN",
                "jira_account_id": "JIRA_ACCOUNT_ID",
                "jira_display_name": "PERSON",
                "slack_user_id": "SLACK_USER_ID",
                "slack_display_name": "PERSON",
                "slack_username": "SLACK_USERNAME",
                "confluence_account_id": "JIRA_ACCOUNT_ID",
            }

            for field, entity_type in field_type_map.items():
                real_val = real.get(field)
                masked_val = masked.get(field)
                if real_val and masked_val:
                    con.execute(
                        "INSERT OR IGNORE INTO roster_entries "
                        "(entity_type, real_value, masked_value, source) "
                        "VALUES (?, ?, ?, ?)",
                        (entity_type, real_val, masked_val, "roster_import"),
                    )
                    store._cache[(entity_type, real_val.lower())] = masked_val
                    count += 1

        con.commit()
        log.info("Imported %d entries from %s into %s",
                 count, json_path, db_path)
        return store

    def export_json(self, path: str) -> None:
        """Export the store to JSON for human review."""
        con = self._get_connection()
        rows = con.execute(
            "SELECT entity_type, real_value, masked_value, source, created_at "
            "FROM roster_entries ORDER BY entity_type, real_value"
        ).fetchall()

        domains = con.execute(
            "SELECT real_domain, masked_domain FROM domain_map"
        ).fetchall()

        data = {
            "domain_map": {r: m for r, m in domains},
            "entries_by_type": {},
            "total_entries": len(rows),
        }

        for etype, real, masked, source, created in rows:
            data["entries_by_type"].setdefault(etype, []).append({
                "real": real, "masked": masked,
                "source": source, "created_at": created,
            })

        with open(path, "w") as f:
            json.dump(data, f, indent=2)
        log.info("Exported %d entries to %s", len(rows), path)

    # -- Stats ------------------------------------------------------------- #

    def stats(self) -> dict[str, int]:
        """Return entry counts by entity type."""
        con = self._get_connection()
        rows = con.execute(
            "SELECT entity_type, COUNT(*) FROM roster_entries "
            "GROUP BY entity_type"
        ).fetchall()
        return {etype: count for etype, count in rows}
