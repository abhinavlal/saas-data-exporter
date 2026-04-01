"""TextScanner — Aho-Corasick multi-pattern text replacement.

Builds an automaton from all roster-derived search terms and scans
freeform text fields in a single O(n) pass, replacing all matches
with the corresponding fake values from the roster.

Also provides regex-based fallback for structural PII (emails, phone
numbers) that aren't in the roster.
"""

import logging
import re

import ahocorasick

from scripts.pii_mask.roster import Roster

log = logging.getLogger(__name__)

# Minimum length for email local-part terms (avoids false positives)
_MIN_LOCAL_PART_LEN = 6

# Regex patterns for structural PII not in the roster
_EMAIL_RE = re.compile(
    r"\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9._-]+\.[a-zA-Z]{2,}\b")
_PHONE_IN_RE = re.compile(r"\b[6-9]\d{9}\b")
_PHONE_INTL_RE = re.compile(r"\+\d{1,3}[\s-]?\d{6,14}\b")


class TextScanner:
    """Aho-Corasick text scanner backed by a roster.

    Build once, call ``scan()`` on every freeform text field.
    Thread-safe after construction (the automaton is read-only).
    """

    def __init__(self, roster: Roster, ner_engine=None):
        self._roster = roster
        self._ner = ner_engine  # Optional NEREngine for second-pass detection
        self._automaton = ahocorasick.Automaton()

        # Track which patterns we've added (for dedup)
        added: set[str] = set()
        term_count = 0

        for entry in roster.users:
            real = entry.real
            masked = entry.masked

            # Full name (case-sensitive)
            if real.get("name") and masked.get("name"):
                term_count += self._add_term(
                    real["name"], masked["name"], added, case_sensitive=True)

            # Email (case-insensitive — stored lowercased)
            if real.get("email") and masked.get("email"):
                term_count += self._add_term(
                    real["email"].lower(), masked["email"],
                    added, case_sensitive=False)

            # GitHub login preceded by @ (case-insensitive)
            if real.get("github_login") and masked.get("github_login"):
                real_at = f"@{real['github_login'].lower()}"
                masked_at = f"@{masked['github_login']}"
                term_count += self._add_term(
                    real_at, masked_at, added, case_sensitive=False)

            # Slack mention syntax: <@U01ABC123>
            if real.get("slack_user_id") and masked.get("slack_user_id"):
                real_mention = f"<@{real['slack_user_id']}>"
                masked_mention = f"<@{masked['slack_user_id']}>"
                term_count += self._add_term(
                    real_mention, masked_mention, added, case_sensitive=True)

            # Email local part (case-insensitive, min length to avoid false positives)
            if real.get("email") and masked.get("email"):
                local = real["email"].split("@")[0].lower()
                masked_local = masked["email"].split("@")[0]
                if len(local) >= _MIN_LOCAL_PART_LEN:
                    term_count += self._add_term(
                        local, masked_local, added, case_sensitive=False)

            # First + last name separately (only if >= 5 chars)
            if real.get("first_name") and masked.get("first_name"):
                if len(real["first_name"]) >= 5:
                    term_count += self._add_term(
                        real["first_name"], masked["first_name"],
                        added, case_sensitive=True)
            if real.get("last_name") and masked.get("last_name"):
                if len(real["last_name"]) >= 5:
                    term_count += self._add_term(
                        real["last_name"], masked["last_name"],
                        added, case_sensitive=True)

        if term_count > 0:
            self._automaton.make_automaton()
        self._has_automaton = term_count > 0

        # Build set of masked values so regex/NER don't re-process them
        self._masked_emails: set[str] = set()
        self._ner_allow_list: list[str] = []
        for entry in roster.users:
            masked = entry.masked
            if masked.get("email"):
                self._masked_emails.add(masked["email"].lower())
            # Populate NER allow list with all masked values
            for val in masked.values():
                if isinstance(val, str) and len(val) >= 3:
                    self._ner_allow_list.append(val)

        log.info("TextScanner built: %d search terms from %d roster entries"
                 " (NER: %s)",
                 term_count, len(roster.users),
                 "enabled" if self._ner else "disabled")

    def _add_term(self, pattern: str, replacement: str,
                  seen: set, case_sensitive: bool) -> int:
        """Add a search term to the automaton. Returns 1 if added, 0 if dup.

        The automaton always stores lowercased keys so we can search
        against lowered text.  For case-sensitive terms, the original
        pattern is kept for post-match verification.
        """
        key = pattern.lower()
        if not key or key in seen:
            return 0
        seen.add(key)
        # Store (replacement, original_pattern, case_sensitive)
        self._automaton.add_word(key, (replacement, pattern, case_sensitive))
        return 1

    # -- Public API -------------------------------------------------------- #

    def scan(self, text: str) -> str:
        """Replace all roster-known PII in freeform text.

        Three-pass pipeline:
        1. Aho-Corasick: O(n) multi-pattern matching for roster identities
        2. Regex: structural PII (emails, phones) not in the roster
        3. NER (optional): Presidio-based detection of remaining PII
        """
        if not text:
            return text

        # Phase 1: Aho-Corasick scan
        if self._has_automaton:
            text = self._ac_replace(text)

        # Phase 2: Regex fallback for structural PII not caught by AC
        text = self._regex_fallback(text)

        # Phase 3: NER (optional) — catches PII not in roster or regex
        if self._ner is not None:
            text = self._ner.mask(text, allow_list=self._ner_allow_list)

        return text

    def scan_email(self, email: str) -> str:
        """Map an email using the roster, with hash fallback."""
        return self._roster.map_email(email)

    def scan_url(self, url: str) -> str:
        """Replace domains in a URL using the roster's domain_map."""
        if not url:
            return url
        for real_domain, fake_domain in self._roster.domain_map.items():
            url = url.replace(real_domain, fake_domain)
        return url

    # -- Internal ---------------------------------------------------------- #

    def _ac_replace(self, text: str) -> str:
        """Run the Aho-Corasick automaton and apply replacements.

        Collects all matches, resolves overlaps (longest match wins),
        then applies replacements right-to-left to preserve offsets.
        """
        text_lower = text.lower()

        # Collect all matches: (start, end, replacement, case_sensitive)
        matches = []
        for end_idx, (replacement, pattern, case_sensitive) in \
                self._automaton.iter(text_lower):
            start_idx = end_idx - len(pattern) + 1

            if case_sensitive:
                # Verify the original text matches case-sensitively
                original_span = text[start_idx:end_idx + 1]
                if original_span != pattern:
                    continue

            matches.append((start_idx, end_idx + 1, replacement))

        if not matches:
            return text

        # Sort by start position, then by length descending (longest first)
        matches.sort(key=lambda m: (m[0], -(m[1] - m[0])))

        # Resolve overlaps: greedy left-to-right, longest match wins
        resolved = []
        last_end = 0
        for start, end, replacement in matches:
            if start >= last_end:
                resolved.append((start, end, replacement))
                last_end = end

        # Apply replacements right-to-left
        result = list(text)
        for start, end, replacement in reversed(resolved):
            result[start:end] = list(replacement)

        return "".join(result)

    def _regex_fallback(self, text: str) -> str:
        """Replace structural PII (emails, phones) not caught by AC."""
        if not text:
            return text

        # Emails: replace with roster-mapped version (skip already-masked)
        def _replace_email(m):
            email = m.group(0)
            if email.lower() in self._masked_emails:
                return email  # already replaced by AC
            return self._roster.map_email(email)

        text = _EMAIL_RE.sub(_replace_email, text)

        # Phone numbers: redact
        text = _PHONE_IN_RE.sub("[PHONE]", text)
        text = _PHONE_INTL_RE.sub("[PHONE]", text)

        return text
