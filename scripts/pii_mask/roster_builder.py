"""Roster builder — pull user lists from service APIs and generate a roster.

Queries GitHub org members, Jira user search, Slack users.list, and
Google Workspace users (via Admin Directory or CSV), cross-references
by email, generates fake identities using Faker, and outputs a draft
roster JSON for human review.

STRICT MODE: every requested source must succeed and return users.
Partial rosters are worse than no roster — they give a false sense
of coverage.  If any source fails, the whole run aborts.

Supports incremental updates: re-running preserves existing mappings
and only adds new users.

Usage:
    python -m scripts.pii_mask.roster_builder \\
        --github-org my-org --jira --slack \\
        --google --google-admin-email admin@co.com --google-domain co.com \\
        --domain-map co.com=example.com \\
        --output roster.json
"""

import argparse
import csv
import json
import logging
import os
import re
import sys
import uuid
from dataclasses import dataclass, field

import requests
from faker import Faker

log = logging.getLogger(__name__)

fake = Faker()
Faker.seed(42)


class RosterBuildError(Exception):
    """Raised when a source pull fails or returns no users."""


@dataclass
class PersonRecord:
    """Intermediate representation of a person across services."""
    email: str = ""
    name: str = ""
    first_name: str = ""
    last_name: str = ""
    github_login: str = ""
    slack_user_id: str = ""
    slack_display_name: str = ""
    slack_username: str = ""
    jira_account_id: str = ""
    jira_display_name: str = ""
    confluence_account_id: str = ""
    sources: list = field(default_factory=list)


# -- Source pullers -------------------------------------------------------- #
# Each puller MUST raise RosterBuildError on failure.
# Each puller MUST raise RosterBuildError if 0 users returned (means
# a permissions issue, not an empty org).

def _github_headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json"}


def pull_github_org_members(token: str, org: str) -> list[PersonRecord]:
    """GET /orgs/{org}/members — returns login + id for each member."""
    log.info("Pulling GitHub org members for %s", org)
    headers = _github_headers(token)
    members = []
    url = f"https://api.github.com/orgs/{org}/members"
    params = {"per_page": 100, "page": 1}

    while True:
        resp = requests.get(url, headers=headers, params=params)
        if resp.status_code == 403:
            raise RosterBuildError(
                f"GitHub: 403 Forbidden for /orgs/{org}/members — "
                f"the token or App installation needs 'members:read' "
                f"organization permission. Response: {resp.text[:200]}")
        if resp.status_code == 404:
            raise RosterBuildError(
                f"GitHub: org '{org}' not found or not accessible")
        resp.raise_for_status()
        page = resp.json()
        if not page:
            break
        for m in page:
            person = PersonRecord(
                github_login=m.get("login", ""),
                sources=["github"],
            )
            detail = _github_user_detail(headers, m["login"])
            if detail.get("email"):
                person.email = detail["email"]
            if detail.get("name"):
                person.name = detail["name"]
                parts = detail["name"].split(None, 1)
                person.first_name = parts[0] if parts else ""
                person.last_name = parts[1] if len(parts) > 1 else ""
            members.append(person)
        params["page"] += 1

    if not members:
        raise RosterBuildError(
            f"GitHub: 0 members returned for org '{org}'. "
            f"This likely means the token/App doesn't have "
            f"'members:read' permission on the org. "
            f"Check: https://github.com/organizations/{org}/settings/apps")

    log.info("GitHub: found %d org members", len(members))
    return members


def _github_user_detail(headers: dict, login: str) -> dict:
    """GET /users/{login} — returns name and public email."""
    try:
        resp = requests.get(f"https://api.github.com/users/{login}",
                            headers=headers)
        resp.raise_for_status()
        return resp.json()
    except Exception:
        log.warning("GitHub: failed to fetch details for %s", login)
        return {}


def get_github_token_from_app(app_id: str, private_key_path: str,
                               installation_id: str) -> str:
    """Get a GitHub installation token from App credentials."""
    from lib.github_auth import GitHubAppAuth
    auth = GitHubAppAuth(app_id=app_id, private_key_path=private_key_path,
                         installation_id=installation_id)
    return auth.get_token()


def pull_jira_users(token: str, email: str,
                    domain: str) -> list[PersonRecord]:
    """GET /rest/api/3/users/search — returns all accessible users."""
    log.info("Pulling Jira users from %s", domain)
    session = requests.Session()
    session.auth = (email, token)
    users = []
    start = 0

    while True:
        resp = session.get(
            f"https://{domain}/rest/api/3/users/search",
            params={"startAt": start, "maxResults": 100},
        )
        if resp.status_code == 401:
            raise RosterBuildError(
                f"Jira: 401 Unauthorized — check JIRA_TOKEN and JIRA_EMAIL")
        if resp.status_code == 403:
            raise RosterBuildError(
                f"Jira: 403 Forbidden — the API token may lack user search "
                f"permissions. Response: {resp.text[:200]}")
        resp.raise_for_status()
        page = resp.json()
        if not page:
            break
        for u in page:
            if u.get("accountType") != "atlassian":
                continue
            person = PersonRecord(
                jira_account_id=u.get("accountId", ""),
                jira_display_name=u.get("displayName", ""),
                confluence_account_id=u.get("accountId", ""),
                sources=["jira"],
            )
            if u.get("emailAddress"):
                person.email = u["emailAddress"]
            if u.get("displayName"):
                person.name = u["displayName"]
                parts = u["displayName"].split(None, 1)
                person.first_name = parts[0] if parts else ""
                person.last_name = parts[1] if len(parts) > 1 else ""
            users.append(person)
        start += len(page)

    if not users:
        raise RosterBuildError(
            f"Jira: 0 users returned from {domain}. "
            f"Check credentials and permissions.")

    log.info("Jira: found %d users", len(users))
    return users


def pull_slack_users(token: str) -> list[PersonRecord]:
    """GET users.list — returns all workspace members."""
    log.info("Pulling Slack workspace members")
    headers = {"Authorization": f"Bearer {token}"}
    users = []
    cursor = ""

    while True:
        params = {"limit": 200}
        if cursor:
            params["cursor"] = cursor
        resp = requests.get("https://slack.com/api/users.list",
                            headers=headers, params=params)
        resp.raise_for_status()
        data = resp.json()
        if not data.get("ok"):
            error = data.get("error", "unknown")
            if error == "missing_scope":
                raise RosterBuildError(
                    "Slack: missing_scope — the bot token needs the "
                    "'users:read' and 'users:read.email' OAuth scopes. "
                    "Add them at https://api.slack.com/apps → OAuth & "
                    "Permissions → Scopes, then reinstall the app.")
            if error == "invalid_auth":
                raise RosterBuildError(
                    "Slack: invalid_auth — SLACK_TOKEN is invalid or expired")
            raise RosterBuildError(
                f"Slack: users.list API error: {error}")

        for m in data.get("members", []):
            if m.get("is_bot") or m.get("id") == "USLACKBOT":
                continue
            profile = m.get("profile", {})
            person = PersonRecord(
                slack_user_id=m.get("id", ""),
                slack_display_name=profile.get("real_name", ""),
                slack_username=m.get("name", ""),
                sources=["slack"],
            )
            if profile.get("email"):
                person.email = profile["email"]
            if profile.get("real_name"):
                person.name = profile["real_name"]
                parts = profile["real_name"].split(None, 1)
                person.first_name = parts[0] if parts else ""
                person.last_name = parts[1] if len(parts) > 1 else ""
            users.append(person)

        cursor = data.get("response_metadata", {}).get("next_cursor", "")
        if not cursor:
            break

    if not users:
        raise RosterBuildError(
            "Slack: 0 members returned. Check bot token permissions.")

    log.info("Slack: found %d members", len(users))
    return users


def load_google_users_csv(path: str) -> list[PersonRecord]:
    """Load user emails from a Google users CSV (one email per row)."""
    if not os.path.exists(path):
        raise RosterBuildError(f"Google CSV: file not found: {path}")

    log.info("Loading Google users from %s", path)
    users = []
    with open(path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            email_val = row.get("user", row.get("email", "")).strip()
            if not email_val:
                continue
            person = PersonRecord(
                email=email_val,
                sources=["google"],
            )
            local = email_val.split("@")[0]
            parts = re.split(r"[._]", local)
            if len(parts) >= 2:
                person.first_name = parts[0].capitalize()
                person.last_name = parts[-1].capitalize()
                person.name = f"{person.first_name} {person.last_name}"
            users.append(person)

    if not users:
        raise RosterBuildError(
            f"Google CSV: 0 users loaded from {path}. "
            f"Check the file has a 'user' or 'email' column.")

    log.info("Google CSV: loaded %d users", len(users))
    return users


def pull_google_directory_users(
        service_account_key: str,
        admin_email: str,
        domain: str) -> list[PersonRecord]:
    """Pull users from Google Admin Directory API.

    Requires the service account to have domain-wide delegation with
    the ``admin.directory.user.readonly`` scope, and *admin_email*
    must be a Workspace admin user to impersonate.
    """
    if not os.path.exists(service_account_key):
        raise RosterBuildError(
            f"Google: service account key not found: {service_account_key}")

    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    SCOPES = ["https://www.googleapis.com/auth/admin.directory.user.readonly"]

    log.info("Pulling Google Workspace users for domain %s", domain)
    try:
        credentials = service_account.Credentials.from_service_account_file(
            service_account_key, scopes=SCOPES,
        )
        credentials = credentials.with_subject(admin_email)
        service = build("admin", "directory_v1", credentials=credentials)
    except Exception as e:
        raise RosterBuildError(
            f"Google: failed to authenticate — {e}. "
            f"Ensure the service account has domain-wide delegation "
            f"with scope: admin.directory.user.readonly") from e

    users = []
    page_token = None
    while True:
        try:
            results = service.users().list(
                domain=domain,
                maxResults=500,
                orderBy="email",
                pageToken=page_token,
            ).execute()
        except Exception as e:
            raise RosterBuildError(
                f"Google: Directory API call failed — {e}. "
                f"Ensure '{admin_email}' is a Workspace admin and "
                f"the service account has domain-wide delegation for "
                f"admin.directory.user.readonly") from e

        for u in results.get("users", []):
            email_val = u.get("primaryEmail", "")
            name_obj = u.get("name", {})
            person = PersonRecord(
                email=email_val,
                name=name_obj.get("fullName", ""),
                first_name=name_obj.get("givenName", ""),
                last_name=name_obj.get("familyName", ""),
                sources=["google"],
            )
            users.append(person)

        page_token = results.get("nextPageToken")
        if not page_token:
            break

    if not users:
        raise RosterBuildError(
            f"Google: 0 users returned for domain '{domain}'. "
            f"Check that the service account has admin.directory.user."
            f"readonly scope and '{admin_email}' is a Workspace admin.")

    log.info("Google Directory: found %d users", len(users))
    return users


# -- Cross-referencing ----------------------------------------------------- #

def _merge_into(existing: PersonRecord, rec: PersonRecord) -> None:
    """Merge fields from *rec* into *existing*, filling blanks."""
    if not existing.name and rec.name:
        existing.name = rec.name
    if not existing.first_name and rec.first_name:
        existing.first_name = rec.first_name
    if not existing.last_name and rec.last_name:
        existing.last_name = rec.last_name
    if not existing.email and rec.email:
        existing.email = rec.email
    if not existing.github_login and rec.github_login:
        existing.github_login = rec.github_login
    if not existing.slack_user_id and rec.slack_user_id:
        existing.slack_user_id = rec.slack_user_id
    if not existing.slack_display_name and rec.slack_display_name:
        existing.slack_display_name = rec.slack_display_name
    if not existing.slack_username and rec.slack_username:
        existing.slack_username = rec.slack_username
    if not existing.jira_account_id and rec.jira_account_id:
        existing.jira_account_id = rec.jira_account_id
    if not existing.jira_display_name and rec.jira_display_name:
        existing.jira_display_name = rec.jira_display_name
    if not existing.confluence_account_id and rec.confluence_account_id:
        existing.confluence_account_id = rec.confluence_account_id
    existing.sources.extend(rec.sources)


def merge_persons(all_records: list[PersonRecord]) -> list[PersonRecord]:
    """Group PersonRecords by email, then fuzzy-match orphans by name.

    Two-pass merge:
    1. Primary: group by email (case-insensitive).
    2. Secondary: for records with no email (typically GitHub members
       whose profiles don't expose email), try to match against
       existing records by name or by deriving email patterns from
       the GitHub login (e.g. login "anvitakamble" → look for
       email locals containing "anvita" + "kamble").
    """
    by_email: dict[str, PersonRecord] = {}
    orphans: list[PersonRecord] = []

    # Pass 1: group by email
    for rec in all_records:
        key = rec.email.lower() if rec.email else ""
        if not key:
            orphans.append(rec)
            continue

        if key not in by_email:
            by_email[key] = rec
        else:
            _merge_into(by_email[key], rec)

    # Pass 2: try to match orphans (records with no email)
    # Build secondary indices from pass 1 results
    by_name: dict[str, PersonRecord] = {}
    by_email_local: dict[str, PersonRecord] = {}
    for person in by_email.values():
        if person.name:
            by_name[person.name.lower()] = person
        if person.email and "@" in person.email:
            local = person.email.split("@")[0].lower()
            by_email_local[local] = person
            # Also index normalized form: first.last → firstlast
            normalized = local.replace(".", "").replace("_", "")
            by_email_local[normalized] = person

    matched = 0
    for orphan in orphans:
        target = None

        # Try exact name match
        if orphan.name and orphan.name.lower() in by_name:
            target = by_name[orphan.name.lower()]

        # Try login-based match: "anvitakamble" → look in email locals
        if not target and orphan.github_login:
            login_lower = orphan.github_login.lower()
            if login_lower in by_email_local:
                target = by_email_local[login_lower]

        # Try name-derived login: "Anvita Kamble" from GitHub name field
        # → normalize to "anvitakamble" and look in email locals
        if not target and orphan.name:
            name_normalized = orphan.name.lower().replace(" ", "")
            if name_normalized in by_email_local:
                target = by_email_local[name_normalized]

        if target:
            _merge_into(target, orphan)
            matched += 1
        else:
            by_email[f"_orphan_{id(orphan)}"] = orphan

    if matched:
        log.info("Fuzzy-matched %d orphan records (no email) by "
                 "name/login", matched)
    if len(orphans) - matched > 0:
        log.info("%d records remain unmatched (no email, no name match)",
                 len(orphans) - matched)

    return list(by_email.values())


# -- Faker identity generation -------------------------------------------- #

def generate_fake_identity(person: PersonRecord,
                           domain_map: dict[str, str]) -> dict:
    """Generate a fake identity dict for one person."""
    fake_first = fake.first_name()
    fake_last = fake.last_name()
    fake_name = f"{fake_first} {fake_last}"

    fake_email = ""
    if person.email and "@" in person.email:
        _, real_domain = person.email.rsplit("@", 1)
        fake_domain = domain_map.get(real_domain.lower(), real_domain)
        fake_local = f"{fake_first.lower()}.{fake_last.lower()}"
        fake_email = f"{fake_local}@{fake_domain}"

    fake_login = f"{fake_first[0].lower()}{fake_last.lower()}"

    masked = {
        "email": fake_email,
        "name": fake_name,
        "first_name": fake_first,
        "last_name": fake_last,
    }

    if person.github_login:
        masked["github_login"] = fake_login
    if person.slack_user_id:
        masked["slack_user_id"] = person.slack_user_id
        masked["slack_display_name"] = fake_name
        masked["slack_username"] = fake_login
    if person.jira_account_id:
        masked["jira_account_id"] = f"mask-{uuid.uuid4().hex[:8]}"
        masked["jira_display_name"] = fake_name
    if person.confluence_account_id:
        masked["confluence_account_id"] = masked.get(
            "jira_account_id", f"mask-{uuid.uuid4().hex[:8]}")

    return masked


# -- Roster build ---------------------------------------------------------- #

def build_roster(persons: list[PersonRecord],
                 domain_map: dict[str, str],
                 existing_roster: dict | None = None) -> dict:
    """Build a roster JSON dict from merged person records."""
    existing_by_email: dict[str, dict] = {}
    if existing_roster:
        for user in existing_roster.get("users", []):
            email_key = user.get("real", {}).get("email", "").lower()
            if email_key:
                existing_by_email[email_key] = user

    users = []
    new_count = 0
    for i, person in enumerate(persons):
        email_key = person.email.lower() if person.email else ""

        if email_key and email_key in existing_by_email:
            users.append(existing_by_email[email_key])
            continue

        real = {
            "email": person.email,
            "name": person.name,
            "first_name": person.first_name,
            "last_name": person.last_name,
        }
        if person.github_login:
            real["github_login"] = person.github_login
        if person.slack_user_id:
            real["slack_user_id"] = person.slack_user_id
            real["slack_display_name"] = person.slack_display_name
            real["slack_username"] = person.slack_username
        if person.jira_account_id:
            real["jira_account_id"] = person.jira_account_id
            real["jira_display_name"] = person.jira_display_name
        if person.confluence_account_id:
            real["confluence_account_id"] = person.confluence_account_id

        masked = generate_fake_identity(person, domain_map)

        users.append({
            "id": f"user-{i + 1:04d}",
            "real": real,
            "masked": masked,
        })
        new_count += 1

    log.info("Roster: %d total users (%d new, %d existing)",
             len(users), new_count, len(users) - new_count)

    return {
        "version": 1,
        "domain_map": domain_map,
        "users": users,
    }


# -- CLI ------------------------------------------------------------------- #

def main():
    from lib.config import load_dotenv, env
    from lib.logging import setup_logging
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Build a PII masking roster from service APIs. "
                    "Every requested source must succeed — partial "
                    "rosters are not written.",
    )

    # Sources
    parser.add_argument("--github-org", default=None,
                        help="GitHub org to pull members from")
    parser.add_argument("--jira", action="store_true",
                        help="Pull users from Jira")
    parser.add_argument("--slack", action="store_true",
                        help="Pull members from Slack workspace")
    parser.add_argument("--google-csv", default=None,
                        help="Path to Google users CSV (user column)")
    parser.add_argument("--google", action="store_true",
                        help="Pull users from Google Admin Directory API")
    parser.add_argument("--google-admin-email", default=None,
                        help="Admin email to impersonate for Directory API")
    parser.add_argument("--google-domain", default=None,
                        help="Domain to list users for (e.g. org_name.com)")

    # Domain mapping
    parser.add_argument("--domain-map", nargs="*", default=[],
                        help="Domain mappings as real=fake pairs "
                             "(e.g. org_name.com=example.com)")

    # Incremental update
    parser.add_argument("--existing", default=None,
                        help="Path to existing roster.json (for incremental)")

    # Output
    parser.add_argument("--output", default="roster.json",
                        help="Output path for roster JSON")

    # Logging
    parser.add_argument("--log-level", default=env("LOG_LEVEL", "INFO"))
    args = parser.parse_args()

    setup_logging(level=args.log_level, json_output=False)

    # Parse domain map
    domain_map = {}
    for pair in args.domain_map:
        if "=" in pair:
            real, fake_d = pair.split("=", 1)
            domain_map[real.lower()] = fake_d

    # Validate at least one source is requested
    sources_requested = []
    if args.github_org:
        sources_requested.append("github")
    if args.jira:
        sources_requested.append("jira")
    if args.slack:
        sources_requested.append("slack")
    if args.google_csv:
        sources_requested.append("google-csv")
    if args.google:
        sources_requested.append("google")

    if not sources_requested:
        parser.error("No sources enabled — pass at least one of: "
                     "--github-org, --jira, --slack, --google, --google-csv")

    log.info("Roster build starting — sources: %s",
             ", ".join(sources_requested))

    # Pull from each source — fail fast on any error
    all_records: list[PersonRecord] = []
    source_counts: dict[str, int] = {}

    try:
        if args.github_org:
            token = env("GITHUB_TOKEN")
            if not token:
                app_id = env("GITHUB_APP_ID")
                app_key = env("GITHUB_APP_PRIVATE_KEY")
                app_install = env("GITHUB_APP_INSTALLATION_ID")
                if app_id and app_key and app_install:
                    aid = app_id.split(",")[0].strip()
                    akey = app_key.split(",")[0].strip()
                    aiid = app_install.split(",")[0].strip()
                    log.info("Using GitHub App auth (app_id=%s)", aid)
                    token = get_github_token_from_app(aid, akey, aiid)
                else:
                    raise RosterBuildError(
                        "GitHub: GITHUB_TOKEN or GitHub App config "
                        "(GITHUB_APP_ID, GITHUB_APP_PRIVATE_KEY, "
                        "GITHUB_APP_INSTALLATION_ID) is required")
            records = pull_github_org_members(token, args.github_org)
            source_counts["github"] = len(records)
            all_records.extend(records)

        if args.jira:
            jira_token = env("JIRA_TOKEN")
            jira_email = env("JIRA_EMAIL")
            jira_domain = env("JIRA_DOMAIN")
            if not jira_token:
                raise RosterBuildError("Jira: JIRA_TOKEN is not set")
            if not jira_email:
                raise RosterBuildError("Jira: JIRA_EMAIL is not set")
            if not jira_domain:
                raise RosterBuildError("Jira: JIRA_DOMAIN is not set")
            records = pull_jira_users(jira_token, jira_email, jira_domain)
            source_counts["jira"] = len(records)
            all_records.extend(records)

        if args.slack:
            slack_token = env("SLACK_TOKEN")
            if not slack_token:
                raise RosterBuildError("Slack: SLACK_TOKEN is not set")
            records = pull_slack_users(slack_token)
            source_counts["slack"] = len(records)
            all_records.extend(records)

        if args.google_csv:
            records = load_google_users_csv(args.google_csv)
            source_counts["google-csv"] = len(records)
            all_records.extend(records)

        if args.google:
            sa_key = env("GOOGLE_SERVICE_ACCOUNT_KEY")
            admin_email = args.google_admin_email
            google_domain = args.google_domain
            if not sa_key:
                raise RosterBuildError(
                    "Google: GOOGLE_SERVICE_ACCOUNT_KEY is not set")
            if not admin_email:
                raise RosterBuildError(
                    "Google: --google-admin-email is required")
            if not google_domain:
                raise RosterBuildError(
                    "Google: --google-domain is required")
            records = pull_google_directory_users(
                sa_key, admin_email, google_domain)
            source_counts["google"] = len(records)
            all_records.extend(records)

    except RosterBuildError as e:
        log.error("FATAL: %s", e)
        log.error("Aborting — no roster written. Fix the issue above "
                  "and retry.")
        sys.exit(1)

    # Report results per source
    log.info("── Source results ──")
    for src_name in sources_requested:
        count = source_counts.get(src_name, 0)
        log.info("  %-12s %d users", src_name, count)

    # Merge by email
    merged = merge_persons(all_records)
    log.info("Merged %d records into %d unique persons",
             len(all_records), len(merged))

    # Load existing roster for incremental update
    existing = None
    if args.existing and os.path.exists(args.existing):
        with open(args.existing) as f:
            existing = json.load(f)
        log.info("Loaded existing roster with %d users",
                 len(existing.get("users", [])))

    # Build roster
    roster = build_roster(merged, domain_map, existing)

    # Write output
    with open(args.output, "w") as f:
        json.dump(roster, f, indent=2)
    log.info("Wrote roster to %s (%d users)", args.output,
             len(roster["users"]))


if __name__ == "__main__":
    main()
