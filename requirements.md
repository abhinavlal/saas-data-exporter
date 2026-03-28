# Source Data Exporter — Complete Documentation

## Overview

Four standalone Python scripts that export data from Org_Name's SaaS tools via their APIs. Each script produces structured JSON + CSV output that feeds into the PII scrubbing pipeline (`org_name_pii_scrub.py`).

```
source-data-exporter/
├── github/
│   └── github_export.py          # GitHub REST API
├── google-workspace/
│   ├── main.py                   # Google Gmail, Calendar, Drive APIs
│   ├── service-account.json      # Service account credentials
│   └── pyproject.toml            # Python dependencies
├── jira/
│   └── jira_export.py            # Jira REST API (Atlassian Cloud)
└── slack/
    ├── slack_channel_export.py   # Slack Web API
    ├── channels.csv              # Input: channel IDs to export
    └── slack-org_name-members.csv  # Workspace member list
```

**Note:** Salesforce data (`salesforce/sf_samples/`) was exported using Salesforce's built-in Data Export tool, not a custom script. It's the only data source without a programmatic exporter.

---

## Script 1: `github_export.py` (GitHub Repository Exporter)

### Authentication

GitHub Personal Access Token (classic or fine-grained):
- Classic: `repo` scope
- Fine-grained: read access to code, pull requests, metadata

### Usage

```bash
python github_export.py --token ghp_YOUR_TOKEN --repo owner/repo-name
python github_export.py --token ghp_YOUR_TOKEN --repo owner/repo-name --pr-limit 200
python github_export.py --token ghp_YOUR_TOKEN --repo owner/repo-name --pr-state all
python github_export.py --token ghp_YOUR_TOKEN --repo owner/repo-name --skip-commits
python github_export.py --token ghp_YOUR_TOKEN --repo owner/repo-name --commit-limit 500
```

### CLI Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `--token` | required | GitHub personal access token |
| `--repo` | required | Repository in `owner/repo` format |
| `--pr-limit` | 500 | Max pull requests to fetch |
| `--pr-state` | `all` | PR state filter: `open`, `closed`, `all` |
| `--commit-limit` | 1000 | Max commits to fetch |
| `--output-dir` | `./github-export` | Output directory |
| `--skip-commits` | false | Skip commit export |
| `--skip-prs` | false | Skip PR export |

### Output Structure

```
github-export/
  owner__repo-name/
    repo_metadata.json       # Repo info + language breakdown (bytes per language)
    contributors.json        # Contributor list with commit counts, sorted by contributions
    commits.json             # Recent commits with full details (stats, files changed, patches)
    pull_requests.json       # PRs with reviews, comments, review comments, commits
    pull_requests.csv        # Flat CSV of PRs (body truncated to 1000 chars)
```

### What Gets Exported

**Repository Metadata** (`repo_metadata.json`):
- Full name, description, private/public, default branch
- Created/updated/pushed timestamps
- Stars, forks, open issues, watchers, topics, license
- Language breakdown (bytes per language + percentages)

**Contributors** (`contributors.json`):
- Login, numeric ID, type (User/Bot), total contributions count, profile URL
- Sorted by contribution count descending

**Commits** (`commits.json`):
- SHA, message, author name/email/login/date, committer name/email/login/date
- Parent SHAs (for merge detection)
- Stats: additions, deletions, total changes
- Per-file changes: filename, status (added/modified/deleted), additions, deletions, patch diff
- HTML URL
- Fetched individually (one API call per commit for full details)
- Incremental save: writes progress every 100 commits

**Pull Requests** (`pull_requests.json`):
- Number, title, state, author login/id, timestamps (created/updated/closed/merged)
- Merge commit SHA, draft status, body, head/base refs
- Labels, assignees, requested reviewers
- Additions/deletions/changed files counts
- **Reviews**: reviewer login, state (APPROVED/CHANGES_REQUESTED/COMMENTED), body, timestamp
- **Review comments** (inline code comments): author, body, file path, diff hunk, timestamp
- **Issue comments** (conversation tab): author, body, timestamp
- **Commits**: SHA, message, author name/email/login, date
- Incremental save: writes progress every 25 PRs

### Rate Limiting

- Checks `X-RateLimit-Remaining` header after each request
- Pauses when remaining requests drop below 50 (configurable `RATE_LIMIT_BUFFER`)
- Handles 429 and 403 responses with `Retry-After` header
- Exponential backoff on connection errors (5 retries)
- 0.1s delay between pages, 0.5s between individual commit/PR detail fetches

### PII Fields in Output

| File | PII Fields |
|------|------------|
| contributors.json | `login`, `profile_url` |
| commits.json | `author_name`, `author_email`, `author_login`, `committer_name`, `committer_email`, `committer_login`, `message` (may mention names) |
| pull_requests.json | `author`, `reviews[].reviewer`, `comments[].author`, `review_comments[].author`, `commits[].author_name/email/login`, `body` (may mention names), `assignees`, `requested_reviewers` |

---

## Script 2: `main.py` (Google Workspace Exporter)

### Authentication

Uses a Google Cloud service account with domain-wide delegation to impersonate individual users.

**Prerequisites:**
1. Google Cloud project with Gmail, Calendar, and Drive APIs enabled
2. Service account with domain-wide delegation enabled
3. Admin console: grant the service account access to the required scopes for the target domain

**Scopes:**
- `https://www.googleapis.com/auth/gmail.readonly`
- `https://www.googleapis.com/auth/calendar.readonly`
- `https://www.googleapis.com/auth/drive.readonly`

### Usage

```bash
uv run main.py --user someone@org_name.com --key service-account.json --output ./export
uv run main.py --user someone@org_name.com --key service-account.json --emails 1000 --events 200 --files 100
uv run main.py --user someone@org_name.com --key service-account.json --skip-gmail --skip-calendar
```

### CLI Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `--user` | required | Target user email (e.g. `someone@org_name.com`) |
| `--key` | required | Path to service account JSON key file |
| `--output` | `./export` | Output directory |
| `--emails` | 500 | Number of emails to export |
| `--events` | 500 | Number of calendar events to export |
| `--files` | 50 | Number of Drive files to export |
| `--skip-gmail` | false | Skip Gmail export |
| `--skip-calendar` | false | Skip Calendar export |
| `--skip-drive` | false | Skip Drive export |

### Output Structure

```
export/
  someone_at_org_name.com/
    gmail/
      <message_id>.eml        # Raw email files
      _index.json              # Metadata index (id, threadId, labels, snippet, attachments)
      attachments/
        <message_id>/
          report.pdf           # Extracted email attachments
    calendar/
      events.json              # Full event data (all fields from API)
      _summary.json            # Lightweight summary (id, title, start, organizer, attendee count)
    drive/
      Document Title.docx      # Google Docs → DOCX
      Spreadsheet.xlsx         # Google Sheets → XLSX
      Presentation.pptx        # Google Slides → PPTX
      Drawing.pdf              # Google Drawings/Forms → PDF
      regular_file.csv         # Non-Google files downloaded as-is
      unknown_file.bin         # Files without extensions get .bin
      _index.json              # Download index (id, name, mimeType, size, downloaded status)
```

### What Gets Exported

**Gmail:**
- Fetches message IDs via `messages.list`, then batch-downloads raw messages (format=raw)
- Saves each message as a `.eml` file (raw RFC 2822 format)
- Extracts inline/attached files from the MIME structure → saves to `attachments/<message_id>/`
- Builds `_index.json` with metadata: id, threadId, labelIds, snippet, internalDate, sizeEstimate, attachment list
- **Resume support:** Skips messages already downloaded (checks .eml file + index entry)
- Batch size: 10 messages per batch API call (kept small to avoid "too many concurrent requests")
- 2-second delay between batches

**Calendar:**
- Fetches events from the last 2 years (730 days back from now)
- Uses `singleEvents=True` (expands recurring events) sorted by start time
- Saves full event objects to `events.json`
- Saves lightweight summary to `_summary.json`: id, title, start, status, organizer email, attendee count, location, hangout link

**Drive:**
- Fetches most recently modified files owned by the target user
- **Google-native docs are exported/converted:**
  - Google Docs → `.docx`
  - Google Sheets → `.xlsx`
  - Google Slides → `.pptx`
  - Google Drawings/Forms → `.pdf`
- **Regular files** (PDFs, CSVs, etc.) downloaded as-is
- **Skipped:** Folders, shortcuts, maps, sites, fusion tables, images (`image/*`), videos (`video/*`)
- Streams downloads via `MediaIoBaseDownload`
- **Resume support:** Skips files already on disk
- Drive API timeout: 300 seconds for large file downloads
- Builds `_index.json` with metadata + download status

### Rate Limiting

- Exponential backoff retry on 429, 500, 503 errors (up to 5 retries)
- 0.3s delay between Drive file downloads
- 2s delay between Gmail batch fetches

### PII Fields in Output

| File | PII Fields |
|------|------------|
| `*.eml` | From/To/CC/BCC headers (name + email), body content |
| `gmail/_index.json` | `snippet` (email preview text) |
| `events.json` | `creator.email`, `organizer.email`, `attendees[].email`, `attendees[].displayName`, `summary`, `description`, `location` |
| `_summary.json` | `organizer` email |
| Drive files | Document content (names, emails, etc. in docs/sheets/slides) |
| `drive/_index.json` | `owners[].displayName`, `owners[].emailAddress`, file `name` |

---

## Script 3: `jira_export.py` (Jira Project Exporter)

### Authentication

Jira API token (create at https://id.atlassian.com/manage-profile/security/api-tokens). Uses HTTP Basic Auth with email + API token.

### Usage

```bash
python jira_export.py --token YOUR_API_TOKEN --project IES
python jira_export.py --token YOUR_API_TOKEN --project IES --limit 500
python jira_export.py --token YOUR_API_TOKEN --project IES --skip-attachments
python jira_export.py --token YOUR_API_TOKEN --project IES --skip-comments
python jira_export.py --token YOUR_API_TOKEN --project IES --output-dir ./my-export
```

### Hard-Coded Defaults (edit in script)

```python
JIRA_DOMAIN = "org_name.atlassian.net"
JIRA_EMAIL = "abhinav@org_name.com"
PROJECTS = ["IES"]
TICKETS_PER_PROJECT = 100
```

### CLI Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `--token` | required | Jira API token |
| `--limit` | 100 | Max tickets per project |
| `--output-dir` | `./jira-export` | Output directory |
| `--skip-attachments` | false | Skip downloading attachments |
| `--skip-comments` | false | Skip fetching comments |
| `--project` | all configured | Export a single project |

### Output Structure

```
jira-export/
  IES/
    tickets.json             # All tickets with fields, comments, attachments, changelog
    tickets.csv              # Flat CSV (one row per ticket, comments joined)
    attachments/
      IES-12345/
        screenshot.png       # Downloaded ticket attachments
        report.pdf
```

### What Gets Exported

**Tickets** (`tickets.json`):

Each ticket contains:

| Category | Fields |
|----------|--------|
| Identity | `key`, `id`, `self` (API URL) |
| Content | `summary`, `description_text` (plain text extracted from ADF), `description_adf` (raw Atlassian Document Format) |
| Classification | `issue_type`, `status`, `status_category`, `priority`, `resolution` |
| Project | `project_key`, `project_name` |
| Timestamps | `created`, `updated`, `resolved`, `due_date` |
| People | `assignee`/`assignee_email`/`assignee_account_id`, same for `reporter` and `creator` |
| Lists | `labels`, `components`, `fix_versions`, `affected_versions` |
| Hierarchy | `sprint`, `parent_key`, `parent_summary` |
| Time tracking | `original_estimate`, `remaining_estimate`, `time_spent` |
| Engagement | `votes`, `watchers` |
| Custom fields | All `customfield_*` fields, renamed to `Custom field (Human Name)` format |

**Comments** (fetched separately per ticket via `/issue/{key}/comment`):
- `id`, `author`, `author_email`, `author_account_id`
- `created`, `updated`
- `body_text` (plain text extracted from ADF), `body_adf` (raw), `rendered_body` (HTML)

**Attachments** (downloaded to `attachments/{ticket_key}/`):
- `id`, `filename`, `size`, `mime_type`, `created`
- `author`, `author_email`, `content_url`
- `_local_file` (added after download — relative path to local file)

**Changelog** (expanded from the search API response):
- `date`, `author`, `field`, `from`, `to`
- One entry per field change per history event

**CSV** (`tickets.csv`):
- One row per ticket
- Comments joined as text: `[timestamp] author: body_text` separated by `---`
- Attachment filenames joined with `|`
- Changelog reduced to count
- ADF JSON excluded
- All custom fields included as columns

### Key Implementation Details

- Uses Jira's new cursor-based pagination (`nextPageToken`) via `POST /search/jql` instead of the older offset-based `GET /search`
- Requests `expand=changelog,renderedFields` to get changelog and HTML-rendered fields in a single API call
- Custom field names resolved via `GET /field` API (maps `customfield_12345` → `Custom field (CC)`)
- ADF (Atlassian Document Format) parsed recursively to extract plain text — handles `text` nodes and `mention` nodes
- Attachment downloads use streaming (`iter_content`) with retry on timeout

### Rate Limiting

- 0.2s delay between paginated requests
- 5 retries on 429 with `Retry-After` header
- Streaming attachment downloads with 2 retries on timeout

### PII Fields in Output

| Location | PII Fields |
|----------|------------|
| Top-level | `assignee`, `assignee_email`, `assignee_account_id`, `reporter`/email/id, `creator`/email/id |
| Custom fields | `Custom field (CC)` (comma-separated names), `Custom field (L2 Assignee)`, `Custom field (Reporter)`, `Custom field (Customer Name)` |
| Comments | `author`, `author_email`, `author_account_id`, `body_text` |
| Attachments | `author`, `author_email`, `filename` (may contain names) |
| Changelog | `author` |
| Content | `summary`, `description_text` (may contain names, emails, phone numbers) |

---

## Script 4: `slack_channel_export.py` (Slack Channel Exporter)

### Authentication

Slack Bot Token (`xoxb-...`) from a Slack App.

**Required Bot Token Scopes:**
- `channels:history` — read public channel messages
- `groups:history` — read private channel messages
- `channels:read` — list public channels
- `groups:read` — list private channels
- `files:read` — download file attachments

**For private channels:** The bot must be invited with `/invite @BotName`.

### Usage

```bash
python slack_channel_export.py --token xoxb-YOUR-BOT-TOKEN
python slack_channel_export.py --token xoxb-YOUR-BOT-TOKEN --include-threads
python slack_channel_export.py --token xoxb-YOUR-BOT-TOKEN --skip-attachments
python slack_channel_export.py --token xoxb-YOUR-BOT-TOKEN --list-channels
python slack_channel_export.py --token xoxb-YOUR-BOT-TOKEN --input-csv my_channels.csv
```

### CLI Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `--token` | required | Slack Bot Token (`xoxb-...`) |
| `--input-csv` | `channels.csv` | CSV file with `channel_id` column |
| `--include-threads` | false | Include thread replies (expanded inline) |
| `--skip-attachments` | false | Skip downloading file attachments |
| `--output-dir` | `./slack-exports` | Output directory |
| `--list-channels` | false | List all accessible channels and exit |

### Input CSV Format

```csv
channel_id
C090J5ZPP51
C0ABC123456
```

### Output Structure

```
slack-exports/
  C090J5ZPP51/
    channel_info.json          # Channel metadata (name, type, members, topic, purpose)
    messages.json              # All messages sorted chronologically, with _local_file refs
    attachments/
      F07ABC_report.pdf        # Downloaded files (prefixed with file_id)
      F07DEF_screenshot.png
```

### What Gets Exported

**Channel Info** (`channel_info.json`):
- Full channel object from `conversations.info` API
- Name, ID, type (public/private), member count, topic, purpose, creator

**Messages** (`messages.json`):
- Full message history fetched via `conversations.history` (paginated, 200 per page)
- All message fields preserved: `user`, `text`, `ts`, `type`, `subtype`, `blocks`, `files`, `reactions`, `thread_ts`, `reply_count`, etc.
- Sorted chronologically by timestamp
- **Thread replies** (optional, `--include-threads`): Fetched via `conversations.replies` and appended inline with `_is_thread_reply=true` and `_parent_ts` metadata
- **File references**: Each file object gets `_local_file` added after download

**Attachments:**
- Scans all messages for `files[]` entries
- Downloads from `url_private_download` (or `url_private` fallback)
- Filename format: `{file_id}_{original_name}` (prevents collisions)
- **Skipped types:** videos (`.mp4`, `.mov`, `.avi`, etc.), `.apk`, `.ipa`, `.ico`, `.heic`, tombstoned/external files
- **Resume support:** Skips files already downloaded (checks file exists and isn't HTML garbage — re-downloads if Slack returned an HTML auth page instead of the file)
- **Hard timeout:** 60-second wall-clock limit per download using `SIGALRM` (prevents indefinite hangs)

### Rate Limiting

- 1.2s delay between paginated API calls
- 5 retries on 429 / `ratelimited` errors with `Retry-After` header
- 0.3s delay between file downloads

### PII Fields in Output

| File | PII Fields |
|------|------------|
| `channel_info.json` | `creator` (user ID) |
| `messages.json` | `user` (user ID), `text` (may contain `<@USERID>` mentions, names, emails), `files[].name` |
| Attachments | Document content |

### Companion: `slack-org_name-members.csv`

This CSV maps Slack user IDs to real identities. It's not generated by the exporter but is required by the PII scrubber to build the `slack_user_map` and `slack_username_map`.

| Column | Description |
|--------|-------------|
| `userid` | Slack user ID (e.g., `U012ABC34`) |
| `username` | Slack handle |
| `displayname` | Display name |
| `fullname` | Full real name |
| `email` | Email address |

---

## Data Flow: Exporter → Scrubber

After export, files are copied into `org_name-data/` for scrubbing:

```
source-data-exporter/github/github-export/repo1/   →  org_name-data/github/repo1/
source-data-exporter/google-workspace/export/user*/ →  org_name-data/google/user*/
source-data-exporter/jira/jira-export/IES/          →  org_name-data/jira/IES/
source-data-exporter/slack/slack-exports/            →  org_name-data/slack/slack-exports/
source-data-exporter/slack/slack-org_name-members.csv  →  org_name-data/slack/slack-org_name-members.csv
salesforce/ (Data Export tool)                        →  org_name-data/salesforce/sf_samples/
```

The git repository clone is placed at `org_name-data/github/repo1/code/` (not exported by the GitHub script — cloned separately with `git clone`).

---

## Dependencies

| Script | Dependencies |
|--------|-------------|
| `github_export.py` | `requests` |
| `main.py` (Google) | `google-api-python-client`, `google-auth`, `google-auth-httplib2` (managed via `uv` / `pyproject.toml`) |
| `jira_export.py` | `requests` |
| `slack_channel_export.py` | `requests` |

All scripts require Python 3.12+.

---

## Common Patterns Across All Exporters

1. **Pagination:** All scripts handle API pagination (cursor-based or page-based) and fetch up to a configurable limit.

2. **Rate limit handling:** All scripts handle HTTP 429 responses with retry logic and `Retry-After` headers. Additional delays between requests prevent hitting limits proactively.

3. **Resume/idempotency:** Gmail, Drive, and Slack attachment downloaders check for existing files on disk and skip already-downloaded items. Jira attachment downloads also skip existing files.

4. **Incremental save:** GitHub commit and PR exports save progress every 100/25 items respectively, preventing data loss on interruption.

5. **Dual output format:** GitHub and Jira export both JSON (full fidelity) and CSV (flat, human-readable) versions.

6. **Timeout protection:** Slack uses `SIGALRM` for hard 60-second download timeouts. Google Drive uses a 300-second HTTP timeout. All scripts use connection + read timeouts on requests.

7. **Error tolerance:** All scripts continue past individual item failures (bad downloads, API errors on specific items) rather than aborting the entire export.