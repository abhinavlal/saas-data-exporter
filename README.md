# Data Exporter

Batch data exporter for GitHub, Google Workspace, Jira, Slack, and Confluence. Exports structured JSON data to S3 with per-item files, checkpointing, rate limiting, and parallel execution.

## Quick Start

```bash
uv sync                  # install dependencies
cp .env.example .env     # configure credentials + targets
uv run python -m exporters.github              # export GitHub repos
uv run python -m exporters.jira                # export Jira projects
uv run python -m exporters.slack               # export Slack channels
uv run python -m exporters.google_workspace    # export Google Workspace users
uv run python -m exporters.confluence          # export Confluence spaces
```

Check export progress:

```bash
uv run python -m scripts.export_status
```

## Installation

```bash
uv sync              # core dependencies
uv sync --extra dev  # adds pytest, moto, responses
uv sync --extra fast # adds boto3[crt] for 2-6x S3 throughput
```

Requires Python 3.12+.

## Configuration

Copy `.env.example` to `.env` and fill in your values. All exporters read from `.env` automatically. CLI arguments override env vars.

**Priority:** CLI args > environment variables > `.env` file > defaults

## Exporters

### GitHub

Exports repo metadata, contributors, commits, and pull requests (with reviews, comments, sub-resources).

```bash
uv run python -m exporters.github
```

**Authentication options:**

| Method | Rate Limit | Config |
|--------|-----------|--------|
| **PAT** (simple) | 5,000 req/hr per token | `GITHUB_TOKEN=ghp_TOKEN1,ghp_TOKEN2` |
| **GitHub App** (recommended) | 5,000-12,500 req/hr per app | See [GitHub App Setup](docs/github-app-setup.md) |

Multiple PATs or Apps are round-robined across repos for independent rate limit pools.

| Argument | Default | Description |
|----------|---------|-------------|
| `--token` | `GITHUB_TOKEN` | PAT(s), comma-separated for round-robin |
| `--app-id` | `GITHUB_APP_ID` | GitHub App ID(s), comma-separated |
| `--app-key` | `GITHUB_APP_PRIVATE_KEY` | Path to .pem key(s), comma-separated |
| `--app-installation-id` | `GITHUB_APP_INSTALLATION_ID` | Installation ID(s), comma-separated |
| `--repo` | | Repository(s) in `owner/repo` format |
| `--input-csv` | `GITHUB_INPUT_CSV` | CSV file with `repo` column |
| `--pr-limit` | `0` (all) | Max PRs per repo |
| `--pr-state` | `all` | `open`, `closed`, or `all` |
| `--include-commits` | `false` | Include commit export (off by default) |
| `--commit-limit` | `0` (all) | Max commits per repo |
| `--commit-details` | `false` | Fetch full commit details (stats, files, patches) |
| `--parallel` | `4` | Repos to export simultaneously |
| `--max-workers` | `10` | Parallel PR/commit detail fetches per repo |

### Jira

Exports tickets (with comments, changelogs, custom fields) and attachments.

```bash
uv run python -m exporters.jira
```

| Argument | Default | Description |
|----------|---------|-------------|
| `--token` | `JIRA_TOKEN` | Jira API token |
| `--email` | `JIRA_EMAIL` | Jira account email |
| `--domain` | `org_name.atlassian.net` | Jira Cloud domain |
| `--project` | `JIRA_PROJECTS` | Project key(s), repeatable |
| `--input-csv` | `JIRA_INPUT_CSV` | CSV file with `project` column |
| `--limit` | `0` (all) | Max tickets per project |
| `--skip-attachments` | `false` | Skip downloading attachments |
| `--skip-comments` | `false` | Skip fetching comments |
| `--parallel` | `3` | Projects to export simultaneously |
| `--max-workers` | `10` | Parallel attachment downloads |

Rate limit: 20 req/s (Jira Cloud Standard with 250 users allows ~28 sustained, 100 burst). All projects share one tenant rate limit — multiple tokens don't help.

### Slack

Exports channel info, messages (with thread replies embedded), and file attachments.

```bash
uv run python -m exporters.slack
```

| Argument | Default | Description |
|----------|---------|-------------|
| `--token` | `SLACK_TOKEN` | Slack Bot Token (`xoxb-...`) |
| `--channel-ids` | | Channel IDs, space-separated |
| `--input-csv` | `SLACK_INPUT_CSV` | CSV with `channel_id` column |
| `--include-threads` | `false` | Fetch and embed thread replies in parent messages |
| `--skip-attachments` | `false` | Skip file downloads |
| `--list-channels` | | List accessible channels and exit |
| `--parallel` | `1` | Channels to export simultaneously |
| `--max-workers` | `10` | Parallel attachment downloads per channel |

Rate limit: 0.8 req/s (Slack Tier 3 ~50/min). All channels share one bot token. Attachment downloads use CDN URLs and are not rate-limited.

### Google Workspace

Exports Gmail (raw .eml + attachments), Calendar events, and Drive files per user.

```bash
uv run python -m exporters.google_workspace
```

Requires a Google service account with domain-wide delegation. See [Google Service Account Setup](docs/google-service-account-setup.md).

| Argument | Default | Description |
|----------|---------|-------------|
| `--user` | | Target user email(s) |
| `--input-csv` | `GOOGLE_INPUT_CSV` | CSV with `user` column |
| `--key` | `GOOGLE_SERVICE_ACCOUNT_KEY` | Service account JSON key file |
| `--emails` | `0` (all) | Max emails per user |
| `--events` | `0` (all) | Max calendar events per user |
| `--files` | `0` (all) | Max Drive files per user |
| `--skip-gmail` | `false` | Skip Gmail export |
| `--skip-calendar` | `false` | Skip Calendar export |
| `--skip-drive` | `false` | Skip Drive export |
| `--parallel` | `50` | Users to export simultaneously |
| `--max-workers` | `5` | Parallel uploads per user |

Each user gets independent API quota via `quotaUser` parameter. 50 parallel users validated with zero rate limit errors across 519 users.

### Confluence

Exports pages (with comments), and attachments per space.

```bash
uv run python -m exporters.confluence
```

Uses the same Atlassian credentials as Jira by default.

| Argument | Default | Description |
|----------|---------|-------------|
| `--space` | `CONFLUENCE_SPACES` | Space key(s), repeatable |
| `--input-csv` | `CONFLUENCE_INPUT_CSV` | CSV with `space` column |
| `--page-limit` | `0` (all) | Max pages per space |
| `--skip-comments` | `false` | Skip page comments |
| `--skip-attachments` | `false` | Skip downloading attachments |
| `--body-format` | `storage` | `storage` (XHTML) or `atlas_doc_format` (ADF JSON) |
| `--parallel` | `1` | Spaces to export simultaneously |

## S3 Output Structure

Every item is its own JSON file. No combined arrays.

```
s3://{bucket}/{prefix}/
  github/{owner}__{repo}/
    repo_metadata.json
    contributors.json
    commits/{sha}.json
    prs/{number}.json               # includes reviews, comments, commits
    _stats.json
  jira/{project}/
    tickets/{key}.json              # includes comments, changelog, custom fields
    tickets/_index.json
    attachments/{key}/{filename}
    _stats.json
  slack/{channel_id}/
    channel_info.json
    messages/{ts}.json              # thread replies embedded as _replies array
    messages/_index.json
    attachments/{file_id}_{name}
    _stats.json
  google/{user_slug}/
    gmail/{message_id}.eml
    gmail/_index.json
    gmail/attachments/{id}/{file}
    calendar/events/{event_id}.json
    calendar/_index.json
    drive/{file_id}_{filename}
    drive/_index.json
    _stats.json
  confluence/{space_key}/
    pages/{page_id}.json
    pages/_index.json
    attachments/{page_id}/{file}
    _stats.json
  _checkpoints/{exporter}/{target}.json
```

## Checkpoint / Resume

Exports are resumable. If interrupted, restart with the same arguments:

- Completed phases are skipped entirely
- Within a phase, completed items are skipped (tracked by ID in checkpoint)
- Checkpoints saved every 30 seconds and on phase completion
- Index files (`_index.json`) preserve pre-crash items on resume

To re-export from scratch, delete the checkpoint file in S3.

## Export Status

```bash
uv run python -m scripts.export_status                    # all exports
uv run python -m scripts.export_status --s3-prefix v31    # specific prefix
```

## Rate Limiting

All HTTP-based exporters use a shared token-bucket rate limiter that:

- Acquires a token before each API request
- Reads `X-RateLimit-Remaining` headers and preemptively slows down (capped at 30s max wait)
- Retries on HTTP 429 with `Retry-After` or exponential backoff
- Retries on HTTP 500/502/503 with exponential backoff
- Handles 403 rate limits (GitHub abuse detection) with wait-and-retry

## Running Tests

```bash
uv run pytest tests/ -v         # all tests, verbose
uv run pytest tests/ -q         # quiet
uv run pytest tests/test_s3.py  # single module
```

## Docs

- [GitHub App Setup](docs/github-app-setup.md) — Creating a GitHub App for higher rate limits
- [Google Service Account Setup](docs/google-service-account-setup.md) — Domain-wide delegation for Workspace export
