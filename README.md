# Data Exporter

Production-grade data exporter for GitHub, Google Workspace, Jira, and Slack. Exports structured JSON/CSV data to S3 with checkpointing, rate limiting, and parallel I/O.

## Installation

```bash
uv sync              # install dependencies
uv sync --extra dev  # install dev dependencies (pytest, moto)
uv sync --extra fast # install boto3[crt] for 2-6x S3 throughput
```

Requires Python 3.12+.

## Configuration

Copy `.env.example` to `.env` and fill in your values:

```bash
cp .env.example .env
```

All exporters read from `.env` automatically. CLI arguments override env vars. The priority is:

**CLI args > environment variables > `.env` file > defaults**

AWS credentials use standard AWS environment variables (or IAM roles):

```bash
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_DEFAULT_REGION=us-east-1
```

With `.env` configured, running an exporter is just:

```bash
uv run python -m exporters.github       # uses GITHUB_TOKEN, GITHUB_REPO, S3_BUCKET from .env
uv run python -m exporters.jira         # uses JIRA_TOKEN, JIRA_EMAIL, JIRA_PROJECTS from .env
uv run python -m exporters.slack        # uses SLACK_TOKEN, SLACK_CHANNEL_IDS from .env
uv run python -m exporters.google_workspace  # uses GOOGLE_USER, GOOGLE_SERVICE_ACCOUNT_KEY from .env
```

Any setting can still be overridden on the CLI:

```bash
uv run python -m exporters.github --repo other/repo --commit-limit 100
```

## Usage (full CLI reference)

### GitHub

```bash
uv run python -m exporters.github \
  --token ghp_YOUR_TOKEN \
  --repo owner/repo-name \
  --s3-bucket my-export-bucket \
  --s3-prefix exports
```

| Argument | Default | Description |
|----------|---------|-------------|
| `--token` | required | GitHub personal access token |
| `--repo` | required | Repository in `owner/repo` format |
| `--s3-bucket` | required | S3 bucket name |
| `--s3-prefix` | `""` | S3 key prefix |
| `--pr-limit` | 500 | Max pull requests to fetch |
| `--pr-state` | `all` | PR state filter: `open`, `closed`, `all` |
| `--commit-limit` | 1000 | Max commits to fetch |
| `--skip-commits` | false | Skip commit export |
| `--skip-prs` | false | Skip PR export |
| `--max-workers` | 5 | Parallel threads for API calls |

### Jira

```bash
uv run python -m exporters.jira \
  --token YOUR_API_TOKEN \
  --email you@company.com \
  --project IES \
  --s3-bucket my-export-bucket
```

| Argument | Default | Description |
|----------|---------|-------------|
| `--token` | required | Jira API token |
| `--email` | required | Jira account email |
| `--domain` | `practo.atlassian.net` | Jira domain |
| `--project` | required | Project key(s), repeatable |
| `--s3-bucket` | required | S3 bucket name |
| `--limit` | 100 | Max tickets per project |
| `--skip-attachments` | false | Skip downloading attachments |
| `--skip-comments` | false | Skip fetching comments |

### Slack

```bash
uv run python -m exporters.slack \
  --token xoxb-YOUR-BOT-TOKEN \
  --channel-ids C090J5ZPP51 C0ABC123456 \
  --s3-bucket my-export-bucket \
  --include-threads
```

| Argument | Default | Description |
|----------|---------|-------------|
| `--token` | required | Slack Bot Token (`xoxb-...`) |
| `--channel-ids` | | Channel IDs (space-separated) |
| `--input-csv` | `channels.csv` | CSV with `channel_id` column |
| `--s3-bucket` | required | S3 bucket name |
| `--include-threads` | false | Include thread replies |
| `--skip-attachments` | false | Skip file downloads |
| `--list-channels` | false | List accessible channels and exit |

### Google Workspace

```bash
uv run python -m exporters.google_workspace \
  --user someone@company.com \
  --key service-account.json \
  --s3-bucket my-export-bucket
```

| Argument | Default | Description |
|----------|---------|-------------|
| `--user` | required | Target user email |
| `--key` | required | Service account JSON key file |
| `--s3-bucket` | required | S3 bucket name |
| `--emails` | 500 | Number of emails to export |
| `--events` | 500 | Number of calendar events |
| `--files` | 50 | Number of Drive files |
| `--skip-gmail` | false | Skip Gmail export |
| `--skip-calendar` | false | Skip Calendar export |
| `--skip-drive` | false | Skip Drive export |

## S3 Output Structure

```
s3://{bucket}/{prefix}/
  github/{owner}__{repo}/
    repo_metadata.json
    contributors.json
    commits.json
    pull_requests.json
    pull_requests.csv
  google/{user_at_domain}/
    gmail/{message_id}.eml
    gmail/_index.json
    gmail/attachments/{message_id}/{filename}
    calendar/events.json
    calendar/_summary.json
    drive/{filename}
    drive/_index.json
  jira/{project}/
    tickets.json
    tickets.csv
    attachments/{ticket_key}/{filename}
  slack/{channel_id}/
    channel_info.json
    messages.json
    attachments/{file_id}_{filename}
  _checkpoints/
    github/{owner}__{repo}.json
    google/{user_at_domain}.json
    jira/{project}.json
    slack/{channel_id}.json
```

## Checkpoint / Resume

Each exporter saves progress to `_checkpoints/` in S3. If an export is interrupted (crash, timeout, Ctrl+C), restart with the same arguments and it will resume from the last checkpoint:

- Completed phases are skipped entirely
- Within a phase, individual items (commits, tickets, messages) that were already processed are skipped
- Checkpoints are saved every 30 seconds (configurable) and on phase completion

To re-export from scratch, delete the checkpoint file in S3.

## Rate Limiting

All exporters use a shared token-bucket rate limiter that:

- Acquires a token before each API request (configurable requests/second)
- Reads `X-RateLimit-Remaining` headers and preemptively waits when quota is low
- Retries on HTTP 429 with `Retry-After` header or exponential backoff
- Retries on HTTP 500/502/503 with exponential backoff

## Running Tests

```bash
uv run pytest tests/ -v         # all tests, verbose
uv run pytest tests/ -q         # quiet output
uv run pytest tests/test_s3.py  # single module
```

Tests use [moto](https://github.com/getmoto/moto) for S3 mocking and [responses](https://github.com/getsentry/responses) for HTTP mocking.
