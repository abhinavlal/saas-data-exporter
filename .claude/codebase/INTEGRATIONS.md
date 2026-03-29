# INTEGRATIONS.md

## External API Integrations

### GitHub REST API

- **Client:** `requests.Session` wrapped by `lib/session.py:make_session()`
- **Base URL:** `https://api.github.com` (constant `API_BASE` in `exporters/github.py:17`)
- **Auth:** Bearer token via `Authorization: Bearer {token}` header; token sourced from `GITHUB_TOKEN` env var
- **Accept header:** `application/vnd.github+json`
- **Rate limit config:** 10 req/s, burst 20, preemptive wait at 50 remaining (`exporters/github.py:44-48`)
- **Endpoints used:**
  - `GET /repos/{owner}/{repo}` — repository metadata
  - `GET /repos/{owner}/{repo}/languages` — language breakdown
  - `GET /repos/{owner}/{repo}/contributors` — contributor list (paginated)
  - `GET /repos/{owner}/{repo}/commits` — commit list (paginated)
  - `GET /repos/{owner}/{repo}/commits/{sha}` — individual commit detail
  - `GET /repos/{owner}/{repo}/pulls` — PR list (paginated)
  - `GET /repos/{owner}/{repo}/pulls/{number}` — PR detail
  - `GET /repos/{owner}/{repo}/pulls/{number}/reviews`
  - `GET /repos/{owner}/{repo}/pulls/{number}/comments`
  - `GET /repos/{owner}/{repo}/issues/{number}/comments`
  - `GET /repos/{owner}/{repo}/pulls/{number}/commits`

### Jira REST API (Atlassian Cloud)

- **Client:** `requests.Session` via `lib/session.py:make_session()`
- **Base URL:** `https://{domain}/rest/api/3` — domain sourced from `JIRA_DOMAIN` env var (default `org_name.atlassian.net`)
- **Auth:** HTTP Basic auth with email + API token (`session.auth = (email, token)` in `exporters/jira.py:80`)
- **Rate limit config:** 5 req/s, burst 10 (`exporters/jira.py:75-79`)
- **Key endpoints used:** JQL search, issue detail, comments, attachments, changelogs (via `/rest/api/3/issue/...`)
- **Attachment download:** Binary content fetched and uploaded directly to S3 via temp file

### Slack Web API

- **Client:** `requests.Session` via `lib/session.py:make_session()`
- **Base URL:** `https://slack.com/api` (constant `SLACK_API` in `exporters/slack.py:17`)
- **Auth:** Bot token via `Authorization: Bearer {token}` header; token sourced from `SLACK_TOKEN` env var (format `xoxb-...`)
- **Rate limit config:** 0.8 req/s, burst specified at init (`exporters/slack.py:59-60`); conservative for Slack Tier 3 methods (~50/min)
- **Key endpoints used:** `conversations.info`, `conversations.history`, `conversations.replies`, `files.info`, file download URLs

### Google Workspace APIs

- **Client:** `googleapiclient.discovery.build()` from `google-api-python-client` (NOT `requests`-based)
- **Auth:** Service account with domain-wide delegation; credentials loaded from a JSON key file via `google.oauth2.service_account.Credentials.from_service_account_file()` and impersonated per-user with `.with_subject(user)` (`exporters/google_workspace.py:88-91`)
- **Key file path:** Sourced from `GOOGLE_SERVICE_ACCOUNT_KEY` env var (default `service-account.json`)
- **OAuth2 scopes:**
  - `https://www.googleapis.com/auth/gmail.readonly`
  - `https://www.googleapis.com/auth/calendar.readonly`
  - `https://www.googleapis.com/auth/drive.readonly`
- **APIs built dynamically:**
  - `build("gmail", "v1", ...)` — list and raw-fetch email messages (batch fetch), export `.eml` files and attachments
  - `build("calendar", "v3", ...)` — list calendar events
  - `build("drive", "v3", ...)` — list Drive files; export Google-native formats via `files.export()`, binary files via `MediaIoBaseDownload`
- **`cache_discovery=False`** passed to all `build()` calls to avoid stale discovery document issues

## Storage Services

### Amazon S3

- **SDK:** `boto3` 1.42.78 via `boto3.session.Session().client("s3", ...)`
- **Abstraction:** `lib/s3.py:S3Store` — wraps all S3 operations; instantiated once per run and shared across threads
- **Connection pool:** `max_pool_connections=50` via `botocore.config.Config`
- **Retry config:** `{"max_attempts": 5, "mode": "adaptive"}` via `BotocoreConfig` (`lib/s3.py:44-48`)
- **Upload modes:**
  - Small files (<64 MB): single-part via `TransferConfig(multipart_threshold=128*MB, use_threads=False)`
  - Large files (≥64 MB): multipart via `TransferConfig(multipart_chunksize=64*MB, max_concurrency=20, use_threads=True)`
- **Key operations:** `upload_file`, `upload_bytes`, `upload_json`, `download_json`, `exists`, `upload_stream`
- **NDJSON writes:** `lib/s3.py:NDJSONWriter` — streams records to a local temp file, uploads to S3 every 500 records and on close
- **Checkpoint storage:** JSON files written to `_checkpoints/{exporter}/{job_id}.json` within the configured bucket/prefix
- **Output key structure:**
  ```
  {prefix}/github/{owner}__{repo}/repo_metadata.json
  {prefix}/github/{owner}__{repo}/contributors.json
  {prefix}/github/{owner}__{repo}/commits.json
  {prefix}/github/{owner}__{repo}/pull_requests.json
  {prefix}/github/{owner}__{repo}/pull_requests.csv
  {prefix}/google/{user_at_domain}/gmail/{message_id}.eml
  {prefix}/google/{user_at_domain}/gmail/_index.json
  {prefix}/google/{user_at_domain}/gmail/attachments/{message_id}/{filename}
  {prefix}/google/{user_at_domain}/calendar/events.json
  {prefix}/google/{user_at_domain}/drive/{filename}
  {prefix}/google/{user_at_domain}/drive/_index.json
  {prefix}/jira/{project}/tickets.json
  {prefix}/jira/{project}/tickets.csv
  {prefix}/jira/{project}/attachments/{ticket_key}/{filename}
  {prefix}/slack/{channel_id}/channel_info.json
  {prefix}/slack/{channel_id}/messages.json
  {prefix}/slack/{channel_id}/attachments/{file_id}_{filename}
  {prefix}/_checkpoints/github/{owner}__{repo}.json
  {prefix}/_checkpoints/google/{user_at_domain}.json
  {prefix}/_checkpoints/jira/{project}.json
  {prefix}/_checkpoints/slack/{channel_id}.json
  ```
- **Bucket config:** `S3_BUCKET` env var (required); `S3_PREFIX` env var (optional, default `""`)
- **AWS credentials:** Standard environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`) or IAM role — not hardcoded

## Rate Limiting (shared infrastructure)

- **`lib/rate_limit.py:TokenBucket`** — thread-safe token bucket; per-exporter rate configured at session creation
- **`lib/session.py:RateLimitedAdapter`** — `HTTPAdapter` subclass; acquires token before each request, reads `X-RateLimit-Remaining` / `X-RateLimit-Reset` headers, retries on HTTP 429 with `Retry-After` or exponential backoff (up to 5 attempts, max 120s)
- **`lib/session.py:RateLimitState`** — thread-safe header state tracker; triggers preemptive waiting when `X-RateLimit-Remaining` < `min_remaining` (default 50)
- **Per-service rates configured:**
  - GitHub: 10 req/s, burst 20
  - Jira: 5 req/s, burst 10
  - Slack: 0.8 req/s (conservative for Tier 3 methods)
  - Google: no token-bucket (uses `googleapiclient` built-in retry via `HttpError`)

## Checkpointing (resumability)

- **`lib/checkpoint.py:CheckpointManager`** — saves/loads `PhaseState` objects to/from S3 as JSON
- Checkpoint save throttled to every 30 seconds (`SAVE_INTERVAL = 30`) unless `force=True`
- Supports per-phase cursor tracking (`set_cursor` / `get_cursor`) and per-item completion tracking (`mark_item_done` / `is_item_done`)
- All four exporters use `CheckpointManager`; Google Workspace exporter uses it without `make_session()` (Google client library handles its own retry)

## Retry (non-HTTP operations)

- **`lib/retry.py:retry`** — decorator with exponential backoff (`backoff_base=2.0`, `max_backoff=120s`, `max_attempts=5`)
- Used in `exporters/google_workspace.py` for Google API calls (`@retry(...)`)
- HTTP retry handled separately by `RateLimitedAdapter` and urllib3's `Retry` strategy (status codes 500, 502, 503)

## Monitoring and Observability

- **`lib/logging.py:JSONFormatter`** — structured JSON logs to `stderr`; fields: `ts`, `level`, `logger`, `msg`, plus optional `phase`, `item`, `progress`, `total`, `source` from log record extras
- **Text format** also available (non-JSON) via `--no-json-logs` CLI flag or `JSON_LOGS=false` env var
- **Log level:** configurable via `LOG_LEVEL` env var (default `INFO`) or `--log-level` CLI argument
- No metrics, tracing, or external observability services (e.g., Datadog, OpenTelemetry) are integrated

## Authentication Providers

- **GitHub:** Personal access token (PAT) passed as Bearer token
- **Jira:** Atlassian API token with Basic auth (email:token)
- **Slack:** Bot OAuth token (`xoxb-` prefix) passed as Bearer token
- **Google Workspace:** Service account with domain-wide delegation; JSON key file loaded from disk; per-user impersonation via `credentials.with_subject(user_email)`
- No JWT libraries, SSO integrations, or OAuth flow implementations are present (all tokens are pre-generated and provided via configuration)

## CI/CD

No CI/CD pipeline configuration files are present in the repository (no `.github/workflows/`, `Jenkinsfile`, `.gitlab-ci.yml`, `.circleci/config.yml`, or `bitbucket-pipelines.yml`).

## Docker

No `Dockerfile` or `docker-compose.yml` is present in the repository.

## Local File Storage

- **Temp files:** `tempfile.NamedTemporaryFile` used in `lib/s3.py:NDJSONWriter` and in exporter attachment-download flows; always cleaned up in `finally` blocks or via `os.unlink`
- **Input CSVs:** Optional CSV files for target lists (e.g., `github_repos.csv`, `slack_channels.csv`) read from the local filesystem via `lib/input.py:read_csv_column()`
- **Service account key:** `service-account.json` read from the local filesystem at `GOOGLE_SERVICE_ACCOUNT_KEY` path
- **`.env` file:** Loaded from `Path.cwd() / ".env"` by `lib/config.py:load_dotenv()`; does not override already-set environment variables