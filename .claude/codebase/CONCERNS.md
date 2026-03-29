# CONCERNS.md — Technical Debt and Potential Issues

## 1. Critical Security Issues

### 1.1 Real Credentials Committed to Git History (HIGH SEVERITY)
`service-account.json` contains a live Google service account private key and is present in the working tree. While `.gitignore` correctly excludes it, the file exists on disk at `/home/ubuntu/data-exporter/service-account.json`. Any accidental `git add .` or future commit could expose it.

**Credentials present in working tree (not in git, but sensitive):**
- `/home/ubuntu/data-exporter/service-account.json` — Google service account private key (`private_key_id: 316f1eed07dadce92236ae4a04deb94b0049afb9`, `client_email: user-data-export@practo-admin-tools.iam.gserviceaccount.com`)
- `/home/ubuntu/data-exporter/.env` — contains live AWS IAM access keys (`AWS_ACCESS_KEY_ID=AKIA23OVNWUEUITZQA4K`, `AWS_SECRET_ACCESS_KEY=...`), GitHub token (`ghp_qsb43vgRXteme...`), Jira API token, and Slack bot token

These files are gitignored but exist on disk. They should be rotated immediately if exposed to any shared environment.

### 1.2 Hardcoded Org-Specific Default in CLI
`exporters/jira.py:473` hardcodes `practo.atlassian.net` as the default Jira domain:
```python
parser.add_argument("--domain", default=env("JIRA_DOMAIN", "practo.atlassian.net"), ...)
```
This is a code-level leak of an internal domain name. The fallback default should be `None` or an empty string, forcing explicit configuration.

### 1.3 Real PII Data Files Present in Working Tree
The following files contain real employee data but are correctly gitignored. They remain on-disk and should be treated as sensitive:
- `/home/ubuntu/data-exporter/google_users.csv` — 519 lines of real `@practo.com` email addresses
- `/home/ubuntu/data-exporter/slack_channels.csv` — real Slack channel IDs
- `/home/ubuntu/data-exporter/github_repos.csv` — real `practo/` repo names

These files are excluded by `.gitignore` (the `github_repos.csv`, `jira_projects.csv`, `slack_channels.csv`, `google_users.csv` patterns are present) and should not be committed.

---

## 2. Files Exceeding 500 Lines

| File | Lines | Notes |
|------|-------|-------|
| `exporters/github.py` | 609 | Largest file; single class + `main()` |
| `exporters/jira.py` | 523 | Single class + module-level helpers + `main()` |
| `exporters/google_workspace.py` | 516 | Single class + `main()` |

All three exporter files are at or above the 500-line threshold. Each file mixes:
- Exporter class with API interaction methods
- CSV serialization logic (e.g., `_upload_pr_csv` in `exporters/github.py:484`, `_upload_csv` in `exporters/jira.py:409`)
- `main()` entrypoint with argument parsing

The CSV serialization and argument parsing could be split into separate modules to reduce per-file size and improve testability.

---

## 3. TODO / FIXME / HACK / XXX Comments

No `TODO`, `FIXME`, `HACK`, `XXX`, `DEPRECATED`, or `WORKAROUND` comments exist anywhere in the codebase. The code is clean of inline debt markers.

---

## 4. Unused Imports (Dead Code)

### 4.1 `import io` in `exporters/slack.py`
`exporters/slack.py:4` imports `io` but the module is never referenced in the file. The Slack exporter does not build any `StringIO` or `BytesIO` objects directly. This is a leftover from an earlier iteration.

---

## 5. Unused Public API (Dead Code)

### 5.1 `TokenBucket.throttle()` and `TokenBucket.restore()`
`lib/rate_limit.py:50-58` defines `throttle(new_rate)` and `restore(original_rate)` methods. Neither is called anywhere in the codebase — not in `lib/session.py`, not in any exporter. The `RateLimitedAdapter` handles 429s by sleeping, not by calling `throttle()`.

### 5.2 `TokenBucket.try_acquire()`
`lib/rate_limit.py:41-48` defines a non-blocking `try_acquire()` method. It is never called anywhere in the codebase.

### 5.3 `S3Store.upload_stream()`
`lib/s3.py:109-117` defines `upload_stream(stream, s3_path, content_type)` which uploads from an `io.IOBase` object. It is never called anywhere in exporters or tests. All streaming uploads use `upload_file()` via `tempfile.NamedTemporaryFile`.

### 5.4 `@retry` Decorator Only Used in One Exporter
`lib/retry.py` implements the `@retry` decorator but it is only applied in `exporters/google_workspace.py` (on `_batch_fetch_raw`, `_export_google_doc`, `_download_drive_file`). The GitHub, Jira, and Slack exporters do not use `@retry` at all — they rely entirely on urllib3's `Retry` strategy inside `make_session()`. This inconsistency means Google Drive downloads get explicit retry coverage at the application level, while other exporters do not.

---

## 6. Hardcoded Values That Should Be Configurable

### 6.1 Sleep Durations in `exporters/google_workspace.py`
Two hardcoded `time.sleep()` calls exist in the Gmail and Drive export paths:
- `exporters/google_workspace.py:158` — `time.sleep(2)` between every Gmail batch of 10 messages. The comment says "2s between batches" but this value is not exposed as a configuration option and could be overly conservative or too aggressive depending on the account.
- `exporters/google_workspace.py:357` — `time.sleep(0.3)` between each Drive file download. Same concern.

These sleeps bypass the shared `TokenBucket`/`RateLimitedAdapter` rate limiting mechanism that all other exporters use. They should either be removed in favor of the session-level rate limiter or made configurable via `ExportConfig` or environment variables.

### 6.2 Per-Exporter Rate Limit Values Are Hardcoded in Constructor
Rate limiting parameters are hardcoded inside each exporter's `__init__`:
- `exporters/github.py:45-47`: `requests_per_second=10, burst=20, min_remaining=50`
- `exporters/jira.py:76-78`: `requests_per_second=5, burst=10, min_remaining=50`
- `exporters/slack.py:60-63`: `requests_per_second=0.8, burst=3, min_remaining=50`

These are reasonable defaults but cannot be overridden without code changes. If an API's rate limits change, the code must be edited.

### 6.3 `SAVE_INTERVAL` in `lib/checkpoint.py` Is a Class Constant
`lib/checkpoint.py:63`: `SAVE_INTERVAL = 30` seconds is hardcoded as a class constant. Large exports with many items may want more or less frequent checkpoint saves without code changes.

---

## 7. Inconsistent Patterns

### 7.1 `@retry` Decorator vs. urllib3 `Retry` Strategy
There are two retry mechanisms in the codebase that serve similar purposes:
- `lib/retry.py` — application-level decorator used only in `exporters/google_workspace.py`
- `lib/session.py:143-148` — urllib3-level `Retry` strategy configured in `make_session()` for all session-based exporters

The Google Workspace exporter uses the Google API client (`googleapiclient`) which does not go through the `requests.Session`, so it legitimately needs the `@retry` decorator. However, the inconsistency means callers must know which mechanism applies to which exporter.

### 7.2 Duplicate `s3_env` Fixture Definition
The `s3_env` pytest fixture is defined identically in five separate test files:
- `tests/test_github.py:21-27`
- `tests/test_jira.py:21-27`
- `tests/test_slack.py:20-26`
- `tests/test_edge_cases.py:19-27`
- `tests/test_google_workspace.py:25-31`

All five definitions are byte-for-byte identical (create moto S3 bucket, return `(S3Store, ExportConfig, boto3.client)` tuple). This should be extracted into a shared `tests/conftest.py`. A `conftest.py` does not exist in the `tests/` directory.

### 7.3 `lib/config.py` and `lib/input.py` Imports Are Deferred to `main()`
All four exporters defer imports of `lib.config` and `lib.input` into the `main()` function body (e.g., `exporters/github.py:533-534`). This is inconsistent with the top-level imports of all other `lib.*` modules. While it prevents circular import issues, the inconsistency is surprising and slightly obscures dependencies.

### 7.4 Calendar Export Does Not Use Checkpointing for Individual Events
`exporters/google_workspace.py:257-316` (`_export_calendar`) collects all events into memory and uploads in one shot. It marks the entire `calendar` phase as complete atomically but does not checkpoint individual events. By contrast, Gmail, Drive, commits, PRs, and Jira tickets all have per-item checkpointing. For very large calendars this means a mid-run interruption would restart the entire calendar fetch.

### 7.5 Gmail Batch Fetch Is Not Truly Batched
`exporters/google_workspace.py:189-203` (`_batch_fetch_raw`) has a docstring that says "Fetch raw messages via individual API calls (batch API is complex with googleapiclient)" and iterates over IDs one by one. The function is named `_batch_fetch_raw`, accepts a list, but makes N sequential API calls. The method is also decorated with `@retry`, but since the retry triggers on `HttpError` from any individual message and re-fetches all messages in the batch on retry, a single 429 causes all messages in the batch to be re-fetched.

---

## 8. Missing or Outdated Tests

### 8.1 No Coverage Configuration
There is no `pytest.ini`, `setup.cfg`, or `[tool.pytest.ini_options]` section in `pyproject.toml` that configures coverage enforcement. The `pyproject.toml` `[project.optional-dependencies]` dev section does not include `pytest-cov`. There are no coverage minimums enforced in CI (no CI configuration exists at all).

### 8.2 No CI/CD Configuration
There is no `.github/workflows/`, `Jenkinsfile`, `.gitlab-ci.yml`, or any other CI pipeline configuration. Tests must be run manually. There is no automated gate preventing regressions.

### 8.3 `tests/fixtures/` Directory Is Empty
`tests/fixtures/__init__.py` exists but the directory contains no fixture files. This appears to be scaffolding that was never populated.

### 8.4 Google Drive File Download Is Not Tested End-to-End
`tests/test_google_workspace.py:262-336` tests the Drive export but uses a mock that intercepts `MediaIoBaseDownload` at the class level (`patch("exporters.google_workspace.MediaIoBaseDownload")`). The mock's `side_effect` in `TestDriveExport.test_exports_drive_index` writes to the file handle via a side effect but the actual content written to S3 is not verified. The `downloaded: True` flag in the index is asserted, but the actual file content in S3 is not checked.

### 8.5 No Tests for `lib/logging.py`
`lib/logging.py` has no corresponding test file. `JSONFormatter.format()` and `setup_logging()` are untested.

### 8.6 `_batch_fetch_raw` Retry Behavior on Partial Failure Is Not Tested
The `@retry` decorator on `exporters/google_workspace.py:189` retries the entire batch when one message raises `HttpError` with status 429/500/503. There is no test verifying that partial-failure-then-retry correctly re-fetches the right messages without duplicating already-fetched ones.

---

## 9. README Documentation Inconsistencies

### 9.1 README Default Values Do Not Match Code
`README.md` documents the following defaults that differ from the actual code:
- `--pr-limit | 500` — actual code default is `0` (no limit); `env_int("GITHUB_PR_LIMIT", 0)` at `exporters/github.py:544`
- `--commit-limit | 1000` — actual code default is `0` (no limit); `env_int("GITHUB_COMMIT_LIMIT", 0)` at `exporters/github.py:546`
- `--emails | 500` — actual code default is `0`; `exporters/google_workspace.py:458`
- `--events | 500` — actual code default is `0`; `exporters/google_workspace.py:459`
- `--files | 50` — actual code default is `0`; `exporters/google_workspace.py:460`
- `--limit | 100` (Jira) — actual code default is `0`; `exporters/jira.py:478`

The README states non-zero defaults, which would cause users to believe the export is capped when it is actually unlimited unless they set a limit explicitly.

---

## 10. Broad Exception Swallowing

All four exporters use `except Exception: log.error(..., exc_info=True)` to swallow exceptions during per-item operations (e.g., commit detail fetch, PR fetch, attachment download). While this is intentional for resilience, it means transient errors (network timeouts, permission errors, throttle failures that exhaust all retries) are silently converted to log messages and missing data. There is no mechanism to distinguish items that failed transiently from items that genuinely do not exist. Examples:
- `exporters/github.py:182-183` — failed commit detail fetches are logged and skipped; the final `commits.json` may silently have fewer commits than expected
- `exporters/jira.py:395-396` — failed attachment downloads are silently skipped
- `exporters/google_workspace.py:145-146` — failed Gmail uploads are silently skipped

Consider adding a per-job failure summary that records failed item IDs to S3 alongside the checkpoint, enabling operators to identify and re-process failed items.

---

## 11. Potential Memory Issue: Large JSON Arrays Loaded Entirely Into Memory

Several code paths download entire `tickets.json` or `messages.json` arrays from S3 into memory for enrichment phases:
- `exporters/jira.py:116` — `tickets = self.s3.download_json(f"{s3_base}/tickets.json") or []` — can be hundreds of megabytes for large Jira projects
- `exporters/slack.py:96` — `messages = self.s3.download_json(f"{s3_base}/messages.json") or []` — loaded for thread enrichment and attachment phases
- `exporters/slack.py:104` — same file loaded again for attachment phase

The `NDJSONWriter` was designed to avoid this pattern, but the enrichment phases (comments, attachments, thread replies) round-trip through full JSON arrays because `download_json` always deserializes the full array. For channels or projects with tens of thousands of items this may cause OOM failures.