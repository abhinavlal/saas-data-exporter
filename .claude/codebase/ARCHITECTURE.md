# Architecture

## System Type

This is a collection of four independent CLI batch-export tools (not a server, not a library). Each exporter is a self-contained Python script invoked as `python -m exporters.<name>`. There is no shared runtime process, no web framework, and no inter-process communication — each exporter runs to completion and exits.

## Core Architectural Pattern

The system uses a **phased pipeline pattern** with S3-backed checkpointing. Each exporter breaks its export job into named phases that execute sequentially. Phases are idempotent and can be skipped on resume. Within a phase, individual items (commits, tickets, messages) are tracked in a checkpoint so partial progress survives interruption.

The structural layers are:

```
CLI entry (main()) → Exporter class (export logic) → lib/ (shared infrastructure)
                                                         ├── S3Store (storage)
                                                         ├── CheckpointManager (resume)
                                                         ├── make_session() (HTTP + rate limiting)
                                                         └── ExportConfig (runtime config)
```

Dependencies flow strictly downward: exporters import from `lib/`, `lib/` modules do not import from `exporters/`.

## Entry Points

Each exporter is invoked via Python's `-m` module flag:

- `exporters/github.py:532` — `def main()` / `if __name__ == "__main__": main()`
- `exporters/jira.py:464` — same pattern
- `exporters/slack.py:338` — same pattern
- `exporters/google_workspace.py:446` — same pattern

Each `main()` function follows the same sequence:
1. Call `lib.config.load_dotenv()` to populate `os.environ` from `.env`
2. Parse CLI arguments with `argparse`, using `env()` helpers as defaults
3. Validate required arguments (token, bucket, target list)
4. Instantiate `S3Store` and `ExportConfig`
5. Iterate over target list (repos / projects / channels / users), running one exporter instance per target
6. Collect failures; log a summary at the end

## Key Abstractions

### `ExportConfig` (`lib/types.py:7`)
Dataclass carrying `s3_bucket`, `s3_prefix`, `max_workers`, `log_level`, and `json_logs`. Passed into every exporter constructor. Holds no mutable state.

### `S3Store` (`lib/s3.py:33`)
Thread-safe wrapper around a single `boto3` client. Provides `upload_bytes`, `upload_json`, `upload_file`, `upload_stream`, `download_json`, and `exists`. Create one instance per run; pass it to threads — the underlying boto3 client is safe to share. `NDJSONWriter` (same file, line 119) uses a disk-backed temp file and uploads to S3 periodically, bounding memory during large paginated fetches.

### `CheckpointManager` (`lib/checkpoint.py:44`)
Stores and retrieves a JSON checkpoint document at `_checkpoints/{job_id}.json` in S3. Tracks per-phase status (`pending` / `in_progress` / `completed`), item-level completion sets, pagination cursors, and item counts. Saves are throttled to once every 30 seconds (`SAVE_INTERVAL = 30`) except when `force=True`, which is used after each phase completes.

### `make_session()` (`lib/session.py:127`)
Factory that returns `(requests.Session, RateLimitState)`. The session is a `_TimeoutSession` (default connect/read timeouts injected on every request) with a `RateLimitedAdapter` mounted on both `https://` and `http://`. The adapter:
1. Checks `RateLimitState.should_preemptive_wait()` before each request — if remaining quota is below `min_remaining`, sleeps to spread requests across the reset window
2. Acquires a `TokenBucket` token (blocks until available)
3. Retries up to `max_retries_on_429` times on HTTP 429, respecting `Retry-After` headers or using exponential backoff
4. Also mounts a `urllib3.Retry` strategy for 500/502/503 errors

Each exporter creates its own session via `make_session()` with tuned `requests_per_second` and `burst` values: GitHub (10 rps / burst 20), Jira (5 rps / burst 10), Slack (0.8 rps / burst 3). The `RateLimitState` instance is kept on the exporter for inspection but is primarily updated inside the adapter.

### Exporter Classes

Each exporter (`GitHubExporter`, `JiraExporter`, `SlackExporter`, `GoogleWorkspaceExporter`) follows a uniform interface:
- Constructor accepts target identifier(s), `S3Store`, `ExportConfig`, and service-specific credentials and options
- `run()` is the public entry point — calls `checkpoint.load()`, then calls phase methods in order, skipping completed phases
- Phase methods are named `_export_<phase>` and follow: `checkpoint.start_phase()` → paginate API → write to `NDJSONWriter` or upload directly → `checkpoint.complete_phase()` → `checkpoint.save(force=True)`

`GoogleWorkspaceExporter` differs in that it uses the Google API Python client (`googleapiclient`) rather than `requests`, and uses the `@retry` decorator from `lib/retry.py` for non-HTTP operations.

## Data Flow

### Synchronous phases (sequential within a target)
Phases within a single export job always run sequentially: metadata → contributors → commits → pull_requests (GitHub); tickets → comments → attachments (Jira); channel_info → messages → threads → attachments (Slack); gmail → calendar → drive (Google).

### Parallel item fetching (within a phase)
Within phases that fetch per-item detail (GitHub PR detail, Jira attachment downloads, Slack thread replies, Gmail message uploads), a `ThreadPoolExecutor` with `config.max_workers` workers is used. The `S3Store` and HTTP session are both thread-safe and shared across workers.

### Disk-buffered writes
`NDJSONWriter` avoids accumulating large datasets in memory by writing records to a temp file on disk. The file is uploaded to S3 every 500 records as an incremental checkpoint. After all records are written, `read_all()` loads them back from disk for sorting and final JSON upload.

### Memory management pattern
After writing a phase's data to S3, exporters reload it from S3 for the next phase (rather than keeping a reference in memory). The pattern is explicit: `del tickets` / `del messages` after uploading. This bounds memory to one phase's data at a time for large datasets.

## Error Handling Strategy

**Per-target isolation:** `main()` wraps each target's export in a `try/except Exception` block. Failures are logged with `exc_info=True` and accumulated in a `failed` list. Processing continues to the next target. A summary is logged at the end.

**Per-item isolation:** Inside parallel phases, each future's result is retrieved in a `try/except Exception` block. Failed items are logged and skipped; the checkpoint does not mark them as done, so they will be retried on the next run.

**HTTP errors:** `resp.raise_for_status()` is called after every API response. 429s are handled by `RateLimitedAdapter` with retry/backoff. 500/502/503 are handled by the `urllib3.Retry` strategy mounted on the session. 404s are treated as warnings (item not found) rather than errors.

**Non-HTTP retries:** The `@retry` decorator (`lib/retry.py:11`) provides configurable exponential backoff for `HttpError` and `IOError` in the Google exporter where the Google client library is used instead of `requests`.

**No custom exception hierarchy.** Errors are standard Python exceptions. `lib/retry.py` accepts an `exceptions` tuple to control which exception types trigger retries.

## S3 Output Layout

Checkpoint files and export data share the same bucket under separate prefixes:

```
{prefix}/
  _checkpoints/
    github/{owner}__{repo}.json
    google/{user_at_domain}.json
    jira/{project}.json
    slack/{channel_id}.json
  github/{owner}__{repo}/
    repo_metadata.json
    contributors.json
    commits.json
    pull_requests.json
    pull_requests.csv
  google/{user_email_with_at_replaced}/
    gmail/{message_id}.eml
    gmail/_index.json
    gmail/attachments/{message_id}/{filename}
    calendar/events.json
    calendar/_summary.json
    drive/{filename}
    drive/_index.json
  jira/{project_key}/
    tickets.json
    tickets.csv
    attachments/{ticket_key}/{filename}
  slack/{channel_id}/
    channel_info.json
    messages.json
    attachments/{file_id}_{filename}
```

Work-in-progress NDJSON files (e.g., `_commits_wip.json`, `_prs_wip.json`, `_tickets_wip.json`, `_messages_wip.json`) are written during a phase and deleted from disk after the phase completes (but may remain in S3 if the process crashes mid-phase).