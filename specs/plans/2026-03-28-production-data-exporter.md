# Production-Grade Data Exporter — Implementation Plan

## Overview

Rebuild the prototype data exporters (GitHub, Google Workspace, Jira, Slack) as a production-grade Python package with shared infrastructure for S3 storage, checkpointing, rate limiting, retry/backoff, and parallel I/O. Each exporter remains independently runnable via CLI.

## Current State

- `requirements.md` documents 4 prototype scripts with duplicated patterns
- No code in repo yet — greenfield implementation based on prototype spec
- Prototype writes to local disk; production target is S3

## Desired End State

- Single Python package (`data-exporter`) managed by `uv`
- Shared `lib/` with S3, checkpoint, rate limiting, retry, and logging modules
- 4 exporter scripts producing identical output structure to prototype, stored in S3
- Kill-and-restart any export → resumes from last checkpoint without re-fetching
- All I/O-bound operations parallelized via `ThreadPoolExecutor`
- Structured JSON logging with progress metrics

## What We're NOT Doing

- Distributed/multi-worker execution (Celery, SQS)
- Web UI or dashboard
- PII scrubbing (separate pipeline, unchanged)
- Scheduling/cron (run externally via cron, Airflow, etc.)
- Salesforce exporter (manual Data Export tool)
- Real-time / incremental sync
- Async rewrite (staying synchronous + threads)

## Project Structure

```
data-exporter/
├── pyproject.toml
├── lib/
│   ├── __init__.py
│   ├── s3.py               # S3 client wrapper (upload, download, checkpoint)
│   ├── checkpoint.py        # Checkpoint manager (save/load/resume state)
│   ├── rate_limit.py        # Token bucket + adaptive rate limiter
│   ├── retry.py             # Retry decorator with exponential backoff
│   ├── session.py           # RateLimitedSession (requests.Session wrapper)
│   ├── logging.py           # Structured JSON logging setup
│   └── types.py             # Shared dataclasses and type aliases
├── exporters/
│   ├── __init__.py
│   ├── github.py
│   ├── google_workspace.py
│   ├── jira.py
│   └── slack.py
└── tests/
    ├── __init__.py
    ├── test_s3.py
    ├── test_checkpoint.py
    ├── test_rate_limit.py
    ├── test_retry.py
    ├── test_session.py
    ├── test_github.py
    ├── test_jira.py
    ├── test_slack.py
    └── test_google_workspace.py
```

## S3 Layout

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

---

## Phase 1: Shared Library (`lib/`)

### Overview

Build all shared infrastructure modules. This is the foundation — nothing else works without it.

### Changes Required

#### 1. `pyproject.toml`

```toml
[project]
name = "data-exporter"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
    "requests>=2.31",
    "boto3>=1.34",
    "google-api-python-client>=2.100",
    "google-auth>=2.23",
    "google-auth-httplib2>=0.2",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0",
    "pytest-mock>=3.12",
    "moto[s3]>=5.0",
]
fast = [
    "boto3[crt]",  # 2-6x S3 throughput
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
```

#### 2. `lib/s3.py` — S3 Client Wrapper

Thread-safe S3 operations. Single client instance created once and shared.

```python
"""Thread-safe S3 client wrapper for upload, download, and checkpoint storage."""

import json
import io
import logging
from pathlib import Path

import boto3
import boto3.session
from boto3.s3.transfer import TransferConfig
from botocore.config import Config as BotocoreConfig
from botocore.exceptions import ClientError

log = logging.getLogger(__name__)

MB = 1024 * 1024

SMALL_FILE_CONFIG = TransferConfig(
    multipart_threshold=128 * MB,
    use_threads=False,  # inter-file parallelism handled by ThreadPoolExecutor
)

LARGE_FILE_CONFIG = TransferConfig(
    multipart_threshold=64 * MB,
    multipart_chunksize=64 * MB,
    max_concurrency=20,
    use_threads=True,
    preferred_transfer_client="auto",
)


class S3Store:
    """
    Thread-safe S3 storage backend.

    Create once, pass to all threads. The underlying boto3 client
    instance is thread-safe (boto3 docs + issue #1512 confirm this).
    """

    def __init__(self, bucket: str, prefix: str = ""):
        session = boto3.session.Session()
        self._client = session.client(
            "s3",
            config=BotocoreConfig(
                retries={"max_attempts": 5, "mode": "adaptive"},
                max_pool_connections=50,
            ),
        )
        self.bucket = bucket
        self.prefix = prefix.strip("/")

    def _key(self, path: str) -> str:
        if self.prefix:
            return f"{self.prefix}/{path}"
        return path

    def upload_file(self, local_path: str | Path, s3_path: str,
                    content_type: str | None = None) -> None:
        extra = {}
        if content_type:
            extra["ContentType"] = content_type
        config = LARGE_FILE_CONFIG if Path(local_path).stat().st_size > 64 * MB else SMALL_FILE_CONFIG
        self._client.upload_file(
            Filename=str(local_path),
            Bucket=self.bucket,
            Key=self._key(s3_path),
            Config=config,
            ExtraArgs=extra or None,
        )

    def upload_bytes(self, data: bytes, s3_path: str,
                     content_type: str = "application/octet-stream") -> None:
        self._client.put_object(
            Bucket=self.bucket,
            Key=self._key(s3_path),
            Body=data,
            ContentType=content_type,
        )

    def upload_json(self, obj: dict | list, s3_path: str) -> None:
        self.upload_bytes(
            json.dumps(obj, indent=2, default=str).encode(),
            s3_path,
            content_type="application/json",
        )

    def download_json(self, s3_path: str) -> dict | list | None:
        try:
            resp = self._client.get_object(
                Bucket=self.bucket, Key=self._key(s3_path)
            )
            return json.loads(resp["Body"].read())
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                return None
            raise

    def exists(self, s3_path: str) -> bool:
        try:
            self._client.head_object(Bucket=self.bucket, Key=self._key(s3_path))
            return True
        except ClientError:
            return False

    def upload_stream(self, stream: io.IOBase, s3_path: str,
                      content_type: str = "application/octet-stream") -> None:
        self._client.upload_fileobj(
            Fileobj=stream,
            Bucket=self.bucket,
            Key=self._key(s3_path),
            Config=LARGE_FILE_CONFIG,
            ExtraArgs={"ContentType": content_type},
        )
```

#### 3. `lib/checkpoint.py` — Checkpoint Manager

Tracks export progress per phase. Stored as JSON in S3.

```python
"""Checkpoint manager for resumable exports.

Checkpoint structure:
{
    "job_id": "github/owner__repo",
    "status": "in_progress",
    "started_at": "2026-03-28T10:00:00Z",
    "updated_at": "2026-03-28T10:05:00Z",
    "phases": {
        "commits": {
            "status": "completed",
            "total": 500,
            "completed": 500,
            "cursor": null,
            "completed_ids": []
        },
        "pull_requests": {
            "status": "in_progress",
            "total": 200,
            "completed": 73,
            "cursor": "page_3",
            "completed_ids": [1, 2, 3, ...]
        }
    }
}
"""

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

from lib.s3 import S3Store


@dataclass
class PhaseState:
    status: str = "pending"          # pending | in_progress | completed
    total: int | None = None
    completed: int = 0
    cursor: str | None = None        # API pagination cursor
    completed_ids: set = field(default_factory=set)  # for dedup on resume


class CheckpointManager:
    """
    Manages checkpoint state in S3.

    Usage:
        cp = CheckpointManager(s3_store, "github/owner__repo")
        cp.load()  # loads existing checkpoint or starts fresh
        if cp.is_phase_done("commits"):
            skip...
        cp.start_phase("pull_requests", total=200)
        for pr in prs:
            if cp.is_item_done("pull_requests", pr["number"]):
                continue
            # ... fetch pr ...
            cp.mark_item_done("pull_requests", pr["number"])
            cp.save()  # periodic save (caller decides frequency)
        cp.complete_phase("pull_requests")
        cp.save()
    """

    SAVE_INTERVAL = 30  # seconds — don't save more often than this

    def __init__(self, s3: S3Store, job_id: str):
        self._s3 = s3
        self.job_id = job_id
        self._s3_path = f"_checkpoints/{job_id}.json"
        self.phases: dict[str, PhaseState] = {}
        self.status = "pending"
        self.started_at: str | None = None
        self.updated_at: str | None = None
        self._last_save_time = 0.0

    def load(self) -> bool:
        """Load checkpoint from S3. Returns True if a checkpoint existed."""
        data = self._s3.download_json(self._s3_path)
        if data is None:
            self.status = "in_progress"
            self.started_at = datetime.now(timezone.utc).isoformat()
            return False

        self.status = data["status"]
        self.started_at = data["started_at"]
        self.updated_at = data.get("updated_at")
        for name, phase_data in data.get("phases", {}).items():
            self.phases[name] = PhaseState(
                status=phase_data["status"],
                total=phase_data.get("total"),
                completed=phase_data.get("completed", 0),
                cursor=phase_data.get("cursor"),
                completed_ids=set(phase_data.get("completed_ids", [])),
            )
        return True

    def save(self, force: bool = False) -> None:
        """Save checkpoint to S3. Throttled to SAVE_INTERVAL unless force=True."""
        now = time.monotonic()
        if not force and (now - self._last_save_time) < self.SAVE_INTERVAL:
            return
        self.updated_at = datetime.now(timezone.utc).isoformat()
        data = {
            "job_id": self.job_id,
            "status": self.status,
            "started_at": self.started_at,
            "updated_at": self.updated_at,
            "phases": {},
        }
        for name, phase in self.phases.items():
            data["phases"][name] = {
                "status": phase.status,
                "total": phase.total,
                "completed": phase.completed,
                "cursor": phase.cursor,
                "completed_ids": list(phase.completed_ids),
            }
        self._s3.upload_json(data, self._s3_path)
        self._last_save_time = now

    def start_phase(self, name: str, total: int | None = None) -> None:
        if name not in self.phases:
            self.phases[name] = PhaseState()
        self.phases[name].status = "in_progress"
        if total is not None:
            self.phases[name].total = total

    def complete_phase(self, name: str) -> None:
        self.phases[name].status = "completed"

    def is_phase_done(self, name: str) -> bool:
        return name in self.phases and self.phases[name].status == "completed"

    def is_item_done(self, phase: str, item_id) -> bool:
        return phase in self.phases and item_id in self.phases[phase].completed_ids

    def mark_item_done(self, phase: str, item_id) -> None:
        self.phases[phase].completed_ids.add(item_id)
        self.phases[phase].completed += 1

    def set_cursor(self, phase: str, cursor: str | None) -> None:
        self.phases[phase].cursor = cursor

    def get_cursor(self, phase: str) -> str | None:
        if phase in self.phases:
            return self.phases[phase].cursor
        return None

    def complete(self) -> None:
        self.status = "completed"
        self.save(force=True)
```

#### 4. `lib/rate_limit.py` — Thread-Safe Token Bucket

```python
"""Thread-safe token bucket rate limiter with adaptive throttling."""

import threading
import time


class TokenBucket:
    """
    Thread-safe token bucket.

    capacity:    max tokens (burst size)
    refill_rate: tokens added per second
    """

    def __init__(self, capacity: float, refill_rate: float):
        self.capacity = capacity
        self.refill_rate = refill_rate
        self._tokens = capacity
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    def _refill_unlocked(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self.capacity, self._tokens + elapsed * self.refill_rate)
        self._last_refill = now

    def acquire(self, tokens: float = 1.0) -> None:
        """Block until tokens are available, then consume them."""
        while True:
            with self._lock:
                self._refill_unlocked()
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return
                wait = (tokens - self._tokens) / self.refill_rate
            time.sleep(wait)

    def throttle(self, new_rate: float) -> None:
        """Dynamically reduce refill rate (adaptive throttling on 429)."""
        with self._lock:
            self.refill_rate = new_rate

    def restore(self, original_rate: float) -> None:
        """Restore original refill rate after throttle period."""
        with self._lock:
            self.refill_rate = original_rate
```

#### 5. `lib/session.py` — Rate-Limited Requests Session

```python
"""Rate-limited requests session with retry, backoff, and header-based adaptation."""

import logging
import time
import threading
from datetime import datetime, timezone

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from lib.rate_limit import TokenBucket

log = logging.getLogger(__name__)


def parse_retry_after(value: str) -> float:
    """Parse Retry-After header (seconds or HTTP-date)."""
    try:
        return float(value)
    except ValueError:
        dt = datetime.strptime(value, "%a, %d %b %Y %H:%M:%S %Z")
        dt = dt.replace(tzinfo=timezone.utc)
        return max(0.0, dt.timestamp() - time.time())


class RateLimitState:
    """Thread-safe tracking of server-reported rate limit state."""

    def __init__(self):
        self.remaining: int | None = None
        self.limit: int | None = None
        self.reset_at: float | None = None
        self._lock = threading.Lock()

    def update(self, headers: dict) -> None:
        with self._lock:
            if "X-RateLimit-Remaining" in headers:
                self.remaining = int(headers["X-RateLimit-Remaining"])
            if "X-RateLimit-Limit" in headers:
                self.limit = int(headers["X-RateLimit-Limit"])
            if "X-RateLimit-Reset" in headers:
                self.reset_at = float(headers["X-RateLimit-Reset"])

    def should_preemptive_wait(self, min_remaining: int = 50) -> float | None:
        """Returns seconds to wait if remaining quota is dangerously low."""
        with self._lock:
            if self.remaining is not None and self.remaining < min_remaining:
                if self.reset_at is not None:
                    wait = max(0.0, self.reset_at - time.time() + 1.0)
                    return wait
            return None


class RateLimitedAdapter(HTTPAdapter):
    """
    HTTPAdapter that:
    1. Pre-request: acquires a token from a shared TokenBucket
    2. Post-response: reads rate-limit headers
    3. On 429: respects Retry-After with exponential backoff
    """

    def __init__(self, bucket: TokenBucket, state: RateLimitState,
                 min_remaining: int = 50, max_retries_on_429: int = 5, **kwargs):
        super().__init__(**kwargs)
        self.bucket = bucket
        self.state = state
        self.min_remaining = min_remaining
        self.max_retries_on_429 = max_retries_on_429

    def send(self, request, **kwargs):
        # Preemptive throttle based on remaining quota
        wait = self.state.should_preemptive_wait(self.min_remaining)
        if wait:
            log.warning("Rate limit low, preemptive wait %.1fs", wait)
            time.sleep(wait)

        # Acquire token (blocks until available)
        self.bucket.acquire()

        # Send request with retry on 429
        for attempt in range(self.max_retries_on_429):
            response = super().send(request, **kwargs)
            self.state.update(response.headers)

            if response.status_code != 429:
                return response

            # 429 — parse Retry-After or use exponential backoff
            if "Retry-After" in response.headers:
                retry_wait = parse_retry_after(response.headers["Retry-After"])
            else:
                retry_wait = min(2 ** attempt + 0.5, 120)

            log.warning("429 rate limited, attempt %d/%d, waiting %.1fs",
                        attempt + 1, self.max_retries_on_429, retry_wait)
            time.sleep(retry_wait)
            self.bucket.acquire()

        return response  # return last 429 if all retries exhausted


def make_session(
    requests_per_second: float = 5.0,
    burst: float = 10.0,
    min_remaining: int = 50,
    connect_timeout: float = 10.0,
    read_timeout: float = 60.0,
    max_retries_on_error: int = 3,
) -> tuple[requests.Session, RateLimitState]:
    """
    Create a rate-limited requests.Session.

    Returns (session, state) — state can be inspected for remaining quota.

    The session:
    - Acquires tokens from a shared bucket before each request
    - Retries on 429 with Retry-After / exponential backoff
    - Retries on 500/502/503 with urllib3 retry
    - Preemptively waits when X-RateLimit-Remaining is low
    """
    bucket = TokenBucket(capacity=burst, refill_rate=requests_per_second)
    state = RateLimitState()

    # urllib3 retry for transient server errors (NOT 429 — handled by adapter)
    retry_strategy = Retry(
        total=max_retries_on_error,
        backoff_factor=1.0,
        status_forcelist=[500, 502, 503],
        allowed_methods=["GET", "POST", "PUT"],
    )

    adapter = RateLimitedAdapter(
        bucket=bucket,
        state=state,
        min_remaining=min_remaining,
        max_retries=retry_strategy,
    )

    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    # Default timeouts
    session.request = _with_default_timeout(session.request, connect_timeout, read_timeout)

    return session, state


def _with_default_timeout(request_fn, connect_timeout, read_timeout):
    """Wrap session.request to inject default timeout if not specified."""
    def wrapper(*args, **kwargs):
        if "timeout" not in kwargs:
            kwargs["timeout"] = (connect_timeout, read_timeout)
        return request_fn(*args, **kwargs)
    return wrapper
```

#### 6. `lib/retry.py` — Retry Decorator

```python
"""Retry decorator with exponential backoff for non-HTTP operations."""

import functools
import logging
import time

log = logging.getLogger(__name__)


def retry(
    max_attempts: int = 5,
    backoff_base: float = 2.0,
    max_backoff: float = 120.0,
    exceptions: tuple = (Exception,),
):
    """
    Retry decorator with exponential backoff.

    Use for non-HTTP operations (S3 uploads, file processing).
    HTTP retry is handled by RateLimitedSession.
    """
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(max_attempts):
                try:
                    return fn(*args, **kwargs)
                except exceptions as e:
                    last_exc = e
                    if attempt == max_attempts - 1:
                        break
                    wait = min(backoff_base ** attempt, max_backoff)
                    log.warning(
                        "%s failed (attempt %d/%d): %s. Retrying in %.1fs",
                        fn.__name__, attempt + 1, max_attempts, e, wait
                    )
                    time.sleep(wait)
            raise last_exc
        return wrapper
    return decorator
```

#### 7. `lib/logging.py` — Structured Logging

```python
"""Structured JSON logging setup."""

import json
import logging
import sys
from datetime import datetime, timezone


class JSONFormatter(logging.Formatter):
    def format(self, record):
        entry = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info and record.exc_info[0]:
            entry["exception"] = self.formatException(record.exc_info)
        # Merge extra fields (e.g., log.info("msg", extra={"phase": "commits"}))
        for key in ("phase", "item", "progress", "total", "source"):
            if hasattr(record, key):
                entry[key] = getattr(record, key)
        return json.dumps(entry)


def setup_logging(level: str = "INFO", json_output: bool = True) -> None:
    handler = logging.StreamHandler(sys.stderr)
    if json_output:
        handler.setFormatter(JSONFormatter())
    else:
        handler.setFormatter(logging.Formatter(
            "%(asctime)s %(levelname)-8s %(name)s — %(message)s"
        ))
    logging.root.handlers = [handler]
    logging.root.setLevel(getattr(logging, level.upper()))
```

#### 8. `lib/types.py` — Shared Types

```python
"""Shared types and configuration dataclasses."""

from dataclasses import dataclass


@dataclass
class ExportConfig:
    """Common configuration for all exporters."""
    s3_bucket: str
    s3_prefix: str = ""
    max_workers: int = 5          # ThreadPoolExecutor workers
    log_level: str = "INFO"
    json_logs: bool = True
```

### Success Criteria

#### Automated Verification
- [x] `uv run pytest tests/test_s3.py` — S3Store upload/download with moto
- [x] `uv run pytest tests/test_checkpoint.py` — checkpoint save/load/resume
- [x] `uv run pytest tests/test_rate_limit.py` — token bucket under concurrent access
- [x] `uv run pytest tests/test_session.py` — 429 handling, retry-after parsing
- [x] `uv run pytest tests/test_retry.py` — retry decorator with mocked failures

#### Manual Verification
- [ ] Import all lib modules without errors: `uv run python -c "from lib import s3, checkpoint, rate_limit, session, retry"`

**Implementation Note**: After completing this phase and all automated verification passes, pause for manual confirmation before proceeding to the next phase.

---

## Phase 2: GitHub Exporter

### Overview

Rewrite `github_export.py` using the shared library. This is the simplest exporter (single API, no file downloads other than patch diffs) — good proving ground for the lib.

### Changes Required

#### 1. `exporters/github.py`

**Architecture:**

```
GitHubExporter
├── __init__(token, repo, s3_store, config)
├── run()                          # orchestrator
│   ├── _export_metadata()         # repo info + languages
│   ├── _export_contributors()     # contributor list
│   ├── _export_commits()          # paginated + parallel detail fetch
│   └── _export_pull_requests()    # paginated + parallel detail fetch
├── _fetch_commit_detail(sha)      # called in ThreadPoolExecutor
├── _fetch_pr_detail(number)       # called in ThreadPoolExecutor
└── _build_csv(prs)                # generate CSV from PR data
```

**Key patterns:**

```python
class GitHubExporter:
    def __init__(self, token: str, repo: str, s3: S3Store, config: ExportConfig,
                 pr_limit: int = 500, commit_limit: int = 1000,
                 pr_state: str = "all",
                 skip_commits: bool = False, skip_prs: bool = False):
        self.repo = repo
        self.s3 = s3
        self.config = config
        self.session, self.rate_state = make_session(
            requests_per_second=10,   # GitHub: 5000/hr ≈ 1.4/s, but bursty
            burst=20,
            min_remaining=50,
        )
        self.session.headers["Authorization"] = f"Bearer {token}"
        self.session.headers["Accept"] = "application/vnd.github+json"
        self.checkpoint = CheckpointManager(s3, f"github/{repo.replace('/', '__')}")
        # ... limits, flags ...

    def run(self):
        self.checkpoint.load()
        repo_slug = self.repo.replace("/", "__")

        if not self.checkpoint.is_phase_done("metadata"):
            self._export_metadata(repo_slug)

        if not self.skip_commits and not self.checkpoint.is_phase_done("commits"):
            self._export_commits(repo_slug)

        if not self.skip_prs and not self.checkpoint.is_phase_done("pull_requests"):
            self._export_pull_requests(repo_slug)

        self.checkpoint.complete()

    def _export_commits(self, repo_slug: str):
        self.checkpoint.start_phase("commits", total=self.commit_limit)

        # 1. Paginate commit list
        shas = self._list_commits()  # returns list of SHAs

        # 2. Parallel detail fetch with ThreadPoolExecutor
        commits = []
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as pool:
            futures = {}
            for sha in shas:
                if self.checkpoint.is_item_done("commits", sha):
                    continue
                futures[pool.submit(self._fetch_commit_detail, sha)] = sha

            for future in as_completed(futures):
                sha = futures[future]
                try:
                    commit = future.result()
                    commits.append(commit)
                    self.checkpoint.mark_item_done("commits", sha)
                    self.checkpoint.save()  # throttled internally
                except Exception:
                    log.error("Failed to fetch commit %s", sha, exc_info=True)

        # 3. Upload results
        self.s3.upload_json(commits, f"github/{repo_slug}/commits.json")
        self.checkpoint.complete_phase("commits")
        self.checkpoint.save(force=True)
```

**CLI entry point (bottom of file):**

```python
def main():
    import argparse
    parser = argparse.ArgumentParser(description="Export GitHub repository data to S3")
    parser.add_argument("--token", required=True, help="GitHub personal access token")
    parser.add_argument("--repo", required=True, help="Repository (owner/repo)")
    parser.add_argument("--s3-bucket", required=True)
    parser.add_argument("--s3-prefix", default="")
    parser.add_argument("--pr-limit", type=int, default=500)
    parser.add_argument("--pr-state", default="all", choices=["open", "closed", "all"])
    parser.add_argument("--commit-limit", type=int, default=1000)
    parser.add_argument("--skip-commits", action="store_true")
    parser.add_argument("--skip-prs", action="store_true")
    parser.add_argument("--max-workers", type=int, default=5)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    setup_logging(level=args.log_level)
    s3 = S3Store(bucket=args.s3_bucket, prefix=args.s3_prefix)
    config = ExportConfig(s3_bucket=args.s3_bucket, s3_prefix=args.s3_prefix,
                          max_workers=args.max_workers, log_level=args.log_level)
    exporter = GitHubExporter(
        token=args.token, repo=args.repo, s3=s3, config=config,
        pr_limit=args.pr_limit, commit_limit=args.commit_limit,
        pr_state=args.pr_state, skip_commits=args.skip_commits,
        skip_prs=args.skip_prs,
    )
    exporter.run()

if __name__ == "__main__":
    main()
```

### Success Criteria

#### Automated Verification
- [x] `uv run pytest tests/test_github.py` — mocked API responses, verify S3 output structure (8 tests)
- [x] Checkpoint resume: mock a mid-export failure, verify restart skips completed items

#### Manual Verification
- [ ] `uv run python -m exporters.github --token $GH_TOKEN --repo owner/repo --s3-bucket test-bucket`
- [ ] Verify S3 output matches prototype structure (`repo_metadata.json`, `commits.json`, `pull_requests.json`, `pull_requests.csv`, `contributors.json`)
- [ ] Kill mid-export, restart — verify it resumes without re-fetching

**Implementation Note**: After completing this phase and all automated verification passes, pause for manual confirmation before proceeding to the next phase.

---

## Phase 3: Jira Exporter

### Overview

Rewrite `jira_export.py`. Adds attachment download parallelism and S3 streaming for attachments.

### Changes Required

#### 1. `exporters/jira.py`

**Architecture:**

```
JiraExporter
├── __init__(token, email, domain, projects, s3_store, config)
├── run()                                  # iterates over projects
│   └── _export_project(project_key)
│       ├── _search_tickets()              # cursor-based pagination via POST /search/jql
│       ├── _fetch_comments(ticket_key)    # per-ticket comment fetch
│       ├── _download_attachments(ticket)  # parallel attachment download → S3
│       ├── _resolve_custom_fields()       # GET /field → name mapping
│       └── _build_csv(tickets)            # flat CSV generation
├── _extract_text_from_adf(adf)            # recursive ADF → plain text
└── _stream_attachment_to_s3(url, path)    # stream download → S3 upload
```

**Key differences from prototype:**
- Attachment downloads are parallelized via `ThreadPoolExecutor`
- Attachments stream directly to S3 (via temp file, not held in memory)
- Uses `requests` streaming (`stream=True`, `iter_content`) → temp file → S3 upload
- Checkpoint tracks: tickets fetched (by key), comments fetched, attachments downloaded
- Custom field resolution cached per export (one API call)

**Attachment streaming pattern:**

```python
import tempfile

def _stream_attachment_to_s3(self, url: str, s3_path: str, filename: str) -> None:
    """Download attachment via streaming and upload to S3."""
    with tempfile.NamedTemporaryFile(suffix=f"_{filename}", delete=True) as tmp:
        resp = self.session.get(url, stream=True, timeout=(10, 300))
        resp.raise_for_status()
        for chunk in resp.iter_content(chunk_size=8192):
            tmp.write(chunk)
        tmp.flush()
        self.s3.upload_file(tmp.name, s3_path)
```

### Success Criteria

#### Automated Verification
- [x] `uv run pytest tests/test_jira.py` — mocked API, verify ticket/comment/attachment structure (14 tests)
- [x] ADF → plain text extraction produces correct output for nested structures (6 tests)

#### Manual Verification
- [ ] Export a real Jira project, verify S3 output matches prototype
- [ ] Verify attachments are present in S3 at correct paths
- [ ] Kill and restart — verify ticket-level resume

**Implementation Note**: After completing this phase, pause for manual confirmation before proceeding.

---

## Phase 4: Slack Exporter

### Overview

Rewrite `slack_channel_export.py`. Replaces `SIGALRM` timeout with proper `requests` timeout. Adds thread reply parallelism.

### Changes Required

#### 1. `exporters/slack.py`

**Architecture:**

```
SlackExporter
├── __init__(token, channel_ids, s3_store, config)
├── run()                                     # iterates over channels
│   └── _export_channel(channel_id)
│       ├── _fetch_channel_info()
│       ├── _fetch_messages()                 # paginated history
│       ├── _fetch_thread_replies(messages)   # parallel thread fetch
│       ├── _download_attachments(messages)   # parallel file download → S3
│       └── _upload_results()
├── _download_one_file(file_obj, channel_id)  # single file → S3
└── _is_skippable_file(file_obj)              # skip videos, apk, etc.
```

**Key differences from prototype:**
- No `SIGALRM` — uses `requests` timeout `(10, 60)` for all downloads
- Thread replies fetched in parallel via `ThreadPoolExecutor`
- File downloads parallelized (shared rate limiter prevents Slack 429)
- Slack-specific rate limiting: tier-aware (conversations.history is Tier 3 = 50+/min)
- HTML auth page detection: check `Content-Type` header instead of reading file content
- Input: still reads `channels.csv` for channel IDs, or accepts `--channel-ids` CLI arg

**Rate limiter config for Slack:**

```python
# Slack Tier 3 methods (conversations.history, conversations.replies): ~50/min
# Slack Tier 4 methods (conversations.info): ~100/min
# File downloads: separate rate, be conservative
self.session, _ = make_session(
    requests_per_second=0.8,   # ~48/min, under Tier 3 limit
    burst=3,
)
```

### Success Criteria

#### Automated Verification
- [x] `uv run pytest tests/test_slack.py` — mocked API, verify message/thread/attachment structure (18 tests)
- [x] No `signal.SIGALRM` anywhere in codebase

#### Manual Verification
- [ ] Export a real Slack channel, verify S3 output
- [ ] Verify thread replies are correctly inlined with `_is_thread_reply` / `_parent_ts`
- [ ] Verify skipped file types (video, apk) are not downloaded

**Implementation Note**: After completing this phase, pause for manual confirmation before proceeding.

---

## Phase 5: Google Workspace Exporter

### Overview

Rewrite `main.py`. Most complex exporter — three sub-services (Gmail, Calendar, Drive) with different auth and different Google API client libraries.

### Changes Required

#### 1. `exporters/google_workspace.py`

**Architecture:**

```
GoogleWorkspaceExporter
├── __init__(user, service_account_key, s3_store, config)
├── run()
│   ├── _export_gmail()
│   │   ├── _list_message_ids()           # paginated
│   │   ├── _batch_fetch_messages()       # batch API (10 per batch)
│   │   ├── _save_eml_to_s3(msg)          # raw → .eml → S3
│   │   └── _extract_attachments(msg)     # MIME parse → S3
│   ├── _export_calendar()
│   │   └── _fetch_events()               # paginated, last 2 years
│   └── _export_drive()
│       ├── _list_files()                 # paginated, owned by user
│       ├── _export_google_doc(file)      # Docs/Sheets/Slides → converted format → S3
│       └── _download_file(file)          # regular files → stream → S3
```

**Key differences from prototype:**
- Gmail batch fetch + parallel S3 upload of .eml files (ThreadPoolExecutor for S3 uploads)
- Drive file downloads: stream to temp file → S3 (same pattern as Jira attachments)
- Google Docs export (Docs→DOCX, Sheets→XLSX, etc.) via `files().export_media()` → temp file → S3
- Rate limiting via Google's built-in exponential backoff + our retry decorator
- Service account credentials built via `google.oauth2.service_account.Credentials`
- Checkpoint phases: `gmail`, `calendar`, `drive`

**Gmail batch pattern:**

```python
def _export_gmail(self):
    self.checkpoint.start_phase("gmail")
    message_ids = self._list_message_ids()

    # Process in batches of 10 (Google batch API limit for Gmail)
    for batch_start in range(0, len(message_ids), 10):
        batch_ids = message_ids[batch_start:batch_start + 10]
        batch_ids = [mid for mid in batch_ids
                     if not self.checkpoint.is_item_done("gmail", mid)]
        if not batch_ids:
            continue

        raw_messages = self._batch_fetch_raw(batch_ids)

        # Parallel upload of .eml files to S3
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as pool:
            futures = []
            for msg_id, raw_data in raw_messages.items():
                futures.append(pool.submit(self._save_eml_to_s3, msg_id, raw_data))
                futures.append(pool.submit(self._extract_and_upload_attachments, msg_id, raw_data))
            for f in as_completed(futures):
                f.result()  # raise on error

        for msg_id in raw_messages:
            self.checkpoint.mark_item_done("gmail", msg_id)
        self.checkpoint.save()

        time.sleep(2)  # 2s between batches (prototype behavior)
```

### Success Criteria

#### Automated Verification
- [x] `uv run pytest tests/test_google_workspace.py` — mocked Google API client (7 tests)
- [x] Gmail: .eml files + attachments uploaded to correct S3 paths
- [x] Drive: Google Docs exported as DOCX/XLSX/PPTX, regular files as-is; images/videos skipped
- [x] Calendar: events.json + _summary.json in S3

#### Manual Verification
- [ ] Export a real user's Gmail/Calendar/Drive, verify S3 output
- [ ] Verify resume after kill (Gmail is the most important — many small files)

**Implementation Note**: After completing this phase, pause for manual confirmation before proceeding.

---

## Phase 6: Integration Testing and Hardening

### Overview

End-to-end testing, edge case handling, and documentation.

### Changes Required

#### 1. Integration test fixtures

- Create `tests/fixtures/` with sample API responses for all 4 services
- Full round-trip tests: mock API → exporter → verify S3 (via moto)
- Checkpoint resume tests: simulate failure at every phase boundary

#### 2. Edge case handling

- Empty exports (no commits, no messages, etc.) — should produce empty JSON arrays, not errors
- Invalid/expired tokens — clear error message, checkpoint preserved
- S3 bucket doesn't exist or no permissions — fail fast with clear message
- Very large attachments (>5GB) — verify multipart upload works
- Unicode filenames in Drive/Jira attachments — sanitize for S3 keys

#### 3. README.md

- Installation: `uv sync`
- Usage for each exporter (CLI examples)
- Environment variables: `AWS_*` for S3 credentials
- S3 output structure diagram
- Checkpoint/resume behavior

### Success Criteria

#### Automated Verification
- [x] `uv run pytest` — all 106 tests pass
- [x] `uv run pytest --tb=short -q` — clean output

#### Manual Verification
- [ ] Run all 4 exporters against real APIs end-to-end
- [ ] Verify all S3 output matches prototype local output structure

---

## Testing Strategy

### Unit Tests (per module)
- `test_s3.py`: S3Store with moto — upload, download, exists, upload_json, download_json
- `test_checkpoint.py`: CheckpointManager save/load/resume with moto S3
- `test_rate_limit.py`: TokenBucket thread safety — concurrent acquire from 10 threads
- `test_session.py`: RateLimitedAdapter 429 handling, Retry-After parsing, preemptive wait
- `test_retry.py`: retry decorator with configurable failures

### Exporter Tests (per exporter)
- Mock API responses with `responses` or `requests_mock`
- Verify S3 output structure (keys, JSON schema) via moto
- Verify checkpoint save/resume — simulate failure, restart, verify no duplicate fetches

### Integration Tests
- Full round-trip with mocked APIs + moto S3
- Checkpoint resume at every phase boundary

### Manual Testing
1. Export a small GitHub repo (~50 commits, ~20 PRs)
2. Export Gmail for a test user (~100 emails)
3. Export a Jira project (~50 tickets)
4. Export a Slack channel (~200 messages)
5. For each: kill at 50% progress, restart, verify completeness

## Performance Considerations

- **ThreadPoolExecutor workers**: Default 5. Increase for I/O-heavy exports (attachments), decrease if hitting API rate limits. CLI-configurable per exporter.
- **S3 upload parallelism**: For thousands of small files (Gmail .eml), bump to 20+ workers.
- **Checkpoint save frequency**: Throttled to every 30s to avoid S3 PUT overhead. Force-save on phase completion.
- **Memory**: Large JSON files (commits.json, tickets.json) are built in memory then serialized. For exports >100k items, consider streaming JSON (newline-delimited) — but the prototype's scale (hundreds to low thousands) fits comfortably in memory.
- **boto3 CRT**: Install `boto3[crt]` for 2-6x S3 throughput. Automatic when available.

## References
- Original requirements: `requirements.md`
- Research findings: `specs/research/production-exporter/findings.md`
