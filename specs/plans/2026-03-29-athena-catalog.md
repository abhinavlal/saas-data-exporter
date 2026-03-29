# Athena Catalog Implementation Plan

## Overview

Add an inventory/catalog system that tracks what data has been exported to S3 — counts, breakdowns by type/language/format, and volume metrics — queryable via AWS Athena. Each exporter emits a `_stats.json` file during export (crash-resistant via periodic saves). A standalone catalog generator aggregates these into Athena-queryable JSON Lines tables.

## Current State Analysis

- Four exporters write per-item files to S3 (`commits/{sha}.json`, `tickets/{key}.json`, `messages/{ts}.json`, `calendar/events/{event_id}.json`)
- Lightweight `_index.json` files exist but contain only keys/IDs (except Gmail and Drive which include some metadata)
- `CheckpointManager` (`lib/checkpoint.py:60-117`) provides a proven throttled-save pattern: save every 30s via `time.monotonic()`, `force=True` bypasses throttle
- No catalog, stats, or Athena integration exists today
- Data is in memory at write time — stat accumulation adds zero extra I/O

## Desired End State

1. Each exporter writes `{s3_base}/_stats.json` with aggregate statistics, persisted periodically during export
2. `exporters/catalog.py` reads all `_stats.json` files and produces JSON Lines tables under `catalog/` in S3
3. Athena `CREATE EXTERNAL TABLE` statements can query the catalog tables
4. Stats survive crashes — on restart, loaded from S3 alongside checkpoint

**Verification:** Run any exporter → `_stats.json` appears with correct counts. Run catalog generator → `catalog/*.jsonl` files appear. Create Athena tables → queries return expected inventory.

## What We're NOT Doing

- Making raw exported data (commits, tickets, messages) directly Athena-queryable
- Converting existing JSON to Parquet or NDJSON
- Creating AWS Glue Data Catalog entries programmatically (manual `CREATE TABLE` for now)
- Adding stats to the checkpoint file itself (separate concern, separate file)
- Real-time or streaming stats updates

## Implementation Approach

Approach C (Hybrid): Each exporter accumulates stats in a `StatsCollector` and periodically flushes to S3. A standalone catalog generator reads the lightweight `_stats.json` files (not the raw data) and writes Athena tables.

---

## Phase 1: StatsCollector Infrastructure

### Overview

Create `lib/stats.py` with a `StatsCollector` class that follows the `CheckpointManager` throttled-save pattern. This is the shared infrastructure all exporters will use.

### Changes Required

#### 1. New file: `lib/stats.py`

**File**: `lib/stats.py`

```python
"""Crash-resistant statistics collector with periodic S3 persistence."""

import logging
import time
from datetime import datetime, timezone

log = logging.getLogger(__name__)

SAVE_INTERVAL = 30  # seconds — match checkpoint cadence


class StatsCollector:
    """Accumulate export statistics with throttled S3 persistence.

    Usage:
        stats = StatsCollector(s3, f"{s3_base}/_stats.json")
        stats.load()                              # restore after crash
        stats.set("exporter", "github")
        stats.increment("commits.total")
        stats.add_to_map("languages", "Python", 150000)
        stats.save()                              # throttled
        stats.save(force=True)                    # immediate (end of phase)
    """

    def __init__(self, s3, s3_path: str, save_interval: int = SAVE_INTERVAL):
        self._s3 = s3
        self._s3_path = s3_path
        self._save_interval = save_interval
        self._last_save = 0.0
        self.data: dict = {}

    def load(self) -> None:
        """Load existing stats from S3 (crash recovery)."""
        existing = self._s3.download_json(self._s3_path)
        if existing and isinstance(existing, dict):
            self.data = existing
            log.info("Loaded existing stats from %s", self._s3_path)

    def save(self, force: bool = False) -> None:
        """Save stats to S3. Throttled to once per save_interval unless force=True."""
        now = time.monotonic()
        if not force and (now - self._last_save) < self._save_interval:
            return
        self.data["updated_at"] = datetime.now(timezone.utc).isoformat()
        self._s3.upload_json(self.data, self._s3_path)
        self._last_save = now

    # -- Mutators ----------------------------------------------------------

    def set(self, key: str, value) -> None:
        """Set a top-level key."""
        self.data[key] = value

    def increment(self, path: str, by: int = 1) -> None:
        """Increment a nested counter. Path uses dots: 'commits.total'."""
        keys = path.split(".")
        d = self.data
        for k in keys[:-1]:
            d = d.setdefault(k, {})
        d[keys[-1]] = d.get(keys[-1], 0) + by

    def add_to_map(self, path: str, key: str, value: int = 1) -> None:
        """Increment a key within a nested map. E.g. add_to_map('pr_states', 'open', 1)."""
        parts = path.split(".")
        d = self.data
        for p in parts:
            d = d.setdefault(p, {})
        d[key] = d.get(key, 0) + value

    def set_nested(self, path: str, value) -> None:
        """Set a value at a dotted path. E.g. set_nested('repo.stars', 1234)."""
        keys = path.split(".")
        d = self.data
        for k in keys[:-1]:
            d = d.setdefault(k, {})
        d[keys[-1]] = value

    def get(self, path: str, default=None):
        """Get a value at a dotted path."""
        keys = path.split(".")
        d = self.data
        for k in keys:
            if not isinstance(d, dict):
                return default
            d = d.get(k)
            if d is None:
                return default
        return d
```

### Success Criteria

#### Automated Verification:
- [ ] `uv run pytest tests/test_stats.py -v` — unit tests for StatsCollector (load, save, increment, add_to_map, throttle, crash recovery)

#### Manual Verification:
- [ ] Inspect `lib/stats.py` — follows same patterns as `lib/checkpoint.py`

**Implementation Note**: After completing this phase and all automated verification passes, pause for manual confirmation before proceeding to the next phase.

---

## Phase 2: Integrate into GitHub Exporter

### Overview

Add StatsCollector to GitHub exporter. Accumulate stats during each phase (metadata, contributors, commits, PRs). Save alongside checkpoint saves.

### Changes Required

#### 1. `exporters/github.py`

**Changes**: Create StatsCollector in `__init__`, accumulate stats in each phase method, save periodically.

In `__init__` (after s3/checkpoint setup):
```python
self.stats = StatsCollector(self.s3, f"{self.s3_base}/_stats.json")
```

In `run()` (before phases):
```python
self.stats.load()
self.stats.set("exporter", "github")
self.stats.set("target", self.repo)
self.stats.set("target_slug", self.repo_slug)
```

In `_export_metadata()` (after building metadata dict):
```python
self.stats.set("repo", {
    "full_name": metadata.get("full_name"),
    "private": metadata.get("private"),
    "default_branch": metadata.get("default_branch"),
    "stars": metadata.get("stargazers_count", 0),
    "forks": metadata.get("forks_count", 0),
    "open_issues": metadata.get("open_issues_count", 0),
    "watchers": metadata.get("watchers_count", 0),
})
self.stats.set("languages", metadata.get("language_breakdown", {}))
self.stats.save(force=True)
```

In `_export_contributors()` (after building list):
```python
self.stats.set_nested("contributors.total", len(contributors))
self.stats.save(force=True)
```

In `_export_commits()` — inside the commit write loop:
```python
self.stats.increment("commits.total")
# track unique authors
authors = self.stats.get("commits.unique_authors_set", [])
author = c.get("author_login") or c.get("author_email")
if author and author not in authors:
    authors.append(author)
    self.stats.set_nested("commits.unique_authors_set", authors)
self.stats.save()  # throttled — piggybacks on checkpoint cadence
```
At phase end:
```python
authors = self.stats.get("commits.unique_authors_set", [])
self.stats.set_nested("commits.unique_authors", len(authors))
self.stats.save(force=True)
```

In `_export_pull_requests()` — inside the PR fetch loop, after writing each PR:
```python
self.stats.increment("pull_requests.total")
state = pr.get("state", "unknown")
merged = pr.get("merged_at") is not None
if merged:
    self.stats.increment("pull_requests.merged")
elif state == "open":
    self.stats.increment("pull_requests.open")
elif state == "closed":
    self.stats.increment("pull_requests.closed")
self.stats.increment("pull_requests.total_reviews", len(pr.get("reviews", [])))
self.stats.increment("pull_requests.total_review_comments", len(pr.get("review_comments", [])))
self.stats.increment("pull_requests.total_comments", len(pr.get("comments", [])))
self.stats.increment("pull_requests.total_additions", pr.get("additions") or 0)
self.stats.increment("pull_requests.total_deletions", pr.get("deletions") or 0)
self.stats.increment("pull_requests.total_changed_files", pr.get("changed_files") or 0)
for label in pr.get("labels", []):
    self.stats.add_to_map("pull_requests.labels", label)
self.stats.save()  # throttled
```
At phase end:
```python
self.stats.set_nested("pull_requests.unique_authors", len(set(
    r.get("author", "") for r in csv_rows if r.get("author")
)))
self.stats.save(force=True)
```

In `run()` (at the very end, after all phases):
```python
self.stats.set("exported_at", datetime.now(timezone.utc).isoformat())
self.stats.save(force=True)
```

### Success Criteria

#### Automated Verification:
- [ ] `uv run pytest tests/test_github.py -v` — existing tests still pass
- [ ] New test: full export produces `_stats.json` with expected counts

#### Manual Verification:
- [ ] Run exporter against a test repo, inspect `_stats.json` contents

---

## Phase 3: Integrate into Jira Exporter

### Overview

Add StatsCollector to Jira exporter. Accumulate ticket breakdowns during the search loop, comment counts during comment fetch, attachment stats during download.

### Changes Required

#### 1. `exporters/jira.py`

In `_export_project()` (after checkpoint.load):
```python
stats = StatsCollector(self.s3, f"{s3_base}/_stats.json")
stats.load()
stats.set("exporter", "jira")
stats.set("target", project_key)
```

In `_search_tickets()` — pass stats as parameter. Inside the ticket write loop:
```python
stats.increment("tickets.total")
stats.add_to_map("tickets.by_type", ticket.get("issue_type", "Unknown"))
stats.add_to_map("tickets.by_status", ticket.get("status", "Unknown"))
stats.add_to_map("tickets.by_status_category", ticket.get("status_category", "Unknown"))
stats.add_to_map("tickets.by_priority", ticket.get("priority", "Unknown"))
for label in ticket.get("labels", []):
    stats.add_to_map("tickets.labels", label)
for comp in ticket.get("components", []):
    stats.add_to_map("tickets.components", comp)
# attachment stats from ticket parse (pre-download)
for att in ticket.get("attachments", []):
    stats.increment("attachments.total")
    mime = att.get("mime_type", "unknown")
    stats.add_to_map("attachments.by_mime_type", mime)
    stats.increment("attachments.total_size_bytes", att.get("size") or 0)
stats.save()  # throttled
```

In comment fetch loop:
```python
stats.increment("comments.total", len(ticket["comments"]))
if ticket["comments"]:
    stats.increment("comments.tickets_with_comments")
stats.save()  # throttled
```

At end of `_export_project`:
```python
stats.set("exported_at", datetime.now(timezone.utc).isoformat())
stats.save(force=True)
```

### Success Criteria

#### Automated Verification:
- [ ] `uv run pytest tests/test_jira.py -v` — existing tests still pass
- [ ] New test: full export produces `_stats.json` with expected counts

---

## Phase 4: Integrate into Slack Exporter

### Overview

Add StatsCollector to Slack exporter. Accumulate message stats during fetch, attachment stats during download.

### Changes Required

#### 1. `exporters/slack.py`

In `_export_channel()` (after checkpoint.load):
```python
stats = StatsCollector(self.s3, f"{s3_base}/_stats.json")
stats.load()
stats.set("exporter", "slack")
stats.set("target", channel_id)
```

In `_fetch_channel_info()` — pass stats. After uploading channel_info:
```python
channel = data.get("channel", {})
stats.set("channel", {
    "name": channel.get("name"),
    "is_private": channel.get("is_private"),
    "num_members": channel.get("num_members"),
})
stats.save(force=True)
```

In `_fetch_messages()` — pass stats. Inside message write loop:
```python
stats.increment("messages.total")
subtype = msg.get("subtype", "user_message")
stats.add_to_map("messages.by_subtype", subtype)
if msg.get("reactions"):
    stats.increment("messages.with_reactions")
    for r in msg.get("reactions", []):
        stats.increment("messages.total_reactions", r.get("count", 0))
# Track files for later (before attachment download)
for f in msg.get("files", []):
    stats.increment("files.total")
    name = f.get("name", "")
    ext = ("." + name.rsplit(".", 1)[-1]).lower() if "." in name else ".unknown"
    stats.add_to_map("files.by_extension", ext)
stats.save()  # throttled
```

In `_fetch_thread_replies()` — pass stats. Inside thread loop:
```python
stats.increment("messages.thread_parents")
stats.increment("messages.total_thread_replies", len(replies))
stats.save()  # throttled
```

In `_download_attachments_from_index()` — pass stats. On successful download:
```python
stats.increment("files.downloaded")
stats.save()  # throttled
```

At end of `_export_channel`:
```python
stats.set("exported_at", datetime.now(timezone.utc).isoformat())
stats.save(force=True)
```

### Success Criteria

#### Automated Verification:
- [ ] `uv run pytest tests/test_slack.py -v` — existing tests still pass
- [ ] New test: full export produces `_stats.json` with expected counts

---

## Phase 5: Integrate into Google Workspace Exporter

### Overview

Add StatsCollector to Google Workspace exporter. Gmail and Drive stats can be derived from index entries already in memory. Calendar stats accumulated during event loop.

### Changes Required

#### 1. `exporters/google_workspace.py`

In `__init__`:
```python
self.stats = StatsCollector(self.s3, f"{self.s3_base}/_stats.json")
```

In `run()`:
```python
self.stats.load()
self.stats.set("exporter", "google_workspace")
self.stats.set("target", self.user)
self.stats.set("target_slug", self.user_slug)
```

In `_export_gmail()` — after building each index entry (inside batch loop):
```python
self.stats.increment("gmail.total_messages")
self.stats.increment("gmail.total_size_bytes", entry.get("sizeEstimate") or 0)
for label in entry.get("labelIds", []):
    self.stats.add_to_map("gmail.labels", label)
attachments = entry.get("attachments", [])
if attachments:
    self.stats.increment("gmail.messages_with_attachments")
for fname in attachments:
    self.stats.increment("gmail.total_attachments")
    ext = ("." + fname.rsplit(".", 1)[-1]).lower() if "." in fname else ".unknown"
    self.stats.add_to_map("gmail.attachments_by_extension", ext)
self.stats.save()  # throttled
```

In `_export_calendar()` — inside event loop:
```python
self.stats.increment("calendar.total_events")
if event.get("attendees"):
    self.stats.increment("calendar.with_attendees")
    self.stats.increment("calendar.total_attendees", len(event["attendees"]))
if event.get("location"):
    self.stats.increment("calendar.with_location")
status = event.get("status", "unknown")
self.stats.add_to_map("calendar.by_status", status)
self.stats.save()  # throttled
```

In `_export_drive()` — after building each index entry:
```python
self.stats.increment("drive.total_files")
if entry.get("downloaded"):
    self.stats.increment("drive.downloaded")
else:
    self.stats.increment("drive.skipped")
mime = entry.get("mimeType", "unknown")
self.stats.add_to_map("drive.by_mime_type", mime)
self.stats.increment("drive.total_size_bytes", int(entry.get("size") or 0))
self.stats.save()  # throttled
```

At end of `run()`:
```python
self.stats.set("exported_at", datetime.now(timezone.utc).isoformat())
self.stats.save(force=True)
```

### Success Criteria

#### Automated Verification:
- [ ] `uv run pytest tests/test_google_workspace.py -v` — existing tests still pass
- [ ] New test: full export produces `_stats.json` with expected counts

---

## Phase 6: Catalog Generator

### Overview

New CLI module `exporters/catalog.py` that reads all `_stats.json` files from S3 and produces Athena-queryable JSON Lines tables under `catalog/`.

### Changes Required

#### 1. New file: `exporters/catalog.py`

**File**: `exporters/catalog.py`

The catalog generator:
1. Lists S3 keys under `{prefix}/` matching `*/_stats.json`
2. Downloads each `_stats.json`
3. Produces these JSON Lines files:

**`catalog/github_repos.jsonl`** — one line per repo:
```json
{"target": "owner/repo", "target_slug": "owner__repo", "exported_at": "...", "private": false, "stars": 1234, "forks": 56, "open_issues": 78, "total_contributors": 25, "total_commits": 500, "commit_unique_authors": 15, "total_prs": 200, "prs_open": 10, "prs_closed": 40, "prs_merged": 150, "total_reviews": 450, "total_pr_comments": 300, "total_additions": 50000, "total_deletions": 30000}
```

**`catalog/github_languages.jsonl`** — one line per language per repo:
```json
{"target": "owner/repo", "language": "Python", "bytes": 150000, "percentage": 60.0}
```

**`catalog/jira_projects.jsonl`** — one line per project:
```json
{"target": "PROJECT", "exported_at": "...", "total_tickets": 500, "total_comments": 1500, "tickets_with_comments": 300, "total_attachments": 200, "total_attachment_size_bytes": 500000000, "by_type": {"Bug": 100, "Story": 200}, "by_status_category": {"Done": 350, "In Progress": 100, "To Do": 50}, "by_priority": {"High": 50, "Medium": 200}}
```

**`catalog/slack_channels.jsonl`** — one line per channel:
```json
{"target": "C01234ABCD", "channel_name": "general", "is_private": false, "num_members": 50, "exported_at": "...", "total_messages": 10000, "thread_parents": 500, "total_thread_replies": 2000, "with_reactions": 800, "total_reactions": 3500, "total_files": 300, "files_downloaded": 280}
```

**`catalog/google_users.jsonl`** — one line per user:
```json
{"target": "user@domain.com", "exported_at": "...", "gmail_messages": 5000, "gmail_size_bytes": 2000000000, "gmail_attachments": 1200, "gmail_messages_with_attachments": 400, "calendar_events": 800, "calendar_with_attendees": 600, "drive_files": 300, "drive_downloaded": 280, "drive_skipped": 20, "drive_size_bytes": 1000000000}
```

**`catalog/file_types.jsonl`** — cross-exporter file type breakdown:
```json
{"exporter": "google_workspace", "target": "user@domain.com", "category": "email_attachment", "file_type": ".pdf", "count": 340}
{"exporter": "slack", "target": "C01234ABCD", "category": "channel_file", "file_type": ".png", "count": 100}
{"exporter": "jira", "target": "PROJECT", "category": "ticket_attachment", "file_type": "image/png", "count": 80}
```

**`catalog/summary.json`** — single aggregate overview:
```json
{
  "generated_at": "2026-03-29T...",
  "github": {"repos": 5, "total_commits": 25000, "total_prs": 1200},
  "jira": {"projects": 3, "total_tickets": 5000, "total_attachments": 800},
  "slack": {"channels": 20, "total_messages": 500000, "total_files": 5000},
  "google": {"users": 100, "total_emails": 150000, "total_attachments": 70000, "total_drive_files": 3000, "total_calendar_events": 8000}
}
```

CLI interface:
```bash
uv run python -m exporters.catalog           # scans all _stats.json, writes catalog/
uv run python -m exporters.catalog --exporter github  # only github
uv run python -m exporters.catalog --dry-run  # print summary without writing
```

#### 2. New file: `catalog/athena_tables.sql`

DDL statements for creating Athena external tables over the JSON Lines files:

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS catalog_github_repos (
  target STRING, target_slug STRING, exported_at STRING,
  private BOOLEAN, stars INT, forks INT, open_issues INT,
  total_contributors INT, total_commits INT, commit_unique_authors INT,
  total_prs INT, prs_open INT, prs_closed INT, prs_merged INT,
  total_reviews INT, total_pr_comments INT,
  total_additions BIGINT, total_deletions BIGINT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://{bucket}/{prefix}/catalog/github_repos/';
-- (one file: github_repos.jsonl)
```

(Similar DDL for each table.)

### Success Criteria

#### Automated Verification:
- [ ] `uv run pytest tests/test_catalog.py -v` — tests using moto S3 with pre-seeded `_stats.json` files

#### Manual Verification:
- [ ] Run catalog generator, inspect JSON Lines output
- [ ] Create Athena tables, run sample queries

---

## Phase 7: Tests

### Overview

Add tests for StatsCollector and catalog generator. Update existing exporter tests to verify `_stats.json` output.

### Changes Required

#### 1. New file: `tests/test_stats.py`

Tests for `StatsCollector`:
- `test_increment` — counter increments correctly
- `test_add_to_map` — distribution maps accumulate
- `test_set_nested` — dotted path setting
- `test_save_throttle` — verify save is throttled (second call within interval is skipped)
- `test_save_force` — verify `force=True` bypasses throttle
- `test_load_crash_recovery` — write stats, create new StatsCollector, load, verify data preserved
- `test_load_empty` — load from non-existent path returns empty data

#### 2. New file: `tests/test_catalog.py`

Tests for catalog generator:
- `test_catalog_from_stats` — seed S3 with `_stats.json` files, run catalog, verify JSONL output
- `test_catalog_summary` — verify `summary.json` aggregate counts
- `test_catalog_file_types` — verify cross-exporter file type breakdown
- `test_catalog_dry_run` — verify dry-run produces no S3 writes
- `test_catalog_exporter_filter` — verify `--exporter` flag filters correctly

#### 3. Update existing exporter tests

In each exporter's full-export test, add assertion that `_stats.json` exists in S3 and contains expected top-level keys:
- `test_github.py`: verify `_stats.json` has `exporter`, `repo`, `languages`, `commits`, `pull_requests`
- `test_jira.py`: verify `_stats.json` has `exporter`, `tickets`, `comments`, `attachments`
- `test_slack.py`: verify `_stats.json` has `exporter`, `channel`, `messages`, `files`
- `test_google_workspace.py`: verify `_stats.json` has `exporter`, `gmail`, `calendar`, `drive`

### Success Criteria

#### Automated Verification:
- [ ] `uv run pytest tests/ -v` — all tests pass (existing + new)

---

## Testing Strategy

### Unit Tests:
- `StatsCollector`: increment, map, nested set/get, throttle, S3 round-trip (moto)
- Catalog generator: JSONL output shape, summary aggregation, filtering

### Integration Tests:
- Each exporter's full-export test verifies `_stats.json` alongside existing S3 file assertions
- Catalog generator test seeds realistic `_stats.json` files and verifies end-to-end

### Manual Testing Steps:
1. Run GitHub exporter against a real repo, kill mid-export, restart — verify `_stats.json` preserves progress
2. Run all 4 exporters, then `python -m exporters.catalog` — inspect output
3. Create Athena tables from DDL, run: `SELECT * FROM catalog_github_repos`, `SELECT file_type, SUM(count) FROM catalog_file_types GROUP BY file_type`

## Performance Considerations

- `StatsCollector.save()` is throttled to 30s intervals — same as checkpoints, adds ~1 extra S3 PUT per 30s per exporter
- Catalog generator reads only `_stats.json` files (one per target), not per-item files — O(targets) not O(items)
- JSON Lines files are small (one line per target) — Athena scans are cheap
- `unique_authors_set` tracking in GitHub commits uses a list (not set) for JSON serializability — acceptable for typical author counts (<1000)

## Migration Notes

- No migration needed — `_stats.json` is a new file alongside existing output
- Existing exports without `_stats.json` are simply skipped by the catalog generator
- Re-running an exporter regenerates `_stats.json` from scratch (stats.load() restores partial progress only during the same export run)

## References

- Original task: Athena-queryable inventory of S3 data
- Research findings: `specs/research/athena-catalog/findings.md`
- Similar implementation: `lib/checkpoint.py:60-117` (throttled S3 persistence pattern)
