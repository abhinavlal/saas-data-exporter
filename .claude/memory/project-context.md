# Project Context — data-exporter

> Last updated: 2026-03-29 by Claude

## What This Service Does

Batch data export tool for backup and data analysis. Pulls data from 4 SaaS platforms (GitHub, Jira, Slack, Google Workspace) and stores it in S3 as JSON, CSV, and binary files (email .eml, attachments). Designed for full org-scale exports (500+ Google users, 19 repos, 10 Slack channels, multiple Jira projects).

## Ownership

Owned by Abhinav (abhinav@org_name.com based on Jira email config). Single-person project.

## Key Architecture Decisions

- **S3-only storage** — no database. Checkpoints, exports, and intermediate state all live in S3. Chosen to keep things simple.
- **No orchestrator** — each exporter is run independently as a CLI command. No scheduler, no coordination between exporters. Simplicity over automation.
- **Phased pipeline with checkpointing** — each export breaks into named phases (metadata, commits, PRs, etc.) tracked in S3. Enables crash recovery without re-fetching completed work.
- **NDJSONWriter for memory safety** — large collections are written to temp files on disk rather than accumulated in Python lists. Brief memory spike on `read_all()` for final sort/upload.
- **Per-API rate limiting** — each exporter tunes its own `TokenBucket` + `RateLimitedAdapter` based on the target API's limits (GitHub 10rps, Jira 5rps, Slack 0.8rps).

## Request Lifecycle Summary

`main()` bootstraps: load .env -> argparse with env-var defaults -> validate -> S3Store + ExportConfig -> per-target loop with try/except. Each target: ExporterClass.__init__ (session + checkpoint) -> run() -> checkpoint.load() -> phase loop (skip completed phases) -> paginate API -> NDJSONWriter -> S3 upload -> checkpoint.save(force=True) -> checkpoint.complete().

## Current Scale

- GitHub: 19 repos (via github_repos.csv), primarily `org_name/` org
- Google Workspace: 519 users (via google_users.csv)
- Slack: 10 channels (via slack_channels.csv)
- Jira: IES project (via .env JIRA_PROJECTS)
- All limits default to 0 (unlimited)

## Known Concerns and Tech Debt

- **Performance unknown** — full-scale behavior with 500+ Google users not yet tested
- **No CI/CD** — tests run manually, no automated regression gate
- **requests.Session thread safety** — used from ThreadPoolExecutor; technically unsafe but works in practice
- **Duplicate test fixtures** — `s3_env` defined identically in 5 test files, should be in conftest.py
- **README stale** — documented limits don't match code defaults (all now 0=unlimited)
- **Google calendar lacks per-event checkpointing** — interruption restarts full phase
- **Broad exception swallowing** — failed items are logged but not tracked for retry; no failure manifest
- **Hardcoded sleep durations** in Google exporter (2s between Gmail batches, 0.3s between Drive files) bypass the rate-limit framework
