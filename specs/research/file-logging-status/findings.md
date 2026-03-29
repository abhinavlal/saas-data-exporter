# File Logging & Status Script — Research Findings

## Current Logging System

- `lib/logging.py` defines `JSONFormatter` and `setup_logging(level, json_output)`
- `setup_logging` creates a single `StreamHandler(sys.stderr)` and replaces `logging.root.handlers`
- All 5 exporters call `setup_logging()` in their `main()` after argparse
- CLI flags: `--log-level` (default from `LOG_LEVEL` env), `--no-json-logs` (default from `JSON_LOGS` env)
- Logger convention: `log = logging.getLogger(__name__)` at module level, always named `log`

## Current Status/Progress Infrastructure

- `lib/checkpoint.py`: `CheckpointManager` stores phase status in S3 at `_checkpoints/{job_id}.json`
  - Fields: `job_id`, `status` (pending/in_progress/completed), `started_at`, `updated_at`, `phases` dict
  - Each phase: `status`, `total`, `completed`, `cursor`, `completed_ids`
- `lib/stats.py`: `StatsCollector` stores counts in S3 at `{target}/_stats.json`
  - Contains: `exporter`, `target`, per-phase counts, `exported_at`, `updated_at`
- `exporters/catalog.py`: Already reads `_stats.json` files from S3 and aggregates them

## Exporter main() Pattern

All exporters share: `load_dotenv()` → argparse → validation → `setup_logging()` → `S3Store` → `ExportConfig` → exporter → `run()`

## S3 Layout for Checkpoints

```
{prefix}/_checkpoints/github/{owner}__{repo}.json
{prefix}/_checkpoints/jira/{project}.json
{prefix}/_checkpoints/slack/{channel_id}.json
{prefix}/_checkpoints/google/{user_slug}.json
```

## File System Notes

- No `logs/`, `bin/`, or `scripts/` directories exist
- `.gitignore` already has `*.log` but not `logs/`
- An empty `jira.log` exists at repo root (stale artifact)
