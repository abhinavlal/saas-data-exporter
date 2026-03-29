# CLAUDE.md — data-exporter

## Service Overview

Batch data export tool for backup and data analysis. Exports data from GitHub, Jira, Slack, and Google Workspace into S3 as JSON/CSV/binary files. Owned by Abhinav.

## Tech Stack

- **Language:** Python 3.12
- **Package manager:** uv (`uv sync`, `uv sync --extra dev`)
- **Build backend:** hatchling
- **HTTP client:** `requests` (GitHub, Jira, Slack) / `googleapiclient` (Google Workspace)
- **Storage:** AWS S3 via `boto3`
- **Auth:** Google service account with domain-wide delegation, GitHub PAT, Jira API token, Slack bot token
- **No web framework** — CLI batch tool, not a server
- **No database** — all state lives in S3 (exports + checkpoints)
- **No CI/CD** — tests run manually
- **No Docker**

## Architecture

Four independent CLI exporters sharing a `lib/` infrastructure layer. Each exporter is a single file with a class + `main()` entry point.

```
exporters/github.py           python -m exporters.github
exporters/jira.py             python -m exporters.jira
exporters/slack.py            python -m exporters.slack
exporters/google_workspace.py python -m exporters.google_workspace
```

**Shared lib modules:** `s3.py` (S3Store, NDJSONWriter), `checkpoint.py` (resumable state), `session.py` (rate-limited HTTP), `rate_limit.py` (TokenBucket), `retry.py` (@retry decorator), `config.py` (.env loader), `input.py` (CSV reader), `logging.py` (JSON formatter), `types.py` (ExportConfig).

**Data flow:** `main()` -> `load_dotenv()` -> argparse -> `S3Store` + `ExportConfig` -> `Exporter.run()` -> checkpoint-gated phases -> paginate API -> NDJSONWriter (disk) -> upload to S3.

**Key design decisions:**
- S3-only storage to keep things simple (no database)
- No orchestrator — exporters are run independently
- Per-phase checkpointing enables crash recovery
- NDJSONWriter spills to temp files to bound memory during large exports
- Each exporter creates its own rate-limited session with per-API tuning

See `.claude/codebase/ARCHITECTURE.md` for full request lifecycle.

## Build & Run

```bash
# Install
uv sync              # production deps
uv sync --extra dev  # + test deps
uv sync --extra fast # + boto3[crt] for faster S3

# Run exporters
uv run python -m exporters.github
uv run python -m exporters.jira
uv run python -m exporters.slack
uv run python -m exporters.google_workspace

# Run tests
uv run pytest tests/ -v

# Environment
cp .env.example .env   # fill in credentials
```

## Coding Standards

- **Files:** `snake_case.py` everywhere
- **Classes:** `PascalCase` — exporters are `{Service}Exporter`, infra uses descriptive nouns (`S3Store`, `CheckpointManager`)
- **Functions:** `snake_case`, private methods prefixed with `_`
- **Constants:** `UPPER_SNAKE_CASE` at module level
- **Logger:** always `log = logging.getLogger(__name__)` (not `logger`)
- **Imports:** stdlib / third-party / local, separated by blank lines. `lib.config` and `lib.input` are deferred inside `main()` to avoid side effects at import time
- **Logging:** use `%`-style in `log.xxx()` calls (deferred interpolation), f-strings elsewhere
- **Section banners:** `# -- Section Name --...` (box-drawing chars) to separate class regions
- **Error handling:** `resp.raise_for_status()` after every HTTP call, `except Exception: log.error(..., exc_info=True)` for per-item/per-target isolation
- **No custom exceptions** — standard Python exceptions throughout

See `.claude/codebase/CONVENTIONS.md` for full details.

## Testing

- **Framework:** pytest 8 + responses (HTTP mock) + moto (S3 mock) + pytest-mock
- **141 tests**, all in `tests/` (flat, one file per source module + `test_edge_cases.py`)
- **Pattern:** `@pytest.fixture` `s3_env` returns `(S3Store, ExportConfig, boto3_client)` inside `mock_aws()`. Exporter tests use `@responses.activate` + `_make_exporter()` factory. Google tests patch `build()` with `MagicMock`.
- **What's NOT mocked:** `time.sleep` (use near-zero values instead), `ThreadPoolExecutor` (real threads), `CheckpointManager` (real S3 round-trip via moto)
- **No coverage enforcement** — `pytest-cov` not installed, no CI

See `.claude/codebase/TESTING.md` for full patterns.

## Common Patterns

**Adding a new exporter:** Follow the pattern in any existing exporter file:
1. Create `exporters/<service>.py` with a class and `main()`
2. Constructor takes credentials, `S3Store`, `ExportConfig`, feature flags
3. `run()` calls `checkpoint.load()`, then checkpoint-gated phase methods, then `checkpoint.complete()`
4. Each phase: `start_phase()` -> paginate API with bounded batch sizes -> write via `NDJSONWriter` -> `complete_phase()` -> `save(force=True)`
5. Parallel detail fetches: `ThreadPoolExecutor` + `as_completed` + per-item try/except

**Adding a new lib module:** Keep it self-contained (only import stdlib or third-party). Only `checkpoint.py` imports another `lib/` module (`s3.py`).

**Configuration priority:** CLI flag > env var > .env file > hardcoded default. Use `env()`, `env_int()`, `env_bool()`, `env_list()` from `lib/config.py`.

**Target list resolution:** CLI args > `--input-csv` > env var list. Always validate with `parser.error()`.

## Anti-Patterns

- **Don't accumulate unbounded data in memory.** Use `NDJSONWriter` for large collections, reload from S3 between phases with explicit `del` afterward.
- **Don't skip per-target error handling.** Every target loop must wrap in try/except so one failure doesn't kill the batch.
- **Don't use `os.environ` directly.** Use the `lib/config` helpers which handle empty-string-as-unset and type conversion.
- **Don't mock `time.sleep` in tests.** Use near-zero backoff values or `Retry-After: 0` headers instead.
- **Don't hardcode API rate limits as magic numbers.** Document the reasoning (e.g., "Slack Tier 3 = ~50/min").
- **README defaults are stale** — documented limits (500 PRs, 100 tickets) no longer match code (0 = unlimited). Update README when changing defaults.

## Known Concerns

- `requests.Session` is used from `ThreadPoolExecutor` threads — technically not thread-safe, works in practice but could cause issues under heavy load
- No CI/CD pipeline — tests must be run manually
- Performance characteristics under full-scale load (500+ Google users, large repos) are not yet established
- Duplicate `s3_env` fixture across 5 test files — should be extracted to `conftest.py`
- Calendar export has no per-event checkpointing (interruption restarts the full phase)
- `_batch_fetch_raw` in Google exporter retries the entire batch on single-message failure

See `.claude/codebase/CONCERNS.md` for comprehensive list.

## S3 Output Layout

```
{prefix}/
  _checkpoints/{exporter}/{job_id}.json
  github/{owner}__{repo}/   repo_metadata.json, contributors.json, commits.json, pull_requests.json/.csv
  jira/{project}/           tickets.json/.csv, attachments/{key}/{file}
  slack/{channel_id}/       channel_info.json, messages.json, attachments/{file_id}_{name}
  google/{user_slug}/       gmail/{id}.eml, gmail/_index.json, calendar/events.json, drive/{file}
```
