# File Logging & Export Status Script — Implementation Plan

## Overview

Add file-based logging to all exporters (in addition to existing stderr output) and create a status script that reads S3 checkpoints/stats to report current export status.

## Current State Analysis

- `lib/logging.py:25-34` — `setup_logging()` creates one `StreamHandler(sys.stderr)`, no file output
- `lib/checkpoint.py` — structured phase/status data already persisted in S3
- `lib/stats.py` — per-target export stats already persisted in S3
- All 5 exporters call `setup_logging()` identically in `main()`
- No `logs/` directory or `scripts/` directory exists

## Desired End State

- Running any exporter creates/appends to `logs/{exporter_name}.log`
- `logs/` directory auto-created on first run
- `uv run python -m scripts.export_status` prints a table of all export statuses from S3
- All existing tests pass unchanged

## What We're NOT Doing

- Log rotation
- Removing stderr output
- Changing log format or checkpoint/stats structure
- Adding new structured fields to logs
- Per-run log files (we use per-exporter-type, appended)

## Implementation Approach

Modify `setup_logging()` to accept an optional `log_file` path and add a `FileHandler` alongside the existing `StreamHandler`. Each exporter's `main()` computes the log file path and passes it in. The status script is a new standalone module that loads `.env`, connects to S3, lists checkpoint/stats files, and prints a summary.

---

## Phase 1: File Logging in `lib/logging.py`

### Overview

Extend `setup_logging()` to optionally write logs to a file. This is the foundation — once this works, all exporters get file logging by passing one extra argument.

### Changes Required

#### 1. `lib/logging.py`

**File**: `lib/logging.py`
**Changes**: Add `log_file` parameter to `setup_logging()`. When provided, create `logs/` dir and add a `FileHandler` with the same formatter as stderr.

```python
import os

def setup_logging(level: str = "INFO", json_output: bool = True, log_file: str | None = None) -> None:
    handler = logging.StreamHandler(sys.stderr)
    if json_output:
        formatter = JSONFormatter()
    else:
        formatter = logging.Formatter(
            "%(asctime)s %(levelname)-8s %(name)s — %(message)s"
        )
    handler.setFormatter(formatter)
    handlers = [handler]

    if log_file:
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        handlers.append(file_handler)

    logging.root.handlers = handlers
    logging.root.setLevel(getattr(logging, level.upper()))
```

#### 2. Update `.gitignore`

**File**: `.gitignore`
**Changes**: Add `logs/` directory entry.

```
# Logs
*.log
logs/
```

### Success Criteria

#### Automated Verification:
- [x] `uv run pytest tests/ -v` — all 165 tests pass (no test changes needed; tests don't call `setup_logging` with `log_file`)

#### Manual Verification:
- [x] Import `setup_logging` in a Python REPL, call with `log_file="logs/test.log"`, verify file is created and written to

**Implementation Note**: After completing this phase and all automated verification passes, pause for manual confirmation before proceeding to the next phase.

---

## Phase 2: Wire File Logging into All Exporters

### Overview

Each exporter's `main()` computes a log file path and passes it to `setup_logging()`. Add `--log-dir` CLI flag and `LOG_DIR` env var.

### Changes Required

#### 1. All 5 exporter `main()` functions

**Files**: `exporters/github.py`, `exporters/jira.py`, `exporters/slack.py`, `exporters/google_workspace.py`, `exporters/catalog.py`

**Changes** (identical pattern in each): Add `--log-dir` argument and compute log file path.

```python
# In argparse section (after --no-json-logs):
parser.add_argument("--log-dir", default=env("LOG_DIR", "logs"), help="Directory for log files")

# Replace the setup_logging call:
log_file = os.path.join(args.log_dir, "{exporter_name}.log")
setup_logging(level=args.log_level, json_output=not args.no_json_logs, log_file=log_file)
```

Where `{exporter_name}` is `github`, `jira`, `slack`, `google_workspace`, or `catalog` respectively.

Each file also needs `import os` added to imports (if not already present).

### Success Criteria

#### Automated Verification:
- [x] `uv run pytest tests/ -v` — all tests pass

#### Manual Verification:
- [ ] Run `uv run python -m exporters.github --help` — see `--log-dir` flag
- [ ] Run any exporter briefly — `logs/{name}.log` is created with log content

**Implementation Note**: After completing this phase and all automated verification passes, pause for manual confirmation before proceeding to the next phase.

---

## Phase 3: Export Status Script

### Overview

Create `scripts/export_status.py` that reads S3 checkpoints and stats to display a summary of all export statuses.

### Changes Required

#### 1. `scripts/__init__.py`

**File**: `scripts/__init__.py`
**Changes**: Empty file to make `scripts/` a package (for `python -m` invocation).

#### 2. `scripts/export_status.py`

**File**: `scripts/export_status.py`
**Changes**: New file — the status script.

**Logic:**
1. Load `.env` via `lib.config.load_dotenv`
2. Parse args: `--s3-bucket`, `--s3-prefix` (same pattern as exporters)
3. Create `S3Store`
4. List all checkpoint files under `_checkpoints/` using `s3.list_keys(prefix="_checkpoints/")`
5. For each checkpoint JSON: download, extract status info
6. For each target: also try to download `{target_path}/_stats.json` for count data
7. Print a formatted table to stdout:

```
Exporter   Target              Status       Phases            Last Updated
─────────  ──────────────────  ───────────  ────────────────  ────────────────────
github     owner/repo          completed    4/4 complete      2026-03-29T10:05:00Z
jira       PROJ                in_progress  2/3 (tickets)     2026-03-29T09:30:00Z
slack      C12345678           completed    3/3 complete      2026-03-28T15:00:00Z
google     user@example.com    failed       1/4 (gmail)       2026-03-29T08:00:00Z
```

8. Exit code: 0 if all completed, 1 if any in-progress/failed

**Key implementation detail**: Need to check what S3 listing methods `S3Store` provides.

#### 3. Check `S3Store` for list capability

**File**: `lib/s3.py`
**Dependency**: If `S3Store` doesn't have a `list_keys()` method, add one. If it does, use it.

### Success Criteria

#### Automated Verification:
- [x] `uv run pytest tests/ -v` — all existing tests pass
- [x] `uv run python -m scripts.export_status --help` — shows usage

#### Manual Verification:
- [ ] Run `uv run python -m scripts.export_status --s3-bucket <bucket> --s3-prefix <prefix>` — displays table with real data from S3

---

## Phase 4: Tests for New Functionality

### Overview

Add tests for the new `setup_logging` file handler behavior and the status script.

### Changes Required

#### 1. `tests/test_logging.py`

**File**: `tests/test_logging.py`
**Changes**: New test file for `lib/logging.py` (currently has zero test coverage).

Tests:
- `test_setup_logging_creates_stderr_handler` — verify stderr handler exists
- `test_setup_logging_with_log_file` — verify file handler is added and log file is created in a tmp dir
- `test_setup_logging_creates_log_dir` — verify parent directory is auto-created
- `test_setup_logging_json_format` — verify JSON output in file
- `test_setup_logging_plain_format` — verify plain text output in file

#### 2. `tests/test_export_status.py`

**File**: `tests/test_export_status.py`
**Changes**: New test file for the status script.

Tests (using moto for S3):
- `test_status_no_checkpoints` — empty S3, shows "no exports found"
- `test_status_completed_export` — one completed checkpoint, displays correctly
- `test_status_mixed_statuses` — multiple checkpoints with different statuses
- `test_status_exit_code` — exit 0 when all completed, exit 1 otherwise

### Success Criteria

#### Automated Verification:
- [x] `uv run pytest tests/ -v` — all tests pass including new ones

---

## Testing Strategy

### Unit Tests:
- `test_logging.py`: test `setup_logging()` with and without `log_file`, verify handler count and file creation
- `test_export_status.py`: test status script against mocked S3 with various checkpoint states

### Integration Tests:
- Not needed — existing exporter tests cover `setup_logging()` indirectly (they don't pass `log_file` so behavior is unchanged)

### Manual Testing Steps:
1. Run `uv run python -m exporters.github` — verify `logs/github.log` is created with JSON log lines
2. Run `uv run python -m scripts.export_status` — verify status table appears
3. Run two exporters — verify both log files exist and status shows both

## Performance Considerations

- `FileHandler` adds negligible overhead (disk I/O is buffered by OS)
- Status script does N+1 S3 calls (1 list + N downloads) — fine for the expected scale (<100 targets)

## References

- Research findings: `specs/research/file-logging-status/findings.md`
- Current logging: `lib/logging.py:25-34`
- Checkpoint structure: `lib/checkpoint.py:1-25`
- Stats collector: `lib/stats.py`
