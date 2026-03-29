# CONVENTIONS.md

Code standards and patterns for the `data-exporter` project.

---

## Naming Patterns

### Files and Modules

- Use `snake_case` for all Python filenames.
- Library modules live in `lib/` and are named by single responsibility: `lib/checkpoint.py`, `lib/rate_limit.py`, `lib/session.py`, `lib/retry.py`, `lib/s3.py`, `lib/config.py`, `lib/input.py`, `lib/logging.py`, `lib/types.py`.
- Each data source has one file in `exporters/`: `exporters/github.py`, `exporters/jira.py`, `exporters/slack.py`, `exporters/google_workspace.py`.
- Test files are named `test_<module>.py` and live in `tests/`.

### Classes

- Exporter classes are named `{Service}Exporter`: `GitHubExporter`, `JiraExporter`, `SlackExporter`, `GoogleWorkspaceExporter`.
- Infrastructure classes use descriptive nouns: `S3Store`, `NDJSONWriter`, `CheckpointManager`, `TokenBucket`, `RateLimitState`, `RateLimitedAdapter`.
- Dataclasses use `PascalCase` with no "Data" suffix: `ExportConfig`, `PhaseState`.

### Functions and Methods

- Module-level functions use `snake_case`: `load_dotenv`, `make_session`, `parse_retry_after`, `setup_logging`, `read_csv_column`.
- Private methods are prefixed with a single underscore: `_export_metadata`, `_list_commits`, `_fetch_pr_detail`, `_upload_pr_csv`, `_key`, `_refill_unlocked`.
- Helper functions that are private to a module use a leading underscore: `_person_fields` in `exporters/jira.py`, `_user_slug` in `exporters/google_workspace.py`, `_safe_ts` and `_is_skippable_file` in `exporters/slack.py`.

### Variables and Constants

- Module-level constants use `UPPER_SNAKE_CASE`: `API_BASE`, `SLACK_API`, `SCOPES`, `GOOGLE_EXPORT_MAP`, `SKIP_DRIVE_TYPES`, `SKIP_EXTENSIONS`, `MB`, `SMALL_FILE_CONFIG`, `LARGE_FILE_CONFIG`.
- Local variables use `snake_case`.
- Module-level logger is always named `log` (not `logger`): `log = logging.getLogger(__name__)`.

---

## Import Patterns

Imports follow the standard Python three-group ordering (stdlib, third-party, local), separated by blank lines:

```python
# stdlib
import argparse
import csv
import io
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

# third-party (blank line)
import requests

# local (blank line)
from lib.s3 import S3Store, NDJSONWriter
from lib.checkpoint import CheckpointManager
from lib.session import make_session
from lib.logging import setup_logging
from lib.types import ExportConfig
```

Local imports inside `main()` are deferred to avoid circular imports or heavy imports at module load time — all `exporters/*.py` import `lib.config`, `lib.input` inside `main()` only:

```python
def main():
    from lib.config import load_dotenv, env, env_int, env_bool, env_list
    from lib.input import read_csv_column
    ...
```

Each exporter module file exports a single public class (`GitHubExporter`, etc.) and a `main()` function used as the CLI entry point. There are no `__all__` declarations.

---

## Error Handling Conventions

### Standard Pattern: `raise_for_status` + `exc_info=True`

HTTP responses always call `resp.raise_for_status()` immediately after receiving a response to surface HTTP errors as exceptions. When catching exceptions at the per-item or per-target level, always pass `exc_info=True` to the logger so the full traceback is preserved:

```python
try:
    exporter.run()
except Exception:
    log.error("Export failed for repo %s, continuing with next", repo, exc_info=True)
    failed.append(repo)
```

### Per-Target Fault Isolation

All `run()` methods on exporters iterate over a list of targets (repos, projects, channels, users) and catch `Exception` per target, logging errors and continuing. Failures are collected into a `failed` list and logged as a summary at the end. This pattern is identical across all four exporters.

### Specific HTTP Status Codes

404 responses on detail fetch endpoints (single commit, single PR) are treated as a non-fatal warning, not an exception:

```python
if resp.status_code == 404:
    log.warning("Commit %s not found (404)", sha)
    return None
resp.raise_for_status()
```

### S3 Errors

`lib/s3.py` catches `botocore.exceptions.ClientError` explicitly. `NoSuchKey` errors return `None` from `download_json`; all other `ClientError` variants are re-raised.

### ValueError for Invalid Input

`lib/input.py` raises `ValueError` with a human-readable message when a CSV column is missing, including what columns are actually available.

---

## Logging Patterns

### Setup

Call `setup_logging(level, json_output)` from `exporters/lib/logging.py` at the start of each `main()` function. This configures a single `StreamHandler` to `stderr`. Never configure logging inside library modules.

### Logger Acquisition

Every module that logs acquires a module-level logger at the top:

```python
log = logging.getLogger(__name__)
```

This is present in: `lib/s3.py`, `lib/input.py`, `lib/retry.py`, `lib/session.py`, `exporters/github.py`, `exporters/jira.py`, `exporters/slack.py`, `exporters/google_workspace.py`.

### JSON vs Human Format

Production mode uses `lib/logging.py`'s `JSONFormatter`, which emits structured JSON to stderr with the following keys: `ts` (UTC ISO-8601), `level`, `logger`, `msg`. Extra contextual fields can be attached to log records using custom attributes: `phase`, `item`, `progress`, `total`, `source`.

Human-readable format (used with `--no-json-logs`) uses: `%(asctime)s %(levelname)-8s %(name)s — %(message)s`.

### Log Levels

- `INFO`: Export start/complete, phase start/complete, item counts.
- `WARNING`: Rate limit preemptive waits, 404 on items, malformed input values, retry attempts.
- `ERROR`: Per-item and per-target failures (always with `exc_info=True`), failed summary counts.
- `DEBUG`: Raw API response diagnostics (e.g., Jira search response keys).

---

## Code Formatting

No linting or formatting configuration files exist (no `.flake8`, `ruff.toml`, `.pylintrc`, `pyproject.toml` `[tool.ruff]` section, or `.black`). Observed conventions from the source:

- **Indentation**: 4 spaces throughout.
- **Line length**: Lines stay well under 100 characters; no observed violations.
- **Trailing commas**: Used consistently in multi-line data literals and function argument lists.
- **Blank lines**: Two blank lines between top-level class and function definitions; one blank line between methods within a class.
- **String formatting**: f-strings are not used for logging calls (uses `%`-style format strings to defer interpolation); f-strings are used elsewhere (S3 path construction, error messages in non-logging code).

---

## Comment Conventions

### Module Docstrings

Every module has a one-line module docstring at the top describing its purpose:

```python
"""Rate-limited requests session with retry, backoff, and header-based adaptation."""
```

### Class Docstrings

Classes have multi-line docstrings with usage examples where the usage pattern is non-obvious. See `CheckpointManager` in `lib/checkpoint.py` and `NDJSONWriter` in `lib/s3.py` for examples.

### Function Docstrings

Public functions and methods have single-line docstrings. Methods that are intended to be called in a specific order or have important preconditions use longer docstrings. For example, `_refill_unlocked` in `lib/rate_limit.py` documents that the caller must hold `self._lock`.

### Inline Comments

Inline comments use `# ── Section Name ──...` banners (using box-drawing characters) to separate major logical sections within a class. Used consistently across all exporter files:

```python
# ── Metadata ──────────────────────────────────────────────────────────
# ── Contributors ──────────────────────────────────────────────────────
# ── Commits ───────────────────────────────────────────────────────────
# ── Pull Requests ─────────────────────────────────────────────────────
# ── CSV ───────────────────────────────────────────────────────────────
```

Short inline comments are used to document non-obvious logic (e.g., "Sleep outside the lock so other threads aren't blocked" in `lib/rate_limit.py`).

---

## Configuration and Environment Variables

All configuration is read via helpers in `lib/config.py`. Use these functions — do not call `os.environ` directly:

- `env(key, default)` — returns `str | None`, treats empty string as unset.
- `env_int(key, default)` — parses integer, returns default on missing or invalid.
- `env_bool(key, default)` — truthy: `"true"`, `"1"`, `"yes"`; falsy: `"false"`, `"0"`, `"no"`.
- `env_list(key, default)` — comma-separated list, strips whitespace from each item.

Required environment variables (documented in `.env.example`):

| Variable | Purpose |
|---|---|
| `S3_BUCKET` | S3 bucket name (all exporters) |
| `S3_PREFIX` | S3 key prefix (optional) |
| `GITHUB_TOKEN` | GitHub personal access token |
| `JIRA_TOKEN` / `JIRA_EMAIL` / `JIRA_DOMAIN` | Jira credentials |
| `SLACK_TOKEN` | Slack bot token (`xoxb-...`) |
| `GOOGLE_SERVICE_ACCOUNT_KEY` | Path to service account JSON |
| `MAX_WORKERS` | Thread pool size (default: 5) |
| `LOG_LEVEL` | Logging level (default: `INFO`) |
| `JSON_LOGS` | Enable JSON logging (default: `true`) |