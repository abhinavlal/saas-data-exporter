# Structure

## Directory Tree

```
data-exporter/
├── exporters/              # Four independent CLI exporters (one file each)
│   ├── __init__.py
│   ├── github.py           # GitHub: repos, contributors, commits, PRs
│   ├── google_workspace.py # Google: Gmail, Calendar, Drive
│   ├── jira.py             # Jira: tickets, comments, attachments
│   └── slack.py            # Slack: channels, messages, threads, files
│
├── lib/                    # Shared infrastructure (no cross-lib imports)
│   ├── __init__.py
│   ├── checkpoint.py       # S3-backed resumable checkpoint state
│   ├── config.py           # .env loader and env var helpers
│   ├── input.py            # CSV column reader for target lists
│   ├── logging.py          # JSON structured log formatter + setup
│   ├── rate_limit.py       # Thread-safe TokenBucket
│   ├── retry.py            # @retry decorator with exponential backoff
│   ├── s3.py               # S3Store + NDJSONWriter (disk-backed NDJSON)
│   ├── session.py          # make_session(): rate-limited requests.Session
│   └── types.py            # ExportConfig dataclass
│
├── tests/                  # All tests (flat structure, one file per source module)
│   ├── __init__.py
│   ├── fixtures/
│   │   └── __init__.py     # (empty — no fixture files currently)
│   ├── test_checkpoint.py
│   ├── test_config.py
│   ├── test_edge_cases.py
│   ├── test_github.py
│   ├── test_google_workspace.py
│   ├── test_input.py
│   ├── test_jira.py
│   ├── test_rate_limit.py
│   ├── test_retry.py
│   ├── test_s3.py
│   ├── test_session.py
│   └── test_slack.py
│
├── specs/                  # Planning and research documents (not code)
│   ├── plans/
│   │   └── 2026-03-28-production-data-exporter.md
│   ├── research/
│   └── tasks/
│
├── .env                    # Local secrets (gitignored)
├── .env.example            # Template for all required env vars
├── .gitignore
├── pyproject.toml          # Package metadata, deps, build config (hatchling)
├── requirements.md         # Human-readable requirements document
├── README.md               # Installation, usage reference, S3 layout
├── service-account.json    # Google service account key (gitignored)
├── uv.lock                 # Locked dependency versions (uv)
│
├── github_repos.csv        # Live input — gitignored, not committed
├── google_users.csv        # Live input — gitignored
├── jira_projects.csv       # Live input — gitignored
├── slack_channels.csv      # Live input — gitignored
│
├── github_repos.csv.example    # Committed example input files
├── google_users.csv.example
├── jira_projects.csv.example
└── slack_channels.csv.example
```

## Entry Points

Each exporter is both importable as a module and runnable as a script.

| Exporter | Entry function | Invocation |
|---|---|---|
| GitHub | `exporters/github.py:532` `main()` | `python -m exporters.github` |
| Jira | `exporters/jira.py:464` `main()` | `python -m exporters.jira` |
| Slack | `exporters/slack.py:338` `main()` | `python -m exporters.slack` |
| Google Workspace | `exporters/google_workspace.py:446` `main()` | `python -m exporters.google_workspace` |

All four `main()` functions use the same bootstrap sequence: `load_dotenv()` → `argparse` parsing with `env()` defaults → validation → `setup_logging()` → `S3Store(...)` → `ExportConfig(...)` → exporter loop.

## Module Boundaries and Dependency Direction

```
exporters/github.py  ─┐
exporters/jira.py    ─┤──→  lib/s3.py
exporters/slack.py   ─┤──→  lib/checkpoint.py
exporters/google_*   ─┘──→  lib/session.py
                         ──→  lib/logging.py
                         ──→  lib/types.py
                         ──→  lib/config.py       (imported inside main() only)
                         ──→  lib/input.py        (imported inside main() only)
                         ──→  lib/retry.py        (google_workspace.py only)

lib/checkpoint.py    ──→  lib/s3.py
lib/session.py       ──→  lib/rate_limit.py
lib/s3.py            ──→  (boto3 only — no internal imports)
lib/rate_limit.py    ──→  (stdlib only)
lib/retry.py         ──→  (stdlib only)
lib/config.py        ──→  (stdlib only)
lib/input.py         ──→  (stdlib only)
lib/logging.py       ──→  (stdlib only)
lib/types.py         ──→  (stdlib only)
```

`lib/config` and `lib/input` are imported inside `main()` functions rather than at module level to keep class constructors free of side effects (`.env` loading, file I/O).

`lib/checkpoint.py` is the only `lib/` module that imports another `lib/` module (`lib/s3`).

## Configuration Files

| File | Purpose |
|---|---|
| `pyproject.toml` | Package name (`data-exporter`), Python ≥ 3.12 constraint, runtime and dev dependencies, hatchling build config |
| `uv.lock` | Pinned dependency tree for reproducible installs (managed by `uv`) |
| `.env.example` | Documents every env var consumed by all four exporters; copy to `.env` and fill in secrets |
| `.env` | Local overrides (gitignored); loaded by `lib/config.load_dotenv()` at startup, does not override already-set env vars |

There are no framework config files (no `settings.py`, no `config.yaml`, no `alembic.ini`). All runtime configuration flows through env vars and CLI arguments.

## Build Output Locations

This project has no compiled artifacts. The `dist/` and `build/` directories are gitignored. If a wheel is built:

```
dist/data_exporter-0.1.0-py3-none-any.whl
```

The `pyproject.toml` specifies `packages = ["lib", "exporters"]` as the wheel contents.

## Generated vs Hand-Written Code

All code in `lib/` and `exporters/` is hand-written. There is no code generation.

`uv.lock` is generated by `uv` from `pyproject.toml` — do not edit it manually.

The `__pycache__/` directories are Python-generated bytecode cache — gitignored.

## File Sizes (lines)

| File | Lines |
|---|---|
| `exporters/github.py` | 609 |
| `exporters/jira.py` | 523 |
| `exporters/google_workspace.py` | 516 |
| `exporters/slack.py` | 402 |
| `lib/s3.py` | 174 |
| `lib/session.py` | 161 |
| `lib/checkpoint.py` | 157 |
| `lib/config.py` | 67 |
| `lib/rate_limit.py` | 59 |
| `lib/retry.py` | 41 |
| `lib/input.py` | 31 |
| `lib/logging.py` | 34 |
| `lib/types.py` | 14 |

`exporters/github.py` at 609 lines is the largest file but is self-contained with no sub-module candidates — each method maps to a distinct API resource.