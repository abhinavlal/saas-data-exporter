# STACK.md

## Language and Runtime

- **Python 3.12** (minimum, declared in `pyproject.toml`: `requires-python = ">=3.12"`)
- Runtime version confirmed as Python 3.12.3 on the deployment host
- No `.python-version` file present; version constraint is enforced via `pyproject.toml` only

## Package Manager and Build Tool

- **uv** is the package manager and virtual-environment tool (lockfile at `/home/ubuntu/data-exporter/uv.lock`)
- **hatchling** is the build backend (`pyproject.toml` `[build-system]` section)
- No `Makefile`, `Taskfile`, or other task runner; all commands are run via `uv run python -m ...`

### uv Install Commands

```
uv sync              # core dependencies
uv sync --extra dev  # adds pytest, pytest-mock, moto[s3], responses
uv sync --extra fast # adds boto3[crt] for higher S3 throughput
```

## Dependency Groups

Declared in `/home/ubuntu/data-exporter/pyproject.toml`.

### Core (production) dependencies

| Package | Version Constraint | Resolved Version | Purpose |
|---|---|---|---|
| `requests` | `>=2.31` | 2.33.0 | HTTP client for GitHub, Jira, Slack APIs |
| `boto3` | `>=1.34` | 1.42.78 | AWS SDK — S3 storage backend |
| `google-api-python-client` | `>=2.100` | 2.193.0 | Google Workspace API (Gmail, Calendar, Drive) |
| `google-auth` | `>=2.23` | 2.49.1 | Google OAuth2 service account credentials |
| `google-auth-httplib2` | `>=0.2` | 0.3.0 | HTTP transport for Google auth |

### Dev dependencies

| Package | Resolved Version | Purpose |
|---|---|---|
| `pytest` | 9.0.2 | Test framework |
| `pytest-mock` | 3.15.1 | Mock fixtures for pytest |
| `moto[s3]` | 5.1.22 | AWS S3 mock for tests |
| `responses` | 0.26.0 | HTTP mock for tests (`requests`-based) |

### Optional fast dependency

| Package | Resolved Version | Purpose |
|---|---|---|
| `boto3[crt]` + `awscrt` | 0.31.2 | Native C extension for 2–6x higher S3 transfer throughput |

## Key Transitive Dependencies (resolved)

| Package | Version | Notes |
|---|---|---|
| `botocore` | 1.42.78 | AWS SDK core |
| `s3transfer` | 0.16.0 | S3 multipart upload engine |
| `urllib3` | 2.6.3 | HTTP connections (requests backend) |
| `google-api-core` | 2.30.0 | Google API base |
| `googleapis-common-protos` | 1.73.1 | Protobuf definitions for Google APIs |
| `httplib2` | 0.31.2 | HTTP transport for Google auth library |
| `certifi` | 2026.2.25 | CA certificates |

## Frameworks and Libraries

- No web framework (this is a CLI / batch tool, not a server)
- **`concurrent.futures.ThreadPoolExecutor`** (stdlib) — parallelism model for all exporters
- **`argparse`** (stdlib) — CLI argument parsing in every exporter's `main()` function
- **`csv`** (stdlib) — CSV reading (input lists) and writing (output artefacts)
- **`dataclasses`** (stdlib) — `ExportConfig` in `lib/types.py`, `PhaseState` in `lib/checkpoint.py`
- **`threading`** (stdlib) — `threading.Lock` used in `TokenBucket` and `RateLimitState`

## Database Technologies

No database is used. All persistent state is stored in **Amazon S3**:
- Export artefacts (JSON, NDJSON, CSV, binary files)
- Checkpoint files under the `_checkpoints/` S3 prefix

## Infrastructure

- **AWS S3** — sole storage backend; no local filesystem persistence beyond temp files during upload
- No Docker, no Kubernetes, no `docker-compose.yml`, no `Dockerfile` present in the repository
- No CI/CD pipeline files (`.github/workflows/`, `Jenkinsfile`, etc.) present in the repository
- AWS credentials are provided via environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`) or IAM instance role; configured in `.env.example`

## Virtual Environment

Located at `/home/ubuntu/data-exporter/.venv/` (git-ignored). Python version inside the venv: 3.12.