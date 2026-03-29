# TESTING.md

Testing patterns, conventions, and infrastructure for the `data-exporter` project.

---

## Test Framework and Libraries

- **Test runner**: `pytest >= 8.0` (configured via `pyproject.toml` `[project.optional-dependencies] dev`).
- **HTTP mocking**: `responses >= 0.25` — intercepts `requests` library calls.
- **S3 mocking**: `moto[s3] >= 5.0` — in-memory AWS S3 mock via `@mock_aws`.
- **Mock utilities**: `pytest-mock >= 3.12` and stdlib `unittest.mock` (`MagicMock`, `patch`, `PropertyMock`).
- **Assertion library**: Standard `pytest` assertions (`assert`, `pytest.raises`, `pytest.approx`).

Install dev dependencies with:

```bash
uv sync --extra dev
```

Run all tests:

```bash
uv run pytest tests/ -v        # verbose
uv run pytest tests/ -q        # quiet
uv run pytest tests/test_s3.py # single module
```

---

## Test File Location

All test files live in `tests/` at the project root. There are no colocated tests adjacent to source files.

```
tests/
  __init__.py
  fixtures/          # empty directory, reserved for future fixture files
  test_checkpoint.py
  test_config.py
  test_edge_cases.py
  test_github.py
  test_google_workspace.py
  test_input.py
  test_jira.py
  test_rate_limit.py
  test_retry.py
  test_s3.py
  test_session.py
  test_slack.py
```

Each test file maps 1:1 to either a `lib/` module or an `exporters/` module:

| Test file | Covers |
|---|---|
| `tests/test_checkpoint.py` | `lib/checkpoint.py` |
| `tests/test_config.py` | `lib/config.py` |
| `tests/test_s3.py` | `lib/s3.py` (`S3Store`, `NDJSONWriter`) |
| `tests/test_session.py` | `lib/session.py` |
| `tests/test_rate_limit.py` | `lib/rate_limit.py` |
| `tests/test_retry.py` | `lib/retry.py` |
| `tests/test_input.py` | `lib/input.py` |
| `tests/test_github.py` | `exporters/github.py` |
| `tests/test_jira.py` | `exporters/jira.py` |
| `tests/test_slack.py` | `exporters/slack.py` |
| `tests/test_google_workspace.py` | `exporters/google_workspace.py` |
| `tests/test_edge_cases.py` | Cross-exporter edge cases and error paths |

---

## Test Naming Conventions

### Test Classes

Tests are organized into classes named `Test{Concept}`, grouping tests by the feature or scenario being tested:

```
TestLoadFresh
TestSaveAndLoad
TestPhaseTracking
TestItemTracking
TestCursor
TestComplete
TestSaveThrottling
TestResume
TestMarkItemDoneDedup

TestMetadataExport
TestContributorsExport
TestCommitsExport
TestPullRequestsExport
TestCheckpointResume
TestFullExport

TestEmptyExports
TestUnicodeFilenames
TestCheckpointOnError
TestS3Errors
TestCheckpointDefensiveness
TestSlackMalformedTimestamp
TestCsvEdgeCases
TestPerTargetErrorHandling
```

### Test Methods

Test methods are named `test_<what_is_being_tested>` using full English descriptions:

```python
def test_exports_metadata_to_s3(self, s3_env):
def test_exports_commits_from_list_api(self, s3_env):
def test_exports_commits_with_details_flag(self, s3_env):
def test_resume_skips_completed_phases(self, s3_env):
def test_resume_commit_details_after_partial(self, s3_env):
def test_duplicate_mark_does_not_double_count(self, s3_store):
def test_only_retries_specified_exceptions(self):
def test_preemptive_wait_returns_seconds_when_low(self):
def test_jira_continues_after_project_failure(self, s3_env):
```

Methods describe the behavior asserted, not the implementation called.

---

## Fixtures

### S3 Fixtures

The `s3_env` fixture is duplicated across multiple test files (`test_github.py`, `test_jira.py`, `test_slack.py`, `test_edge_cases.py`, `test_google_workspace.py`). It creates a moto-mocked S3 bucket and returns a tuple `(S3Store, ExportConfig, boto3_client)`:

```python
@pytest.fixture
def s3_env():
    with mock_aws():
        conn = boto3.client("s3", region_name="us-east-1")
        conn.create_bucket(Bucket="test-bucket")
        store = S3Store(bucket="test-bucket")
        config = ExportConfig(s3_bucket="test-bucket", max_workers=2)
        yield store, config, conn
```

`test_checkpoint.py` and `test_s3.py` define their own simplified variants (`s3_store`, `store`) that return only what they need.

### Exporter Factory Helpers

Each exporter test file defines a `_make_exporter(s3_env, **kwargs)` module-level function (not a fixture) to construct an exporter with safe defaults, allowing per-test customization via `**kwargs`:

```python
def _make_exporter(s3_env, **kwargs):
    store, config, _ = s3_env
    defaults = dict(
        token="fake-token",
        repo=REPO,
        s3=store,
        config=config,
        pr_limit=5,
        commit_limit=5,
        skip_commits=True,
        skip_prs=True,
    )
    defaults.update(kwargs)
    return GitHubExporter(**defaults)
```

### Google Credentials Fixture

`test_edge_cases.py` and `test_google_workspace.py` define a `mock_google_credentials` fixture that patches `service_account.Credentials` to return a `MagicMock` that passes `with_subject()` chaining:

```python
@pytest.fixture
def mock_google_credentials():
    with patch("exporters.google_workspace.service_account.Credentials") as mc:
        mi = MagicMock()
        mi.with_subject.return_value = mi
        mc.from_service_account_file.return_value = mi
        yield mc
```

---

## Mocking Patterns

### HTTP APIs (requests-based exporters)

GitHub, Jira, and Slack use the `responses` library. Test methods are decorated with `@responses.activate`. Mock helper functions follow the naming convention `mock_<resource>_api()` and are defined at module level:

```python
def mock_repo_api():
    responses.add(responses.GET, f"{API}/repos/{REPO}", json={...}, status=200)

def mock_contributors_api():
    responses.add(responses.GET, f"{API}/repos/{REPO}/contributors", json=[...], status=200)
    responses.add(responses.GET, f"{API}/repos/{REPO}/contributors", json=[], status=200)  # empty page
```

Paginated endpoints always mock at least two responses: the data page and an empty termination page.

### Google API (googleapiclient-based)

Google Workspace tests use `unittest.mock.patch` and `MagicMock`, since `googleapiclient` uses its own HTTP transport. `build()` is patched at the import path `"exporters.google_workspace.build"`. Separate helper functions build mock service objects:

```python
def _mock_gmail_service(message_ids, raw_messages):
    service = MagicMock()
    service.users().messages().list().execute.return_value = {...}
    ...
    return service

# Used in test:
with patch("exporters.google_workspace.build", return_value=gmail_service):
    exporter.run()
```

### S3 (boto3)

All S3 interactions are mocked with `moto`. Tests use `mock_aws()` as a context manager (not as a decorator) to keep the fixture pattern consistent. The real `boto3` client and `S3Store` are instantiated inside the mock context — this is mandatory for moto to intercept the calls.

### What Is Not Mocked

- `time.sleep` is never mocked in tests — rate-limiting and retry tests use near-zero `backoff_base` values (e.g., `backoff_base=0.01`) or `Retry-After: 0` headers to make tests fast without mocking time.
- Thread execution (`ThreadPoolExecutor`) is not mocked; tests run actual threads against mocked HTTP/S3.
- `CheckpointManager` is not mocked in exporter tests — the real checkpoint round-trips through the moto S3 mock, validating resume behavior end-to-end.

---

## Integration vs Unit Test Separation

There is no enforced separation between unit and integration tests — all tests live in `tests/`. However, there is a logical split:

- **Unit-style tests** (`test_config.py`, `test_rate_limit.py`, `test_retry.py`, `test_input.py`, `test_session.py`): test library modules in isolation with no S3 or network dependencies.
- **Integration-style tests** (`test_github.py`, `test_jira.py`, `test_slack.py`, `test_google_workspace.py`, `test_checkpoint.py`, `test_s3.py`): exercise full exporter behavior with mocked HTTP and mocked S3, validating the data that lands in S3.
- **Cross-cutting tests** (`test_edge_cases.py`): cover scenarios that span multiple exporters or test failure modes (empty exports, unicode filenames, partial checkpoint recovery, S3 errors, per-target error isolation).

---

## Test Data Management

### Inline Data Builders

Test data is built inline using helper functions named `_make_<resource>(...)`:

```python
def _commit_list_item(i):
    return {
        "sha": f"sha{i}",
        "commit": {"message": f"Commit message {i}", ...},
        ...
    }

def _make_issue(key, summary="Test ticket", has_attachment=False):
    return {"key": key, "id": ..., "fields": {...}}
```

These helpers are module-level functions (not fixtures) and support parameterization.

### Raw Email Builder

`test_google_workspace.py` includes `_make_raw_email(subject, body, attachment_name, attachment_content)` that builds fully valid RFC 2822 MIME messages encoded as base64url strings, matching the Gmail API response format.

### No External Fixture Files

The `tests/fixtures/` directory is empty (`__init__.py` only). All test data is constructed in code.

---

## Coverage Requirements

There are no coverage requirements configured (`pytest-cov` is not in the dev dependencies, no `.coveragerc`, no `[tool.coverage]` in `pyproject.toml`). Coverage is not enforced by CI (no CI pipeline exists — no `.github/workflows/`, no `Jenkinsfile`, no `.gitlab-ci.yml`).

---

## Key Test Patterns to Follow

### Asserting S3 Output

After running an exporter, download the output from the moto S3 mock and assert on the content directly:

```python
store, _, _ = s3_env
metadata = store.download_json(f"github/{SLUG}/repo_metadata.json")
assert metadata["full_name"] == REPO
assert metadata["language_breakdown"]["Python"]["bytes"] == 10000
```

For CSV output, use the raw boto3 client (the third element of `s3_env`) to read the bytes:

```python
store, _, conn = s3_env
resp = conn.get_object(Bucket="test-bucket", Key=f"github/{SLUG}/pull_requests.csv")
csv_content = resp["Body"].read().decode()
assert "alice" in csv_content
```

### Asserting Checkpoint State

For checkpoint resume tests, pre-populate a `CheckpointManager` directly before constructing the exporter under test:

```python
cp = CheckpointManager(store, f"github/{SLUG}")
cp.load()
cp.start_phase("metadata"); cp.complete_phase("metadata")
cp.start_phase("commits", total=3); cp.mark_item_done("commits", "sha0")
cp.save(force=True)
# Now construct the exporter and run — it should skip completed work
```

### Verifying Error Resilience

Tests for per-target error handling construct exporters with multiple targets, mock one to fail (e.g., HTTP 500), and verify the other target's output is still present in S3:

```python
exporter = JiraExporter(projects=["FAIL", "OK"], ...)
exporter.run()  # should not raise
tickets = store.download_json("jira/OK/tickets.json")
assert tickets == []
```

### Floating-Point Assertions

Use `pytest.approx` for calculated float values:

```python
assert metadata["language_breakdown"]["Python"]["percentage"] == pytest.approx(95.24, abs=0.01)
```