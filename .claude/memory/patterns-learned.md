# Patterns Learned — data-exporter

> Last updated: 2026-03-29 by Claude

## 1. Exporter Class Structure

Every exporter follows: `__init__(credentials, S3Store, ExportConfig, flags)` + `run()` + `_export_<phase>()` methods. Multi-target exporters (Jira, Slack) loop in `run()`; single-target (GitHub, Google) loop in `main()`.

**Canonical example:** `exporters/github.py:20-73`

## 2. Checkpoint-Gated Phases

Every phase is wrapped: `is_phase_done()` -> skip | `start_phase()` -> work -> `complete_phase()` -> `save(force=True)`. Item-level: `is_item_done()` -> skip | process -> `mark_item_done()` -> `save()` (throttled).

**Canonical example:** `exporters/jira.py:107-146`

## 3. Paginated API Calls

`while True` loop with bounded `per_page`/`maxResults`. Break on empty batch or no next cursor. Limit check: `if self.X_limit and len(items) >= self.X_limit: return items`.

**Canonical example:** `exporters/github.py:197-231` (page-based), `exporters/slack.py:153-184` (cursor-based)

## 4. Parallel Detail Fetch with Error Isolation

`ThreadPoolExecutor` + dict comprehension `{pool.submit(fn, id): id}` + `as_completed` loop with per-future `try/except Exception`. Failed items logged with `exc_info=True`, not marked done in checkpoint (will retry on next run).

**Canonical example:** `exporters/github.py:292-316`

## 5. 404 Handling in Detail Fetchers

404 on individual items returns `None` with a warning log. Other non-2xx calls `raise_for_status()`. Sub-resource fetchers (reviews, comments) use `if resp.status_code != 200: break`.

**Canonical example:** `exporters/github.py:233-237`

## 6. main() Bootstrap Sequence

Deferred import of `lib.config` + `lib.input` -> `load_dotenv()` -> argparse with `default=env(...)` -> validation via `parser.error()` -> target resolution (CLI > CSV > env) -> `setup_logging()` -> `S3Store()` -> `ExportConfig()` -> per-target loop with try/except.

**Canonical example:** `exporters/github.py:532-609`

## 7. NDJSONWriter for Memory-Bounded Accumulation

Create `NDJSONWriter(s3, wip_path)` -> `append()` records (written to temp file) -> `read_all()` for final sort -> `close()` (uploads + cleans temp file). Use `_wip` suffix for intermediate S3 path to avoid overwriting the final JSON.

**Canonical example:** `exporters/slack.py:148-188`

## 8. Test Setup Pattern

`@pytest.fixture s3_env` returns `(S3Store, ExportConfig, boto3.client)` inside `mock_aws()`. `_make_exporter(s3_env, **kwargs)` factory fills defaults. `@responses.activate` on each test method. Mock helpers: `mock_<resource>_api()` with empty second page for pagination termination.

**Canonical example:** `tests/test_github.py:21-44`, `tests/test_github.py:49-135`

## 9. Memory Release Between Phases

After saving phase output to S3, `del` the list and reload from S3 for the next phase. Prevents holding all data in memory across hour-long exports.

**Canonical example:** `exporters/jira.py:117-140`

## 10. @retry for Non-HTTP Operations

`@retry(max_attempts, backoff_base, exceptions)` decorator used only in Google exporter where `googleapiclient` bypasses the `requests` session. HTTP retries for requests-based exporters handled by `RateLimitedAdapter` + urllib3 `Retry`.

**Canonical example:** `exporters/google_workspace.py:189`, `lib/retry.py:10-41`
