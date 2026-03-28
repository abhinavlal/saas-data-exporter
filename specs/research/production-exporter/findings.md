# Research Findings: Production-Grade Data Exporter

## S3 Patterns (boto3)

### Thread Safety
- `boto3.client("s3")` call is NOT thread-safe (races on credential init)
- A client **instance** (once created) IS thread-safe — create once, share across threads
- Pattern: `session = boto3.session.Session(); s3 = session.client("s3")` before ThreadPoolExecutor

### Bulk Small Files (.eml, attachments)
- Bottleneck is HTTP round-trip latency, not bandwidth
- Use `ThreadPoolExecutor(max_workers=64)` with `TransferConfig(use_threads=False)` per file
- Benchmarks show ~72x speedup over sequential at 100 workers

### Large Files (multipart)
- `upload_file()` / `upload_fileobj()` auto-switch to multipart above `multipart_threshold`
- Recommended: `multipart_threshold=64MB`, `multipart_chunksize=64MB`, `max_concurrency=20`
- Install `boto3[crt]` for 2-6x throughput improvement (AWS CRT)

### Checkpoints in S3
- Use `put_object` / `get_object` directly (not managed transfer) for small JSON
- S3 is strongly consistent since Dec 2020 — safe for checkpoints
- S3 scales to 3,500 PUT/s and 5,500 GET/s per prefix — not a bottleneck
- Conditional writes available via `IfNoneMatch` / `IfMatch` for distributed locking (not needed for single-process)

### TransferConfig Recommendations
```python
# For many small files (inter-file parallelism via ThreadPoolExecutor)
SMALL_FILE_CONFIG = TransferConfig(multipart_threshold=128*MB, use_threads=False)

# For large files (intra-file parallelism via boto3 threads)
LARGE_FILE_CONFIG = TransferConfig(
    multipart_threshold=64*MB, multipart_chunksize=64*MB,
    max_concurrency=20, use_threads=True,
    preferred_transfer_client="auto",
)
```

## Rate Limiting Patterns

### Token Bucket (thread-safe)
- `threading.Lock` for atomicity; sleep OUTSIDE the lock
- Use `time.monotonic()` not `time.time()`
- `_refill_unlocked()` pattern to avoid deadlock

### Adaptive Rate Limiting
- Parse `X-RateLimit-Remaining`, `X-RateLimit-Reset` (GitHub/Slack style)
- Parse `Retry-After` (seconds or HTTP-date) on 429 responses
- Proactive throttle: slow down when remaining < 10% of limit

### Integration with requests.Session
- Best approach: custom `HTTPAdapter.send()` override
- Pre-request: acquire token from bucket
- Post-response: update rate limit state from headers
- On 429: exponential backoff with Retry-After

### Library Assessment
- `requests-ratelimiter` + `pyrate-limiter`: best off-the-shelf, but in-memory bucket not shared across threads without SQLite backend
- `ratelimit` (tomasbasham) and `ratelimiter` (RazerM): confirmed threading bugs — avoid
- **Recommendation**: Roll our own TokenBucket + RateLimitedSession for full control over per-API header parsing. Minimal code, zero extra deps.
