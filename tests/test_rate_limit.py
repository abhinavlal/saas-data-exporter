"""Tests for lib.rate_limit — TokenBucket thread safety and correctness."""

import threading
import time

from lib.rate_limit import TokenBucket


class TestBasicAcquire:
    def test_acquire_within_capacity(self):
        bucket = TokenBucket(capacity=5, refill_rate=10)
        # Should not block — 5 tokens available
        start = time.monotonic()
        for _ in range(5):
            bucket.acquire()
        elapsed = time.monotonic() - start
        assert elapsed < 0.1

    def test_acquire_blocks_when_empty(self):
        bucket = TokenBucket(capacity=1, refill_rate=10)
        bucket.acquire()  # consume the one token
        start = time.monotonic()
        bucket.acquire()  # should wait ~0.1s for refill
        elapsed = time.monotonic() - start
        assert 0.05 < elapsed < 0.5


class TestTryAcquire:
    def test_returns_true_when_available(self):
        bucket = TokenBucket(capacity=1, refill_rate=1)
        assert bucket.try_acquire() is True

    def test_returns_false_when_empty(self):
        bucket = TokenBucket(capacity=1, refill_rate=1)
        bucket.acquire()
        assert bucket.try_acquire() is False


class TestThrottle:
    def test_throttle_reduces_rate(self):
        bucket = TokenBucket(capacity=10, refill_rate=100)
        bucket.acquire(10)  # drain bucket
        bucket.throttle(1.0)  # slow to 1/s
        start = time.monotonic()
        bucket.acquire(1)
        elapsed = time.monotonic() - start
        assert elapsed > 0.5  # should take ~1s at 1/s rate

    def test_restore_increases_rate(self):
        bucket = TokenBucket(capacity=10, refill_rate=1)
        bucket.acquire(10)
        bucket.restore(1000)  # very fast
        start = time.monotonic()
        bucket.acquire(1)
        elapsed = time.monotonic() - start
        assert elapsed < 0.1


class TestThreadSafety:
    def test_concurrent_acquire(self):
        """10 threads each acquire 10 tokens from a bucket with capacity=100."""
        bucket = TokenBucket(capacity=100, refill_rate=1000)
        results = []
        errors = []

        def worker():
            try:
                for _ in range(10):
                    bucket.acquire()
                results.append(True)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker) for _ in range(10)]
        start = time.monotonic()
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)
        elapsed = time.monotonic() - start

        assert len(errors) == 0
        assert len(results) == 10
        assert elapsed < 2.0  # should be fast with high refill rate

    def test_concurrent_acquire_respects_rate(self):
        """Verify that rate limiting actually constrains throughput."""
        bucket = TokenBucket(capacity=5, refill_rate=20)
        count = 0
        lock = threading.Lock()

        def worker():
            nonlocal count
            for _ in range(5):
                bucket.acquire()
                with lock:
                    count += 1

        threads = [threading.Thread(target=worker) for _ in range(4)]
        start = time.monotonic()
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)
        elapsed = time.monotonic() - start

        assert count == 20
        # 20 tokens at 20/s = ~1s, but burst covers first 5
        # So ~0.75s minimum. Allow generous bounds.
        assert elapsed > 0.3
