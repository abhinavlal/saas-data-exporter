"""Thread-safe token bucket rate limiter with adaptive throttling."""

import threading
import time


class TokenBucket:
    """
    Thread-safe token bucket.

    capacity:    max tokens (burst size)
    refill_rate: tokens added per second
    """

    def __init__(self, capacity: float, refill_rate: float):
        self.capacity = capacity
        self.refill_rate = refill_rate
        self._tokens = capacity
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    def _refill_unlocked(self) -> None:
        """Refill tokens based on elapsed time. Caller MUST hold self._lock."""
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self.capacity, self._tokens + elapsed * self.refill_rate)
        self._last_refill = now

    def acquire(self, tokens: float = 1.0) -> None:
        """Block until tokens are available, then consume them."""
        while True:
            with self._lock:
                self._refill_unlocked()
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return
                wait = (tokens - self._tokens) / self.refill_rate
            # Sleep outside the lock so other threads aren't blocked
            time.sleep(wait)

    def try_acquire(self, tokens: float = 1.0) -> bool:
        """Non-blocking acquire. Returns True if tokens were available."""
        with self._lock:
            self._refill_unlocked()
            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
            return False

    def throttle(self, new_rate: float) -> None:
        """Dynamically reduce refill rate (adaptive throttling on 429)."""
        with self._lock:
            self.refill_rate = new_rate

    def restore(self, original_rate: float) -> None:
        """Restore original refill rate after throttle period."""
        with self._lock:
            self.refill_rate = original_rate
