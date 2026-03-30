"""Rate-limited requests session with retry, backoff, and header-based adaptation."""

import logging
import time
import threading
from datetime import datetime, timezone

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from lib.rate_limit import TokenBucket

log = logging.getLogger(__name__)


def parse_retry_after(value: str) -> float:
    """Parse Retry-After header (seconds or HTTP-date)."""
    try:
        return float(value)
    except ValueError:
        dt = datetime.strptime(value, "%a, %d %b %Y %H:%M:%S %Z")
        dt = dt.replace(tzinfo=timezone.utc)
        return max(0.0, dt.timestamp() - time.time())


class RateLimitState:
    """Thread-safe tracking of server-reported rate limit state."""

    def __init__(self):
        self.remaining: int | None = None
        self.limit: int | None = None
        self.reset_at: float | None = None
        self._lock = threading.Lock()

    def update(self, headers: dict) -> None:
        with self._lock:
            if "X-RateLimit-Remaining" in headers:
                self.remaining = int(headers["X-RateLimit-Remaining"])
            if "X-RateLimit-Limit" in headers:
                self.limit = int(headers["X-RateLimit-Limit"])
            if "X-RateLimit-Reset" in headers:
                self.reset_at = float(headers["X-RateLimit-Reset"])

    def should_preemptive_wait(self, min_remaining: int = 50) -> float | None:
        """Returns seconds to wait if remaining quota is dangerously low.

        When remaining is 0: wait until full reset.
        When remaining < min_remaining: spread remaining requests across the
        remaining window to avoid bursting into a wall.

        Pass min_remaining < 0 to disable all preemptive waiting (used when
        an external token pool manages rate limits across multiple tokens).
        """
        if min_remaining < 0:
            return None
        with self._lock:
            if self.remaining is None or self.reset_at is None:
                return None
            if self.remaining <= 0:
                # Fully exhausted — must wait for reset
                return max(0.0, self.reset_at - time.time() + 1.0)
            if self.remaining < min_remaining:
                # Spread remaining requests across the window, capped at 30s
                time_left = max(1.0, self.reset_at - time.time())
                return min(time_left / self.remaining, 30.0)
            return None


class RateLimitedAdapter(HTTPAdapter):
    """
    HTTPAdapter that:
    1. Pre-request: acquires a token from a shared TokenBucket
    2. Post-response: reads rate-limit headers
    3. On 429: respects Retry-After with exponential backoff
    """

    def __init__(self, bucket: TokenBucket, state: RateLimitState,
                 min_remaining: int = 50, max_retries_on_429: int = 5, **kwargs):
        super().__init__(**kwargs)
        self.bucket = bucket
        self.state = state
        self.min_remaining = min_remaining
        self.max_retries_on_429 = max_retries_on_429

    def send(self, request, **kwargs):
        # Preemptive throttle based on remaining quota
        wait = self.state.should_preemptive_wait(self.min_remaining)
        if wait:
            log.warning("Rate limit low, preemptive wait %.1fs", wait)
            time.sleep(wait)

        # Acquire token (blocks until available)
        self.bucket.acquire()

        # Send request with retry on 429
        for attempt in range(self.max_retries_on_429):
            response = super().send(request, **kwargs)
            self.state.update(response.headers)

            if response.status_code != 429:
                return response

            if "Retry-After" in response.headers:
                retry_wait = parse_retry_after(response.headers["Retry-After"])
            else:
                retry_wait = min(2 ** attempt + 0.5, 120)

            log.warning(
                "429 rate limited, attempt %d/%d, waiting %.1fs",
                attempt + 1, self.max_retries_on_429, retry_wait,
            )
            time.sleep(retry_wait)
            self.bucket.acquire()

        return response


class _TimeoutSession(requests.Session):
    """Session subclass that injects default timeouts."""

    def __init__(self, connect_timeout: float, read_timeout: float):
        super().__init__()
        self._default_timeout = (connect_timeout, read_timeout)

    def request(self, method, url, **kwargs):
        if "timeout" not in kwargs:
            kwargs["timeout"] = self._default_timeout
        return super().request(method, url, **kwargs)


def make_session(
    requests_per_second: float = 5.0,
    burst: float = 10.0,
    min_remaining: int = 50,
    connect_timeout: float = 10.0,
    read_timeout: float = 60.0,
    max_retries_on_error: int = 3,
) -> tuple[requests.Session, RateLimitState]:
    """
    Create a rate-limited requests.Session.

    Returns (session, state) -- state can be inspected for remaining quota.
    """
    bucket = TokenBucket(capacity=burst, refill_rate=requests_per_second)
    state = RateLimitState()

    retry_strategy = Retry(
        total=max_retries_on_error,
        backoff_factor=1.0,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET", "POST", "PUT"],
    )

    adapter = RateLimitedAdapter(
        bucket=bucket,
        state=state,
        min_remaining=min_remaining,
        max_retries=retry_strategy,
    )

    session = _TimeoutSession(connect_timeout, read_timeout)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    return session, state
