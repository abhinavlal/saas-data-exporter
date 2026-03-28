"""Retry decorator with exponential backoff for non-HTTP operations."""

import functools
import logging
import time

log = logging.getLogger(__name__)


def retry(
    max_attempts: int = 5,
    backoff_base: float = 2.0,
    max_backoff: float = 120.0,
    exceptions: tuple = (Exception,),
):
    """
    Retry decorator with exponential backoff.

    Use for non-HTTP operations (S3 uploads, file processing).
    HTTP retry is handled by RateLimitedSession.
    """
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(max_attempts):
                try:
                    return fn(*args, **kwargs)
                except exceptions as e:
                    last_exc = e
                    if attempt == max_attempts - 1:
                        break
                    wait = min(backoff_base ** attempt, max_backoff)
                    log.warning(
                        "%s failed (attempt %d/%d): %s. Retrying in %.1fs",
                        fn.__name__, attempt + 1, max_attempts, e, wait,
                    )
                    time.sleep(wait)
            raise last_exc
        return wrapper
    return decorator
