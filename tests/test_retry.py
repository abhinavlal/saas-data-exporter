"""Tests for lib.retry — retry decorator with backoff."""

import pytest

from lib.retry import retry


class TestRetrySuccess:
    def test_no_retry_on_success(self):
        call_count = 0

        @retry(max_attempts=3, backoff_base=0.01)
        def succeeds():
            nonlocal call_count
            call_count += 1
            return "ok"

        assert succeeds() == "ok"
        assert call_count == 1


class TestRetryOnFailure:
    def test_retries_and_succeeds(self):
        call_count = 0

        @retry(max_attempts=3, backoff_base=0.01)
        def fails_twice():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("not yet")
            return "ok"

        assert fails_twice() == "ok"
        assert call_count == 3

    def test_exhausts_retries(self):
        call_count = 0

        @retry(max_attempts=3, backoff_base=0.01, exceptions=(ValueError,))
        def always_fails():
            nonlocal call_count
            call_count += 1
            raise ValueError("always")

        with pytest.raises(ValueError, match="always"):
            always_fails()
        assert call_count == 3


class TestExceptionFilter:
    def test_only_retries_specified_exceptions(self):
        call_count = 0

        @retry(max_attempts=3, backoff_base=0.01, exceptions=(ValueError,))
        def raises_type_error():
            nonlocal call_count
            call_count += 1
            raise TypeError("wrong type")

        with pytest.raises(TypeError):
            raises_type_error()
        assert call_count == 1  # no retry — TypeError not in exceptions list


class TestBackoff:
    def test_backoff_capped(self):
        call_count = 0

        @retry(max_attempts=5, backoff_base=0.01, max_backoff=0.02)
        def always_fails():
            nonlocal call_count
            call_count += 1
            raise ValueError("fail")

        with pytest.raises(ValueError):
            always_fails()
        assert call_count == 5
