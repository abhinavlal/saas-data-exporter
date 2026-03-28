"""Tests for lib.session — RateLimitedSession, 429 handling, Retry-After parsing."""

import time

import pytest
import responses

from lib.session import make_session, parse_retry_after, RateLimitState


class TestParseRetryAfter:
    def test_parse_seconds(self):
        assert parse_retry_after("30") == 30.0
        assert parse_retry_after("0") == 0.0
        assert parse_retry_after("1.5") == 1.5

    def test_parse_http_date(self):
        # Use a date far in the future to avoid negative values
        result = parse_retry_after("Fri, 28 Mar 2098 12:00:00 GMT")
        assert result > 0


class TestRateLimitState:
    def test_update_from_headers(self):
        state = RateLimitState()
        state.update({
            "X-RateLimit-Remaining": "42",
            "X-RateLimit-Limit": "5000",
            "X-RateLimit-Reset": "1711627200",
        })
        assert state.remaining == 42
        assert state.limit == 5000
        assert state.reset_at == 1711627200.0

    def test_update_ignores_missing_headers(self):
        state = RateLimitState()
        state.update({"Content-Type": "application/json"})
        assert state.remaining is None
        assert state.limit is None

    def test_preemptive_wait_none_when_plenty(self):
        state = RateLimitState()
        state.update({
            "X-RateLimit-Remaining": "4000",
            "X-RateLimit-Limit": "5000",
            "X-RateLimit-Reset": str(time.time() + 3600),
        })
        assert state.should_preemptive_wait(min_remaining=50) is None

    def test_preemptive_wait_returns_seconds_when_low(self):
        state = RateLimitState()
        reset_time = time.time() + 60
        state.update({
            "X-RateLimit-Remaining": "10",
            "X-RateLimit-Limit": "5000",
            "X-RateLimit-Reset": str(reset_time),
        })
        wait = state.should_preemptive_wait(min_remaining=50)
        assert wait is not None
        assert 50 < wait < 70  # ~61 seconds


class TestMakeSession:
    @responses.activate
    def test_successful_request(self):
        responses.add(
            responses.GET,
            "https://api.example.com/data",
            json={"ok": True},
            status=200,
        )
        session, state = make_session(requests_per_second=100, burst=100)
        resp = session.get("https://api.example.com/data")
        assert resp.status_code == 200
        assert resp.json() == {"ok": True}

    @responses.activate
    def test_429_retry_with_retry_after(self):
        # First response: 429 with Retry-After
        responses.add(
            responses.GET,
            "https://api.example.com/data",
            status=429,
            headers={"Retry-After": "0"},
        )
        # Second response: success
        responses.add(
            responses.GET,
            "https://api.example.com/data",
            json={"ok": True},
            status=200,
        )
        session, state = make_session(requests_per_second=100, burst=100)
        resp = session.get("https://api.example.com/data")
        assert resp.status_code == 200
        assert len(responses.calls) == 2

    @responses.activate
    def test_429_exhausts_retries(self):
        for _ in range(6):
            responses.add(
                responses.GET,
                "https://api.example.com/data",
                status=429,
                headers={"Retry-After": "0"},
            )
        session, state = make_session(
            requests_per_second=100, burst=100,
        )
        resp = session.get("https://api.example.com/data")
        assert resp.status_code == 429

    @responses.activate
    def test_rate_limit_headers_tracked(self):
        responses.add(
            responses.GET,
            "https://api.example.com/data",
            json={"ok": True},
            status=200,
            headers={
                "X-RateLimit-Remaining": "99",
                "X-RateLimit-Limit": "100",
            },
        )
        session, state = make_session(requests_per_second=100, burst=100)
        session.get("https://api.example.com/data")
        assert state.remaining == 99
        assert state.limit == 100
