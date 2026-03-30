"""GitHub App authentication — JWT signing and installation token management.

Provides higher API rate limits than PATs:
- PAT: 5,000 requests/hour (per user, shared across all tokens)
- GitHub App: 5,000–12,500 requests/hour per installation (independent pool)
- Multiple Apps: each gets its own pool — 4 apps = up to 50,000 req/hr

Usage::

    auth = GitHubAppAuth(
        app_id="123456",
        private_key_path="github-app.pem",
        installation_id="78901234",
    )
    token = auth.get_token()   # auto-refreshes when near expiry

    # Multiple apps — shared pool picks the best token per request:
    pool = GitHubAppPool([auth1, auth2, auth3, auth4])
    token = pool.get_best_token()  # picks app with most remaining budget
    pool.update_remaining(token, remaining, reset)  # feed back rate limit headers
"""

import logging
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import jwt
import requests

log = logging.getLogger(__name__)

API_BASE = "https://api.github.com"

# Refresh token when it has less than this many seconds remaining
REFRESH_BUFFER_SECONDS = 300  # 5 minutes


class GitHubAppAuth:
    """Manages a single GitHub App's installation token lifecycle."""

    def __init__(self, app_id: str, private_key_path: str,
                 installation_id: str):
        self.app_id = app_id
        self.installation_id = installation_id
        self._private_key = Path(private_key_path).read_text()
        self._token: str | None = None
        self._expires_at: float = 0.0
        self._lock = threading.Lock()

    def _build_jwt(self) -> str:
        """Build a short-lived JWT for authenticating as the GitHub App."""
        now = int(time.time())
        payload = {
            "iat": now - 60,       # issued at (60s clock skew buffer)
            "exp": now + 600,      # expires in 10 minutes (max allowed)
            "iss": self.app_id,
        }
        return jwt.encode(payload, self._private_key, algorithm="RS256")

    def _exchange_for_installation_token(self) -> tuple[str, float]:
        """Exchange JWT for an installation access token (1 hour lifetime)."""
        app_jwt = self._build_jwt()
        resp = requests.post(
            f"{API_BASE}/app/installations/{self.installation_id}/access_tokens",
            headers={
                "Authorization": f"Bearer {app_jwt}",
                "Accept": "application/vnd.github+json",
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        token = data["token"]
        expires_at = datetime.fromisoformat(
            data["expires_at"].replace("Z", "+00:00")
        ).timestamp()
        return token, expires_at

    def get_token(self) -> str:
        """Return a valid installation token, refreshing if needed.

        Thread-safe — multiple worker threads can call this concurrently.
        """
        with self._lock:
            now = time.time()
            if self._token and (self._expires_at - now) > REFRESH_BUFFER_SECONDS:
                return self._token

            log.info("Refreshing GitHub App installation token (app_id=%s, installation=%s)",
                     self.app_id, self.installation_id)
            self._token, self._expires_at = self._exchange_for_installation_token()
            remaining_min = (self._expires_at - time.time()) / 60
            log.info("Token refreshed, expires in %.0f minutes", remaining_min)
            return self._token


class GitHubAppPool:
    """Shared pool of GitHub App installations that picks the best token
    per request based on remaining rate limit budget.

    Each app has its own independent rate limit pool (up to 12,500 req/hr).
    The pool tracks remaining budget from ``X-RateLimit-Remaining`` headers
    and routes each request to the app with the most headroom.  When an app
    is exhausted, requests seamlessly shift to other apps — no idle budget.

    Thread-safe: multiple exporter threads call ``get_best_token()``
    and ``update_remaining()`` concurrently.
    """

    def __init__(self, apps: list[GitHubAppAuth]):
        if not apps:
            raise ValueError("At least one GitHubAppAuth is required")
        self._apps = apps
        self._lock = threading.Lock()
        # Track remaining budget per app: token_str -> remaining count
        self._remaining: dict[str, int] = {}
        # Track reset time per app: token_str -> reset timestamp
        self._reset_at: dict[str, float] = {}
        # Map token_str back to app for logging
        self._token_to_app: dict[str, GitHubAppAuth] = {}

    def get_best_token(self) -> str:
        """Return the token with the most remaining budget.

        On first call for each app (before any headers are seen), assumes
        full budget (12,500).  Thread-safe.
        """
        with self._lock:
            best_token = None
            best_remaining = -1

            now = time.time()
            for app in self._apps:
                token = app.get_token()
                self._token_to_app[token] = app

                remaining = self._remaining.get(token, 12500)
                reset_at = self._reset_at.get(token, 0)

                # If past reset time, assume budget is restored
                if remaining <= 0 and now > reset_at:
                    remaining = 12500
                    self._remaining[token] = remaining

                if remaining > best_remaining:
                    best_remaining = remaining
                    best_token = token

            # Optimistic decrement — actual value updated by update_remaining()
            if best_token and best_remaining > 0:
                self._remaining[best_token] = best_remaining - 1

            if best_token:
                return best_token

        # Fallback: all exhausted, return the one that resets soonest
        return self._get_soonest_reset_token()

    def update_remaining(self, token: str, remaining: int,
                         reset: float | None = None) -> None:
        """Update the tracked remaining budget for an app from response headers.

        Call this after every API response with the ``X-RateLimit-Remaining``
        and ``X-RateLimit-Reset`` header values.
        """
        with self._lock:
            self._remaining[token] = remaining
            if reset is not None:
                self._reset_at[token] = reset

    def _get_soonest_reset_token(self) -> str:
        """When all apps are exhausted, return the one that resets soonest."""
        with self._lock:
            soonest_token = None
            soonest_reset = float("inf")
            for app in self._apps:
                token = app.get_token()
                reset = self._reset_at.get(token, 0)
                if reset < soonest_reset:
                    soonest_reset = reset
                    soonest_token = token
            return soonest_token or self._apps[0].get_token()

    def __len__(self) -> int:
        return len(self._apps)
