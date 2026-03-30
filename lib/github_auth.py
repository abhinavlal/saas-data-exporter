"""GitHub App authentication — JWT signing and installation token management.

Provides higher API rate limits than PATs:
- PAT: 5,000 requests/hour (per user, shared across all tokens)
- GitHub App: 5,000–12,500 requests/hour per installation (independent pool)
- Multiple Apps: each gets its own pool — 2 apps = up to 25,000 req/hr

Usage::

    auth = GitHubAppAuth(
        app_id="123456",
        private_key_path="github-app.pem",
        installation_id="78901234",
    )
    token = auth.get_token()   # auto-refreshes when near expiry

    # Multiple apps for round-robin:
    pool = GitHubAppPool([auth1, auth2])
    token = pool.get_token()   # rotates across apps
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
    """Round-robin pool of GitHub App installations for multiplied rate limits.

    Each app has its own independent rate limit pool (up to 12,500 req/hr),
    so N apps = N × 12,500 req/hr throughput.
    """

    def __init__(self, apps: list[GitHubAppAuth]):
        if not apps:
            raise ValueError("At least one GitHubAppAuth is required")
        self._apps = apps
        self._index = 0
        self._lock = threading.Lock()

    def get_token(self, index: int | None = None) -> str:
        """Get a token from a specific app (by index) or round-robin."""
        if index is not None:
            return self._apps[index % len(self._apps)].get_token()
        with self._lock:
            app = self._apps[self._index % len(self._apps)]
            self._index += 1
        return app.get_token()

    def __len__(self) -> int:
        return len(self._apps)

    def get_auth_for_index(self, index: int) -> 'GitHubAppAuth':
        """Get a specific app auth instance for dedicated use by a repo."""
        return self._apps[index % len(self._apps)]
