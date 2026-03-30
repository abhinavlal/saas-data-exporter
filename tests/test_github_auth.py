"""Tests for lib.github_auth — GitHubAppAuth and GitHubAppPool."""

import json
import time

import pytest
import responses
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization

from lib.github_auth import GitHubAppAuth, GitHubAppPool

API_BASE = "https://api.github.com"


# ── Fixtures ──────────────────────────────────────────────────────────────

@pytest.fixture
def rsa_key_file(tmp_path):
    """Generate a temporary RSA private key file for testing."""
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    key_path = tmp_path / "test-app.pem"
    key_path.write_bytes(pem)
    return str(key_path)


def _mock_installation_token(installation_id="12345", token="ghs_test_token_123"):
    """Mock the installation token exchange endpoint."""
    expires_at = time.strftime(
        "%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() + 3600)
    )
    responses.add(
        responses.POST,
        f"{API_BASE}/app/installations/{installation_id}/access_tokens",
        json={"token": token, "expires_at": expires_at},
        status=201,
    )


# ── Tests ────────────────────────────────────────────────────────────────

class TestGitHubAppAuth:
    @responses.activate
    def test_get_token_returns_installation_token(self, rsa_key_file):
        _mock_installation_token()
        auth = GitHubAppAuth(
            app_id="99999",
            private_key_path=rsa_key_file,
            installation_id="12345",
        )
        token = auth.get_token()
        assert token == "ghs_test_token_123"

    @responses.activate
    def test_get_token_caches_until_near_expiry(self, rsa_key_file):
        _mock_installation_token()
        auth = GitHubAppAuth(
            app_id="99999",
            private_key_path=rsa_key_file,
            installation_id="12345",
        )
        token1 = auth.get_token()
        token2 = auth.get_token()
        # Should reuse cached token — only 1 API call
        assert token1 == token2
        assert len(responses.calls) == 1

    @responses.activate
    def test_get_token_refreshes_when_expired(self, rsa_key_file):
        _mock_installation_token(token="ghs_first")
        _mock_installation_token(token="ghs_second")
        auth = GitHubAppAuth(
            app_id="99999",
            private_key_path=rsa_key_file,
            installation_id="12345",
        )
        token1 = auth.get_token()
        assert token1 == "ghs_first"

        # Force expiry
        auth._expires_at = time.time() - 1
        token2 = auth.get_token()
        assert token2 == "ghs_second"
        assert len(responses.calls) == 2

    @responses.activate
    def test_jwt_sent_in_exchange_request(self, rsa_key_file):
        _mock_installation_token()
        auth = GitHubAppAuth(
            app_id="99999",
            private_key_path=rsa_key_file,
            installation_id="12345",
        )
        auth.get_token()
        # Verify the exchange request used Bearer JWT auth
        assert len(responses.calls) == 1
        auth_header = responses.calls[0].request.headers["Authorization"]
        assert auth_header.startswith("Bearer ey")  # JWT starts with ey


class TestGitHubAppPool:
    @responses.activate
    def test_get_best_token_returns_valid_token(self, rsa_key_file):
        _mock_installation_token(installation_id="111", token="ghs_app1")
        _mock_installation_token(installation_id="222", token="ghs_app2")

        auth1 = GitHubAppAuth("app1", rsa_key_file, "111")
        auth2 = GitHubAppAuth("app2", rsa_key_file, "222")
        pool = GitHubAppPool([auth1, auth2])

        token = pool.get_best_token()
        assert token in ("ghs_app1", "ghs_app2")

    @responses.activate
    def test_shifts_to_other_app_when_exhausted(self, rsa_key_file):
        _mock_installation_token(installation_id="111", token="ghs_app1")
        _mock_installation_token(installation_id="222", token="ghs_app2")

        auth1 = GitHubAppAuth("app1", rsa_key_file, "111")
        auth2 = GitHubAppAuth("app2", rsa_key_file, "222")
        pool = GitHubAppPool([auth1, auth2])

        # Mark app1 as exhausted
        pool.update_remaining("ghs_app1", 0, time.time() + 3600)

        # Should pick app2 since app1 has 0 remaining
        token = pool.get_best_token()
        assert token == "ghs_app2"

    @responses.activate
    def test_update_remaining_tracks_budget(self, rsa_key_file):
        _mock_installation_token(installation_id="111", token="ghs_app1")

        auth1 = GitHubAppAuth("app1", rsa_key_file, "111")
        pool = GitHubAppPool([auth1])

        pool.update_remaining("ghs_app1", 5000, time.time() + 3600)
        # Should still return the token (has budget)
        assert pool.get_best_token() == "ghs_app1"

    def test_empty_pool_raises(self):
        with pytest.raises(ValueError):
            GitHubAppPool([])

    @responses.activate
    def test_len(self, rsa_key_file):
        auth1 = GitHubAppAuth("app1", rsa_key_file, "111")
        pool = GitHubAppPool([auth1])
        assert len(pool) == 1
