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
    def test_round_robin_across_apps(self, rsa_key_file):
        _mock_installation_token(installation_id="111", token="ghs_app1")
        _mock_installation_token(installation_id="222", token="ghs_app2")

        auth1 = GitHubAppAuth("app1", rsa_key_file, "111")
        auth2 = GitHubAppAuth("app2", rsa_key_file, "222")
        pool = GitHubAppPool([auth1, auth2])

        t1 = pool.get_token()
        t2 = pool.get_token()
        t3 = pool.get_token()

        assert t1 == "ghs_app1"
        assert t2 == "ghs_app2"
        assert t3 == "ghs_app1"  # wraps around

    @responses.activate
    def test_get_auth_for_index(self, rsa_key_file):
        _mock_installation_token(installation_id="111", token="ghs_app1")
        _mock_installation_token(installation_id="222", token="ghs_app2")

        auth1 = GitHubAppAuth("app1", rsa_key_file, "111")
        auth2 = GitHubAppAuth("app2", rsa_key_file, "222")
        pool = GitHubAppPool([auth1, auth2])

        assert pool.get_auth_for_index(0).get_token() == "ghs_app1"
        assert pool.get_auth_for_index(1).get_token() == "ghs_app2"
        assert pool.get_auth_for_index(2).get_token() == "ghs_app1"  # wraps

    def test_empty_pool_raises(self):
        with pytest.raises(ValueError):
            GitHubAppPool([])

    @responses.activate
    def test_len(self, rsa_key_file):
        auth1 = GitHubAppAuth("app1", rsa_key_file, "111")
        pool = GitHubAppPool([auth1])
        assert len(pool) == 1
