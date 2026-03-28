"""Tests for lib.config — .env loading and env var helpers."""

import os
import tempfile

import pytest

from lib.config import load_dotenv, env, env_int, env_bool, env_list


class TestLoadDotenv:
    def test_loads_env_file(self, tmp_path):
        env_file = tmp_path / ".env"
        env_file.write_text("TEST_DOTENV_VAR=hello\n")

        # Ensure not already set
        os.environ.pop("TEST_DOTENV_VAR", None)

        load_dotenv(env_file)
        assert os.environ.get("TEST_DOTENV_VAR") == "hello"

        # Cleanup
        os.environ.pop("TEST_DOTENV_VAR", None)

    def test_does_not_override_existing(self, tmp_path):
        env_file = tmp_path / ".env"
        env_file.write_text("TEST_EXISTING=from_file\n")

        os.environ["TEST_EXISTING"] = "from_env"
        load_dotenv(env_file)
        assert os.environ["TEST_EXISTING"] == "from_env"

        os.environ.pop("TEST_EXISTING", None)

    def test_skips_comments_and_blanks(self, tmp_path):
        env_file = tmp_path / ".env"
        env_file.write_text("# comment\n\nTEST_REAL=value\n")

        os.environ.pop("TEST_REAL", None)
        load_dotenv(env_file)
        assert os.environ.get("TEST_REAL") == "value"

        os.environ.pop("TEST_REAL", None)

    def test_missing_file_is_noop(self, tmp_path):
        load_dotenv(tmp_path / "nonexistent.env")  # should not raise


class TestEnv:
    def test_returns_value(self):
        os.environ["TEST_ENV"] = "val"
        assert env("TEST_ENV") == "val"
        os.environ.pop("TEST_ENV")

    def test_returns_default_when_missing(self):
        os.environ.pop("TEST_MISSING", None)
        assert env("TEST_MISSING", "default") == "default"

    def test_returns_default_when_empty(self):
        os.environ["TEST_EMPTY"] = ""
        assert env("TEST_EMPTY", "fallback") == "fallback"
        os.environ.pop("TEST_EMPTY")


class TestEnvInt:
    def test_parses_int(self):
        os.environ["TEST_INT"] = "42"
        assert env_int("TEST_INT", 0) == 42
        os.environ.pop("TEST_INT")

    def test_returns_default_on_missing(self):
        os.environ.pop("TEST_NOINT", None)
        assert env_int("TEST_NOINT", 99) == 99

    def test_returns_default_on_invalid(self):
        os.environ["TEST_BADINT"] = "not_a_number"
        assert env_int("TEST_BADINT", 10) == 10
        os.environ.pop("TEST_BADINT")


class TestEnvBool:
    def test_true_values(self):
        for val in ("true", "True", "TRUE", "1", "yes"):
            os.environ["TEST_BOOL"] = val
            assert env_bool("TEST_BOOL") is True
        os.environ.pop("TEST_BOOL")

    def test_false_values(self):
        for val in ("false", "False", "0", "no"):
            os.environ["TEST_BOOL"] = val
            assert env_bool("TEST_BOOL", True) is False
        os.environ.pop("TEST_BOOL")

    def test_default(self):
        os.environ.pop("TEST_NOBOOL", None)
        assert env_bool("TEST_NOBOOL") is False
        assert env_bool("TEST_NOBOOL", True) is True


class TestEnvList:
    def test_parses_csv(self):
        os.environ["TEST_LIST"] = "a, b, c"
        assert env_list("TEST_LIST") == ["a", "b", "c"]
        os.environ.pop("TEST_LIST")

    def test_returns_default_on_missing(self):
        os.environ.pop("TEST_NOLIST", None)
        assert env_list("TEST_NOLIST") == []
        assert env_list("TEST_NOLIST", ["x"]) == ["x"]

    def test_strips_whitespace(self):
        os.environ["TEST_LIST2"] = " foo , bar "
        assert env_list("TEST_LIST2") == ["foo", "bar"]
        os.environ.pop("TEST_LIST2")
