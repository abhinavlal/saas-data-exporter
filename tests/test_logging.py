"""Tests for lib.logging — setup_logging with file handler."""

import json
import logging
import os

import pytest

from lib.logging import setup_logging, JSONFormatter


class TestSetupLoggingStderr:
    def test_creates_stderr_handler(self):
        setup_logging(level="INFO", json_output=True)
        assert len(logging.root.handlers) == 1
        assert isinstance(logging.root.handlers[0], logging.StreamHandler)

    def test_sets_level(self):
        setup_logging(level="DEBUG")
        assert logging.root.level == logging.DEBUG

    def test_json_formatter(self):
        setup_logging(level="INFO", json_output=True)
        assert isinstance(logging.root.handlers[0].formatter, JSONFormatter)

    def test_plain_formatter(self):
        setup_logging(level="INFO", json_output=False)
        assert not isinstance(logging.root.handlers[0].formatter, JSONFormatter)


class TestSetupLoggingFile:
    def test_adds_file_handler(self, tmp_path):
        log_file = str(tmp_path / "test.log")
        setup_logging(level="INFO", json_output=True, log_file=log_file)
        assert len(logging.root.handlers) == 2
        assert isinstance(logging.root.handlers[1], logging.FileHandler)

    def test_creates_log_dir(self, tmp_path):
        log_dir = tmp_path / "nested" / "logs"
        log_file = str(log_dir / "test.log")
        setup_logging(level="INFO", log_file=log_file)
        assert log_dir.exists()

    def test_writes_json_to_file(self, tmp_path):
        log_file = str(tmp_path / "test.log")
        setup_logging(level="INFO", json_output=True, log_file=log_file)

        log = logging.getLogger("test.json_output")
        log.info("hello from test")

        # Close file handler so content is flushed
        for h in logging.root.handlers:
            if isinstance(h, logging.FileHandler):
                h.flush()

        content = open(log_file).read()
        assert content.strip()
        entry = json.loads(content.strip().split("\n")[-1])
        assert entry["level"] == "INFO"
        assert entry["msg"] == "hello from test"

    def test_writes_plain_to_file(self, tmp_path):
        log_file = str(tmp_path / "test.log")
        setup_logging(level="INFO", json_output=False, log_file=log_file)

        log = logging.getLogger("test.plain_output")
        log.info("plain message")

        for h in logging.root.handlers:
            if isinstance(h, logging.FileHandler):
                h.flush()

        content = open(log_file).read()
        assert "plain message" in content
        assert "INFO" in content

    def test_no_file_handler_when_none(self):
        setup_logging(level="INFO", log_file=None)
        assert len(logging.root.handlers) == 1
        assert not any(
            isinstance(h, logging.FileHandler) for h in logging.root.handlers
        )
