"""Tests for lib.input — CSV input reader."""

import pytest

from lib.input import read_csv_column


class TestReadCsvColumn:
    def test_reads_column(self, tmp_path):
        csv_file = tmp_path / "repos.csv"
        csv_file.write_text("repo\nowner/repo-1\nowner/repo-2\n")
        result = read_csv_column(csv_file, "repo")
        assert result == ["owner/repo-1", "owner/repo-2"]

    def test_skips_empty_values(self, tmp_path):
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("project\nIES\n\nPLAT\n")
        result = read_csv_column(csv_file, "project")
        assert result == ["IES", "PLAT"]

    def test_strips_whitespace(self, tmp_path):
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("channel_id\n  C123  \n C456 \n")
        result = read_csv_column(csv_file, "channel_id")
        assert result == ["C123", "C456"]

    def test_raises_on_missing_column(self, tmp_path):
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("name\nAlice\n")
        with pytest.raises(ValueError, match="Column 'repo' not found"):
            read_csv_column(csv_file, "repo")

    def test_error_message_lists_available_columns(self, tmp_path):
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("name,email\nAlice,a@b.com\n")
        with pytest.raises(ValueError, match="name, email"):
            read_csv_column(csv_file, "repo")

    def test_works_with_extra_columns(self, tmp_path):
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("channel_id,channel_name,notes\nC123,general,main channel\nC456,random,\n")
        result = read_csv_column(csv_file, "channel_id")
        assert result == ["C123", "C456"]

    def test_empty_file_returns_empty(self, tmp_path):
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("user\n")
        result = read_csv_column(csv_file, "user")
        assert result == []
