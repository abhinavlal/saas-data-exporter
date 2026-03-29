"""CSV input reader for target lists (repos, projects, channels, users)."""

import csv
import logging
import re
from pathlib import Path

log = logging.getLogger(__name__)

# Minimum pattern: at least one alphanumeric character
_VALID_VALUE_RE = re.compile(r"[a-zA-Z0-9]")


def read_csv_column(path: str | Path, column: str) -> list[str]:
    """Read a single column from a CSV file, skipping empty and malformed values."""
    items = []
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        if column not in (reader.fieldnames or []):
            available = ", ".join(reader.fieldnames or [])
            raise ValueError(
                f"Column '{column}' not found in {path}. Available columns: {available}"
            )
        for line_num, row in enumerate(reader, start=2):
            val = row.get(column, "").strip()
            if not val:
                continue
            if not _VALID_VALUE_RE.search(val):
                log.warning("Skipping malformed value %r at %s line %d", val, path, line_num)
                continue
            items.append(val)
    return items
