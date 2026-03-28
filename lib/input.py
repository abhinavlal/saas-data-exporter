"""CSV input reader for target lists (repos, projects, channels, users)."""

import csv
from pathlib import Path


def read_csv_column(path: str | Path, column: str) -> list[str]:
    """Read a single column from a CSV file, skipping empty values."""
    items = []
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        if column not in (reader.fieldnames or []):
            available = ", ".join(reader.fieldnames or [])
            raise ValueError(
                f"Column '{column}' not found in {path}. Available columns: {available}"
            )
        for row in reader:
            val = row.get(column, "").strip()
            if val:
                items.append(val)
    return items
