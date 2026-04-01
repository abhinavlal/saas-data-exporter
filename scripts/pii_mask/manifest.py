"""Manifest — output statistics for a masking pipeline run."""

import time
import logging
from datetime import datetime, timezone

log = logging.getLogger(__name__)


class Manifest:
    """Collects stats during a pipeline run and writes a summary JSON."""

    def __init__(self, source_bucket: str, destination_bucket: str):
        self.source_bucket = source_bucket
        self.destination_bucket = destination_bucket
        self.started_at = datetime.now(timezone.utc).isoformat()
        self._start_time = time.monotonic()
        self.stats: dict[str, dict] = {}
        self.total_files = 0
        self.masked_files = 0
        self.skipped_files = 0
        self.failed_files = 0

    def record(self, masker_name: str, status: str) -> None:
        if masker_name not in self.stats:
            self.stats[masker_name] = {
                "total": 0, "masked": 0, "skipped": 0, "failed": 0}
        s = self.stats[masker_name]
        s["total"] += 1
        self.total_files += 1
        if status == "ok":
            s["masked"] += 1
            self.masked_files += 1
        elif status.startswith("skipped"):
            s["skipped"] += 1
            self.skipped_files += 1
        else:
            s["failed"] += 1
            self.failed_files += 1

    def to_dict(self) -> dict:
        elapsed = time.monotonic() - self._start_time
        return {
            "source_bucket": self.source_bucket,
            "destination_bucket": self.destination_bucket,
            "started_at": self.started_at,
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "duration_seconds": round(elapsed, 1),
            "total_files": self.total_files,
            "masked_files": self.masked_files,
            "skipped_files": self.skipped_files,
            "failed_files": self.failed_files,
            "stats_by_exporter": self.stats,
        }
