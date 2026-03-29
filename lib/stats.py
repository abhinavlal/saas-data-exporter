"""Crash-resistant statistics collector with periodic S3 persistence."""

import logging
import time
from datetime import datetime, timezone

log = logging.getLogger(__name__)

SAVE_INTERVAL = 30  # seconds — match checkpoint cadence


class StatsCollector:
    """Accumulate export statistics with throttled S3 persistence.

    Follows the same periodic-save pattern as CheckpointManager: saves are
    throttled to at most once per ``save_interval`` seconds unless
    ``force=True`` is passed.  On crash recovery, call ``load()`` to restore
    previously persisted counts so that checkpoint-gated items (already
    skipped by the exporter) are not lost.

    Usage::

        stats = StatsCollector(s3, f"{s3_base}/_stats.json")
        stats.load()                              # restore after crash
        stats.set("exporter", "github")
        stats.increment("commits.total")
        stats.add_to_map("languages", "Python", 150000)
        stats.save()                              # throttled
        stats.save(force=True)                    # immediate (end of phase)
    """

    def __init__(self, s3, s3_path: str, save_interval: int = SAVE_INTERVAL):
        self._s3 = s3
        self._s3_path = s3_path
        self._save_interval = save_interval
        self._last_save: float = 0.0
        self.data: dict = {}

    def load(self) -> None:
        """Load existing stats from S3 (crash recovery)."""
        existing = self._s3.download_json(self._s3_path)
        if existing and isinstance(existing, dict):
            self.data = existing
            log.info("Loaded existing stats from %s", self._s3_path)

    def save(self, force: bool = False) -> None:
        """Save stats to S3.  Throttled unless *force* is True."""
        now = time.monotonic()
        if not force and (now - self._last_save) < self._save_interval:
            return
        self.data["updated_at"] = datetime.now(timezone.utc).isoformat()
        self._s3.upload_json(self.data, self._s3_path)
        self._last_save = now

    # -- Mutators ----------------------------------------------------------

    def set(self, key: str, value) -> None:
        """Set a top-level key."""
        self.data[key] = value

    def increment(self, path: str, by: int = 1) -> None:
        """Increment a nested counter.  Path uses dots: ``'commits.total'``."""
        keys = path.split(".")
        d = self.data
        for k in keys[:-1]:
            d = d.setdefault(k, {})
        d[keys[-1]] = d.get(keys[-1], 0) + by

    def add_to_map(self, path: str, key: str, value: int = 1) -> None:
        """Increment *key* inside a nested map.

        Example: ``add_to_map("pull_requests.by_state", "open")``
        """
        parts = path.split(".")
        d = self.data
        for p in parts:
            d = d.setdefault(p, {})
        d[key] = d.get(key, 0) + value

    def set_nested(self, path: str, value) -> None:
        """Set a value at a dotted path: ``set_nested("repo.stars", 1234)``."""
        keys = path.split(".")
        d = self.data
        for k in keys[:-1]:
            d = d.setdefault(k, {})
        d[keys[-1]] = value

    def get(self, path: str, default=None):
        """Get a value at a dotted path."""
        keys = path.split(".")
        d = self.data
        for k in keys:
            if not isinstance(d, dict):
                return default
            d = d.get(k)
            if d is None:
                return default
        return d
