"""Checkpoint manager for resumable exports.

Checkpoint structure:
{
    "job_id": "github/owner__repo",
    "status": "in_progress",
    "started_at": "2026-03-28T10:00:00Z",
    "updated_at": "2026-03-28T10:05:00Z",
    "phases": {
        "commits": {
            "status": "completed",
            "total": 500,
            "completed": 500,
            "cursor": null,
            "completed_ids": []
        },
        "pull_requests": {
            "status": "in_progress",
            "total": 200,
            "completed": 73,
            "cursor": "page_3",
            "completed_ids": [1, 2, 3, ...]
        }
    }
}
"""

import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

from lib.s3 import S3Store


@dataclass
class PhaseState:
    status: str = "pending"
    total: int | None = None
    completed: int = 0
    cursor: str | None = None
    completed_ids: set = field(default_factory=set)


class CheckpointManager:
    """
    Manages checkpoint state in S3.

    Usage:
        cp = CheckpointManager(s3_store, "github/owner__repo")
        cp.load()
        if cp.is_phase_done("commits"):
            skip...
        cp.start_phase("pull_requests", total=200)
        for pr in prs:
            if cp.is_item_done("pull_requests", pr["number"]):
                continue
            cp.mark_item_done("pull_requests", pr["number"])
            cp.save()
        cp.complete_phase("pull_requests")
        cp.save()
    """

    SAVE_INTERVAL = 30  # seconds

    def __init__(self, s3: S3Store, job_id: str):
        self._s3 = s3
        self.job_id = job_id
        self._s3_path = f"_checkpoints/{job_id}.json"
        self.phases: dict[str, PhaseState] = {}
        self.status = "pending"
        self.started_at: str | None = None
        self.updated_at: str | None = None
        self._last_save_time = 0.0
        self._lock = threading.Lock()

    def load(self) -> bool:
        """Load checkpoint from S3. Returns True if a checkpoint existed."""
        data = self._s3.download_json(self._s3_path)
        if data is None:
            self.status = "in_progress"
            self.started_at = datetime.now(timezone.utc).isoformat()
            return False

        self.status = data["status"]
        self.started_at = data["started_at"]
        self.updated_at = data.get("updated_at")
        for name, phase_data in data.get("phases", {}).items():
            self.phases[name] = PhaseState(
                status=phase_data["status"],
                total=phase_data.get("total"),
                completed=phase_data.get("completed", 0),
                cursor=phase_data.get("cursor"),
                completed_ids=set(phase_data.get("completed_ids", [])),
            )
        return True

    def save(self, force: bool = False) -> None:
        """Save checkpoint to S3. Throttled to SAVE_INTERVAL unless force=True.
        Thread-safe — serializes concurrent saves."""
        with self._lock:
            now = time.monotonic()
            if not force and (now - self._last_save_time) < self.SAVE_INTERVAL:
                return
            self.updated_at = datetime.now(timezone.utc).isoformat()
            data = {
                "job_id": self.job_id,
                "status": self.status,
                "started_at": self.started_at,
                "updated_at": self.updated_at,
                "phases": {},
            }
            for name, phase in self.phases.items():
                data["phases"][name] = {
                    "status": phase.status,
                    "total": phase.total,
                    "completed": phase.completed,
                    "cursor": phase.cursor,
                    "completed_ids": list(phase.completed_ids),
                }
            self._s3.upload_json(data, self._s3_path)
            self._last_save_time = now

    def start_phase(self, name: str, total: int | None = None) -> None:
        if name not in self.phases:
            self.phases[name] = PhaseState()
        self.phases[name].status = "in_progress"
        if total is not None:
            self.phases[name].total = total

    def complete_phase(self, name: str) -> None:
        if name not in self.phases:
            self.phases[name] = PhaseState()
        self.phases[name].status = "completed"

    def is_phase_done(self, name: str) -> bool:
        return name in self.phases and self.phases[name].status == "completed"

    def is_item_done(self, phase: str, item_id) -> bool:
        return phase in self.phases and item_id in self.phases[phase].completed_ids

    def mark_item_done(self, phase: str, item_id) -> None:
        with self._lock:
            if phase not in self.phases:
                self.phases[phase] = PhaseState(status="in_progress")
            if item_id not in self.phases[phase].completed_ids:
                self.phases[phase].completed_ids.add(item_id)
                self.phases[phase].completed += 1

    def set_cursor(self, phase: str, cursor: str | None) -> None:
        if phase not in self.phases:
            self.phases[phase] = PhaseState(status="in_progress")
        self.phases[phase].cursor = cursor

    def get_cursor(self, phase: str) -> str | None:
        if phase in self.phases:
            return self.phases[phase].cursor
        return None

    def complete(self) -> None:
        self.status = "completed"
        self.save(force=True)
