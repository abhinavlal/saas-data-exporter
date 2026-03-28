"""Shared types and configuration dataclasses."""

from dataclasses import dataclass


@dataclass
class ExportConfig:
    """Common configuration for all exporters."""
    s3_bucket: str
    s3_prefix: str = ""
    max_workers: int = 5
    log_level: str = "INFO"
    json_logs: bool = True
