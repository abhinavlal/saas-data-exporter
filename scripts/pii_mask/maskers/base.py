"""BaseMasker — shared infrastructure for per-exporter maskers."""

import logging

from scripts.pii_mask.scanner import TextScanner
from lib.s3 import S3Store

log = logging.getLogger(__name__)


class BaseMasker:
    """Base class for per-exporter maskers.

    Skips exporter infrastructure files (_index.json, _stats.json).
    The ``_scan_obj`` method runs Presidio on every string value.
    """

    # Infrastructure files created by exporters — skip during masking.
    # These are checkpoint/index/stats files, not user data.
    _SKIP_FILENAMES = frozenset({
        "_index.json",
        "_stats.json",
    })
    _SKIP_PREFIXES = (
        "_checkpoints/",
        "_manifest/",
    )

    prefix: str = ""  # S3 prefix, e.g. "github/"

    def __init__(self, scanner: TextScanner):
        self.scanner = scanner

    def list_keys(self, src: S3Store) -> list[str]:
        """List S3 keys this masker should process.

        Default: full S3 listing. Subclasses should override with
        index-based enumeration to avoid listing millions of keys.
        """
        return [k for k in src.list_keys(self.prefix)
                if self.should_process(k)]

    def _list_entities(self, src: S3Store) -> list[str]:
        """List top-level entity dirs using S3 delimiter (no file enumeration).

        Returns entity names like ["org__repo", "PROJ", "C07FMF9U08M"].
        Single paginated S3 call with Delimiter='/' — O(entities), not O(files).
        """
        full_prefix = f"{src.prefix}/{self.prefix}" if src.prefix \
            else self.prefix
        paginator = src._client.get_paginator("list_objects_v2")
        entities = []
        for page in paginator.paginate(Bucket=src.bucket,
                                       Prefix=full_prefix,
                                       Delimiter="/"):
            for cp in page.get("CommonPrefixes", []):
                # "v31/github/org__repo/" → "org__repo"
                name = cp["Prefix"][len(full_prefix):].strip("/")
                if name:
                    entities.append(name)
        return sorted(entities)

    def should_process(self, key: str) -> bool:
        """Whether this masker should handle the given S3 key.

        Skips exporter infrastructure files (_index.json, _stats.json,
        _checkpoints/) — these are not user data.
        """
        if any(key.startswith(p) for p in self._SKIP_PREFIXES):
            return False
        filename = key.rsplit("/", 1)[-1]
        if filename in self._SKIP_FILENAMES:
            return False
        return key.endswith(".json") or key.endswith(".eml")

    def mask_file(self, src: S3Store, dst: S3Store, key: str) -> str:
        """Download, mask, and upload one file. Returns status string."""
        raise NotImplementedError

    def rewrite_key(self, key: str) -> str:
        """Rewrite S3 key for destination bucket."""
        return self.scanner.scan_url(key)

    def _scan_obj(self, obj):
        """Recursively run scanner.scan() on every string value.

        Single-pass per string: Presidio detects PII on the original,
        PIIStore provides consistent replacement, applied at once.
        No double-replacement risk.
        """
        if isinstance(obj, str):
            return self.scanner.scan(obj) if len(obj) >= 3 else obj
        if isinstance(obj, dict):
            return {k: self._scan_obj(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self._scan_obj(v) for v in obj]
        return obj
