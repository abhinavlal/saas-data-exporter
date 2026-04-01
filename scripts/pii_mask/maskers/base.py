"""BaseMasker — shared infrastructure for per-exporter maskers."""

import logging

from scripts.pii_mask.roster import Roster
from scripts.pii_mask.scanner import TextScanner
from lib.s3 import S3Store

log = logging.getLogger(__name__)


class BaseMasker:
    """Base class for per-exporter maskers.

    Subclasses declare which S3 prefix they handle and implement
    ``mask_file`` for per-file masking logic.
    """

    prefix: str = ""  # S3 prefix, e.g. "github/"

    def __init__(self, roster: Roster, scanner: TextScanner):
        self.roster = roster
        self.scanner = scanner

    def list_keys(self, src: S3Store) -> list[str]:
        """List S3 keys this masker should process."""
        return [k for k in src.list_keys(self.prefix)
                if self.should_process(k)]

    def should_process(self, key: str) -> bool:
        """Whether this masker should handle the given S3 key."""
        return key.endswith(".json")

    def mask_file(self, src: S3Store, dst: S3Store, key: str) -> str:
        """Download, mask, and upload one file. Returns status string."""
        raise NotImplementedError

    def rewrite_key(self, key: str) -> str:
        """Rewrite S3 key for destination bucket.

        Default: replace real domains with masked domains.
        """
        result = key
        for real_domain, fake_domain in self.roster.domain_map.items():
            result = result.replace(real_domain, fake_domain)
        return result

    def _replace_domains_in_obj(self, obj):
        """Recursively replace domains in all string values."""
        if isinstance(obj, str):
            result = obj
            for real, fake in self.roster.domain_map.items():
                result = result.replace(real, fake)
                # Also handle capitalized variants
                result = result.replace(real.capitalize(), fake.capitalize())
            return result
        if isinstance(obj, dict):
            return {k: self._replace_domains_in_obj(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self._replace_domains_in_obj(v) for v in obj]
        return obj
