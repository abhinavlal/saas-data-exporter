"""Pipeline — orchestrator for roster-based PII masking.

Iterates over all registered maskers, lists their S3 keys, and
runs mask_file for each key with ProcessPoolExecutor parallelism
and per-masker checkpoint phases.

Uses processes (not threads) because spaCy NER is CPU-bound and
Python's GIL prevents threads from parallelizing CPU work.
"""

import logging
from concurrent.futures import ProcessPoolExecutor, as_completed

from lib.checkpoint import CheckpointManager
from lib.s3 import S3Store
from scripts.pii_mask.manifest import Manifest
from scripts.pii_mask.maskers.base import BaseMasker

log = logging.getLogger(__name__)

DEFAULT_WORKERS = 32


# -- Worker process state -------------------------------------------------- #
# Each worker process initializes its own scanner, S3 clients, and maskers.
# State lives in a module-level dict because ProcessPoolExecutor workers
# are long-lived and reuse their state across submitted tasks.

_w: dict = {}


def _init_worker(store_path, threshold, src_bucket, src_prefix,
                 dst_bucket, dst_prefix):
    """Initialize PII scanner + S3 clients in each worker process."""
    from scripts.pii_mask.pii_store import PIIStore
    from scripts.pii_mask.scanner import TextScanner

    store = PIIStore(store_path)
    _w["scanner"] = TextScanner(store, threshold=threshold)
    _w["src"] = S3Store(bucket=src_bucket, prefix=src_prefix)
    _w["dst"] = S3Store(bucket=dst_bucket, prefix=dst_prefix)
    _w["maskers"] = {}


def _get_masker(name):
    """Lazily create a masker for the given exporter name."""
    if name not in _w["maskers"]:
        from scripts.pii_mask.maskers.github import GitHubMasker
        from scripts.pii_mask.maskers.jira import JiraMasker
        from scripts.pii_mask.maskers.confluence import ConfluenceMasker
        from scripts.pii_mask.maskers.slack import SlackMasker
        from scripts.pii_mask.maskers.google import GoogleMasker

        classes = {
            "github": GitHubMasker,
            "jira": JiraMasker,
            "confluence": ConfluenceMasker,
            "slack": SlackMasker,
            "google": GoogleMasker,
        }
        _w["maskers"][name] = classes[name](_w["scanner"])
    return _w["maskers"][name]


def _mask_file(args):
    """Mask one file in a worker process. Returns status string."""
    masker_name, key = args
    masker = _get_masker(masker_name)
    return masker.mask_file(_w["src"], _w["dst"], key)


# -- Pipeline -------------------------------------------------------------- #

def run_pipeline(
    src: S3Store,
    dst: S3Store,
    maskers: list[BaseMasker],
    checkpoint: CheckpointManager,
    manifest: Manifest,
    max_workers: int = DEFAULT_WORKERS,
    store_path: str = "",
    threshold: float = 0.5,
):
    """Run the masking pipeline across all registered maskers.

    Each masker gets its own checkpoint phase: ``mask/{prefix}``.
    Files are processed in parallel with ProcessPoolExecutor.
    """
    pool = None
    if max_workers > 1 and store_path:
        log.info("Creating process pool with %d workers...", max_workers)
        pool = ProcessPoolExecutor(
            max_workers=max_workers,
            initializer=_init_worker,
            initargs=(
                store_path,
                threshold,
                src.bucket,
                src.prefix,
                dst.bucket,
                dst.prefix,
            ),
        )

    try:
        for masker in maskers:
            name = masker.prefix.strip("/")
            phase = f"mask/{name}"

            if checkpoint.is_phase_done(phase):
                log.info("Skipping %s — already done", name)
                continue

            keys = masker.list_keys(src)
            log.info("Found %d files for %s", len(keys), name)

            checkpoint.start_phase(phase, total=len(keys))
            to_mask = [k for k in keys
                       if not checkpoint.is_item_done(phase, k)]
            done = len(keys) - len(to_mask)
            log.info("Masking %d files for %s (%d already done, workers=%d)",
                     len(to_mask), name, done, max_workers)

            if to_mask and pool is not None:
                _run_parallel(pool, name, to_mask, checkpoint,
                              phase, manifest)
            elif to_mask:
                _run_serial(src, dst, masker, to_mask, checkpoint,
                            phase, manifest, name)

            checkpoint.complete_phase(phase)
            checkpoint.save(force=True)
            log.info("Completed %s — %d files", name, len(keys))
    finally:
        if pool is not None:
            pool.shutdown(wait=True)

    # Write manifest
    dst.upload_json(manifest.to_dict(), "_manifest/pii_mask.json")
    log.info("Manifest written to _manifest/pii_mask.json")

    checkpoint.complete()
    log.info("Pipeline complete — %d total files", manifest.total_files)


def _run_parallel(pool, masker_name, keys, checkpoint, phase, manifest):
    futures = {
        pool.submit(_mask_file, (masker_name, key)): key
        for key in keys
    }
    completed = 0
    for future in as_completed(futures):
        key = futures[future]
        try:
            status = future.result()
            manifest.record(masker_name, status)
            completed += 1
            if completed % 1000 == 0:
                log.info("Progress %s: %d/%d files",
                         masker_name, completed, len(keys))
        except Exception:
            log.error("Failed %s", key, exc_info=True)
            manifest.record(masker_name, "error")

        checkpoint.mark_item_done(phase, key)
        checkpoint.save()


def _run_serial(src, dst, masker, keys, checkpoint, phase,
                manifest, name):
    for key in keys:
        try:
            status = masker.mask_file(src, dst, key)
            manifest.record(name, status)
        except Exception:
            log.error("Failed %s", key, exc_info=True)
            manifest.record(name, "error")
        checkpoint.mark_item_done(phase, key)
        checkpoint.save()
