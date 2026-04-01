"""Pipeline — orchestrator for roster-based PII masking.

Iterates over all registered maskers, lists their S3 keys, and
runs mask_file for each key with ThreadPoolExecutor parallelism
and per-masker checkpoint phases.
"""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from lib.checkpoint import CheckpointManager
from lib.s3 import S3Store
from scripts.pii_mask.manifest import Manifest
from scripts.pii_mask.maskers.base import BaseMasker

log = logging.getLogger(__name__)

DEFAULT_WORKERS = 32


def run_pipeline(
    src: S3Store,
    dst: S3Store,
    maskers: list[BaseMasker],
    checkpoint: CheckpointManager,
    manifest: Manifest,
    max_workers: int = DEFAULT_WORKERS,
):
    """Run the masking pipeline across all registered maskers.

    Each masker gets its own checkpoint phase: ``mask/{prefix}``.
    Files are processed in parallel with ThreadPoolExecutor.
    """
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

        if to_mask and max_workers > 1:
            _run_parallel(src, dst, masker, to_mask, checkpoint,
                          phase, manifest, name, max_workers)
        elif to_mask:
            _run_serial(src, dst, masker, to_mask, checkpoint,
                        phase, manifest, name)

        checkpoint.complete_phase(phase)
        checkpoint.save(force=True)
        log.info("Completed %s — %d files", name, len(keys))

    # Write manifest
    dst.upload_json(manifest.to_dict(), "_manifest/pii_mask.json")
    log.info("Manifest written to _manifest/pii_mask.json")

    checkpoint.complete()
    log.info("Pipeline complete — %d total files", manifest.total_files)


def _run_parallel(src, dst, masker, keys, checkpoint, phase,
                  manifest, name, max_workers):
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(masker.mask_file, src, dst, key): key
            for key in keys
        }
        completed = 0
        for future in as_completed(futures):
            key = futures[future]
            try:
                status = future.result()
                manifest.record(name, status)
                completed += 1
                if completed % 1000 == 0:
                    log.info("Progress %s: %d/%d files",
                             name, completed, len(keys))
            except Exception:
                log.error("Failed %s", key, exc_info=True)
                manifest.record(name, "error")

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
