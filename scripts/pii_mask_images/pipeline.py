"""Image masking pipeline — walks S3 for images, processes in parallel.

Independent from the text masking pipeline. Shares PIIStore and
TextScanner for consistent masking, but has its own checkpoint
phase and ProcessPoolExecutor.
"""

import logging
from concurrent.futures import ProcessPoolExecutor, as_completed

from lib.checkpoint import CheckpointManager
from lib.s3 import S3Store

log = logging.getLogger(__name__)

DEFAULT_WORKERS = 16

# S3 prefixes where images live, per exporter
IMAGE_PREFIXES = [
    "jira/",         # jira/{project}/attachments/{key}/{file}
    "slack/",        # slack/{channel}/attachments/{file_id}_{name}
    "confluence/",   # confluence/{space}/attachments/...
    "google/",       # google/{user}/drive/{file}
]


# -- Worker process state ------------------------------------------------- #

_w: dict = {}


def _init_worker(store_path, threshold, src_bucket, src_prefix,
                 dst_bucket, dst_prefix):
    """Initialize scanner + S3 clients in each worker process."""
    from scripts.pii_mask.pii_store import PIIStore
    from scripts.pii_mask.scanner import TextScanner

    store = PIIStore(store_path)
    _w["scanner"] = TextScanner(store, threshold=threshold)
    _w["src"] = S3Store(bucket=src_bucket, prefix=src_prefix)
    _w["dst"] = S3Store(bucket=dst_bucket, prefix=dst_prefix)


def _process_image(key):
    """Download, OCR, scan, blur, upload one image. Returns status."""
    from scripts.pii_mask_images.image import mask_image

    src, dst = _w["src"], _w["dst"]
    image_bytes = src.download_bytes(key)
    if image_bytes is None:
        return "skipped (not found)"

    try:
        masked = mask_image(image_bytes, _w["scanner"])
    except Exception:
        log.error("OCR/blur failed for %s", key, exc_info=True)
        return "error (processing)"

    if masked is not None:
        dst.upload_bytes(masked, key)
        return "ok (blurred)"
    else:
        dst.upload_bytes(image_bytes, key)
        return "ok (no pii)"


# -- Pipeline ------------------------------------------------------------- #

def list_image_keys(src: S3Store, prefixes: list[str] | None = None
                    ) -> list[str]:
    """List all image files under the given S3 prefixes."""
    from scripts.pii_mask_images.image import is_image

    prefixes = prefixes or IMAGE_PREFIXES
    keys = []
    for prefix in prefixes:
        for key in src.list_keys(prefix):
            if is_image(key):
                keys.append(key)
    log.info("Found %d image files across %d prefixes",
             len(keys), len(prefixes))
    return keys


def run_pipeline(
    src: S3Store,
    dst: S3Store,
    checkpoint: CheckpointManager,
    max_workers: int = DEFAULT_WORKERS,
    store_path: str = "",
    threshold: float = 0.5,
    prefixes: list[str] | None = None,
):
    """Walk S3 for images, OCR + blur PII, upload to destination."""
    phase = "mask/images"

    if checkpoint.is_phase_done(phase):
        log.info("Skipping images — already done")
        return

    keys = list_image_keys(src, prefixes)
    checkpoint.start_phase(phase, total=len(keys))

    to_process = [k for k in keys if not checkpoint.is_item_done(phase, k)]
    done = len(keys) - len(to_process)
    log.info("Processing %d images (%d already done, workers=%d)",
             len(to_process), done, max_workers)

    if not to_process:
        checkpoint.complete_phase(phase)
        checkpoint.complete()
        return

    if max_workers > 1 and store_path:
        _run_parallel(to_process, checkpoint, phase, max_workers,
                      store_path, threshold, src, dst)
    else:
        _run_serial(src, dst, to_process, checkpoint, phase,
                    store_path, threshold)

    checkpoint.complete_phase(phase)
    checkpoint.save(force=True)
    checkpoint.complete()
    log.info("Image pipeline complete — %d files", len(keys))


def _run_parallel(keys, checkpoint, phase, max_workers,
                  store_path, threshold, src, dst):
    log.info("Creating process pool with %d workers...", max_workers)
    with ProcessPoolExecutor(
        max_workers=max_workers,
        initializer=_init_worker,
        initargs=(store_path, threshold,
                  src.bucket, src.prefix,
                  dst.bucket, dst.prefix),
    ) as pool:
        futures = {
            pool.submit(_process_image, key): key
            for key in keys
        }
        completed = 0
        stats = {"blurred": 0, "no_pii": 0, "error": 0, "skipped": 0}
        for future in as_completed(futures):
            key = futures[future]
            try:
                status = future.result()
                if "blurred" in status:
                    stats["blurred"] += 1
                elif "no pii" in status:
                    stats["no_pii"] += 1
                elif "error" in status:
                    stats["error"] += 1
                else:
                    stats["skipped"] += 1
            except Exception:
                log.error("Failed %s", key, exc_info=True)
                stats["error"] += 1

            checkpoint.mark_item_done(phase, key)
            checkpoint.save()
            completed += 1
            if completed % 500 == 0:
                log.info("Progress: %d/%d images %s",
                         completed, len(keys), stats)

        log.info("Final stats: %s", stats)


def _run_serial(src, dst, keys, checkpoint, phase,
                store_path, threshold):
    from scripts.pii_mask.pii_store import PIIStore
    from scripts.pii_mask.scanner import TextScanner
    from scripts.pii_mask_images.image import mask_image

    store = PIIStore(store_path)
    scanner = TextScanner(store, threshold=threshold)

    for key in keys:
        try:
            image_bytes = src.download_bytes(key)
            if image_bytes is None:
                checkpoint.mark_item_done(phase, key)
                continue

            masked = mask_image(image_bytes, scanner)
            if masked is not None:
                dst.upload_bytes(masked, key)
            else:
                dst.upload_bytes(image_bytes, key)
        except Exception:
            log.error("Failed %s", key, exc_info=True)

        checkpoint.mark_item_done(phase, key)
        checkpoint.save()
