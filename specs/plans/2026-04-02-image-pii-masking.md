# Image PII Masking — Implementation Plan

## Overview

Standalone pipeline for masking PII in image files exported to S3. Runs independently from the text masking pipeline — can be deployed on a separate system (e.g., a high-CPU machine). Uses Tesseract OCR to extract text with bounding boxes, runs extracted text through the existing `TextScanner` (Presidio + roster AC + custom recognizers) to identify PII, and applies Gaussian blur over PII regions using Pillow.

Invoked as: `python -m scripts.pii_mask_images --store pii_store.db --src-bucket ... --dst-bucket ...`

## Current State Analysis

- The text masking pipeline (`scripts/pii_mask/`) processes JSON, EML, and Parquet files
- All maskers explicitly **skip** image/attachment files (`jira.py:15`, `slack.py:15`, `confluence.py:15`, `base.py:74`)
- No image processing code exists in the codebase
- Image files live in S3 under: `jira/{project}/attachments/`, `slack/{channel}/attachments/`, `confluence/{space}/attachments/`, `google/{user}/drive/`
- Shared infrastructure available: `PIIStore` (`scripts/pii_mask/pii_store.py`), `TextScanner` (`scripts/pii_mask/scanner.py`), `S3Store` (`lib/s3.py`), `CheckpointManager` (`lib/checkpoint.py`)

## Desired End State

- New package `scripts/pii_mask_images/` with its own `__main__.py` entry point
- Walks S3 source bucket for image files across all exporter prefixes
- OCR → PII detection → Gaussian blur → upload to destination bucket
- Shares `PIIStore` + `TextScanner` with the text pipeline for consistent masking
- Own checkpoint phase — can crash and resume independently
- Runs on a different machine from the text pipeline

## What We're NOT Doing

- Modifying the existing text masking pipeline
- Fake text rendering over blurred regions (customer confirmed blur-only)
- PDF masking
- Video processing
- Inline images in EML MIME parts or HTML data URIs
- Processing non-image binary files (`.docx`, `.pdf`, `.zip` — copied as-is)

## Implementation Approach

A new self-contained package `scripts/pii_mask_images/` that reuses `PIIStore`, `TextScanner`, `S3Store`, and `CheckpointManager` from the existing codebase. Its own `ProcessPoolExecutor`-based pipeline walks S3 for image files and processes them in parallel. No changes to the existing text masking code.

```
scripts/pii_mask_images/
    __init__.py
    __main__.py      # CLI: python -m scripts.pii_mask_images
    image.py         # Core: OCR → scan → blur
    pipeline.py      # S3 walker, parallel processing, checkpointing
```

---

## Phase 1: Core Image Masking Module

### Overview

Create the standalone image masking function: OCR → PII detection → blur. Pure logic, no S3 or pipeline concerns.

### Changes Required

#### 1. New module: `scripts/pii_mask_images/image.py`

**File**: `scripts/pii_mask_images/image.py`
**Changes**: New file

```python
"""Image PII masker — Tesseract OCR + Presidio scanner + Gaussian blur.

Extracts text with bounding boxes via Tesseract, runs through
TextScanner to detect PII, and applies Gaussian blur over PII
regions using Pillow.
"""

import io
import logging

log = logging.getLogger(__name__)

IMAGE_EXTENSIONS = frozenset({
    ".png", ".jpg", ".jpeg", ".tiff", ".tif", ".bmp", ".gif", ".webp",
})


def is_image(key: str) -> bool:
    """Check if an S3 key is a supported image file."""
    lower = key.lower()
    return any(lower.endswith(ext) for ext in IMAGE_EXTENSIONS)


def mask_image(image_bytes: bytes, scanner) -> bytes | None:
    """OCR an image, detect PII, blur PII regions.

    Returns masked image bytes, or None if no PII found.
    """
    from PIL import Image, ImageFilter
    import pytesseract

    img = Image.open(io.BytesIO(image_bytes))
    original_format = img.format or "PNG"
    if img.mode == "RGBA":
        img = img.convert("RGB")

    # OCR — word-level bounding boxes
    data = pytesseract.image_to_data(img, output_type=pytesseract.Output.DICT)
    if not data["text"]:
        return None

    lines = _group_into_lines(data)

    # Detect PII per line, collect bounding boxes to blur
    boxes_to_blur = []
    for line in lines:
        line_text = " ".join(w["text"] for w in line["words"])
        if len(line_text) < 3:
            continue

        scanned = scanner.scan(line_text)
        if scanned == line_text:
            continue

        pii_boxes = _find_changed_word_boxes(line, scanned)
        boxes_to_blur.extend(pii_boxes)

    if not boxes_to_blur:
        return None

    # Apply Gaussian blur to each PII region
    for box in boxes_to_blur:
        region = img.crop(box)
        radius = max(region.height // 2, 10)
        blurred = region.filter(ImageFilter.GaussianBlur(radius=radius))
        img.paste(blurred, box)

    buf = io.BytesIO()
    img.save(buf, format=original_format)
    return buf.getvalue()


def _group_into_lines(data: dict) -> list[dict]:
    """Group Tesseract word data into lines by block/par/line number."""
    lines = {}
    for i in range(len(data["text"])):
        text = data["text"][i].strip()
        if not text:
            continue
        if int(data["conf"][i]) < 0:
            continue

        line_key = (data["block_num"][i], data["par_num"][i],
                    data["line_num"][i])
        if line_key not in lines:
            lines[line_key] = {"words": []}
        lines[line_key]["words"].append({
            "text": text,
            "left": data["left"][i],
            "top": data["top"][i],
            "width": data["width"][i],
            "height": data["height"][i],
        })

    return list(lines.values())


def _find_changed_word_boxes(line: dict, scanned: str) -> list[tuple]:
    """Identify which words were PII-replaced by comparing to scanned output."""
    boxes = []
    scanned_lower = scanned.lower()

    for word in line["words"]:
        if word["text"].lower() not in scanned_lower:
            left = word["left"]
            top = word["top"]
            right = left + word["width"]
            bottom = top + word["height"]
            pad = max(word["height"] // 4, 2)
            boxes.append((
                max(0, left - pad),
                max(0, top - pad),
                right + pad,
                bottom + pad,
            ))

    return boxes
```

#### 2. Package init

**File**: `scripts/pii_mask_images/__init__.py`
**Changes**: Empty file

### Success Criteria

#### Automated Verification:
- [x] Unit test: `mask_image()` with synthetic image containing PII → returns blurred bytes
- [x] Unit test: blank image → returns None
- [x] Unit test: `is_image()` matches expected extensions

#### Manual Verification:
- [ ] Synthetic image with name + email → visually confirm blur placement

**Implementation Note**: Pause for manual confirmation before proceeding.

---

## Phase 2: Pipeline — S3 Walker + Parallel Processing

### Overview

S3-based pipeline that discovers image files, processes them in parallel, and tracks progress via checkpointing. Mirrors the architecture of the text pipeline but is fully independent.

### Changes Required

#### 1. New module: `scripts/pii_mask_images/pipeline.py`

**File**: `scripts/pii_mask_images/pipeline.py`
**Changes**: New file

```python
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
```

### Success Criteria

#### Automated Verification:
- [x] Unit test: `list_image_keys()` returns only image files (moto S3 mock)
- [x] Unit test: `_process_image()` handles missing files gracefully
- [x] Integration test: end-to-end with moto — upload image to src, run pipeline, verify blurred image in dst

#### Manual Verification:
- [ ] Run pipeline against a test S3 prefix with known images

---

## Phase 3: CLI Entry Point

### Overview

CLI interface for running the image masking pipeline independently.

### Changes Required

#### 1. New module: `scripts/pii_mask_images/__main__.py`

**File**: `scripts/pii_mask_images/__main__.py`
**Changes**: New file

```python
"""CLI entry point for the image PII masking pipeline.

Standalone pipeline — runs independently from the text masking
pipeline. Can be deployed on a separate system.

Usage:
    python -m scripts.pii_mask_images --store pii_store.db \
        --src-bucket my-bucket --s3-prefix v31/ \
        --dst-bucket my-bucket --dst-prefix masked-v1/

    # Only process specific exporters:
    python -m scripts.pii_mask_images --store pii_store.db \
        --prefixes jira/ slack/ \
        --src-bucket ... --dst-bucket ...
"""

import argparse
import logging

from lib.checkpoint import CheckpointManager
from lib.logging import setup_logging
from lib.s3 import S3Store
from scripts.pii_mask_images.pipeline import (
    DEFAULT_WORKERS, IMAGE_PREFIXES, run_pipeline,
)

log = logging.getLogger(__name__)


def main():
    from lib.config import load_dotenv, env, env_int
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Image PII masking pipeline — OCR + blur",
    )

    # Store
    parser.add_argument("--store", required=True,
                        help="Path to PIIStore SQLite database")

    # S3
    parser.add_argument("--src-bucket", default=env("S3_BUCKET"))
    parser.add_argument("--dst-bucket", default=env("S3_MASKED_BUCKET"))
    parser.add_argument("--s3-prefix", default=env("S3_PREFIX", ""))
    parser.add_argument("--dst-prefix", default=env("S3_DST_PREFIX"))

    # Processing
    parser.add_argument("--prefixes", nargs="*", default=None,
                        help=f"S3 prefixes to scan (default: {IMAGE_PREFIXES})")
    parser.add_argument("--max-workers", type=int,
                        default=env_int("IMAGE_MAX_WORKERS", DEFAULT_WORKERS))
    parser.add_argument("--presidio-threshold", type=float, default=0.5)

    # Logging
    parser.add_argument("--log-level", default=env("LOG_LEVEL", "INFO"))
    parser.add_argument("--no-json-logs", action="store_true")
    parser.add_argument("--log-dir", default=env("LOG_DIR", "logs"))
    args = parser.parse_args()

    import os
    log_file = os.path.join(args.log_dir, "pii_mask_images.log")
    setup_logging(
        level=args.log_level,
        json_output=not args.no_json_logs,
        log_file=log_file,
    )

    if not args.src_bucket:
        parser.error("--src-bucket is required (or set S3_BUCKET)")
    if not args.dst_bucket:
        parser.error("--dst-bucket is required (or set S3_MASKED_BUCKET)")
    if not args.dst_prefix:
        parser.error("--dst-prefix is required (or set S3_DST_PREFIX)")

    src = S3Store(bucket=args.src_bucket, prefix=args.s3_prefix)
    dst = S3Store(bucket=args.dst_bucket, prefix=args.dst_prefix)
    checkpoint = CheckpointManager(dst, "pii_mask_images/pipeline")
    checkpoint.load()

    src_path = f"{args.src_bucket}/{args.s3_prefix}" if args.s3_prefix \
        else args.src_bucket
    dst_path = f"{args.dst_bucket}/{args.dst_prefix}"
    log.info("Starting image PII masking: %s -> %s (workers: %d)",
             src_path, dst_path, args.max_workers)

    run_pipeline(
        src=src, dst=dst,
        checkpoint=checkpoint,
        max_workers=args.max_workers,
        store_path=args.store,
        threshold=args.presidio_threshold,
        prefixes=args.prefixes,
    )


if __name__ == "__main__":
    main()
```

#### 2. Optional dependency
**File**: `pyproject.toml`
**Changes**: Add `images` extra.

```toml
[project.optional-dependencies]
images = ["pytesseract>=0.3.10", "Pillow>=10.0"]
```

### Success Criteria

#### Automated Verification:
- [x] `uv sync --extra images` installs pytesseract + Pillow
- [x] `python -m scripts.pii_mask_images --help` shows usage

#### Manual Verification:
- [ ] Full run against a test bucket with Jira/Slack attachments
- [ ] Checkpoint resume: kill mid-run, restart, verify it skips completed files

---

## Phase 4: Tests

### Overview

Unit and integration tests for the image masking pipeline.

### Changes Required

#### 1. Test file
**File**: `tests/test_pii_mask_images.py`
**Changes**: New file

Tests:
- `test_is_image()` — extension matching for supported and unsupported types
- `test_mask_image_with_pii()` — synthetic image with "John Doe john@example.com", mock scanner returns modified text, verify returned bytes differ from input
- `test_mask_image_no_text()` — blank image → returns None
- `test_mask_image_no_pii()` — image with non-PII text, scanner returns unchanged → returns None
- `test_mask_image_corrupt()` — garbage bytes raise PIL error, handled gracefully
- `test_group_into_lines()` — unit test line grouping from Tesseract dict
- `test_find_changed_word_boxes()` — verify correct boxes when words are PII-replaced
- `test_list_image_keys()` — moto S3 with mixed files, verify only images returned
- `test_pipeline_end_to_end()` — moto: upload synthetic image to src, run pipeline, verify blurred image in dst
- `test_pipeline_checkpoint_resume()` — verify already-done items are skipped

Pattern: Create synthetic images with `PIL.Image.new()` + `ImageDraw.text()`. Mock `TextScanner` to control PII detection. Use moto for S3.

### Success Criteria

#### Automated Verification:
- [x] `uv run pytest tests/test_pii_mask_images.py -v` — all pass

---

## Testing Strategy

### Unit Tests:
- `mask_image()` with synthetic Pillow-generated images
- `is_image()` extension matching
- Line grouping and changed-word detection
- `list_image_keys()` with moto

### Integration Tests:
- Full pipeline with moto S3 (src → process → dst)
- Checkpoint resume after simulated interruption

### Manual Testing Steps:
1. Create test images with names, emails, phone numbers using Pillow
2. Run `mask_image()` directly, inspect output visually
3. Upload test images to S3, run `python -m scripts.pii_mask_images`
4. Compare source and destination images

## Performance Considerations

- Tesseract OCR: ~100-500ms per image on CPU
- Gaussian blur via Pillow: ~1-5ms per region
- At 16 workers (default): ~30-150 images/second throughput
- Memory: Pillow loads full image into RAM — large images may spike per-worker memory
- S3 download/upload is the likely bottleneck, not OCR
- Workers default to 16 (not 32 like text pipeline) — OCR is more CPU-intensive than JSON processing

## Dependencies

- `pytesseract>=0.3.10` — Python wrapper for Tesseract OCR
- `Pillow>=10.0` — image manipulation
- **System dependency**: `tesseract` binary
  - macOS: `brew install tesseract`
  - Ubuntu/Debian: `apt install tesseract-ocr`
  - Amazon Linux: `yum install tesseract`

## References

- Research findings: `specs/research/image-pii-masking/findings.md`
- Existing text pipeline: `scripts/pii_mask/pipeline.py`
- PIIStore: `scripts/pii_mask/pii_store.py`
- TextScanner: `scripts/pii_mask/scanner.py`
