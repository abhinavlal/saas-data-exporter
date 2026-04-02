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
