"""Tests for scripts.pii_mask_images — image PII masking pipeline."""

import io
import json
from unittest.mock import MagicMock, patch

import boto3
import pytest
from moto import mock_aws

from lib.checkpoint import CheckpointManager
from lib.s3 import S3Store
from scripts.pii_mask_images.image import (
    IMAGE_EXTENSIONS, _find_changed_word_boxes, _group_into_lines,
    is_image, mask_image,
)
from scripts.pii_mask_images.pipeline import (
    list_image_keys, run_pipeline,
)

SRC_BUCKET = "src-bucket"
DST_BUCKET = "dst-bucket"


# -- Helpers -------------------------------------------------------------- #

def _make_image_bytes(text: str = "", width: int = 400, height: int = 100,
                      fmt: str = "PNG") -> bytes:
    """Create a synthetic image, optionally with text drawn on it."""
    from PIL import Image, ImageDraw

    img = Image.new("RGB", (width, height), color="white")
    if text:
        draw = ImageDraw.Draw(img)
        draw.text((10, 10), text, fill="black")
    buf = io.BytesIO()
    img.save(buf, format=fmt)
    return buf.getvalue()


def _make_tesseract_data(words: list[dict]) -> dict:
    """Build a dict matching pytesseract.image_to_data output format."""
    data = {
        "text": [], "conf": [], "left": [], "top": [],
        "width": [], "height": [], "block_num": [], "par_num": [],
        "line_num": [],
    }
    for w in words:
        data["text"].append(w.get("text", ""))
        data["conf"].append(w.get("conf", 90))
        data["left"].append(w.get("left", 0))
        data["top"].append(w.get("top", 0))
        data["width"].append(w.get("width", 50))
        data["height"].append(w.get("height", 20))
        data["block_num"].append(w.get("block", 1))
        data["par_num"].append(w.get("par", 1))
        data["line_num"].append(w.get("line", 1))
    return data


# -- Fixtures ------------------------------------------------------------- #

@pytest.fixture
def s3_env():
    with mock_aws():
        conn = boto3.client("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=SRC_BUCKET)
        conn.create_bucket(Bucket=DST_BUCKET)
        src = S3Store(bucket=SRC_BUCKET)
        dst = S3Store(bucket=DST_BUCKET)
        yield src, dst, conn


# -- is_image tests ------------------------------------------------------- #

class TestIsImage:
    def test_supported_extensions(self):
        for ext in IMAGE_EXTENSIONS:
            assert is_image(f"some/path/file{ext}") is True

    def test_case_insensitive(self):
        assert is_image("photo.JPG") is True
        assert is_image("photo.Png") is True

    def test_unsupported_extensions(self):
        assert is_image("doc.pdf") is False
        assert is_image("file.json") is False
        assert is_image("archive.zip") is False
        assert is_image("readme.md") is False
        assert is_image("data.csv") is False

    def test_no_extension(self):
        assert is_image("noext") is False


# -- _group_into_lines tests --------------------------------------------- #

class TestGroupIntoLines:
    def test_groups_by_line(self):
        data = _make_tesseract_data([
            {"text": "Hello", "block": 1, "par": 1, "line": 1,
             "left": 0, "top": 0, "width": 50, "height": 20},
            {"text": "world", "block": 1, "par": 1, "line": 1,
             "left": 60, "top": 0, "width": 50, "height": 20},
            {"text": "Second", "block": 1, "par": 1, "line": 2,
             "left": 0, "top": 30, "width": 60, "height": 20},
        ])
        lines = _group_into_lines(data)
        assert len(lines) == 2
        assert len(lines[0]["words"]) == 2
        assert len(lines[1]["words"]) == 1

    def test_skips_empty_and_low_conf(self):
        data = _make_tesseract_data([
            {"text": "Good", "conf": 90},
            {"text": "", "conf": 90},
            {"text": "Bad", "conf": -1},
        ])
        lines = _group_into_lines(data)
        assert len(lines) == 1
        assert lines[0]["words"][0]["text"] == "Good"


# -- _find_changed_word_boxes tests --------------------------------------- #

class TestFindChangedWordBoxes:
    def test_detects_changed_words(self):
        line = {"words": [
            {"text": "John", "left": 10, "top": 5, "width": 40, "height": 20},
            {"text": "Doe", "left": 60, "top": 5, "width": 30, "height": 20},
            {"text": "hello", "left": 100, "top": 5, "width": 50, "height": 20},
        ]}
        scanned = "[PERSON-abc123] hello"
        boxes = _find_changed_word_boxes(line, scanned)
        # "John" and "Doe" should be detected as changed
        assert len(boxes) == 2
        # "hello" should NOT be changed
        for box in boxes:
            assert box[0] < 100  # left of "hello"

    def test_no_changes(self):
        line = {"words": [
            {"text": "hello", "left": 0, "top": 0, "width": 50, "height": 20},
        ]}
        boxes = _find_changed_word_boxes(line, "hello")
        assert len(boxes) == 0

    def test_padding_applied(self):
        line = {"words": [
            {"text": "John", "left": 10, "top": 10, "width": 40, "height": 20},
        ]}
        scanned = "[PERSON-x]"
        boxes = _find_changed_word_boxes(line, scanned)
        assert len(boxes) == 1
        left, top, right, bottom = boxes[0]
        pad = max(20 // 4, 2)  # height=20 -> pad=5
        assert left == 10 - pad
        assert top == 10 - pad
        assert right == 10 + 40 + pad
        assert bottom == 10 + 20 + pad


# -- mask_image tests ----------------------------------------------------- #

class TestMaskImage:
    @patch("pytesseract.image_to_data")
    def test_with_pii(self, mock_ocr):
        """Image with PII text should return blurred bytes."""
        mock_ocr.return_value = _make_tesseract_data([
            {"text": "John", "left": 10, "top": 10, "width": 40, "height": 20},
            {"text": "Doe", "left": 60, "top": 10, "width": 30, "height": 20},
        ])

        scanner = MagicMock()
        scanner.scan.return_value = "[PERSON-abc] [PERSON-def]"

        image_bytes = _make_image_bytes("John Doe")
        result = mask_image(image_bytes, scanner)

        assert result is not None
        assert result != image_bytes
        # Result should be valid image
        from PIL import Image
        img = Image.open(io.BytesIO(result))
        assert img.size[0] > 0

    @patch("pytesseract.image_to_data")
    def test_no_text(self, mock_ocr):
        """Blank image with no OCR text should return None."""
        mock_ocr.return_value = {"text": []}

        scanner = MagicMock()
        image_bytes = _make_image_bytes()
        result = mask_image(image_bytes, scanner)
        assert result is None

    @patch("pytesseract.image_to_data")
    def test_no_pii(self, mock_ocr):
        """Image with non-PII text — scanner returns unchanged."""
        mock_ocr.return_value = _make_tesseract_data([
            {"text": "Hello", "left": 10, "top": 10, "width": 50, "height": 20},
            {"text": "world", "left": 70, "top": 10, "width": 50, "height": 20},
        ])

        scanner = MagicMock()
        scanner.scan.return_value = "Hello world"

        image_bytes = _make_image_bytes("Hello world")
        result = mask_image(image_bytes, scanner)
        assert result is None

    @patch("pytesseract.image_to_data")
    def test_rgba_conversion(self, mock_ocr):
        """RGBA images should be converted to RGB before processing."""
        from PIL import Image

        mock_ocr.return_value = _make_tesseract_data([
            {"text": "John", "left": 10, "top": 10, "width": 40, "height": 20},
        ])

        scanner = MagicMock()
        scanner.scan.return_value = "[PERSON-x]"

        # Create RGBA image
        img = Image.new("RGBA", (200, 50), color=(255, 255, 255, 128))
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        rgba_bytes = buf.getvalue()

        result = mask_image(rgba_bytes, scanner)
        assert result is not None

    def test_corrupt_bytes(self):
        """Garbage bytes should raise an exception."""
        scanner = MagicMock()
        with pytest.raises(Exception):
            mask_image(b"not an image at all", scanner)


# -- list_image_keys tests ------------------------------------------------ #

class TestListImageKeys:
    def test_filters_images(self, s3_env):
        src, _, conn = s3_env
        # Upload mix of image and non-image files
        conn.put_object(Bucket=SRC_BUCKET, Key="jira/P1/attachments/photo.png",
                        Body=b"png")
        conn.put_object(Bucket=SRC_BUCKET, Key="jira/P1/attachments/doc.pdf",
                        Body=b"pdf")
        conn.put_object(Bucket=SRC_BUCKET, Key="slack/C1/attachments/img.jpg",
                        Body=b"jpg")
        conn.put_object(Bucket=SRC_BUCKET, Key="slack/C1/messages.json",
                        Body=b"{}")
        conn.put_object(Bucket=SRC_BUCKET, Key="google/user/drive/pic.webp",
                        Body=b"webp")
        conn.put_object(Bucket=SRC_BUCKET,
                        Key="confluence/sp/attachments/chart.tiff",
                        Body=b"tiff")

        keys = list_image_keys(src)
        assert len(keys) == 4
        assert "jira/P1/attachments/photo.png" in keys
        assert "slack/C1/attachments/img.jpg" in keys
        assert "google/user/drive/pic.webp" in keys
        assert "confluence/sp/attachments/chart.tiff" in keys
        # Non-images excluded
        assert "jira/P1/attachments/doc.pdf" not in keys
        assert "slack/C1/messages.json" not in keys

    def test_custom_prefixes(self, s3_env):
        src, _, conn = s3_env
        conn.put_object(Bucket=SRC_BUCKET, Key="jira/P1/photo.png",
                        Body=b"png")
        conn.put_object(Bucket=SRC_BUCKET, Key="slack/C1/img.jpg",
                        Body=b"jpg")

        keys = list_image_keys(src, prefixes=["jira/"])
        assert len(keys) == 1
        assert "jira/P1/photo.png" in keys


# -- Pipeline integration tests ------------------------------------------ #

class TestPipeline:
    @patch("scripts.pii_mask_images.image.mask_image")
    def test_end_to_end(self, mock_mask, s3_env):
        """Upload image to src, run pipeline, verify image in dst."""
        src, dst, conn = s3_env

        image_bytes = _make_image_bytes("John Doe")
        masked_bytes = _make_image_bytes("MASKED")
        conn.put_object(Bucket=SRC_BUCKET,
                        Key="jira/P1/attachments/photo.png",
                        Body=image_bytes)

        mock_mask.return_value = masked_bytes

        checkpoint = CheckpointManager(dst, "test/images")
        checkpoint.load()

        with patch("scripts.pii_mask.pii_store.PIIStore"), \
             patch("scripts.pii_mask.scanner.TextScanner"):
            run_pipeline(
                src=src, dst=dst, checkpoint=checkpoint,
                max_workers=1, store_path="", threshold=0.5,
                prefixes=["jira/"],
            )

        # Verify masked image uploaded to dst
        result = dst.download_bytes("jira/P1/attachments/photo.png")
        assert result == masked_bytes

    @patch("scripts.pii_mask_images.image.mask_image")
    def test_no_pii_copies_original(self, mock_mask, s3_env):
        """Images without PII should be copied as-is to dst."""
        src, dst, conn = s3_env

        image_bytes = _make_image_bytes("Hello world")
        conn.put_object(Bucket=SRC_BUCKET,
                        Key="slack/C1/attachments/safe.jpg",
                        Body=image_bytes)

        mock_mask.return_value = None  # No PII found

        checkpoint = CheckpointManager(dst, "test/images")
        checkpoint.load()

        with patch("scripts.pii_mask.pii_store.PIIStore"), \
             patch("scripts.pii_mask.scanner.TextScanner"):
            run_pipeline(
                src=src, dst=dst, checkpoint=checkpoint,
                max_workers=1, store_path="", threshold=0.5,
                prefixes=["slack/"],
            )

        result = dst.download_bytes("slack/C1/attachments/safe.jpg")
        assert result == image_bytes

    @patch("scripts.pii_mask_images.image.mask_image")
    def test_checkpoint_resume(self, mock_mask, s3_env):
        """Already-done items should be skipped on resume."""
        src, dst, conn = s3_env

        for name in ("a.png", "b.png"):
            conn.put_object(Bucket=SRC_BUCKET,
                            Key=f"jira/P1/attachments/{name}",
                            Body=_make_image_bytes(name))

        # Simulate partial prior run — a.png already done
        cp = CheckpointManager(dst, "test/images")
        cp.load()
        cp.start_phase("mask/images", total=2)
        cp.mark_item_done("mask/images", "jira/P1/attachments/a.png")
        cp.save(force=True)

        # Upload a sentinel to dst for a.png
        dst.upload_bytes(b"sentinel", "jira/P1/attachments/a.png")

        masked_b = _make_image_bytes("MASKED_B")
        mock_mask.return_value = masked_b

        # Resume
        cp2 = CheckpointManager(dst, "test/images")
        cp2.load()

        with patch("scripts.pii_mask.pii_store.PIIStore"), \
             patch("scripts.pii_mask.scanner.TextScanner"):
            run_pipeline(
                src=src, dst=dst, checkpoint=cp2,
                max_workers=1, store_path="", threshold=0.5,
                prefixes=["jira/"],
            )

        # a.png should NOT have been re-processed (sentinel preserved)
        assert dst.download_bytes("jira/P1/attachments/a.png") == b"sentinel"
        # b.png should be processed
        assert dst.download_bytes("jira/P1/attachments/b.png") == masked_b

    @patch("scripts.pii_mask_images.image.mask_image")
    def test_skips_completed_phase(self, mock_mask, s3_env):
        """Pipeline should skip if phase is already marked done."""
        src, dst, conn = s3_env

        conn.put_object(Bucket=SRC_BUCKET,
                        Key="jira/P1/attachments/photo.png",
                        Body=_make_image_bytes())

        # Mark phase as done
        cp = CheckpointManager(dst, "test/images")
        cp.load()
        cp.start_phase("mask/images", total=1)
        cp.complete_phase("mask/images")
        cp.save(force=True)

        cp2 = CheckpointManager(dst, "test/images")
        cp2.load()

        run_pipeline(
            src=src, dst=dst, checkpoint=cp2,
            max_workers=1, store_path="", threshold=0.5,
            prefixes=["jira/"],
        )

        # mask_image should never have been called
        mock_mask.assert_not_called()
        # No image should be in dst
        assert dst.download_bytes("jira/P1/attachments/photo.png") is None
