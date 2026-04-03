# Office Document PII Masking — Implementation Plan

## Overview

Add PII masking for `.docx`, `.xlsx`, and `.pptx` files across all export sources (Google Drive, Jira attachments, Slack attachments, Gmail attachments). These files currently pass through the pipeline unmasked.

The approach follows the `eml.py` pattern: a shared `documents.py` module with `mask_docx()`, `mask_xlsx()`, `mask_pptx()` functions, called by each per-service masker via extension-based dispatch.

## Current State Analysis

**What exists:** The masking pipeline processes JSON and EML files. Binary attachments are explicitly skipped:
- `BaseMasker.should_process()` only accepts `.json`/`.eml` (`maskers/base.py:74`)
- `JiraMasker.should_process()` rejects `/attachments/` paths (`maskers/jira.py:14`)
- `SlackMasker.should_process()` rejects `/attachments/` paths (`maskers/slack.py:14`)
- `GoogleMasker._keys_from_indexes()` skips Drive files (`maskers/google.py:68`)
- Gmail attachments are not enumerated at all

**What we need:** Each masker must:
1. Enumerate Office doc keys from its indexes/metadata
2. Dispatch to the shared `documents.py` functions by extension
3. Upload masked bytes to the destination bucket

## Desired End State

All `.docx`, `.xlsx`, `.pptx` files in the exported dataset are masked:
- All text content scanned with Presidio (same as JSON/EML)
- Document properties (author, title) masked
- Comments, footnotes, speaker notes, SmartArt masked via secondary XML pass
- Formatting preserved
- Pipeline checkpoints track per-file completion
- Corrupted files logged and skipped without crashing workers

Verification: run the pipeline on a test dataset, grep the masked output for known roster names/emails — zero matches in Office doc content.

## What We're NOT Doing

- PDF masking (separate phase)
- Image/OCR masking (separate phase)
- Legacy binary formats (`.doc`, `.xls`, `.ppt`)
- Password-protected documents
- Macro preservation (openpyxl strips macros; acceptable for this use case)

## Implementation Approach

**Approach B from brainstorm:** Shared `documents.py` module + masker dispatch.

- `scripts/pii_mask/documents.py` — three public functions: `mask_docx()`, `mask_xlsx()`, `mask_pptx()`. Each takes `(raw_bytes, scanner)` and returns masked `bytes`. Handles the run-split problem, secondary XML pass, and error recovery internally.
- Each masker (`GoogleMasker`, `JiraMasker`, `SlackMasker`) expands `should_process()` and `list_keys()` to include Office doc paths, and adds extension-based dispatch in `mask_file()`.
- `BaseMasker` gets a shared `_mask_document_file()` helper that handles the download → `mask_docx/xlsx/pptx` → upload flow, since all maskers do this identically.

---

## Phase 1: Core Document Masking Module (`documents.py`)

### Overview

Create the shared document masking module with three public functions. This is the foundation — all masker integration depends on it.

### Changes Required

#### 1. Add dependencies to `pyproject.toml`

**File**: `pyproject.toml`
**Changes**: Add `python-docx`, `openpyxl`, `python-pptx` to the `[project.dependencies]` or the appropriate extras group.

```toml
# Under dependencies or [project.optional-dependencies.mask]
"python-docx>=1.1",
"openpyxl>=3.1",
"python-pptx>=1.0",
```

#### 2. Create `scripts/pii_mask/documents.py`

**File**: `scripts/pii_mask/documents.py` (new)
**Changes**: Three public functions following the `eml.py` pattern.

```python
"""Office document masker — PII replacement in DOCX, XLSX, PPTX.

Uses python-docx, openpyxl, python-pptx for high-level text access
(preserves formatting), then a secondary ZIP+XML pass for content
the high-level APIs miss (footnotes, endnotes, SmartArt, app properties).
"""

import io
import logging
import re
import zipfile

from lxml import etree

from scripts.pii_mask.scanner import TextScanner

log = logging.getLogger(__name__)

# Office doc extensions we handle
OFFICE_EXTENSIONS = frozenset({".docx", ".xlsx", ".pptx"})

# XML namespaces
_W_NS = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
_A_NS = "http://schemas.openxmlformats.org/drawingml/2006/main"
_DC_NS = "http://purl.org/dc/elements/1.1/"
_CP_NS = "http://schemas.openxmlformats.org/package/2006/metadata/core-properties"
_VT_NS = "http://schemas.openxmlformats.org/officeDocument/2006/extended-properties"


def mask_docx(raw_bytes: bytes, scanner: TextScanner) -> bytes:
    """Mask PII in a DOCX file. Returns masked bytes."""
    from docx import Document

    doc = Document(io.BytesIO(raw_bytes))

    # Body: paragraphs and tables in document order
    for item in doc.iter_inner_content():
        if hasattr(item, "runs"):
            _mask_paragraph(item, scanner)
        else:
            _mask_table(item, scanner)

    # Headers and footers
    for section in doc.sections:
        for hf in (section.header, section.footer,
                    section.even_page_header, section.even_page_footer,
                    section.first_page_header, section.first_page_footer):
            if hf.is_linked_to_previous:
                continue
            for item in hf.iter_inner_content():
                if hasattr(item, "runs"):
                    _mask_paragraph(item, scanner)
                else:
                    _mask_table(item, scanner)

    # Comments (python-docx >= 1.2.0)
    if hasattr(doc, "comments"):
        for comment in doc.comments:
            if hasattr(comment, "author") and comment.author:
                comment.author = scanner.scan(comment.author)
            for para in comment.paragraphs:
                _mask_paragraph(para, scanner)

    # Core properties
    _mask_core_properties(doc.core_properties, scanner)

    # Save primary pass
    buf = io.BytesIO()
    doc.save(buf)
    primary_bytes = buf.getvalue()

    # Secondary XML pass: footnotes, endnotes, app properties
    return _secondary_xml_pass(primary_bytes, scanner, format="docx")


def mask_xlsx(raw_bytes: bytes, scanner: TextScanner) -> bytes:
    """Mask PII in an XLSX file. Returns masked bytes."""
    from openpyxl import load_workbook

    wb = load_workbook(io.BytesIO(raw_bytes))

    # All cells across all sheets (including hidden/veryHidden)
    for ws in wb.worksheets:
        for row in ws.iter_rows():
            for cell in row:
                if isinstance(cell.value, str) and len(cell.value) >= 3:
                    cell.value = scanner.scan(cell.value)
                # Cell comments
                if cell.comment:
                    if cell.comment.text and len(cell.comment.text) >= 3:
                        cell.comment.text = scanner.scan(cell.comment.text)
                    if cell.comment.author and len(cell.comment.author) >= 3:
                        cell.comment.author = scanner.scan(
                            cell.comment.author)

    # Sheet titles
    for ws in wb.worksheets:
        if ws.title and len(ws.title) >= 3:
            ws.title = scanner.scan(ws.title)

    # Core properties
    _mask_wb_properties(wb.properties, scanner)

    buf = io.BytesIO()
    wb.save(buf)
    primary_bytes = buf.getvalue()

    # Secondary XML pass: app properties
    return _secondary_xml_pass(primary_bytes, scanner, format="xlsx")


def mask_pptx(raw_bytes: bytes, scanner: TextScanner) -> bytes:
    """Mask PII in a PPTX file. Returns masked bytes."""
    from pptx import Presentation
    from pptx.enum.shapes import MSO_SHAPE_TYPE

    prs = Presentation(io.BytesIO(raw_bytes))

    for slide in prs.slides:
        for shape in slide.shapes:
            _mask_shape(shape, scanner, MSO_SHAPE_TYPE)

        # Speaker notes
        if slide.has_notes_slide:
            notes_tf = slide.notes_slide.notes_text_frame
            if notes_tf:
                _mask_text_frame(notes_tf, scanner)

    # Core properties
    _mask_core_properties(prs.core_properties, scanner)

    buf = io.BytesIO()
    prs.save(buf)
    primary_bytes = buf.getvalue()

    # Secondary XML pass: SmartArt, app properties
    return _secondary_xml_pass(primary_bytes, scanner, format="pptx")


# -- Internal helpers ------------------------------------------------------ #

def _mask_paragraph(para, scanner: TextScanner) -> None:
    """Mask a paragraph by joining runs, scanning, and distributing back.

    Office XML often splits a single word across multiple runs for
    formatting reasons (e.g. "John" = ["J", "ohn"] due to spellcheck).
    Scanning per-run would miss PII spanning run boundaries.

    Strategy: join all run texts, scan the joined string, then
    distribute the masked text back proportionally across runs.
    """
    runs = para.runs
    if not runs:
        return

    texts = [r.text or "" for r in runs]
    joined = "".join(texts)

    if len(joined) < 3:
        return

    masked = scanner.scan(joined)

    if masked == joined:
        return

    # Distribute masked text back across runs.
    # If lengths match, split at original boundaries.
    # If lengths differ (replacement is shorter/longer), put all text
    # in the first run and clear the rest — preserves first run's format.
    if len(masked) == len(joined):
        pos = 0
        for run, orig_text in zip(runs, texts):
            run.text = masked[pos:pos + len(orig_text)]
            pos += len(orig_text)
    else:
        runs[0].text = masked
        for run in runs[1:]:
            run.text = ""


def _mask_table(table, scanner: TextScanner) -> None:
    """Mask all text in a table, handling nested tables."""
    for row in table.rows:
        for cell in row.cells:
            for item in cell.iter_inner_content():
                if hasattr(item, "runs"):
                    _mask_paragraph(item, scanner)
                else:
                    _mask_table(item, scanner)


def _mask_text_frame(tf, scanner: TextScanner) -> None:
    """Mask all paragraphs in a text frame (PPTX)."""
    for para in tf.paragraphs:
        _mask_paragraph(para, scanner)


def _mask_shape(shape, scanner: TextScanner, MSO_SHAPE_TYPE) -> None:
    """Recursively mask text in a shape (handles groups and tables)."""
    if shape.shape_type == MSO_SHAPE_TYPE.GROUP:
        for child in shape.shapes:
            _mask_shape(child, scanner, MSO_SHAPE_TYPE)
    elif shape.has_text_frame:
        _mask_text_frame(shape.text_frame, scanner)
    elif shape.has_table:
        for row in shape.table.rows:
            for cell in row.cells:
                _mask_text_frame(cell.text_frame, scanner)


def _mask_core_properties(cp, scanner: TextScanner) -> None:
    """Mask core document properties (author, title, etc.)."""
    for attr in ("author", "last_modified_by", "subject", "title",
                 "keywords", "comments", "description"):
        val = getattr(cp, attr, None)
        if isinstance(val, str) and len(val) >= 3:
            setattr(cp, attr, scanner.scan(val))


def _mask_wb_properties(props, scanner: TextScanner) -> None:
    """Mask openpyxl workbook properties (different attribute names)."""
    for attr in ("creator", "lastModifiedBy", "subject", "title",
                 "keywords", "description"):
        val = getattr(props, attr, None)
        if isinstance(val, str) and len(val) >= 3:
            setattr(props, attr, scanner.scan(val))


# -- Secondary XML pass ---------------------------------------------------- #

# XML paths to scan per format.
# Each entry: (path_or_glob, namespace_uri, element_local_name)
_DOCX_XML_TARGETS = [
    ("word/footnotes.xml", _W_NS, "t"),
    ("word/endnotes.xml", _W_NS, "t"),
]
_PPTX_XML_TARGETS = [
    ("ppt/diagrams/data*.xml", _A_NS, "t"),
]
# App properties (company, manager) — same path for all formats
_APP_PROPS_PATH = "docProps/app.xml"


def _secondary_xml_pass(raw_bytes: bytes, scanner: TextScanner,
                        format: str) -> bytes:
    """Scan XML files inside the ZIP that high-level libraries miss."""
    targets = []
    if format == "docx":
        targets = _DOCX_XML_TARGETS
    elif format == "pptx":
        targets = _PPTX_XML_TARGETS

    in_buf = io.BytesIO(raw_bytes)
    out_buf = io.BytesIO()

    with zipfile.ZipFile(in_buf, "r") as zin, \
         zipfile.ZipFile(out_buf, "w", zipfile.ZIP_DEFLATED) as zout:
        for item in zin.infolist():
            data = zin.read(item.filename)

            # Check against targets (supports glob patterns)
            matched = False
            for path_pattern, ns, elem_name in targets:
                if _match_path(item.filename, path_pattern):
                    data = _mask_xml_text_elements(
                        data, ns, elem_name, scanner)
                    matched = True
                    break

            # App properties (all formats)
            if item.filename == _APP_PROPS_PATH:
                data = _mask_app_properties(data, scanner)

            zout.writestr(item, data)

    return out_buf.getvalue()


def _match_path(filename: str, pattern: str) -> bool:
    """Match a ZIP entry path against a pattern (supports * glob)."""
    if "*" not in pattern:
        return filename == pattern
    # Convert glob to regex: "ppt/diagrams/data*.xml" → exact match
    regex = re.escape(pattern).replace(r"\*", ".*")
    return re.fullmatch(regex, filename) is not None


def _mask_xml_text_elements(xml_bytes: bytes, ns: str,
                            elem_name: str,
                            scanner: TextScanner) -> bytes:
    """Replace text in all matching XML elements."""
    try:
        root = etree.fromstring(xml_bytes)
    except etree.XMLSyntaxError:
        return xml_bytes  # corrupted XML — pass through

    for elem in root.iter(f"{{{ns}}}{elem_name}"):
        if elem.text and len(elem.text) >= 3:
            elem.text = scanner.scan(elem.text)

    return etree.tostring(root, xml_declaration=True,
                          encoding="UTF-8", standalone=True)


def _mask_app_properties(xml_bytes: bytes,
                         scanner: TextScanner) -> bytes:
    """Mask company/manager in docProps/app.xml."""
    try:
        root = etree.fromstring(xml_bytes)
    except etree.XMLSyntaxError:
        return xml_bytes

    # App properties use the extended-properties namespace
    # Tags: Company, Manager (direct children of Properties)
    for tag in ("Company", "Manager", "HyperlinkBase"):
        for elem in root.iter(f"{{{_VT_NS}}}{tag}"):
            if elem.text and len(elem.text) >= 3:
                elem.text = scanner.scan(elem.text)
        # Also try without namespace (some files use bare tags)
        for elem in root.iter(tag):
            if elem.text and len(elem.text) >= 3:
                elem.text = scanner.scan(elem.text)

    return etree.tostring(root, xml_declaration=True,
                          encoding="UTF-8", standalone=True)
```

### Success Criteria

#### Automated Verification:
- [ ] `uv run pytest tests/test_documents.py -v` passes
- [ ] `uv run pytest tests/ -v` — all existing tests still pass

#### Manual Verification:
- [ ] Create a DOCX with names/emails in body, header, footer, comment → verify all masked
- [ ] Create an XLSX with names in cells, sheet title, comment → verify all masked
- [ ] Create a PPTX with names in slide text, speaker notes → verify all masked
- [ ] Open masked files in Word/Excel/PowerPoint — formatting preserved

**Implementation Note**: After completing this phase and all automated verification passes, pause for manual confirmation before proceeding to the next phase.

---

## Phase 2: BaseMasker Document Support

### Overview

Add shared document masking infrastructure to `BaseMasker` so all maskers can dispatch Office docs without duplicating logic.

### Changes Required

#### 1. Update `BaseMasker.should_process()`

**File**: `scripts/pii_mask/maskers/base.py`
**Changes**: Accept `.docx`, `.xlsx`, `.pptx` in addition to `.json` and `.eml`.

```python
def should_process(self, key: str) -> bool:
    # ... existing skip logic ...
    return (key.endswith(".json") or key.endswith(".eml")
            or key.endswith(".docx") or key.endswith(".xlsx")
            or key.endswith(".pptx"))
```

#### 2. Add `_mask_document_file()` helper to `BaseMasker`

**File**: `scripts/pii_mask/maskers/base.py`
**Changes**: Shared download → mask → upload flow for Office docs.

```python
def _mask_document_file(self, src: S3Store, dst: S3Store,
                        key: str) -> str:
    """Download, mask, and upload an Office document."""
    from scripts.pii_mask.documents import (
        mask_docx, mask_xlsx, mask_pptx,
    )

    raw_bytes = src.download_bytes(key)
    if raw_bytes is None:
        return "skipped (not found)"

    ext = key.rsplit(".", 1)[-1].lower() if "." in key else ""
    mask_fn = {"docx": mask_docx, "xlsx": mask_xlsx,
               "pptx": mask_pptx}.get(ext)
    if mask_fn is None:
        return "skipped (unsupported ext)"

    content_types = {
        "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    }

    try:
        masked_bytes = mask_fn(raw_bytes, self.scanner)
    except Exception:
        log.error("Failed to mask document %s", key, exc_info=True)
        return "error (document masking failed)"

    dst_key = self.rewrite_key(key)
    dst.upload_bytes(masked_bytes, dst_key,
                     content_type=content_types[ext])
    return "ok"
```

### Success Criteria

#### Automated Verification:
- [ ] `BaseMasker.should_process("jira/PROJ/attachments/PROJ-1/report.docx")` returns `True`
- [ ] Existing tests pass (JSON/EML processing unchanged)

---

## Phase 3: Masker Integration — Google

### Overview

Update `GoogleMasker` to enumerate and mask Drive files and Gmail attachments.

### Changes Required

#### 1. Update `_keys_from_indexes()` to include Drive files

**File**: `scripts/pii_mask/maskers/google.py`
**Changes**: Add Drive file keys (not just `_index.json`) for Office extensions.

```python
def _keys_from_indexes(self, src: S3Store, base: str) -> list[str]:
    keys = []

    # Gmail: _index.json + .eml files (existing)
    # ... unchanged ...

    # Gmail attachments: enumerate from gmail/_index.json
    # Each entry has "id" → check gmail/attachments/{id}/ for office docs
    if gmail_idx:
        for entry in gmail_idx:
            msg_id = entry.get("id") if isinstance(entry, dict) else None
            if not msg_id:
                continue
            # List attachment keys for this message
            att_prefix = f"{base}/gmail/attachments/{msg_id}/"
            att_keys = src.list_keys(att_prefix)
            keys.extend(k for k in att_keys
                        if self._is_office_doc(k))

    # Calendar: unchanged ...

    # Drive: index + office doc files
    drive_idx = src.download_json(f"{base}/drive/_index.json")
    if drive_idx is not None:
        keys.append(f"{base}/drive/_index.json")
        if isinstance(drive_idx, list):
            for entry in drive_idx:
                if not isinstance(entry, dict):
                    continue
                filename = entry.get("s3_filename") or entry.get("filename", "")
                file_id = entry.get("id", "")
                if filename and self._is_office_doc(filename):
                    # S3 key: google/{slug}/drive/{file_id}_{filename}
                    s3_name = f"{file_id}_{filename}" if file_id else filename
                    keys.append(f"{base}/drive/{s3_name}")

    return keys

@staticmethod
def _is_office_doc(key: str) -> bool:
    lower = key.lower()
    return (lower.endswith(".docx") or lower.endswith(".xlsx")
            or lower.endswith(".pptx"))
```

#### 2. Update `mask_file()` dispatch

**File**: `scripts/pii_mask/maskers/google.py`
**Changes**: Add document dispatch.

```python
def mask_file(self, src: S3Store, dst: S3Store, key: str) -> str:
    if key.endswith(".eml"):
        return self._mask_eml_file(src, dst, key)
    if key.endswith((".docx", ".xlsx", ".pptx")):
        return self._mask_document_file(src, dst, key)
    return self._mask_json_file(src, dst, key)
```

### Success Criteria

#### Automated Verification:
- [ ] `test_masker_google.py` updated with Drive docx/xlsx/pptx masking tests
- [ ] Gmail attachment enumeration tested
- [ ] All existing Google masker tests still pass

---

## Phase 4: Masker Integration — Jira

### Overview

Update `JiraMasker` to enumerate and mask attachment files.

### Changes Required

#### 1. Update `should_process()`

**File**: `scripts/pii_mask/maskers/jira.py`
**Changes**: Allow `/attachments/` paths for Office docs only.

```python
def should_process(self, key: str) -> bool:
    if "/attachments/" in key:
        # Only mask Office documents in attachments
        lower = key.lower()
        return (lower.endswith(".docx") or lower.endswith(".xlsx")
                or lower.endswith(".pptx"))
    return super().should_process(key)
```

#### 2. Update `list_keys()` to enumerate attachment paths

**File**: `scripts/pii_mask/maskers/jira.py`
**Changes**: After enumerating ticket JSON keys, also collect Office doc attachment paths from ticket metadata.

```python
def list_keys(self, src: S3Store) -> list[str]:
    keys = []
    projects = self._list_entities(src)
    for i, project in enumerate(projects, 1):
        base = f"{self.prefix}{project}"
        idx = src.download_json(f"{base}/tickets/_index.json")
        if not idx:
            continue
        keys.append(f"{base}/tickets/_index.json")
        for ticket_key in idx.get("keys", []):
            keys.append(f"{base}/tickets/{ticket_key}.json")

            # Enumerate Office doc attachments from ticket metadata
            ticket = src.download_json(
                f"{base}/tickets/{ticket_key}.json")
            if ticket:
                for att in ticket.get("attachments", []):
                    fname = att.get("filename", "")
                    lower = fname.lower()
                    if (lower.endswith(".docx") or lower.endswith(".xlsx")
                            or lower.endswith(".pptx")):
                        keys.append(
                            f"{base}/attachments/{ticket_key}/{fname}")

        if i % 10 == 0:
            log.info("jira: enumerated %d/%d projects (%d files)",
                     i, len(projects), len(keys))
    log.info("jira: %d files across %d projects", len(keys), len(projects))
    return keys
```

#### 3. Update `mask_file()` dispatch

**File**: `scripts/pii_mask/maskers/jira.py`

```python
def mask_file(self, src: S3Store, dst: S3Store, key: str) -> str:
    if key.endswith((".docx", ".xlsx", ".pptx")):
        return self._mask_document_file(src, dst, key)

    data = src.download_json(key)
    # ... existing JSON masking ...
```

### Success Criteria

#### Automated Verification:
- [ ] New tests for Jira attachment enumeration and masking
- [ ] Existing Jira masker tests still pass

#### Manual Verification:
- [ ] On real Jira export, Office doc attachments are discovered and masked

**Implementation Note**: The `list_keys()` change downloads each ticket JSON to find attachments. This is O(tickets) S3 GETs during enumeration — acceptable since ticket JSON is already cached/small, and this is a one-time cost per pipeline run. If performance becomes an issue, we could build a separate attachment index during export.

---

## Phase 5: Masker Integration — Slack

### Overview

Update `SlackMasker` to enumerate and mask attachment files.

### Changes Required

#### 1. Update `should_process()`

**File**: `scripts/pii_mask/maskers/slack.py`
**Changes**: Same pattern as Jira.

```python
def should_process(self, key: str) -> bool:
    if "/attachments/" in key:
        lower = key.lower()
        return (lower.endswith(".docx") or lower.endswith(".xlsx")
                or lower.endswith(".pptx"))
    return super().should_process(key)
```

#### 2. Update `list_keys()` to enumerate attachment paths

**File**: `scripts/pii_mask/maskers/slack.py`
**Changes**: After enumerating message JSON keys, collect Office doc paths from message `files[]._local_file` fields.

```python
def list_keys(self, src: S3Store) -> list[str]:
    keys = []
    channels = self._list_entities(src)
    for channel in channels:
        base = f"{self.prefix}{channel}"
        keys.append(f"{base}/channel_info.json")
        idx = src.download_json(f"{base}/messages/_index.json")
        if idx:
            keys.append(f"{base}/messages/_index.json")
            for ts in idx:
                if isinstance(ts, str):
                    keys.append(f"{base}/messages/{ts}.json")

        # Enumerate Office doc attachments via S3 listing
        att_prefix = f"{self.prefix}{channel}/attachments/"
        att_keys = src.list_keys(att_prefix)
        for k in att_keys:
            lower = k.lower()
            if (lower.endswith(".docx") or lower.endswith(".xlsx")
                    or lower.endswith(".pptx")):
                keys.append(k)

    log.info("slack: %d files across %d channels",
             len(keys), len(channels))
    return keys
```

#### 3. Update `mask_file()` dispatch

**File**: `scripts/pii_mask/maskers/slack.py`

```python
def mask_file(self, src: S3Store, dst: S3Store, key: str) -> str:
    if key.endswith((".docx", ".xlsx", ".pptx")):
        return self._mask_document_file(src, dst, key)

    data = src.download_json(key)
    # ... existing JSON masking ...
```

### Success Criteria

#### Automated Verification:
- [ ] New tests for Slack attachment enumeration and masking
- [ ] Existing Slack masker tests still pass

---

## Phase 6: Pipeline Worker Integration

### Overview

Ensure the `ProcessPoolExecutor` workers can import and use the document masking libraries. Update `_init_worker` and `_get_masker` if needed.

### Changes Required

#### 1. Verify worker compatibility

**File**: `scripts/pii_mask/pipeline.py`
**Changes**: The existing worker architecture should work without changes because:
- `_mask_file()` calls `masker.mask_file()` which now dispatches to `_mask_document_file()` for Office docs
- Document libraries (`python-docx`, `openpyxl`, `python-pptx`) are imported lazily inside the `mask_*` functions (deferred imports in `documents.py`)
- Each worker process has its own memory space — no shared state issues

No code changes needed in `pipeline.py` if the masker integration is done correctly. Verify with an integration test.

#### 2. Update `_get_masker` for Gmail attachment user filtering

**File**: `scripts/pii_mask/pipeline.py`
**Changes**: `GoogleMasker` constructor takes `users` parameter. The current `_get_masker` doesn't pass it. This needs to be threaded through.

```python
def _init_worker(store_path, threshold, src_bucket, src_prefix,
                 dst_bucket, dst_prefix, google_users=None):
    # ... existing init ...
    _w["google_users"] = google_users

def _get_masker(name):
    if name not in _w["maskers"]:
        # ... existing imports ...
        classes = { ... }  # existing
        if name == "google":
            _w["maskers"][name] = GoogleMasker(
                _w["scanner"], users=_w.get("google_users"))
        else:
            _w["maskers"][name] = classes[name](_w["scanner"])
    return _w["maskers"][name]
```

Note: check if this is already handled. If `google_users` is only used for `list_keys()` (which runs in the main process, not workers), this change may not be needed.

### Success Criteria

#### Automated Verification:
- [ ] Pipeline integration test: upload a mix of JSON, EML, and Office docs → run pipeline → verify all masked
- [ ] `uv run pytest tests/ -v` — all tests pass

---

## Phase 7: Tests

### Overview

Comprehensive test coverage for document masking.

### Changes Required

#### 1. Create `tests/test_documents.py`

**File**: `tests/test_documents.py` (new)
**Changes**: Unit tests for `mask_docx`, `mask_xlsx`, `mask_pptx`.

Test structure follows existing `test_eml.py` pattern:
- Fixture: `store`, `_analyzer` (module-scoped), `scanner`
- Helper functions to create minimal Office docs with known PII
- Assert PII is replaced, non-PII preserved, formatting intact

```python
"""Tests for scripts.pii_mask.documents — Office document masking."""

import io
import pytest
from scripts.pii_mask.pii_store import PIIStore
from scripts.pii_mask.scanner import TextScanner


@pytest.fixture
def store(tmp_path):
    s = PIIStore(str(tmp_path / "test.db"))
    s.add_domain("org_name.com", "example.com")
    s.get_or_create("EMAIL_ADDRESS", "john.doe@org_name.com")
    s.get_or_create("PERSON", "John Doe")
    return s


@pytest.fixture(scope="module")
def _analyzer():
    from presidio_analyzer import AnalyzerEngine
    return AnalyzerEngine()


@pytest.fixture
def scanner(store, _analyzer):
    s = TextScanner.__new__(TextScanner)
    s._store = store
    s._threshold = 0.5
    s._analyzer = _analyzer
    return s


def _make_docx(paragraphs: list[str]) -> bytes:
    """Create a minimal DOCX with given paragraph texts."""
    from docx import Document
    doc = Document()
    for text in paragraphs:
        doc.add_paragraph(text)
    buf = io.BytesIO()
    doc.save(buf)
    return buf.getvalue()


def _make_xlsx(cells: dict[str, str]) -> bytes:
    """Create minimal XLSX. cells maps 'A1' -> value."""
    from openpyxl import Workbook
    wb = Workbook()
    ws = wb.active
    for coord, val in cells.items():
        ws[coord] = val
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def _make_pptx(slide_texts: list[str]) -> bytes:
    """Create minimal PPTX with one text box per slide."""
    from pptx import Presentation
    from pptx.util import Inches
    prs = Presentation()
    for text in slide_texts:
        slide = prs.slides.add_slide(prs.slide_layouts[6])  # blank
        txBox = slide.shapes.add_textbox(Inches(1), Inches(1),
                                          Inches(5), Inches(1))
        txBox.text_frame.text = text
    buf = io.BytesIO()
    prs.save(buf)
    return buf.getvalue()


class TestMaskDocx:
    def test_name_in_paragraph_masked(self, scanner):
        from scripts.pii_mask.documents import mask_docx
        raw = _make_docx(["Meeting with John Doe tomorrow"])
        masked = mask_docx(raw, scanner)
        # Verify by reading back
        from docx import Document
        doc = Document(io.BytesIO(masked))
        text = doc.paragraphs[0].text
        assert "John Doe" not in text
        assert "Meeting with" in text

    def test_email_in_paragraph_masked(self, scanner):
        from scripts.pii_mask.documents import mask_docx
        raw = _make_docx(["Contact john.doe@org_name.com for details"])
        masked = mask_docx(raw, scanner)
        from docx import Document
        doc = Document(io.BytesIO(masked))
        text = doc.paragraphs[0].text
        assert "john.doe@org_name.com" not in text
        assert "org_name.com" not in text

    def test_no_pii_preserved(self, scanner):
        from scripts.pii_mask.documents import mask_docx
        raw = _make_docx(["Quarterly revenue report Q4 2025"])
        masked = mask_docx(raw, scanner)
        from docx import Document
        doc = Document(io.BytesIO(masked))
        assert doc.paragraphs[0].text == "Quarterly revenue report Q4 2025"

    def test_core_properties_masked(self, scanner, store):
        from scripts.pii_mask.documents import mask_docx
        from docx import Document
        doc = Document()
        doc.core_properties.author = "John Doe"
        doc.core_properties.title = "Report by John Doe"
        doc.add_paragraph("Content")
        buf = io.BytesIO()
        doc.save(buf)
        raw = buf.getvalue()

        masked = mask_docx(raw, scanner)
        doc2 = Document(io.BytesIO(masked))
        assert "John Doe" not in (doc2.core_properties.author or "")
        assert "John Doe" not in (doc2.core_properties.title or "")


class TestMaskXlsx:
    def test_name_in_cell_masked(self, scanner):
        from scripts.pii_mask.documents import mask_xlsx
        raw = _make_xlsx({"A1": "John Doe", "B1": "john.doe@org_name.com"})
        masked = mask_xlsx(raw, scanner)
        from openpyxl import load_workbook
        wb = load_workbook(io.BytesIO(masked))
        ws = wb.active
        assert "John Doe" not in str(ws["A1"].value)
        assert "john.doe@org_name.com" not in str(ws["B1"].value)

    def test_no_pii_preserved(self, scanner):
        from scripts.pii_mask.documents import mask_xlsx
        raw = _make_xlsx({"A1": "Revenue", "B1": "1000000"})
        masked = mask_xlsx(raw, scanner)
        from openpyxl import load_workbook
        wb = load_workbook(io.BytesIO(masked))
        ws = wb.active
        assert ws["A1"].value == "Revenue"

    def test_numeric_cells_unchanged(self, scanner):
        from scripts.pii_mask.documents import mask_xlsx
        from openpyxl import Workbook
        wb = Workbook()
        ws = wb.active
        ws["A1"] = 42
        ws["B1"] = 3.14
        buf = io.BytesIO()
        wb.save(buf)

        masked = mask_xlsx(buf.getvalue(), scanner)
        from openpyxl import load_workbook
        wb2 = load_workbook(io.BytesIO(masked))
        ws2 = wb2.active
        assert ws2["A1"].value == 42
        assert ws2["B1"].value == 3.14


class TestMaskPptx:
    def test_name_in_slide_masked(self, scanner):
        from scripts.pii_mask.documents import mask_pptx
        raw = _make_pptx(["Presentation by John Doe"])
        masked = mask_pptx(raw, scanner)
        from pptx import Presentation
        prs = Presentation(io.BytesIO(masked))
        all_text = " ".join(
            shape.text_frame.text
            for slide in prs.slides
            for shape in slide.shapes
            if shape.has_text_frame
        )
        assert "John Doe" not in all_text

    def test_no_pii_preserved(self, scanner):
        from scripts.pii_mask.documents import mask_pptx
        raw = _make_pptx(["Q4 Business Review"])
        masked = mask_pptx(raw, scanner)
        from pptx import Presentation
        prs = Presentation(io.BytesIO(masked))
        shapes = [s for s in prs.slides[0].shapes if s.has_text_frame]
        assert shapes[0].text_frame.text == "Q4 Business Review"
```

#### 2. Update masker integration tests

**Files**: `tests/test_masker_google.py`, `tests/test_masker_jira.py`, `tests/test_masker_slack.py`
**Changes**: Add test cases for Office doc masking through each masker's `mask_file()` method. Follow the existing pattern (upload to mock S3 → call `mask_file` → verify output).

### Success Criteria

#### Automated Verification:
- [ ] `uv run pytest tests/test_documents.py -v` — all pass
- [ ] `uv run pytest tests/ -v` — all tests pass (no regressions)

---

## Testing Strategy

### Unit Tests (`tests/test_documents.py`):
- DOCX: paragraphs, tables, headers/footers, comments, core properties, empty doc, no-PII passthrough
- XLSX: string cells, numeric cells (unchanged), comments, sheet titles, core properties, hidden sheets
- PPTX: slide text, tables, group shapes, speaker notes, core properties
- Run-split handling: PII spanning multiple runs
- Corrupted file handling: invalid bytes → graceful error
- Secondary XML pass: footnotes (DOCX), SmartArt (PPTX), app properties (all)

### Integration Tests (masker tests):
- Each masker: upload Office doc → `mask_file()` → verify PII replaced in destination
- Key enumeration: verify `list_keys()` includes Office doc paths from indexes/metadata
- Key rewriting: verify destination S3 key has masked user slug (Google)

### Manual Testing Steps:
1. Export a Google user's Drive with known Docs/Sheets/Slides
2. Run `python -m scripts.pii_mask --exporters google --store test.db ...`
3. Download masked Drive files, open in Office apps — verify text replaced, formatting intact
4. `grep -ri "real_name" masked_output/` — zero matches in Office doc content

## Performance Considerations

- **~46K Drive files + unknown attachment count**: At ~10 files/sec per CPU process (library parse + scan + save), 46K files takes ~5 min on 16 processes. Well within the 12-hour budget.
- **Memory**: Each file is loaded entirely into memory via BytesIO. Largest Office docs from Google export are typically < 10 MB. No memory concern.
- **openpyxl shape loss**: Acceptable for Google-exported files (no shapes). User-uploaded XLSX files from Jira/Slack may lose embedded images. Consider logging a warning when shapes are detected.
- **Jira `list_keys()` downloads each ticket**: O(tickets) S3 GETs during enumeration. For 138K tickets this adds ~2-3 min. Acceptable as a one-time cost per run.

## Migration Notes

None — this is additive. Existing JSON/EML masking is unchanged. Office docs that were previously skipped will now be masked.

## References

- Original strategy: `specs/plans/2026-03-31-pii-masking-strategy.md` (Phase 4: Binary File Handling, lines 298-313)
- Research findings: `specs/research/office-doc-masking/findings.md`
- EML masking pattern: `scripts/pii_mask/eml.py`
- Masker base class: `scripts/pii_mask/maskers/base.py`
- Google masker: `scripts/pii_mask/maskers/google.py`
- Jira masker: `scripts/pii_mask/maskers/jira.py`
- Slack masker: `scripts/pii_mask/maskers/slack.py`
