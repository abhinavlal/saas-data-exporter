# Office Document PII Masking — Research Findings

## Current State

The PII masking pipeline (`scripts/pii_mask/`) handles JSON, EML, and Parquet files. Binary documents (docx, xlsx, pptx) are **completely skipped**:

- `BaseMasker.should_process()` (`maskers/base.py:74`) only returns `True` for `.json` and `.eml`
- `JiraMasker` and `SlackMasker` skip all `/attachments/` paths (`should_process` returns False for `/attachments/`)
- `GoogleMasker._keys_from_indexes()` (`maskers/google.py:68`) excludes Drive files: `"binary files are not masked"`
- Gmail attachments are not enumerated at all

No Office document libraries are installed (`python-docx`, `openpyxl`, `python-pptx` absent from `pyproject.toml`).

## Document Sources

| Source | S3 path pattern | How files get there |
|--------|----------------|---------------------|
| Google Drive | `google/{slug}/drive/{file_id}_{name}.docx/xlsx/pptx` | Google Docs/Sheets/Slides exported as Office XML via `files().export_media()` |
| Jira attachments | `jira/{project}/attachments/{ticket}/{filename}` | User-uploaded files streamed to S3 |
| Slack attachments | `slack/{channel}/attachments/{file_id}_{filename}` | User-shared files streamed to S3 |
| Gmail attachments | `google/{slug}/gmail/attachments/{msg_id}/{filename}` | MIME attachment payloads extracted and uploaded |

## Key Enumeration Today

Each masker uses index files to enumerate keys (avoiding expensive S3 listing):

- **GoogleMasker**: Reads `gmail/_index.json`, `calendar/_index.json`, `drive/_index.json` per user. Drive index exists but only `_index.json` itself is added to key list, not the actual files.
- **JiraMasker**: Reads `tickets/_index.json` per project. Attachment paths are in each ticket's `attachments[].content_url` field but currently ignored.
- **SlackMasker**: Reads `messages/_index.json` per channel. File paths are in messages' `files[]._local_file` field but currently ignored.

## Library Research

### python-docx (v1.2.0)

**High-level API covers:**
- Body paragraphs and runs (via `doc.iter_inner_content()`)
- Tables including nested (via `cell.iter_inner_content()`)
- Headers and footers (via `section.header/footer`)
- Comments (new in 1.2.0: `doc.comments`)
- Core properties (`doc.core_properties`: author, title, subject, etc.)

**Not covered (needs secondary XML pass):**
- Footnotes (`word/footnotes.xml`)
- Endnotes (`word/endnotes.xml`)
- Tracked changes (`<w:ins>/<w:del>` in `word/document.xml`)
- App properties (`docProps/app.xml` — company name)

**Key pattern:** Modify `run.text` (not `paragraph.text`) to preserve character formatting. `paragraph.text =` destroys all formatting by replacing all runs with one.

**Round-trip:** Preserves most features. Macros and some OLE objects may be dropped.

### openpyxl (v3.1.5)

**High-level API covers:**
- All cell values across all sheets (including hidden/veryHidden)
- Cell comments (text + author)
- Sheet titles
- Core properties (creator, title, subject, etc.)

**Not covered:**
- App properties (`docProps/app.xml`)

**Critical limitation:** Shapes, drawings, and embedded images are **silently dropped on save**. This is acceptable for Google-exported files (no shapes) but may damage user-uploaded files from Jira/Slack/Gmail.

**Round-trip:** Most lossy of the three. Charts may break, shapes dropped, VBA stripped unless `keep_vba=True`.

### python-pptx (v1.0.2)

**High-level API covers:**
- All slide shapes with text frames (paragraphs → runs)
- Group shapes (recursive `shape.shapes`)
- Tables in slides
- Speaker notes (`slide.notes_slide.notes_text_frame`)
- Core properties

**Not covered:**
- SmartArt text (`ppt/diagrams/data*.xml` — only accessible via ZIP+XML)
- App properties (`docProps/app.xml`)

**Round-trip:** Best of the three. Unsupported features preserved through the package layer.

### BytesIO Support

All three libraries support `Document(BytesIO(raw_bytes))` → modify → `doc.save(BytesIO())` → `buf.getvalue()`. No temp files needed.

### Secondary ZIP+XML Pass

All Office XML formats are ZIP archives. Content missed by high-level APIs can be reached by:
1. Opening the saved bytes as a ZIP
2. Parsing specific XML files with `lxml.etree`
3. Running text replacement on `<w:t>` / `<a:t>` elements
4. Reconstructing the ZIP

Key XML paths:
- DOCX: `word/footnotes.xml`, `word/endnotes.xml`, `docProps/app.xml`
- PPTX: `ppt/diagrams/data*.xml` (SmartArt), `docProps/app.xml`
- XLSX: `docProps/app.xml`

## Run-Split Problem

Office XML often splits a single word across multiple runs for formatting reasons. E.g., "John Doe" might be stored as runs ["J", "ohn", " Doe"] due to spellcheck or editing history.

**Impact:** Running `scanner.scan(run.text)` per-run won't detect "John Doe" spanning runs.

**Solution:** Concatenate all run texts in a paragraph, scan the joined text, then distribute replacements back to runs proportionally. This is the standard approach used by PII masking tools operating on Office XML.

## Existing Patterns to Follow

- **`eml.py`**: Shared module with a public `mask_eml(bytes, scanner) -> bytes` function. Called by `GoogleMasker._mask_eml_file()`. This is the exact pattern to replicate.
- **`BaseMasker.mask_file()`**: Downloads bytes/JSON, transforms, uploads. Returns status string.
- **Pipeline**: `ProcessPoolExecutor` workers call `_mask_file((masker_name, key))`. Each worker has its own scanner/S3 clients. Document masking must be stateless per-call.
- **Error handling**: Per-file try/except, log error, return "error" status. Never crash the worker.
