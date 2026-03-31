# PII Masking Strategy — High-Level Plan

## Problem

Our data exporters pull data from 6 SaaS platforms (GitHub, Jira, Slack, Google Workspace, Confluence, BigQuery) and store it in S3 as JSON, EML, Parquet, and binary attachments. This data contains PII — names, emails, usernames, account IDs, phone numbers, and potentially anything users typed into these services.

We need a post-processing pipeline that takes the raw exported data and produces a **masked copy** where all PII is replaced with fake but structurally consistent data. The masked dataset must be safe to share for analysis, ML training, or third-party access.

## Goals

1. **Cross-service consistency** — the same real person maps to the same fake identity everywhere. If "John Doe" appears in a GitHub PR review, a Jira ticket, and a Slack message, all three must show the same fake name.
2. **Structural preservation** — emails look like emails, names look like names, the data remains usable for analysis. No hash gibberish.
3. **Completeness** — cover structured fields, freeform text, email files, columnar data, and binary documents (Office, PDF, images).
4. **Non-destructive** — the pipeline reads from a source bucket and writes to a separate destination bucket. The original data is never modified.
5. **Resumable** — crash and restart without reprocessing completed files.

## Non-Goals

- Masking during export (this is strictly post-processing)
- Salesforce data masking
- Re-importing masked data back into source services

---

## Architecture Overview

```
                     ┌──────────────────┐
                     │   Roster (JSON)  │
                     │                  │
                     │  real identity   │
                     │      ↕           │
                     │  fake identity   │
                     └────────┬─────────┘
                              │
           ┌──────────────────┼──────────────────┐
           │                  │                  │
    ┌──────▼──────┐   ┌──────▼──────┐   ┌──────▼──────┐
    │  Structured │   │    Text     │   │   Binary    │
    │   Field     │   │  Scanning   │   │    File     │
    │ Replacement │   │  (AC + NER) │   │  Handling   │
    └──────┬──────┘   └──────┬──────┘   └──────┬──────┘
           │                 │                  │
           ▼                 ▼                  ▼
    Known JSON/       Freeform text      Office XML,
    Parquet fields    in any format       PDF, Images
```

**Pipeline flow:**

```
Source S3 Bucket (raw exports)
        │
        ▼
  ┌─────────────┐     ┌─────────────────┐
  │  S3 Walker   │────▶│  Route by S3    │
  │  list_keys() │     │  prefix + ext   │
  └─────────────┘     └────────┬────────┘
                               │
              ┌────────────────┼────────────────┐
              ▼                ▼                ▼
     GitHub Masker      Jira Masker      Google Masker   ...
     (knows PR/commit   (knows ticket    (knows EML,
      field schemas)     + ADF schemas)   calendar, drive)
              │                │                │
              ▼                ▼                ▼
        Destination S3 Bucket (masked exports)
```

Each masker module is exporter-specific — it knows exactly which fields in that exporter's output contain structured PII vs. freeform text vs. binary content.

---

## Component 1: The Roster

The roster is the single source of truth for identity mapping. It's a JSON file that maps every known person to a fake identity, covering all their identifiers across all services.

### Structure

```json
{
  "version": 1,
  "domain_map": {
    "org_name.com": "example.com"
  },
  "users": [
    {
      "id": "user-001",
      "real": {
        "email": "john.doe@org_name.com",
        "name": "John Doe",
        "first_name": "John",
        "last_name": "Doe",
        "github_login": "johndoe",
        "slack_user_id": "U01ABC123",
        "slack_display_name": "John Doe",
        "slack_username": "johndoe",
        "jira_account_id": "5f7abc12345",
        "jira_display_name": "John Doe",
        "confluence_account_id": "5f7abc12345"
      },
      "masked": {
        "email": "alice.chen@example.com",
        "name": "Alice Chen",
        "first_name": "Alice",
        "last_name": "Chen",
        "github_login": "achen",
        "slack_user_id": "U01ABC123",
        "slack_display_name": "Alice Chen",
        "slack_username": "achen",
        "jira_account_id": "mask-001",
        "jira_display_name": "Alice Chen",
        "confluence_account_id": "mask-001"
      }
    }
  ]
}
```

### Why a roster (not algorithmic generation)

| Approach | Consistency | Auditable | Editable | Realistic output |
|----------|------------|-----------|----------|-----------------|
| SHA-256 hash | Yes | No | No | No (gibberish) |
| Seeded Faker | Yes | Partially | No (regenerates all) | Yes |
| **Roster file** | **Yes** | **Yes** | **Yes** | **Yes** |

The roster is version-controllable, manually reviewable, and allows overrides for edge cases.

### Roster Builder Tool

A CLI tool that seeds the roster by pulling user lists from each service API:

| Source | API | Join Key |
|--------|-----|----------|
| GitHub | Org members API | `login` + commit emails |
| Slack | `users.list` + `slack-org_name-members.csv` | `email` |
| Jira | User search API | `emailAddress`, `accountId` |
| Google | Admin Directory API | `primaryEmail` |
| Confluence | (shares Jira Atlassian accounts) | `accountId` |

**Cross-referencing:** Email is the natural join key across services. The builder:
1. Pulls user lists from all configured services
2. Groups by email into canonical person records
3. Generates fake replacements using Faker (name, email, username — structurally matching)
4. Outputs roster JSON for manual review and editing
5. Supports incremental updates — new users get added, existing mappings are preserved

**The roster is reviewed by a human before masking runs.** This is intentional — automated generation handles the 95% case, but edge cases (external users, service accounts, bots) need human judgment.

---

## Component 2: Masking Layers

PII appears in three fundamentally different forms in the exported data. Each requires a different detection and replacement strategy.

### Layer 1: Structured Field Replacement

**What:** Known JSON field paths and Parquet columns that always contain PII.

**How:** Each exporter masker declares which fields are PII and what type (email, name, username, account_id). The masker walks the JSON tree, looks up each value in the roster, and replaces it.

**Examples:**
```
github/prs/{n}.json  →  $.author         (github_login)
                     →  $.reviews[*].reviewer  (github_login)
                     →  $.commits[*].author_email  (email)

jira/tickets/{k}.json →  $.assignee       (jira_display_name)
                      →  $.reporter_email  (email)
                      →  $.comments[*].author_account_id  (jira_account_id)

google/calendar/events/{id}.json → $.attendees[*].email  (email)
                                 → $.organizer.displayName  (name)
```

**Performance:** O(fields) per record. The fastest layer — pure dictionary lookups.

**Coverage:** ~30 distinct field paths across all exporters. 100% replacement rate for known fields.

### Layer 2: Roster-Based Text Scanning (Aho-Corasick)

**What:** Freeform text fields — message bodies, ticket descriptions, PR descriptions, commit messages, email bodies, calendar event descriptions.

**How:** Build an Aho-Corasick automaton from all roster-derived search terms. Scan each text field in a single pass, replace all matches.

**Search terms per roster entry:**
- Full name: "John Doe"
- Email: "john.doe@org_name.com"
- GitHub login: "johndoe" (context-dependent — only match in GitHub output or when preceded by `@`)
- Slack mention: "<@U01ABC123>"
- Email local part: "john.doe" (only if >= 6 chars to avoid false positives)

**Library: `pyahocorasick`** — C-extension, O(n) scan time where n = text length, regardless of pattern count. Building the automaton for ~10K patterns takes milliseconds. Scanning a 100KB text field takes microseconds.

**Match rules:**
- Replace longest match first ("John Doe" takes priority over "John")
- Skip isolated common first names (< 5 chars) unless preceded by @, followed by last name, or part of an email
- Case-sensitive matching for names, case-insensitive for emails

**Coverage:** Catches all roster-known identities in freeform text. Does NOT catch people not in the roster.

### Layer 3: NER-Based PII Detection

**What:** PII in freeform text that isn't in the roster — external people mentioned by name, addresses, and other entities the roster doesn't know about.

**How:** Run a Named Entity Recognition model over freeform text fields after Aho-Corasick replacement. Detect remaining PII entities and either replace or flag them.

**Recommended library: Microsoft Presidio**
- Built on spaCy NER models + regex-based recognizers
- Out-of-the-box detection for: person names, emails, phone numbers, credit card numbers, IP addresses, physical addresses, medical record numbers, and more
- Supports custom recognizers (e.g., for Org_Name-specific patterns like employee IDs)
- Pluggable anonymization: replace, hash, redact, or encrypt per entity type
- Production-ready, actively maintained by Microsoft

**Architecture:**
```
Text field
    │
    ▼
┌──────────────┐     Already handled by
│  Presidio    │     Aho-Corasick? ──▶ Skip
│  Analyzer    │
│  (detect)    │     New entity found?
└──────┬───────┘            │
       ▼                    ▼
┌──────────────┐     Generate consistent
│  Presidio    │     fake replacement
│  Anonymizer  │     (or flag for review)
│  (replace)   │
└──────────────┘
```

**When NER adds value over the roster:**
- External contacts mentioned in emails: "Meeting with Sarah from Acme Corp"
- Phone numbers in Jira tickets: "Call me at 9876543210"
- Addresses in calendar locations: "123 MG Road, Bangalore"
- Credit card or ID numbers pasted into Slack

**Performance:** spaCy NER is ~10K tokens/second on CPU. For a Slack message (avg ~50 tokens), that's ~200 messages/second. Slower than Aho-Corasick but not a bottleneck if applied only to freeform text fields.

**Dependencies:** `presidio-analyzer`, `presidio-anonymizer`, `spacy`, `en_core_web_lg` model (~560 MB download).

### Layer 4: Regex Patterns (Structural PII)

**What:** PII that has a recognizable structure but isn't in the roster — email addresses from external domains, phone numbers, IP addresses.

**How:** Regex patterns applied to freeform text as a complement to NER. Simpler and faster than NER for structural patterns.

**Patterns:**
- Email addresses not already replaced by roster: `\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b`
- Indian phone numbers: `\b[6-9]\d{9}\b`, `\+91[\s-]?\d{10}\b`
- International phone: `\+\d{1,3}[\s-]?\d{6,14}\b`
- IP addresses: `\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b`

**Note:** Presidio (Layer 3) includes these regex patterns built-in. If we adopt Presidio, Layer 4 is effectively subsumed. Listed separately because regex-only detection could be implemented as a lightweight alternative if NER is too heavy for initial deployment.

---

## Component 3: File Format Handlers

### JSON Files (all exporters)

The bulk of exported data. Each exporter masker knows:
- Which field paths are structured PII (Layer 1)
- Which fields contain freeform text (Layers 2-4)
- Special structures like ADF mention nodes (Jira/Confluence)

**ADF (Atlassian Document Format) handling:** Jira and Confluence store rich text as ADF JSON trees. These contain `{"type": "mention", "attrs": {"id": "account-id", "text": "@John Doe"}}` nodes that must be walked and replaced, not just string-searched.

**Slack message text:** Contains `<@U01ABC123>` mention syntax. The Slack masker pattern-matches these and replaces using the roster's Slack user ID mapping.

### EML Files (Gmail)

Raw RFC 822 email files with MIME structure.

**Approach:**
1. Parse with Python's `email` stdlib (`email.message_from_bytes`)
2. Replace structured headers: `From`, `To`, `CC`, `BCC` — parse with `email.utils.parseaddr`, replace name and email via roster, reconstruct
3. Decode text/HTML body parts, apply text scanning (Layers 2-4)
4. Re-encode the modified message preserving MIME structure
5. Leave binary MIME attachments for binary file handling

**Edge cases:** Quoted-printable encoding, multipart/alternative (text + HTML), nested `message/rfc822` (forwarded emails), non-UTF-8 charsets.

### Parquet Files (BigQuery)

Columnar data from GA4 exports.

**Approach:**
1. Read with `pyarrow.parquet.read_table()`
2. Identify string columns that may contain PII (configurable per dataset)
3. Apply structured replacement and text scanning column-by-column
4. Write back with `pyarrow.parquet.write_table()` preserving schema and compression

**Performance:** pyarrow operates on columnar batches, so string replacement on a column of 1M values is efficient. No need for row-by-row iteration.

### Office Documents (.docx, .xlsx, .pptx)

Google Drive exports Google Docs/Sheets/Slides as Office XML format.

**Approach:** Two-pass strategy using `python-docx`, `openpyxl`, `python-pptx` + raw XML for gaps:

**Primary pass (high-level libraries):**
- **DOCX:** Iterate paragraphs and runs, apply text replacement
- **XLSX:** Iterate cells, apply replacement to string values
- **PPTX:** Iterate slides → shapes → text frames → paragraphs → runs

These libraries preserve formatting, styles, and embedded objects while allowing text-level access. They correctly handle text runs split across multiple XML elements (a single visible word can span 3-5 `<w:r>` elements due to formatting) — naive regex on raw XML would miss PII straddling run boundaries.

**Secondary pass (raw ZIP + XML):** The high-level libraries don't expose Word comments, tracked changes/revision history, custom document properties, or PowerPoint speaker notes. These can contain PII. After the primary pass, open the saved file as a ZIP archive and apply text replacement in `word/comments.xml`, `word/revisions.xml`, `ppt/notesSlides/*.xml`, and `docProps/core.xml`.

**Caveat:** All three libraries reconstruct the file from their object model on save, which can silently drop features they don't understand (macros, some embedded OLE objects). Test round-trip fidelity on representative files.

### PDF Files

**Approach:** Use `PyMuPDF` (imported as `fitz`):
1. Extract text per page with position information
2. Run PII detection (roster + NER) on extracted text
3. Apply redaction annotations over detected PII regions (`page.add_redact_annot()`)
4. Apply redactions (`page.apply_redactions()`) — this permanently removes the underlying text and optionally overlays replacement text
5. Save the modified PDF

**Limitations:**
- Scanned PDFs (image-only) need OCR first
- Complex layouts (multi-column, tables) may have imperfect text extraction
- Redaction changes visual appearance (redacted regions get a fill color or replacement text)
- Embedded fonts may not support replacement text — fallback to black-box redaction

**PyMuPDF** is the only mainstream Python library that supports true PDF redaction (not just annotation overlay). `pikepdf` is lower-level (good for metadata stripping but no text redaction). `pdfplumber` is extraction-only.

### Images

Attachments from Jira, Slack, Google Drive, and Gmail total ~4.3M images. Most are not documents — tracking pixels, email signature logos, GIFs, profile photos. A naive "OCR everything" approach is both slow and expensive. The strategy is a **multi-stage triage pipeline** that minimizes the number of images requiring full OCR.

**Stage 0 — Heuristic pre-filter (zero cost, CPU-only)**

Skip images that cannot contain meaningful text:
- File size < 5 KB (tracking pixels, favicons)
- Dimensions < 100x100 px
- File type: `.gif`, `.ico`, `.svg`, `.webp` (animations, icons, vector graphics)
- Known patterns: `1x1`, `spacer`, `pixel` in filename

Expected elimination: ~35-40% of images.

**Stage 1 — Text detection pre-filter (cheap, CPU or GPU)**

Run PaddleOCR's text detection model only (4.4 MB DB-ResNet, no recognition) on surviving images. The detector outputs bounding boxes — if zero boxes above confidence 0.85, the image contains no text. Detection-only is ~5-10x faster than full OCR because it skips the recognition forward pass entirely.

Expected elimination: ~20-30% more (logos, photos, charts without text labels).

After both stages, ~1.0-1.2M images remain for full OCR.

**Stage 2 — Full OCR on GPU spot instances**

**Recommended engine: PaddleOCR PP-OCRv5** — best balance of accuracy, speed, and cost at this scale.

| Engine | GPU Throughput | VRAM | Accuracy (typed) | Model Size |
|--------|---------------|------|-------------------|------------|
| PaddleOCR PP-OCRv5 | ~12.7 fps (T4) | ~1.2 GB | ~93-97% | 15 MB |
| Surya OCR | ~7.7 fps (A10) | 16-20 GB | ~98.5% | Large |
| EasyOCR | ~97 fps (detect only, T4) | 2.8-3.4 GB | ~92% | 100+ MB |
| Tesseract 5 | ~8 fps (CPU only) | 0 | ~97% clean, poor on noise | 40 MB |

PaddleOCR on a `g4dn.xlarge` spot instance ($0.21/hr):
- ~45K images/hour per instance
- 1.2M images ÷ 45K/hr = ~26 hours on 1 instance
- **4 spot instances in parallel: ~6.5 hours, ~$5.50 total**
- With pre-filter reducing to 600K: ~3.3 hours, **~$3 total**

For highest accuracy (scanned documents, complex layouts), **Surya OCR** on `g5.xlarge` spot instances ($0.60/hr):
- ~28K images/hour per instance
- 1.2M images: ~43 hours, or ~11 hours on 4 instances, **~$26 total**

**Stage 3 — PII detection + redaction**

1. Run extracted text through the same PII pipeline (roster lookup + Aho-Corasick + NER regex)
2. Map detected PII text spans back to bounding box coordinates from OCR
3. Use `Pillow` to draw filled rectangles over PII regions
4. Save the redacted image

**Stage 4 (optional) — LLM spot-check for low-confidence images**

For images where OCR confidence is low or output is garbled, send a sample through a vision LLM:
- Gemini 2.5 Flash batch API: ~$0.44/1K images
- Claude Haiku 4.5 batch: ~$0.80-$1.60/1K images
- At 5% of corpus (60K images): **~$26-$96**

This does OCR + PII detection in one call — useful as a quality backstop, not the primary path.

**Cost comparison for 1.2M images:**

| Approach | Cost per Run | Notes |
|----------|-------------|-------|
| AWS Textract | ~$1,620 | Volume tier at 1M/month |
| Google Cloud Vision | ~$1,800 | No tier break until 5M |
| Mistral OCR 3 (batch) | ~$1,200 | SaaS only, data leaves environment |
| **PaddleOCR on spot GPU** | **~$3-6** | Self-hosted, data stays in VPC |
| Surya on spot GPU | ~$13-26 | Higher accuracy, larger GPU needed |
| Gemini 2.5 Flash (batch, all images) | ~$530 | OCR + PII in one shot |

**Recommendation:** PaddleOCR on spot GPU for bulk processing ($3-6), with optional Gemini 2.5 Flash spot-check for low-confidence images ($26). Total: **~$30-60 per full run** vs. $1,620 for Textract.

**Dependencies:** `paddleocr`, `paddlepaddle-gpu`, `Pillow`, `opencv-python` (pre-processing).
**Infrastructure:** 1-4x `g4dn.xlarge` spot instances (same AWS region as S3 bucket).

---

## Component 4: S3 Key Path Masking

Some S3 key paths contain PII:

| Exporter | Path Pattern | PII in Path |
|----------|-------------|-------------|
| Google | `google/{user_at_domain}/gmail/...` | User's email (mangled: `@` → `_at_`) |
| Google | `google/{user_at_domain}/calendar/...` | Same |
| Google | `google/{user_at_domain}/drive/...` | Same |
| GitHub | `github/{owner}__{repo}/...` | Org name (usually not personal PII) |
| Jira | `jira/{project}/...` | Project key (not PII) |
| Slack | `slack/{channel_id}/...` | Channel ID (not PII) |

**Approach:** The Google masker applies the roster's domain_map + email mapping to rewrite the `{user_at_domain}` path segment. All files under that prefix get the rewritten key in the destination bucket.

**Internal references:** `_index.json` files contain IDs and paths that reference other files. After key rewriting, these references must be updated for the masked dataset to be internally consistent.

---

## Component 5: Roster Builder Tool

### Purpose

Automate the tedious part of building the roster — pulling user lists from each service, cross-referencing by email, generating fake identities. The output is a draft roster JSON for human review.

### Data Sources

```
┌────────────┐   ┌────────────┐   ┌────────────┐   ┌────────────┐
│  GitHub    │   │   Slack    │   │   Jira     │   │  Google    │
│  Org API   │   │ users.list │   │ User search│   │  Admin     │
│            │   │ + members  │   │            │   │ Directory  │
│  login,    │   │ CSV        │   │ accountId, │   │            │
│  email     │   │            │   │ email,     │   │ email,     │
│            │   │ userId,    │   │ displayName│   │ name       │
│            │   │ email,     │   │            │   │            │
│            │   │ displayName│   │            │   │            │
└─────┬──────┘   └─────┬──────┘   └─────┬──────┘   └─────┬──────┘
      │                │                │                │
      └────────────────┼────────────────┼────────────────┘
                       ▼
              ┌─────────────────┐
              │  Join by email  │
              │  Deduplicate    │
              │  Generate fakes │
              └────────┬────────┘
                       ▼
              ┌─────────────────┐
              │  roster.json    │
              │  (draft)        │
              └─────────────────┘
                       │
                  Human review
                       │
                       ▼
              ┌─────────────────┐
              │  roster.json    │
              │  (final)        │
              └─────────────────┘
```

### Incremental Updates

On re-run, the builder:
1. Loads the existing roster
2. Pulls fresh user lists from APIs
3. Adds new users (generates new fake identities)
4. Flags removed users (does not delete — they may still appear in historical data)
5. Never changes existing mappings (stability guarantee)

---

## Per-Exporter Masking Summary

| Exporter | Structured Fields | Freeform Text | Binary/Special |
|----------|------------------|---------------|----------------|
| **GitHub** | PR author/reviewers, commit author/email, contributor logins | PR body/title, commit messages, review comments, diff hunks | None |
| **Jira** | Assignee/reporter/creator (name + email + account_id), comment authors, attachment authors, changelog authors | Ticket summary/description (ADF + plain text), comment bodies (ADF + plain text + HTML), changelog from/to values, custom field values | Attachments (Office, PDF, images) |
| **Slack** | Message `user` ID, reaction user IDs, file user IDs | Message `text`, thread replies, blocks/attachments | File attachments (Office, PDF, images) |
| **Google** | Calendar attendee/organizer emails + names, Drive owner emails + names, Gmail index snippets | Calendar event summary/description/location, email bodies (via EML) | EML files, Gmail attachments, Drive files (Office, PDF, images) |
| **Confluence** | Page/comment `author_id` | Page `title`, page `body` (HTML/ADF), comment bodies | Page attachments (Office, PDF, images) |
| **BigQuery** | Depends on GA4 schema — `user_id`, `user_pseudo_id`, geo fields | Event param values (search terms, page titles) | None (Parquet is columnar, not binary) |

---

## Performance Analysis — 12-Hour Budget

### Dataset Scale

| Category | Objects | Storage | Dominant Format |
|----------|--------:|--------:|-----------------|
| Gmail EML | 7.7M | 1,176 GB | RFC 822 binary |
| Gmail attachments | 4.3M | ~200 GB | 66% PNG, 11% JPG, 7% PDF |
| Jira tickets + comments | 138K + 371K | ~5 GB | JSON |
| Jira attachments | ~500K | 188 GB | Mixed binary |
| GitHub PRs + commits | ~80K files | ~15 GB | JSON |
| Slack messages | 74K | ~1 GB | JSON |
| Calendar events | 564K | ~2 GB | JSON |
| Confluence pages | 4.4K | 3.6 GB | JSON + binary |
| Drive files | 106K | ~50 GB | Office XML + mixed |
| **Total** | **~12M** | **~2 TB** | |

### Processing Time Estimates (Single-Threaded Baseline)

| Operation | Volume | Rate (1 thread) | Single-Thread Time |
|-----------|--------|------------------|--------------------|
| S3 I/O (read + write) | 4 TB round-trip | ~1 GB/s (parallel streams) | ~1 hr |
| JSON structured replacement | ~860K files | ~1,000/sec | ~15 min |
| EML parse + mask + re-encode | 7.7M files | ~300/sec | **~7 hrs** |
| Aho-Corasick text scan | ~8.9M text fields | ~50,000/sec | ~3 min |
| NER (Presidio + spaCy) | ~8.9M text fields | ~1,000/sec | **~2.5 hrs** |
| Office docs (docx/xlsx/pptx) | ~46K files | ~10/sec | ~1.3 hrs |
| PDF redaction (PyMuPDF) | ~180K files | ~3/sec | **~17 hrs** |
| Image OCR | ~3.3M images | ~5/sec | **~7.6 days** |

**Three bottlenecks:** EML processing, PDF redaction, and image OCR. The JSON/text/Office workloads are manageable.

### The Image Problem

3.3M images at any OCR speed is the dominant cost. But most Gmail image attachments are **not documents containing PII**:

| Image Category | Est. Share | Contains Readable PII? |
|----------------|-----------|----------------------|
| Tracking pixels (1x1, tiny) | ~30% | No |
| Email signature logos/icons | ~25% | No |
| GIF animations/emoji | ~5% | No |
| Screenshots | ~15% | Possibly |
| Photos | ~10% | Rarely |
| Scanned documents | ~5% | Yes |
| Charts/diagrams | ~10% | Possibly |

**Strategy: Multi-stage triage pipeline before OCR.**

```
3.3M images
    │
    ▼
┌──────────────────────────┐
│  Stage 0: Heuristics     │  Skip < 5 KB, < 100x100 px,
│  (eliminates ~35%)       │  .gif/.ico/.svg/.webp
│  Cost: $0 (CPU)          │  → ~2.1M remain
└──────────┬───────────────┘
           ▼
┌──────────────────────────┐
│  Stage 1: PaddleOCR      │  Detection model only (4.4 MB)
│  text detection filter   │  Zero bounding boxes → no text
│  (eliminates ~25%)       │  → ~1.2M remain
│  Cost: ~$2 (CPU/GPU)     │
└──────────┬───────────────┘
           ▼
┌──────────────────────────┐
│  Stage 2: Full OCR       │  PaddleOCR PP-OCRv5 on spot GPU
│  on ~1.2M images         │  4x g4dn.xlarge: ~3-6 hrs
│  Cost: ~$3-6 (spot GPU)  │  
└──────────┬───────────────┘
           ▼
┌──────────────────────────┐
│  Stage 3 (optional):     │  Gemini 2.5 Flash batch on ~5%
│  LLM spot-check          │  low-confidence images
│  Cost: ~$26              │
└──────────────────────────┘

Total OCR cost per run: ~$30-60 (vs. $1,620 for Textract)
```

After triage, OCR volume drops from 3.3M to ~1.2M images. With 4 spot GPU instances in parallel, this completes in ~3-6 hours.

### Parallelism Architecture

Single-threaded won't work. The pipeline needs **two kinds of parallelism**:

**ThreadPoolExecutor** for I/O-bound work (S3 reads/writes, JSON processing):
- JSON/NDJSON masking
- S3 upload/download
- Simple binary passthrough (copy unchanged files)

**ProcessPoolExecutor** for CPU-bound work (NER, OCR, PDF, EML):
- spaCy NER is CPU-bound and holds the GIL — threads don't help, need separate processes
- OCR is CPU-intensive per image
- EML parsing + re-encoding is CPU-bound at volume
- PDF text extraction + redaction is CPU-bound

```
┌─────────────────────────────────────────────────────┐
│                  Main Orchestrator                    │
│  (S3 walker, checkpoint manager, routing)            │
└──────────────┬──────────────────────┬────────────────┘
               │                      │
    ┌──────────▼──────────┐  ┌───────▼────────────────┐
    │  I/O Thread Pool    │  │  CPU Process Pool       │
    │  (32 threads)       │  │  (N workers = CPU cores)│
    │                     │  │                         │
    │  - S3 get/put       │  │  - EML parse + mask     │
    │  - JSON mask        │  │  - NER (Presidio)       │
    │  - Binary copy      │  │  - PDF redaction        │
    │  - Checkpoint save  │  │  - OCR + image redact   │
    │                     │  │  - Office doc masking   │
    └─────────────────────┘  └─────────────────────────┘
```

### Projected Timeline (16-core machine, 32 I/O threads)

| Phase | Work | Parallelism | Est. Time |
|-------|------|-------------|-----------|
| 1. Load roster + build AC automaton | 1 roster, ~10K patterns | Single | ~1 sec |
| 2. S3 list_keys | ~12M keys | Single (paginated) | ~5 min |
| 3. JSON masking (all exporters) | ~860K files, ~25 GB | 32 I/O threads | ~20 min |
| 4. EML processing | 7.7M files, 1.2 TB | 16 CPU processes | **~30 min** |
| 5. NER pass (all text fields) | ~8.9M fields | 16 CPU processes (batched `nlp.pipe()`) | **~15 min** |
| 6. Office doc masking | ~46K files | 16 CPU processes | ~10 min |
| 7. PDF redaction | ~180K files | 16 CPU processes | **~1.5 hrs** |
| 8. Image triage + OCR | ~1.2M after triage | 4x g4dn.xlarge spot (PaddleOCR) | **~3-6 hrs** |
| 9. Binary passthrough (non-processable) | ~2M files, ~300 GB | 32 I/O threads | ~30 min |
| **Total (pipelined)** | | | **~6-8 hrs** |

Phases 3-9 can overlap since they process different files. With good pipelining, total wall-clock time is dominated by the slowest phase (OCR) plus I/O overhead.

**The 12-hour budget is achievable on a single 16-core machine.** A 32-core machine or running on an EC2 c6i.8xlarge would give comfortable margin.

### Key Performance Decisions

**1. NER must use `nlp.pipe()` batching, not individual calls.**
spaCy's `nlp.pipe(texts, batch_size=256)` is 5-10x faster than calling `nlp(text)` in a loop. The pipeline should batch text fields before sending to NER.

**2. Cloud OCR is strongly preferred for images.**
AWS Textract async batch API or Google Cloud Vision batch can process ~100 images/sec aggregate without any local CPU cost. Local OCR (EasyOCR, 16 processes) achieves ~80/sec but consumes all CPU cores. Cloud OCR frees CPU for NER and PDF work to run concurrently.

**3. EML processing must stream, not buffer.**
1.2 TB of EML cannot be held in memory. Each process reads one EML → masks → writes → moves to next. No accumulation.

**4. Checkpoint granularity trades off resume speed vs. checkpoint size.**
12M items in `completed_ids` set = ~300 MB JSON checkpoint. This is large but manageable. Alternative: checkpoint per S3 prefix (per-user, per-repo) to keep sets smaller and enable per-exporter parallelism.

**5. Process different exporters' data concurrently.**
Gmail EML (I/O + CPU heavy) can process alongside GitHub JSON (I/O light). The pipeline should interleave file types across workers rather than processing all of one type before the next.

---

## Execution Model

### Pipeline Configuration

```bash
python -m scripts.pii_mask \
    --src-bucket    raw-exports \
    --dst-bucket    masked-exports \
    --s3-prefix     v31/ \
    --roster        roster.json \
    --io-workers    32 \
    --cpu-workers   16 \
    --enable-ner \
    --ocr-mode      cloud          # cloud | local | skip
    --ocr-provider  textract       # textract | vision
```

### Processing Flow

```
1. Load roster → build Aho-Corasick automaton + lookup indices
2. Load NER model into each CPU worker process (fork after loading → shared memory)
3. S3 list_keys("") on source bucket → classify files into work queues:
   - json_queue:    JSON files → I/O thread pool
   - eml_queue:     EML files → CPU process pool
   - text_queue:    Freeform text fields extracted by other handlers → CPU process pool (NER)
   - office_queue:  .docx/.xlsx/.pptx → CPU process pool
   - pdf_queue:     .pdf files → CPU process pool
   - image_queue:   images (after triage) → cloud OCR or CPU process pool
   - copy_queue:    non-processable binaries → I/O thread pool (passthrough)
4. Process all queues concurrently with backpressure
5. Per-file checkpoint after each item completes
6. Write manifest (original → masked key mapping, flagged items, stats)
```

### Checkpointing

Reuse existing `CheckpointManager` with per-exporter phases:
- Phase per exporter prefix: `"mask/github"`, `"mask/jira"`, `"mask/google"`, etc.
- Each S3 key is an item within its phase: `mark_item_done("mask/google", s3_key)`
- Keeps `completed_ids` sets smaller (~2M per exporter vs. 12M in one set)
- Checkpoint stored in destination bucket at `_checkpoints/pii_mask/{job_id}.json`

### Output Manifest

```json
{
  "source_bucket": "raw-exports",
  "destination_bucket": "masked-exports",
  "roster_version": 1,
  "started_at": "2026-04-01T00:00:00Z",
  "completed_at": "2026-04-01T07:23:00Z",
  "duration_hours": 7.4,
  "total_files": 12000000,
  "masked_files": 10500000,
  "copied_unchanged": 1200000,
  "skipped_files": 250000,
  "flagged_files": 50000,
  "stats_by_type": {
    "json": {"count": 860000, "duration_sec": 1200},
    "eml": {"count": 7700000, "duration_sec": 1800},
    "pdf": {"count": 180000, "duration_sec": 5400},
    "image_ocr": {"count": 1200000, "duration_sec": 14400},
    "office": {"count": 46000, "duration_sec": 600},
    "binary_copy": {"count": 2000000, "duration_sec": 1800}
  },
  "flagged": [
    {"key": "jira/IES/attachments/IES-100/scan.pdf", "reason": "low OCR confidence"},
    {"key": "slack/C090/attachments/F07_photo.heic", "reason": "unsupported image format"}
  ]
}
```

---

## Phased Delivery

### Phase 1: Foundation (Roster + Structured Fields + Pipeline)
- Roster format definition and loader
- Roster builder tool (pull from APIs, cross-reference, generate fakes)
- Structured field replacement for all JSON exporters
- S3 key path rewriting
- Pipeline CLI with multi-queue architecture, checkpointing, and manifest
- Binary passthrough (copy without masking) for non-processable files
- **Result:** All known structured PII fields masked, consistent across exporters. Pipeline runs end-to-end.

### Phase 2: Freeform Text (Aho-Corasick + Regex + EML)
- Aho-Corasick automaton builder from roster search terms
- Text scanning integration into all exporter maskers
- Regex patterns for structural PII (emails, phones)
- EML file parsing and masking (Gmail) — the largest single workload
- Parquet column masking (BigQuery)
- Process pool for CPU-bound EML work
- **Result:** Freeform text in all structured formats is scanned and masked

### Phase 3: NER Integration
- Presidio integration as a second-pass detector
- Batched `nlp.pipe()` processing across CPU workers
- Custom recognizer for Org_Name-specific patterns
- Confidence thresholds and flagging for low-confidence detections
- **Result:** Non-roster PII in freeform text is caught

### Phase 4: Binary File Handling
- **Office XML:** python-docx, openpyxl, python-pptx + raw XML for comments/tracked changes
- **PDF:** PyMuPDF text extraction + redaction (180K files, ~1.5 hrs with 16 processes)
- **Images:** Multi-stage triage pipeline (heuristic filter → PaddleOCR text detection filter → full OCR on spot GPU → Pillow redaction). ~$30-60 per full run.
- Optional LLM vision spot-check (Gemini 2.5 Flash batch) for low-confidence images
- Format-specific skip/flag logic for unsupported types
- **Result:** Binary attachments are masked where technically feasible

### Phase 5: Validation and Hardening
- Automated validation: grep masked output for all roster real values → zero matches
- Cross-exporter consistency checks
- Performance profiling at full scale
- Checkpoint recovery testing (kill and resume)
- **Result:** Production-ready pipeline, confirmed < 12 hours on target hardware

---

## Infrastructure Requirements

| Resource | Minimum | Recommended |
|----------|---------|-------------|
| CPU cores | 8 | 16-32 |
| RAM | 16 GB | 32 GB (NER models + Parquet) |
| Local disk | 50 GB (temp files) | 100 GB SSD |
| Network to S3 | 1 Gbps | 10 Gbps (EC2 same-region) |
| Instance type (AWS) | c6i.4xlarge | c6i.8xlarge |

**For image OCR:** 1-4x `g4dn.xlarge` spot instances ($0.21/hr each) for PaddleOCR. Same AWS region as S3 bucket.

### Cost Per Full Run

| Component | Instance | Hours | Cost |
|-----------|----------|-------|------|
| Main pipeline (JSON, EML, NER, PDF, Office) | c6i.8xlarge | ~8 hrs | ~$13 |
| Image OCR (PaddleOCR on spot GPU) | 4x g4dn.xlarge spot | ~3-6 hrs | ~$3-6 |
| LLM spot-check (optional, 5% of images) | Gemini 2.5 Flash batch API | — | ~$26 |
| S3 requests (12M GET + 12M PUT) | — | — | ~$7 |
| **Total per run** | | | **~$50-52** |

Compare: AWS Textract alone would cost ~$1,620 for the OCR component.

---

## New Dependencies

| Library | Purpose | Size | Phase |
|---------|---------|------|-------|
| `Faker` | Generate fake identities for roster | ~2 MB | 1 |
| `pyahocorasick` | Multi-pattern text matching | ~100 KB (C ext) | 2 |
| `presidio-analyzer` | NER-based PII detection | ~5 MB + spaCy | 3 |
| `presidio-anonymizer` | PII replacement/redaction | ~1 MB | 3 |
| `spacy` + `en_core_web_lg` | NER model backend for Presidio | ~560 MB model | 3 |
| `python-docx` | DOCX text replacement | ~1 MB | 4 |
| `openpyxl` | XLSX cell replacement | ~4 MB | 4 |
| `python-pptx` | PPTX text replacement | ~2 MB | 4 |
| `PyMuPDF` | PDF text extraction + redaction | ~15 MB | 4 |
| `paddleocr` + `paddlepaddle-gpu` | OCR engine (self-hosted on spot GPU) | ~200 MB + 15 MB models | 4 |
| `Pillow` | Image manipulation for redaction | ~3 MB | 4 |
| `opencv-python` | Image pre-processing (deskew, binarize) | ~30 MB | 4 |

---

## Open Questions for Discussion

1. **Roster storage:** Should the roster live in S3 alongside exports, in a separate config bucket, or in version control? It contains the real↔fake mapping, which is itself sensitive.

2. **NER false positives:** Presidio will flag common nouns as names sometimes ("Will" the person vs. "will" the verb). What's our strategy — accept some noise, or require high confidence thresholds and miss more?

3. **Diff hunks and code patches:** GitHub PR exports include `diff_hunk` and `patch` fields containing raw code. Should we mask PII that appears inside code diffs (e.g., hardcoded emails in config files)?

4. **Calendar locations:** "Dr. Sharma's Clinic, 45 MG Road" — is the doctor's name PII we need to mask? What about business names?

5. **Custom Jira fields:** These are arbitrary and project-specific. Do we scan all custom field values with NER, or maintain a manual allowlist/blocklist?

6. **GPU spot instance strategy:** The OCR pipeline needs 1-4x `g4dn.xlarge` spot instances for ~3-6 hours per run (~$3-6). Should this spin up on-demand via a script, or do we want a pre-configured AMI/launch template? Spot interruption handling is needed.

7. **Image triage calibration:** The triage filters aim to skip ~65% of images. Before committing to thresholds, we should run a calibration pass on a random 10K sample to measure actual false-negative rate. How much manual review effort is acceptable for tuning?

8. **Run environment:** The main pipeline (JSON, EML, NER, PDF) needs a 16-32 core CPU instance (c6i.8xlarge, ~$13/run). The OCR pipeline runs separately on GPU instances (~$6-30/run). Both must be same-region as S3. Should these be coordinated by a single orchestrator or run independently?

9. **Incremental masking:** When new exports are added to the source bucket, should the pipeline mask only the delta? The checkpoint model supports this, but key-path changes across runs could cause inconsistencies.
