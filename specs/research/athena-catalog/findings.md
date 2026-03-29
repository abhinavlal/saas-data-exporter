# Athena Catalog — Research Findings

## Current S3 Layout (post-refactor)

Exporters now write **per-item files** with lightweight index files instead of monolithic JSON arrays.

```
{prefix}/
  _checkpoints/{exporter}/{job_id}.json

  github/{owner}__{repo}/
    repo_metadata.json                 JSON object — upload_json
    contributors.json                  JSON array — upload_json
    commits/{sha}.json                 JSON object per commit — upload_json
    prs/{number}.json                  JSON object per PR — upload_json
    pull_requests.csv                  CSV — upload_bytes

  jira/{PROJECT}/
    tickets/{key}.json                 JSON object per ticket — upload_json
    tickets/_index.json                JSON {keys: [...], custom_fields: [...]}
    tickets.csv                        CSV — upload_bytes
    attachments/{KEY}/{filename}       Binary — upload_file

  slack/{channel_id}/
    channel_info.json                  JSON object (raw Slack API) — upload_json
    messages/{ts}.json                 JSON object per message — upload_json
    messages/_index.json               JSON array of ts strings
    attachments/{file_id}_{name}       Binary — upload_file

  google/{user_slug}/
    gmail/{msg_id}.eml                 RFC 2822 — upload_bytes
    gmail/_index.json                  JSON array [{id, threadId, labelIds, snippet, internalDate, sizeEstimate, attachments}]
    gmail/attachments/{msg_id}/{fn}    Binary — upload_bytes
    calendar/events/{event_id}.json    JSON object per event — upload_json
    calendar/_index.json               JSON array of event_id strings
    drive/{name}                       Binary — upload_file
    drive/_index.json                  JSON array [{id, name, mimeType, size, modifiedTime, owners, downloaded, skip_reason?}]
```

## Key Observations

1. **NDJSONWriter is no longer used** — all `_wip` files are gone
2. **Index files are lightweight** — just keys/IDs, not full data (except gmail/_index.json and drive/_index.json which have some metadata)
3. **Per-item files mean post-export scanning is expensive** — would require N S3 GETs per target
4. **Checkpoint pattern** (`lib/checkpoint.py`) provides a proven model for throttled S3 persistence: save every 30s unless `force=True`
5. **Data is in memory at write time** — each exporter has full item data before calling `upload_json`, making inline stat accumulation zero-cost

## Available Fields per Exporter

### GitHub
- **repo_metadata**: full_name, private, stars, forks, open_issues, watchers, language_breakdown, topics, license
- **contributors**: login, id, type, contributions
- **commits**: sha, author_name/email/login, author_date, committer_*, parents; optionally stats.{additions,deletions,total}, files[]
- **PRs**: number, title, state, author, created/updated/closed/merged_at, draft, labels[], assignees[], additions/deletions/changed_files, reviews[], review_comments[], comments[], commits[]

### Jira
- **tickets**: key, summary, issue_type, status, status_category, priority, resolution, assignee, reporter, labels[], components[], fix_versions[], comments[], attachments[{mime_type, size}], changelog[{field}], custom fields
- **_index.json**: keys list + custom_fields list

### Slack
- **channel_info**: name, is_private, num_members, topic, purpose
- **messages**: ts, user, subtype, text, reactions[{count}], files[], thread_ts, reply_count; after threads phase: _replies[]
- **_index.json**: ts list

### Google
- **gmail/_index.json**: id, threadId, labelIds, snippet, internalDate, sizeEstimate, attachments (filenames)
- **calendar events**: full Calendar API objects (id, summary, start, status, organizer, attendees[], location, hangoutLink)
- **drive/_index.json**: id, name, mimeType, size, modifiedTime, owners, downloaded, skip_reason
