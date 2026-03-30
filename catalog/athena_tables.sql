-- Athena DDL for catalog tables
-- Replace {BUCKET} and {PREFIX} with your S3 bucket and prefix.
-- Run in the Athena console after running: python -m exporters.catalog

CREATE DATABASE IF NOT EXISTS data_export_catalog;

-- GitHub repos inventory
CREATE EXTERNAL TABLE IF NOT EXISTS data_export_catalog.github_repos (
  target STRING,
  target_slug STRING,
  exported_at STRING,
  private BOOLEAN,
  stars INT,
  forks INT,
  open_issues INT,
  watchers INT,
  total_contributors INT,
  total_commits INT,
  commit_unique_authors INT,
  total_prs INT,
  prs_open INT,
  prs_closed INT,
  prs_merged INT,
  total_reviews INT,
  total_review_comments INT,
  total_comments INT,
  total_additions BIGINT,
  total_deletions BIGINT,
  total_changed_files BIGINT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://{BUCKET}/{PREFIX}/catalog/github_repos/';

-- GitHub language breakdown
CREATE EXTERNAL TABLE IF NOT EXISTS data_export_catalog.github_languages (
  target STRING,
  language STRING,
  bytes BIGINT,
  percentage DOUBLE
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://{BUCKET}/{PREFIX}/catalog/github_languages/';

-- Jira projects inventory
CREATE EXTERNAL TABLE IF NOT EXISTS data_export_catalog.jira_projects (
  target STRING,
  exported_at STRING,
  total_tickets INT,
  by_type MAP<STRING, INT>,
  by_status MAP<STRING, INT>,
  by_status_category MAP<STRING, INT>,
  by_priority MAP<STRING, INT>,
  total_comments INT,
  tickets_with_comments INT,
  total_attachments INT,
  total_attachment_size_bytes BIGINT,
  attachments_by_mime_type MAP<STRING, INT>,
  total_changelog_entries INT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://{BUCKET}/{PREFIX}/catalog/jira_projects/';

-- Slack channels inventory
CREATE EXTERNAL TABLE IF NOT EXISTS data_export_catalog.slack_channels (
  target STRING,
  channel_name STRING,
  is_private BOOLEAN,
  num_members INT,
  exported_at STRING,
  total_messages INT,
  thread_parents INT,
  total_thread_replies INT,
  with_reactions INT,
  total_reactions INT,
  by_subtype MAP<STRING, INT>,
  total_files INT,
  files_downloaded INT,
  files_by_extension MAP<STRING, INT>
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://{BUCKET}/{PREFIX}/catalog/slack_channels/';

-- Google Workspace users inventory
CREATE EXTERNAL TABLE IF NOT EXISTS data_export_catalog.google_users (
  target STRING,
  target_slug STRING,
  exported_at STRING,
  gmail_messages INT,
  gmail_size_bytes BIGINT,
  gmail_attachments INT,
  gmail_messages_with_attachments INT,
  gmail_attachments_by_extension MAP<STRING, INT>,
  calendar_events INT,
  calendar_with_attendees INT,
  calendar_with_location INT,
  drive_files INT,
  drive_downloaded INT,
  drive_skipped INT,
  drive_size_bytes BIGINT,
  drive_by_mime_type MAP<STRING, INT>
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://{BUCKET}/{PREFIX}/catalog/google_users/';

-- Confluence spaces inventory
CREATE EXTERNAL TABLE IF NOT EXISTS data_export_catalog.confluence_spaces (
  target STRING,
  exported_at STRING,
  total_pages INT,
  by_status MAP<STRING, INT>,
  total_comments INT,
  pages_with_comments INT,
  total_attachments INT,
  total_attachment_size_bytes BIGINT,
  attachments_by_media_type MAP<STRING, INT>
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://{BUCKET}/{PREFIX}/catalog/confluence_spaces/';

-- Cross-exporter file type breakdown
CREATE EXTERNAL TABLE IF NOT EXISTS data_export_catalog.file_types (
  exporter STRING,
  target STRING,
  category STRING,
  file_type STRING,
  count INT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://{BUCKET}/{PREFIX}/catalog/file_types/';

-- Example queries:
--
-- Total inventory by exporter:
--   SELECT 'github' as exporter, COUNT(*) as targets, SUM(total_prs) as prs FROM data_export_catalog.github_repos
--   UNION ALL
--   SELECT 'jira', COUNT(*), SUM(total_tickets) FROM data_export_catalog.jira_projects
--   UNION ALL
--   SELECT 'slack', COUNT(*), SUM(total_messages) FROM data_export_catalog.slack_channels
--   UNION ALL
--   SELECT 'google', COUNT(*), SUM(gmail_messages) FROM data_export_catalog.google_users;
--
-- Email attachments by file type:
--   SELECT file_type, SUM(count) as total
--   FROM data_export_catalog.file_types
--   WHERE category = 'email_attachment'
--   GROUP BY file_type
--   ORDER BY total DESC;
--
-- GitHub repos by language:
--   SELECT language, SUM(bytes) as total_bytes
--   FROM data_export_catalog.github_languages
--   GROUP BY language
--   ORDER BY total_bytes DESC;
