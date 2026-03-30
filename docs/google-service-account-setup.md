# Google Service Account Setup

The Google Workspace exporter uses a service account with domain-wide delegation to access Gmail, Calendar, and Drive on behalf of users in your organization.

## Step 1: Create a Google Cloud Project

1. Go to [Google Cloud Console](https://console.cloud.google.com)
2. Click **Select a project > New Project**
3. Name it (e.g., `data-exporter`) and create

## Step 2: Enable APIs

In the Cloud Console, go to **APIs & Services > Enable APIs and Services** and enable:

- **Gmail API**
- **Google Calendar API**
- **Google Drive API**

## Step 3: Create a Service Account

1. Go to **IAM & Admin > Service Accounts**
2. Click **Create Service Account**
3. Name: `data-exporter` (or any name)
4. Click **Create and Continue**
5. Skip the optional role grants, click **Done**
6. Click on the created service account
7. Go to **Keys > Add Key > Create new key > JSON**
8. Download the key file and save as `service-account.json` in your project directory
9. Note the **Client ID** (a numeric ID like `123456789012345678901`) — you'll need it for domain-wide delegation

The JSON key file contains:

```json
{
  "type": "service_account",
  "project_id": "your-project",
  "private_key_id": "...",
  "private_key": "-----BEGIN PRIVATE KEY-----\n...",
  "client_email": "data-exporter@your-project.iam.gserviceaccount.com",
  "client_id": "123456789012345678901",
  ...
}
```

## Step 4: Enable Domain-Wide Delegation

1. On the service account page, click **Edit** (pencil icon)
2. Expand **Show advanced settings**
3. Check **Enable Google Workspace Domain-wide Delegation**
4. Click **Save**

## Step 5: Authorize in Google Workspace Admin

This step requires a Google Workspace admin.

1. Go to [Google Workspace Admin Console](https://admin.google.com)
2. Navigate to **Security > Access and data control > API controls**
3. Click **Manage Domain Wide Delegation**
4. Click **Add new**
5. Enter the **Client ID** from the service account (the numeric ID, not the email)
6. Enter the following OAuth scopes (comma-separated):

```
https://www.googleapis.com/auth/gmail.readonly,https://www.googleapis.com/auth/calendar.readonly,https://www.googleapis.com/auth/drive.readonly
```

7. Click **Authorize**

## Step 6: Configure the Exporter

Add to your `.env`:

```bash
GOOGLE_SERVICE_ACCOUNT_KEY=service-account.json
GOOGLE_INPUT_CSV=google_users.csv          # CSV with 'user' column
# Or list users directly:
# GOOGLE_USERS=alice@company.com,bob@company.com
```

The CSV should have one column `user`:

```csv
user
alice@company.com
bob@company.com
charlie@company.com
```

## Step 7: Test

```bash
uv run python -m exporters.google_workspace \
  --user one-user@company.com \
  --emails 3 --events 3 --files 3 \
  --no-json-logs
```

You should see:

```
INFO  Starting Google Workspace export for one-user@company.com
INFO  Exporting Gmail for one-user@company.com (limit=3)
INFO  Found 3 Gmail message IDs
...
INFO  Google Workspace export complete for one-user@company.com
```

## How It Works

- The service account impersonates each user via `credentials.with_subject(user_email)`
- Each API call includes `quotaUser=user_email` so Google charges the user's own quota, not the service account's shared pool
- This enables safe parallelism: `--parallel 50` runs 50 users simultaneously with independent rate limits

## Rate Limits

| API | Per-User Limit | With 50 parallel users |
|-----|---------------|----------------------|
| Gmail | 15,000 quota units/min | 750,000 units/min combined |
| Drive | 12,000 req/min | 600,000 req/min combined |
| Calendar | ~500-1,000 req/min | ~25,000-50,000 req/min combined |

The exporter was validated with 519 users at `--parallel 50` with zero rate limit errors.

## Troubleshooting

**`403: Not Authorized to access this resource/api`**
- The service account doesn't have domain-wide delegation enabled, or the OAuth scopes aren't authorized in Workspace Admin

**`403: Delegation denied for user@company.com`**
- The user doesn't exist, is suspended, or is outside the organizational unit the delegation applies to

**`exportSizeLimitExceeded` on Drive files**
- Google Sheets/Docs larger than 10MB can't be exported via API. The exporter logs a warning and skips the file.

**Slow Gmail export**
- Gmail fetches in batches of 10 with a 2s sleep between batches. A user with 50K emails takes ~2.7 hours. Increase `--parallel` to process more users simultaneously.
