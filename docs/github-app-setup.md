# GitHub App Setup

GitHub Apps provide higher rate limits than Personal Access Tokens (PATs) and don't expire:

| Auth Method | Rate Limit | Expiry |
|-------------|-----------|--------|
| PAT | 5,000 req/hr (shared per user) | configurable (30-365 days) |
| GitHub App | 5,000-12,500 req/hr per installation | tokens auto-refresh (1hr) |

Multiple Apps give independent rate limit pools. 2 Apps = up to 25,000 req/hr.

## Step 1: Create the App

1. Go to your GitHub org: **Settings > Developer settings > GitHub Apps > New GitHub App**
2. Fill in:
   - **App name:** `data-exporter` (or any name)
   - **Homepage URL:** any URL (e.g., `https://github.com/your-org`)
   - **Webhook:** uncheck "Active" (not needed)
3. **Permissions** (Repository permissions):
   - **Contents:** Read-only (for commits, files)
   - **Issues:** Read-only (for issue comments on PRs)
   - **Metadata:** Read-only (always required)
   - **Pull requests:** Read-only (for PR data, reviews, comments)
4. **Where can this app be installed?** Select "Only on this account"
5. Click **Create GitHub App**
6. Note the **App ID** displayed on the app's settings page

## Step 2: Generate a Private Key

1. On the app's settings page, scroll to **Private keys**
2. Click **Generate a private key**
3. A `.pem` file downloads automatically
4. Save it to your project directory (e.g., `github-app.pem`)
5. Add `*.pem` to `.gitignore` if not already there

## Step 3: Install the App

1. On the app's settings page, click **Install App** in the left sidebar
2. Select your organization
3. Choose **All repositories** or select specific repos
4. Click **Install**
5. Note the **Installation ID** from the URL: `github.com/organizations/ORG/settings/installations/INSTALLATION_ID`

## Step 4: Configure the Exporter

Add to your `.env`:

```bash
# Single app
GITHUB_APP_ID=123456
GITHUB_APP_PRIVATE_KEY=github-app.pem
GITHUB_APP_INSTALLATION_ID=78901234
```

Or via CLI:

```bash
uv run python -m exporters.github \
  --app-id 123456 \
  --app-key github-app.pem \
  --app-installation-id 78901234
```

The exporter auto-refreshes installation tokens (they expire hourly).

## Multiple Apps for Higher Throughput

Each App gets its own independent rate limit pool. Create 2-4 Apps and the exporter round-robins repos across them:

```bash
# .env — 2 apps, comma-separated
GITHUB_APP_ID=111,222
GITHUB_APP_PRIVATE_KEY=app1.pem,app2.pem
GITHUB_APP_INSTALLATION_ID=AAA,BBB
```

With 2 Apps and `--parallel 4`: 4 repos run simultaneously, each pair sharing an App's rate limit pool.

## PAT Fallback

If GitHub App is not configured, the exporter falls back to PAT auth. Multiple PATs can also be round-robined:

```bash
GITHUB_TOKEN=ghp_TOKEN1,ghp_TOKEN2,ghp_TOKEN3
```

PATs share the same per-user rate limit, so multiple PATs from the same account don't multiply throughput. Use different accounts or GitHub Apps for true parallelism.

## Verifying the Setup

Test with a single repo:

```bash
uv run python -m exporters.github \
  --app-id 123456 \
  --app-key github-app.pem \
  --app-installation-id 78901234 \
  --repo your-org/your-repo \
  --pr-limit 5 \
  --no-json-logs
```

You should see:

```
INFO  Refreshing GitHub App installation token (app_id=123456, installation=78901234)
INFO  Token refreshed, expires in 60 minutes
INFO  Starting GitHub export for your-org/your-repo
```
