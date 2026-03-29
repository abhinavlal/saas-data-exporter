"""Slack Channel Exporter — exports channel info, messages, threads, and attachments to S3."""

import argparse
import logging
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed

from lib.s3 import S3Store
from lib.checkpoint import CheckpointManager
from lib.stats import StatsCollector
from lib.session import make_session
from lib.logging import setup_logging
from lib.types import ExportConfig

log = logging.getLogger(__name__)

SLACK_API = "https://slack.com/api"

def _safe_ts(msg: dict) -> float:
    """Extract timestamp as float, defaulting to 0 on malformed values."""
    try:
        return float(msg.get("ts", "0"))
    except (ValueError, TypeError):
        return 0.0


SKIP_EXTENSIONS = {
    ".mp4", ".mov", ".avi", ".wmv", ".flv", ".mkv", ".webm",
    ".apk", ".ipa", ".ico", ".heic",
}


def _is_skippable_file(file_obj: dict) -> bool:
    """Check if a file should be skipped (videos, apk, tombstoned, external)."""
    if file_obj.get("mode") in ("tombstone", "external"):
        return True
    name = file_obj.get("name", "")
    ext = "." + name.rsplit(".", 1)[-1].lower() if "." in name else ""
    return ext in SKIP_EXTENSIONS


class SlackExporter:
    def __init__(
        self,
        token: str,
        channel_ids: list[str],
        s3: S3Store,
        config: ExportConfig,
        include_threads: bool = False,
        skip_attachments: bool = False,
        parallel: int = 1,
    ):
        self.channel_ids = channel_ids
        self.parallel = parallel
        self.s3 = s3
        self.config = config
        self.include_threads = include_threads
        self.skip_attachments = skip_attachments

        # Slack Tier 3 methods: ~50/min. Be conservative.
        self.session, self.rate_state = make_session(
            requests_per_second=0.8,
            burst=3,
            min_remaining=50,
            read_timeout=60,
        )
        self.session.headers["Authorization"] = f"Bearer {token}"

    def run(self):
        failed = []
        log.info("Exporting %d channels (%d in parallel)", len(self.channel_ids), self.parallel)
        with ThreadPoolExecutor(max_workers=self.parallel) as pool:
            futures = {pool.submit(self._export_channel, ch): ch for ch in self.channel_ids}
            for future in as_completed(futures):
                channel_id = futures[future]
                try:
                    future.result()
                except Exception:
                    log.error("Export failed for channel %s, continuing with next", channel_id, exc_info=True)
                    failed.append(channel_id)
        if failed:
            log.error("Failed channels (%d/%d): %s",
                      len(failed), len(self.channel_ids), ", ".join(failed))

    def _export_channel(self, channel_id: str):
        log.info("Starting Slack export for channel %s", channel_id)
        checkpoint = CheckpointManager(self.s3, f"slack/{channel_id}")
        checkpoint.load()
        s3_base = f"slack/{channel_id}"
        stats = StatsCollector(self.s3, f"{s3_base}/_stats.json")
        stats.load()
        stats.set("exporter", "slack")
        stats.set("target", channel_id)

        # Step 1: channel info
        if not checkpoint.is_phase_done("channel_info"):
            self._fetch_and_upload_channel_info(channel_id, s3_base, checkpoint, stats)

        # Step 2: messages — each written to messages/{ts}.json
        if not checkpoint.is_phase_done("messages"):
            self._fetch_messages(channel_id, s3_base, checkpoint, stats)

        # Step 3: thread replies — load each parent, fetch replies, update file
        if self.include_threads and not checkpoint.is_phase_done("threads"):
            self._fetch_thread_replies(channel_id, s3_base, checkpoint, stats)

        # Step 4: download attachments — iterate message index
        if not self.skip_attachments and not checkpoint.is_phase_done("attachments"):
            self._download_attachments_from_index(channel_id, s3_base, checkpoint, stats)

        # Load index for final count
        index = self.s3.download_json(f"{s3_base}/messages/_index.json") or []

        from datetime import datetime, timezone
        stats.set("exported_at", datetime.now(timezone.utc).isoformat())
        stats.save(force=True)
        checkpoint.complete()
        log.info("Slack export complete for channel %s (%d messages)", channel_id, len(index))

    # ── Channel Info ──────────────────────────────────────────────────────

    def _fetch_and_upload_channel_info(self, channel_id: str, s3_base: str,
                                       checkpoint: CheckpointManager,
                                       stats: StatsCollector) -> None:
        checkpoint.start_phase("channel_info")
        resp = self.session.get(
            f"{SLACK_API}/conversations.info",
            params={"channel": channel_id},
        )
        resp.raise_for_status()
        data = resp.json()
        if not data.get("ok"):
            log.error("conversations.info failed: %s", data.get("error"))
            return

        channel = data.get("channel", {})
        self.s3.upload_json(channel, f"{s3_base}/channel_info.json")

        stats.set("channel", {
            "name": channel.get("name"),
            "is_private": channel.get("is_private"),
            "num_members": channel.get("num_members"),
        })
        stats.save(force=True)

        checkpoint.complete_phase("channel_info")
        checkpoint.save(force=True)

    # ── Messages ──────────────────────────────────────────────────────────

    def _fetch_messages(self, channel_id: str, s3_base: str,
                        checkpoint: CheckpointManager,
                        stats: StatsCollector) -> None:
        """Fetch messages and write each to messages/{ts}.json. Builds _index.json."""
        checkpoint.start_phase("messages")
        ts_list: list[str] = []
        cursor = checkpoint.get_cursor("messages")
        params = {"channel": channel_id, "limit": 200}
        if cursor:
            params["cursor"] = cursor

        while True:
            resp = self.session.get(f"{SLACK_API}/conversations.history", params=params)
            resp.raise_for_status()
            data = resp.json()

            if not data.get("ok"):
                log.error("conversations.history failed: %s", data.get("error"))
                break

            for msg in data.get("messages", []):
                ts = msg.get("ts", "0")
                self.s3.upload_json(msg, f"{s3_base}/messages/{ts}.json")
                ts_list.append(ts)

                # Accumulate message stats
                stats.increment("messages.total")
                subtype = msg.get("subtype", "user_message")
                stats.add_to_map("messages.by_subtype", subtype)
                if msg.get("reactions"):
                    stats.increment("messages.with_reactions")
                    for r in msg.get("reactions", []):
                        stats.increment("messages.total_reactions", r.get("count", 0))
                for f in msg.get("files", []):
                    stats.increment("files.total")
                    name = f.get("name", "")
                    ext = ("." + name.rsplit(".", 1)[-1]).lower() if "." in name else ".unknown"
                    stats.add_to_map("files.by_extension", ext)
                stats.save()

                checkpoint.mark_item_done("messages", ts)

            next_cursor = data.get("response_metadata", {}).get("next_cursor")
            checkpoint.set_cursor("messages", next_cursor)
            checkpoint.save()

            if not next_cursor:
                break
            params["cursor"] = next_cursor

        # Write lightweight index (just timestamps)
        self.s3.upload_json(ts_list, f"{s3_base}/messages/_index.json")
        stats.save(force=True)
        checkpoint.complete_phase("messages")
        checkpoint.save(force=True)
        log.info("Fetched %d messages from %s", len(ts_list), channel_id)

    # ── Thread Replies ────────────────────────────────────────────────────

    def _fetch_thread_replies(self, channel_id: str, s3_base: str,
                              checkpoint: CheckpointManager,
                              stats: StatsCollector) -> None:
        """Fetch thread replies and embed them as a `_replies` array inside
        the parent message file. One parent loaded at a time."""
        checkpoint.start_phase("threads")
        index = self.s3.download_json(f"{s3_base}/messages/_index.json") or []

        # Find thread parents by loading each message
        thread_parents = []
        for ts in index:
            msg = self.s3.download_json(f"{s3_base}/messages/{ts}.json")
            if msg and msg.get("reply_count", 0) > 0 and msg.get("thread_ts"):
                thread_parents.append(ts)

        log.info("Fetching replies for %d threads", len(thread_parents))
        reply_count = 0

        for thread_ts in thread_parents:
            if checkpoint.is_item_done("threads", thread_ts):
                continue
            try:
                replies = self._fetch_single_thread(channel_id, thread_ts)
                # Load parent, embed replies, re-upload
                parent = self.s3.download_json(f"{s3_base}/messages/{thread_ts}.json")
                if parent:
                    parent["_replies"] = replies
                    self.s3.upload_json(parent, f"{s3_base}/messages/{thread_ts}.json")
                reply_count += len(replies)

                stats.increment("messages.thread_parents")
                stats.increment("messages.total_thread_replies", len(replies))
                stats.save()

                checkpoint.mark_item_done("threads", thread_ts)
                checkpoint.save()
            except Exception:
                log.error("Failed to fetch thread %s", thread_ts, exc_info=True)

        stats.save(force=True)
        checkpoint.complete_phase("threads")
        checkpoint.save(force=True)
        log.info("Fetched %d thread replies across %d threads", reply_count, len(thread_parents))

    def _fetch_single_thread(self, channel_id: str, thread_ts: str) -> list[dict]:
        replies = []
        cursor = None
        while True:
            params = {"channel": channel_id, "ts": thread_ts, "limit": 200}
            if cursor:
                params["cursor"] = cursor
            resp = self.session.get(f"{SLACK_API}/conversations.replies", params=params)
            resp.raise_for_status()
            data = resp.json()

            if not data.get("ok"):
                break

            for msg in data.get("messages", []):
                # Skip the parent message (first message in replies is the parent)
                if msg.get("ts") == thread_ts and msg.get("thread_ts") == thread_ts:
                    if not msg.get("parent_user_id"):
                        continue
                replies.append(msg)

            next_cursor = data.get("response_metadata", {}).get("next_cursor")
            if not next_cursor:
                break
            cursor = next_cursor

        return replies

    # ── Attachments ───────────────────────────────────────────────────────

    def _download_attachments_from_index(self, channel_id: str, s3_base: str,
                                        checkpoint: CheckpointManager,
                                        stats: StatsCollector) -> None:
        """Download attachments by iterating message index — one message at a time."""
        checkpoint.start_phase("attachments")
        index = self.s3.download_json(f"{s3_base}/messages/_index.json") or []
        download_count = 0

        for ts in index:
            msg = self.s3.download_json(f"{s3_base}/messages/{ts}.json")
            if not msg or "files" not in msg:
                continue
            updated = False
            for file_obj in msg.get("files", []):
                if _is_skippable_file(file_obj):
                    continue
                file_id = file_obj.get("id", "unknown")
                if checkpoint.is_item_done("attachments", file_id):
                    continue
                name = file_obj.get("name", "unknown")
                url = file_obj.get("url_private_download") or file_obj.get("url_private")
                if not url:
                    continue
                s3_filename = f"{file_id}_{name}"
                s3_path = f"{s3_base}/attachments/{s3_filename}"
                try:
                    self._download_one_file(url, s3_path)
                    file_obj["_local_file"] = f"attachments/{s3_filename}"
                    updated = True
                    download_count += 1
                    stats.increment("files.downloaded")
                    stats.save()
                    checkpoint.mark_item_done("attachments", file_id)
                    checkpoint.save()
                except Exception:
                    log.error("Failed to download file %s", file_id, exc_info=True)
            # Re-upload message with _local_file references
            if updated:
                self.s3.upload_json(msg, f"{s3_base}/messages/{ts}.json")

        stats.save(force=True)
        checkpoint.complete_phase("attachments")
        checkpoint.save(force=True)
        log.info("Downloaded %d attachments for channel %s", download_count, channel_id)

    def _download_one_file(self, url: str, s3_path: str) -> None:
        """Download a file and upload to S3. Uses requests timeout (no signal-based timeout)."""
        with tempfile.NamedTemporaryFile(delete=True) as tmp:
            resp = self.session.get(url, stream=True, timeout=(10, 60))
            resp.raise_for_status()

            # Check Content-Type to detect HTML auth pages
            content_type = resp.headers.get("Content-Type", "")
            if "text/html" in content_type:
                log.warning("Got HTML response for %s — likely auth failure, skipping", s3_path)
                return

            for chunk in resp.iter_content(chunk_size=8192):
                tmp.write(chunk)
            tmp.flush()
            self.s3.upload_file(tmp.name, s3_path)

    # ── List Channels ─────────────────────────────────────────────────────

    def list_channels(self) -> list[dict]:
        """List all accessible channels (public + private the bot is in)."""
        channels = []
        for types in ["public_channel", "private_channel"]:
            cursor = None
            while True:
                params = {"types": types, "limit": 200, "exclude_archived": "true"}
                if cursor:
                    params["cursor"] = cursor
                resp = self.session.get(f"{SLACK_API}/conversations.list", params=params)
                resp.raise_for_status()
                data = resp.json()
                if not data.get("ok"):
                    break
                channels.extend(data.get("channels", []))
                next_cursor = data.get("response_metadata", {}).get("next_cursor")
                if not next_cursor:
                    break
                cursor = next_cursor
        return channels


def main():
    from lib.config import load_dotenv, env, env_int, env_bool, env_list
    from lib.input import read_csv_column

    load_dotenv()

    parser = argparse.ArgumentParser(description="Export Slack channel data to S3")
    parser.add_argument("--token", default=env("SLACK_TOKEN"), help="Slack Bot Token (xoxb-...)")
    parser.add_argument("--input-csv", default=env("SLACK_INPUT_CSV"), help="CSV file with 'channel_id' column")
    parser.add_argument("--channel-ids", nargs="+", help="Channel IDs (alternative to CSV)")
    parser.add_argument("--include-threads", action="store_true", default=env_bool("SLACK_INCLUDE_THREADS"))
    parser.add_argument("--skip-attachments", action="store_true", default=env_bool("SLACK_SKIP_ATTACHMENTS"))
    parser.add_argument("--list-channels", action="store_true")
    parser.add_argument("--s3-bucket", default=env("S3_BUCKET"))
    parser.add_argument("--s3-prefix", default=env("S3_PREFIX", ""))
    parser.add_argument("--parallel", type=int, default=env_int("SLACK_PARALLEL", 1),
                        help="Channels to export in parallel (default 1, all share one bot token)")
    parser.add_argument("--max-workers", type=int, default=env_int("SLACK_MAX_WORKERS", env_int("MAX_WORKERS", 3)))
    parser.add_argument("--log-level", default=env("LOG_LEVEL", "INFO"))
    parser.add_argument("--no-json-logs", action="store_true", default=not env_bool("JSON_LOGS", True))
    args = parser.parse_args()

    if not args.token:
        parser.error("--token is required (or set SLACK_TOKEN)")
    if not args.s3_bucket:
        parser.error("--s3-bucket is required (or set S3_BUCKET)")

    setup_logging(level=args.log_level, json_output=not args.no_json_logs)
    s3 = S3Store(bucket=args.s3_bucket, prefix=args.s3_prefix)
    config = ExportConfig(
        s3_bucket=args.s3_bucket,
        s3_prefix=args.s3_prefix,
        max_workers=args.max_workers,
        log_level=args.log_level,
    )

    if args.list_channels:
        exporter = SlackExporter(
            token=args.token, channel_ids=[], s3=s3, config=config,
        )
        channels = exporter.list_channels()
        for ch in channels:
            print(f"{ch.get('id')}\t{ch.get('name')}\t{'private' if ch.get('is_private') else 'public'}")
        return

    # CLI --channel-ids > CSV > env var
    channel_ids = args.channel_ids
    if not channel_ids and args.input_csv:
        channel_ids = read_csv_column(args.input_csv, "channel_id")
    if not channel_ids:
        channel_ids = env_list("SLACK_CHANNEL_IDS")
    if not channel_ids:
        parser.error("No channel IDs provided. Use --channel-ids, --input-csv, or set SLACK_CHANNEL_IDS.")

    exporter = SlackExporter(
        token=args.token,
        channel_ids=channel_ids,
        s3=s3,
        config=config,
        include_threads=args.include_threads,
        skip_attachments=args.skip_attachments,
        parallel=args.parallel,
    )
    exporter.run()


if __name__ == "__main__":
    main()
