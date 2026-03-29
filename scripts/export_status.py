"""Export Status — reads S3 checkpoints and stats to display current export status."""

import argparse
import sys


def _phase_summary(phases: dict) -> str:
    """Summarize phase progress, e.g. '3/4 (pull_requests)' or '4/4 complete'."""
    if not phases:
        return "no phases"
    total = len(phases)
    done = sum(1 for p in phases.values() if p.get("status") == "completed")
    if done == total:
        return f"{done}/{total} complete"
    # Find the current in-progress phase
    current = next(
        (name for name, p in phases.items() if p.get("status") == "in_progress"),
        None,
    )
    suffix = f" ({current})" if current else ""
    return f"{done}/{total}{suffix}"


def _format_row(row: dict) -> str:
    """Format a single status row as a fixed-width table line."""
    return (
        f"{row['exporter']:<12s}"
        f"{row['target']:<30s}"
        f"{row['status']:<14s}"
        f"{row['phases']:<20s}"
        f"{row['updated']}"
    )


def get_status_rows(s3) -> list[dict]:
    """Fetch checkpoint data from S3 and return status rows."""
    checkpoint_keys = [
        k for k in s3.list_keys("_checkpoints/") if k.endswith(".json")
    ]

    rows = []
    for ck_key in checkpoint_keys:
        data = s3.download_json(ck_key)
        if not data or not isinstance(data, dict):
            continue

        job_id = data.get("job_id", ck_key)
        # job_id is like "github/owner__repo" or "jira/PROJ"
        parts = job_id.split("/", 1)
        exporter = parts[0] if len(parts) > 1 else "unknown"
        target = parts[1] if len(parts) > 1 else job_id

        # Restore human-readable target names
        if exporter == "github":
            target = target.replace("__", "/")

        rows.append({
            "exporter": exporter,
            "target": target,
            "status": data.get("status", "unknown"),
            "phases": _phase_summary(data.get("phases", {})),
            "updated": data.get("updated_at", "—"),
        })

    rows.sort(key=lambda r: (r["exporter"], r["target"]))
    return rows


def print_status_table(rows: list[dict]) -> None:
    """Print the status table to stdout."""
    if not rows:
        print("No exports found.")
        return

    header = {
        "exporter": "Exporter",
        "target": "Target",
        "status": "Status",
        "phases": "Phases",
        "updated": "Last Updated",
    }
    print(_format_row(header))
    print("─" * 12 + "─" * 30 + "─" * 14 + "─" * 20 + "─" * 25)
    for row in rows:
        print(_format_row(row))


def main():
    from lib.config import load_dotenv, env
    from lib.s3 import S3Store

    load_dotenv()

    parser = argparse.ArgumentParser(description="Show current status of all exports")
    parser.add_argument("--s3-bucket", default=env("S3_BUCKET"))
    parser.add_argument("--s3-prefix", default=env("S3_PREFIX", ""))
    args = parser.parse_args()

    if not args.s3_bucket:
        parser.error("--s3-bucket is required (or set S3_BUCKET)")

    s3 = S3Store(bucket=args.s3_bucket, prefix=args.s3_prefix)
    rows = get_status_rows(s3)
    print_status_table(rows)

    # Exit code: 0 if all completed, 1 otherwise
    if not rows or any(r["status"] != "completed" for r in rows):
        sys.exit(1)


if __name__ == "__main__":
    main()
