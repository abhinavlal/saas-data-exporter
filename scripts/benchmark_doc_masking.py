"""Benchmark Office document masking performance on real files.

Measures end-to-end mask_docx/xlsx/pptx timing per file,
then extrapolates to 0.5M documents with various parallelism levels.

Usage:
    PII_STORE_PATH=pii_store.db uv run python scripts/benchmark_doc_masking.py
"""

import io
import os
import sys
import time
import statistics
from collections import defaultdict

OUT_DIR = "/tmp/doc_masking_validation/original"
RUNS = 5


def main():
    if not os.path.isdir(OUT_DIR):
        print(f"ERROR: {OUT_DIR} not found. Run validate_doc_masking.py first.")
        sys.exit(1)

    files = []
    for name in sorted(os.listdir(OUT_DIR)):
        ext = os.path.splitext(name)[1].lower()
        if ext in (".docx", ".xlsx", ".pptx"):
            path = os.path.join(OUT_DIR, name)
            size = os.path.getsize(path)
            files.append((name, ext, path, size))

    if not files:
        print("No office docs found in", OUT_DIR)
        sys.exit(1)

    store_path = os.environ.get("PII_STORE_PATH")
    if not store_path:
        for candidate in [
            "pii_store.db",
            os.path.expanduser("~/pii_store.db"),
            "/Users/abhinavlal/Code/saas-data-exporter/.claude/worktrees/pii-masking-v2/pii_store.db",
        ]:
            if os.path.exists(candidate):
                store_path = candidate
                break
    if not store_path:
        print("ERROR: Set PII_STORE_PATH")
        sys.exit(1)

    from scripts.pii_mask.pii_store import PIIStore
    from scripts.pii_mask.scanner import TextScanner
    from scripts.pii_mask.documents import mask_docx, mask_xlsx, mask_pptx

    print("Loading scanner...")
    t0 = time.monotonic()
    store = PIIStore(store_path)
    scanner = TextScanner(store, threshold=0.5)
    scanner_init_time = time.monotonic() - t0
    print(f"  Scanner init: {scanner_init_time:.2f}s "
          f"({len(store._cache):,} entries)")

    mask_fns = {".docx": mask_docx, ".xlsx": mask_xlsx, ".pptx": mask_pptx}

    print(f"\nBenchmarking {len(files)} files x {RUNS} runs each...\n")

    all_results = []

    for name, ext, path, size in files:
        with open(path, "rb") as f:
            raw_bytes = f.read()

        mask_fn = mask_fns[ext]
        times = []
        for _ in range(RUNS):
            t0 = time.monotonic()
            mask_fn(raw_bytes, scanner)
            elapsed = time.monotonic() - t0
            times.append(elapsed)

        avg = statistics.mean(times)
        std = statistics.stdev(times) if len(times) > 1 else 0

        all_results.append({
            "name": name, "ext": ext, "size": size,
            "avg": avg, "std": std,
        })

        short_name = name[:60] + "..." if len(name) > 60 else name
        print(f"  {short_name}")
        print(f"    Size: {size:>12,} bytes  |  Type: {ext}")
        print(f"    Time: {avg*1000:>8.1f} ms  (+/- {std*1000:.1f})")
        print(f"    Throughput: {1/avg:.1f} files/sec")
        print()

    # -- Summary by type ---
    print("=" * 70)
    print("SUMMARY BY FILE TYPE")
    print("=" * 70)

    by_type = defaultdict(list)
    for r in all_results:
        by_type[r["ext"]].append(r)

    for ext in sorted(by_type):
        results = by_type[ext]
        avg_total = statistics.mean(r["avg"] for r in results)
        avg_size = statistics.mean(r["size"] for r in results)
        print(f"\n  {ext}  ({len(results)} files)")
        print(f"    Avg size:       {avg_size:>12,.0f} bytes")
        print(f"    Avg time:       {avg_total*1000:>8.1f} ms/file")
        print(f"    Throughput:     {1/avg_total:>8.1f} files/sec")

    overall_avg = statistics.mean(r["avg"] for r in all_results)
    print(f"\n  OVERALL ({len(all_results)} files)")
    print(f"    Avg time:       {overall_avg*1000:>8.1f} ms/file")
    print(f"    Throughput:     {1/overall_avg:>8.1f} files/sec")

    # -- Extrapolation ---
    print(f"\n{'=' * 70}")
    print("EXTRAPOLATION TO 500,000 DOCUMENTS")
    print("=" * 70)

    target = 500_000
    total_single = target * overall_avg

    print(f"\n  Single-threaded: {fmt_duration(total_single)}")

    print(f"\n  With ProcessPoolExecutor:")
    print(f"  {'Workers':<10} {'Wall time':<18} {'Files/sec':<12}")
    print(f"  {'-'*40}")

    for workers in [1, 4, 8, 16, 32]:
        efficiency = 0.85 if workers > 1 else 1.0
        wall_time = (total_single / (workers * efficiency)) + scanner_init_time
        fps = target / wall_time
        print(f"  {workers:<10} {fmt_duration(wall_time):<18} {fps:<12.0f}")

    # Cost
    print(f"\n  Cost estimate (AWS EC2):")
    for instance, cores, hourly in [
        ("c6i.4xlarge", 16, 0.68),
        ("c6i.8xlarge", 32, 1.36),
    ]:
        wall_hrs = (total_single / (cores * 0.85)) / 3600
        cost = wall_hrs * hourly
        print(f"    {instance} ({cores} cores): "
              f"{wall_hrs:.1f} hrs, ${cost:.2f}")


def fmt_duration(seconds):
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        return f"{seconds/60:.1f} min"
    else:
        return f"{seconds/3600:.1f} hrs"


if __name__ == "__main__":
    main()
