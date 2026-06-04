"""
Sweep runner: chạy pipeline với nhiều siêu tham số khác nhau,
thu thập kết quả và xuất ra JSON để notebook phân tích.

Chạy: python -m src.sweep_runner [--csv]
"""
import argparse
import json
import logging
import os
import sys
import time
import io
from pathlib import Path
from typing import Any

if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import (
    LOG_FILES,
    OUTPUT_PATH,
    DEFAULT_N_REDUCERS,
    GEODB_DEFAULT_PATH,
    MEMORY_THRESHOLD_PCT,
    SKEW_THRESHOLD,
    OUTPUT_DIR,
    DATA_SOURCE_MODE,
)
from src.coordinator import Coordinator


SWEEP_DIR = OUTPUT_DIR / "sweeps"
SWEEP_DIR.mkdir(parents=True, exist_ok=True)


# =====================================================================
# CẤU HÌNH SWEEP — sửa ở đây để thêm bớt
# =====================================================================

# 1. Sweep số reducer (Speedup)
SWEEP_N_REDUCERS = [1, 2, 4, 6, 8, 12]

# 2. Sweep ngưỡng memory (%)
SWEEP_MEMORY_THRESHOLDS = [70, 80, 90, 95]

# 3. Sweep ngưỡng skew
SWEEP_SKEW_THRESHOLDS = [0.2, 0.3, 0.5, 0.8]

# Cờ: test nhanh
FAST_MODE = os.environ.get("SWEEP_FAST", "0") == "1"
if FAST_MODE:
    SWEEP_N_REDUCERS = [1, 2, 4]
    SWEEP_MEMORY_THRESHOLDS = [80, 90]
    SWEEP_SKEW_THRESHOLDS = [0.3, 0.5]


# =====================================================================
# SWEEP LOGIC
# =====================================================================

def _safe_run_sweep(label: str, params: dict[str, Any], data_source: str) -> tuple[dict[str, Any], Any]:
    """Chạy 1 lần pipeline với params cho trước. Trả về (result_dict, coordinator)."""
    print(f"  {label} ...", end="", flush=True)
    start = time.perf_counter()
    result: dict[str, Any] = {
        "label": label,
        "params": params,
        "data_source": data_source,
        "ok": False,
        "error": None,
    }
    coord = None
    try:
        coord = Coordinator(
            n_reducers=params.get("n_reducers", DEFAULT_N_REDUCERS),
            output_path=OUTPUT_PATH,
            geodb_path=GEODB_DEFAULT_PATH if Path(GEODB_DEFAULT_PATH).exists() else None,
            track_skew=True,
            track_memory=True,
            track_latency=True,
            data_source=data_source,
        )
        stats = coord.run()
        result.update(
            {
                "ok": True,
                "total_rows_read": stats.total_rows_read,
                "total_ip_local": stats.total_ip_local_unique,
                "total_ip_global": stats.total_ip_global_unique,
                "duplicates_skipped": stats.total_ip_skipped_dup,
                "duration_s": stats.duration_s,
                "merge_duration_s": stats.merge_duration_s,
                "throughput_rows_per_sec": stats.throughput_rows_per_sec,
                "throughput_ips_per_sec": stats.throughput_ips_per_sec,
                "n_reducers": stats.n_reducers,
                "skew_factor": stats.skew_factor,
                "ip_counts_per_reducer": stats.ip_counts_per_reducer,
            }
        )
        print(f" OK ({stats.duration_s:.2f}s, {stats.throughput_rows_per_sec:,.0f} rows/s)")
    except Exception as e:
        result["error"] = f"{type(e).__name__}: {e}"
        print(f" FAILED: {result['error']}")

    wall_time = time.perf_counter() - start
    result["wall_clock_s"] = wall_time
    return result, coord


def _sweep_with_speedup(
    name: str,
    reducer_list: list[int],
    data_source: str,
    with_validation: bool = False,
) -> list[dict[str, Any]]:
    """Chạy speedup: thay đổi số reducer."""
    print(f"\n# SWEEP: {name}")
    print("#" * 60)
    results = []
    base_duration = None
    for n in reducer_list:
        label = f"n={n}" + (" +val" if with_validation else "")
        params = {"n_reducers": n}
        r, coord = _safe_run_sweep(label, params, data_source)
        if r["ok"]:
            if base_duration is None:
                base_duration = r["duration_s"]
            r["speedup"] = base_duration / r["duration_s"] if r["duration_s"] > 0 else 0
            r["ideal_speedup"] = n
            r["efficiency"] = r["speedup"] / n * 100 if n > 0 else 0

            if with_validation:
                from src.validation import CorrectnessValidator
                validator = CorrectnessValidator(
                    log_files=coord.log_files,
                    output_path=OUTPUT_PATH,
                    data_source=data_source,
                )
                baseline = validator.count_baseline()
                pipeline = validator.count_pipeline()
                r["baseline_count"] = len(baseline)
                r["pipeline_count"] = len(pipeline)
                r["validation_pass"] = len(baseline) == len(pipeline)
        results.append(r)
    return results


def sweep_speedup_csv() -> list[dict[str, Any]]:
    return _sweep_with_speedup(
        "Speedup — CSV Dataset (550K unique IPs)",
        SWEEP_N_REDUCERS,
        data_source="csv",
        with_validation=True,
    )


def sweep_speedup_log() -> list[dict[str, Any]]:
    return _sweep_with_speedup(
        "Speedup — Log Files (100MB)",
        SWEEP_N_REDUCERS,
        data_source="log",
        with_validation=False,
    )


def sweep_memory(data_source: str) -> list[dict[str, Any]]:
    print(f"\n# SWEEP: Memory Thresholds ({data_source})")
    print("#" * 60)
    results = []
    for th in SWEEP_MEMORY_THRESHOLDS:
        r, _ = _safe_run_sweep(
            f"mem={th}%",
            {"n_reducers": 4, "memory_threshold": th},
            data_source,
        )
        results.append(r)
    return results


def sweep_skew(data_source: str) -> list[dict[str, Any]]:
    print(f"\n# SWEEP: Skew Thresholds ({data_source})")
    print("#" * 60)
    results = []
    for th in SWEEP_SKEW_THRESHOLDS:
        r, _ = _safe_run_sweep(
            f"skew={th}",
            {"n_reducers": 4, "skew_threshold": th},
            data_source,
        )
        if r["ok"]:
            r["skew_warning_triggered"] = r["skew_factor"] > th
        results.append(r)
    return results


# =====================================================================
# MAIN
# =====================================================================

def main():
    parser = argparse.ArgumentParser(description="Sweep runner")
    parser.add_argument("--csv", action="store_true", help="Use CSV dataset (550K unique IPs)")
    parser.add_argument("--log", action="store_true", help="Use log files (100MB)")
    parser.add_argument("--all", action="store_true", help="Run both CSV and log sweeps")
    args = parser.parse_args()

    logging.disable(logging.CRITICAL)

    if args.all:
        sources = ["csv", "log"]
    elif args.csv:
        sources = ["csv"]
    elif args.log:
        sources = ["log"]
    else:
        sources = [DATA_SOURCE_MODE]

    timestamp = time.strftime("%Y%m%d_%H%M%S")
    out_file = SWEEP_DIR / f"sweep_{timestamp}.json"

    all_results: dict[str, Any] = {
        "timestamp": timestamp,
        "data_sources": sources,
        "config": {
            "SWEEP_N_REDUCERS": SWEEP_N_REDUCERS,
            "SWEEP_MEMORY_THRESHOLDS": SWEEP_MEMORY_THRESHOLDS,
            "SWEEP_SKEW_THRESHOLDS": SWEEP_SKEW_THRESHOLDS,
        },
        "sweeps": {},
    }

    for src in sources:
        prefix = f"{src}_"
        src_label = "CSV (550K unique IPs)" if src == "csv" else "LOG files (100MB)"

        print(f"\n{'#' * 60}")
        print(f"# DATA SOURCE: {src_label}")
        print(f"{'#' * 60}")

        try:
            all_results["sweeps"][f"{prefix}speedup"] = sweep_speedup_csv() if src == "csv" else sweep_speedup_log()
        except Exception as e:
            all_results["sweeps"][f"{prefix}speedup"] = [{"error": f"{type(e).__name__}: {e}"}]

        try:
            all_results["sweeps"][f"{prefix}memory"] = sweep_memory(src)
        except Exception as e:
            all_results["sweeps"][f"{prefix}memory"] = [{"error": f"{type(e).__name__}: {e}"}]

        try:
            all_results["sweeps"][f"{prefix}skew"] = sweep_skew(src)
        except Exception as e:
            all_results["sweeps"][f"{prefix}skew"] = [{"error": f"{type(e).__name__}: {e}"}]

    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2, ensure_ascii=False, default=str)

    print(f"\n{'=' * 60}")
    print(f"Sweep results saved to: {out_file}")
    print(f"Open the notebook (notebooks/visualize_sweeps.ipynb) to see charts.")
    print(f"{'=' * 60}")
    return out_file


if __name__ == "__main__":
    main()
