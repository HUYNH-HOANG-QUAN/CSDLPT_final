"""
main.py — Entry point for the Distributed ETL Pipeline.
Supports multiple run modes:
  1. Single pipeline run with full evaluation
  2. Benchmark mode (Speedup/Scaleup/Sizeup)
  3. Validation only (correctness check)
  4. CSV data source mode (550K unique IPs dataset)
"""
import argparse
import logging
import sys
import io
from pathlib import Path

if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import (
    LOG_FILES,
    OUTPUT_PATH,
    GEODB_DEFAULT_PATH,
    DEFAULT_N_REDUCERS,
    DATA_SOURCE_MODE,
)
from src.coordinator import Coordinator, PipelineStats
from src.validation import CorrectnessValidator
from src.benchmark import BenchmarkRunner


def setup_logging(verbose: bool = False):
    level = logging.DEBUG if verbose else logging.INFO
    fmt = "%(message)s"

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(fmt))
    if sys.platform == "win32":
        handler.stream = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.handlers.clear()
    root_logger.addHandler(handler)

    for logger_name in ["etl"]:
        log = logging.getLogger(logger_name)
        log.setLevel(level)


def run_single_pipeline(args):
    print("\n" + "=" * 65)
    print("  DISTRIBUTED ETL PIPELINE — HỆ THỐNG GIÁM SÁT AN NINH MẠNG")
    print("=" * 65)

    if args.csv9k:
        data_source = "csv9k"
    elif args.csv:
        data_source = "csv"
    else:
        data_source = DATA_SOURCE_MODE

    coord = Coordinator(
        n_reducers=args.reducers,
        output_path=args.output or OUTPUT_PATH,
        geodb_path=GEODB_DEFAULT_PATH if Path(GEODB_DEFAULT_PATH).exists() else None,
        track_skew=True,
        track_memory=True,
        track_latency=True,
        data_source=data_source,
        partition_strategy=args.partition,
    )

    stats = coord.run()

    if args.validate:
        print("\n" + "=" * 65)
        print("  CORRECTNESS VALIDATION")
        print("=" * 65)
        validator = CorrectnessValidator(
            log_files=coord.log_files,
            output_path=args.output or OUTPUT_PATH,
            data_source=data_source,
        )
        passed = validator.validate()
        if not passed:
            print("\n  WARNING: Validation failed — de-duplication may have issues.")

    return stats


def run_benchmarks(args):
    print("\n" + "#" * 65)
    print("#  BENCHMARK MODE — Scalability Evaluation")
    print("#  Özsu & Valduriez: Speedup / Scaleup / Sizeup")
    print("#" * 65)

    runner = BenchmarkRunner(log_files=LOG_FILES)

    if args.benchmark == "all":
        runner.run_all()
    elif args.benchmark == "speedup":
        runner.run_speedup()
    elif args.benchmark == "scaleup":
        runner.run_scaleup()
    elif args.benchmark == "sizeup":
        runner.run_sizeup()


def run_validation_only(args):
    print("\n" + "=" * 65)
    print("  CORRECTNESS VALIDATION")
    print("=" * 65)

    output = args.output or OUTPUT_PATH
    if args.csv9k:
        data_source = "csv9k"
    elif args.csv:
        data_source = "csv"
    else:
        data_source = DATA_SOURCE_MODE

    validator = CorrectnessValidator(
        log_files=LOG_FILES,
        output_path=output,
        data_source=data_source,
    )

    if not Path(output).exists():
        print(f"\n  ERROR: Output file not found: {output}")
        print("  Run the pipeline first: python main.py --csv")
        return

    passed = validator.validate()
    sys.exit(0 if passed else 1)


def main():
    parser = argparse.ArgumentParser(
        description="Distributed ETL Pipeline — Network Security Monitor",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py --csv                    # Run pipeline (CSV dataset mode)
  python main.py --csv --partition range # Run with Range Partitioning + Sampling
  python main.py --csv --partition hash  # Run with Hash Partitioning (default)
  python main.py --csv -r 8             # Run with 8 reducers
  python main.py --csv --validate        # Run + validation
  python main.py --benchmark speedup     # Speedup benchmark
        """,
    )

    parser.add_argument(
        "-r", "--reducers",
        type=int,
        default=DEFAULT_N_REDUCERS,
        help=f"Number of reducers (default: {DEFAULT_N_REDUCERS})",
    )
    parser.add_argument(
        "-o", "--output",
        type=str,
        default=None,
        help=f"Output JSON path (default: {OUTPUT_PATH})",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose debug output",
    )
    parser.add_argument(
        "--benchmark",
        type=str,
        choices=["all", "speedup", "scaleup", "sizeup"],
        help="Run benchmark mode (Speedup/Scaleup/Sizeup)",
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Run correctness validation after pipeline",
    )
    parser.add_argument(
        "--validation-only",
        action="store_true",
        help="Run validation only (no pipeline execution)",
    )
    parser.add_argument(
        "--csv",
        action="store_true",
        help="Use CSV dataset (apache_550k_unique_ips.csv, 550K unique IPs)",
    )
    parser.add_argument(
        "--csv9k",
        action="store_true",
        help="Use CSV dataset with 9K duplicate IPs (640K rows, 559K unique IPs, 81K true dupes)",
    )
    parser.add_argument(
        "--partition",
        type=str,
        choices=["hash", "range"],
        default=None,
        help="Partitioning strategy: 'hash' (default) or 'range' (sampling-based, better load balance)",
    )

    args = parser.parse_args()
    setup_logging(args.verbose)

    if args.validation_only:
        run_validation_only(args)
    elif args.benchmark:
        run_benchmarks(args)
    else:
        stats = run_single_pipeline(args)
        print("\n  Pipeline finished successfully.")
        print(f"  Total time: {stats.duration_s:.3f}s")
        print(f"  Throughput: {stats.throughput_rows_per_sec:,.0f} rows/s")


if __name__ == "__main__":
    main()
