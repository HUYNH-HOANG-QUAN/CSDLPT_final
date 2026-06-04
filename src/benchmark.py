"""
Benchmark module: Speedup, Scaleup, Sizeup measurements.
PHẦN 8 — O&V scalability metrics.
"""
import logging
import time
from dataclasses import dataclass
from typing import Callable, Optional

from .coordinator import Coordinator, PipelineStats
from .config import (
    LOG_FILES,
    OUTPUT_PATH,
    N_REDUCER_VALUES_SPEEDUP,
)

logger = logging.getLogger("etl.benchmark")


@dataclass
class SpeedupResult:
    n_reducers: int
    duration_s: float
    throughput_rows: float
    throughput_ips: float
    speedup_factor: float


@dataclass
class ScaleupResult:
    n_reducers: int
    data_size_mb: int
    duration_s: float


@dataclass
class SizeupResult:
    data_size_mb: int
    duration_s: float
    throughput: float


class BenchmarkRunner:
    """
    Runs Speedup, Scaleup, and Sizeup benchmarks.
    Speedup:     N increases, data stays constant → ideal speedup ≈ N
    Scaleup:     N increases, data/node stays constant → ideal time ≈ constant
    Sizeup:      N constant, data increases → ideal time increases linearly
    """

    def __init__(self, log_files: dict[int, str] = LOG_FILES):
        self.log_files = log_files

    def run_speedup(self) -> list[SpeedupResult]:
        """
        Measure Speedup: add more reducers while keeping data constant.
        Ideal: Speedup = N (linear). Real: Speedup < N due to Communication Cost.
        """
        logger.info("")
        logger.info("#" * 65)
        logger.info("# SPEEDUP BENCHMARK")
        logger.info("# Adding reducers, keeping data constant")
        logger.info("#" * 65)

        results: list[SpeedupResult] = []
        baseline_time = 0.0

        for n in N_REDUCER_VALUES_SPEEDUP:
            logger.info(f"\n  Running with N_REDUCERS = {n}...")

            coord = Coordinator(
                log_files=self.log_files,
                n_reducers=n,
                output_path=OUTPUT_PATH,
                track_skew=True,
                track_memory=False,
                track_latency=False,
            )
            stats = coord.run()

            if n == 1:
                baseline_time = stats.duration_s

            speedup = baseline_time / stats.duration_s if stats.duration_s > 0 else 0

            result = SpeedupResult(
                n_reducers=n,
                duration_s=stats.duration_s,
                throughput_rows=stats.throughput_rows_per_sec,
                throughput_ips=stats.throughput_ips_per_sec,
                speedup_factor=speedup,
            )
            results.append(result)

            logger.info(
                f"  → {n} reducer(s): {stats.duration_s:.3f}s | "
                f"Speedup: {speedup:.2f}x | "
                f"Throughput: {stats.throughput_rows_per_sec:,.0f} rows/s"
            )

        # Summary table
        logger.info("")
        logger.info("  SPEEDUP SUMMARY:")
        logger.info(f"  {'N':>3} | {'Time (s)':>10} | {'Speedup':>10} | {'Ideal':>8} | {'Efficiency':>10}")
        logger.info("  " + "-" * 50)
        for r in results:
            ideal = r.n_reducers
            efficiency = (r.speedup_factor / ideal * 100) if ideal > 0 else 0
            logger.info(
                f"  {r.n_reducers:>3} | {r.duration_s:>10.3f} | "
                f"{r.speedup_factor:>10.3f} | {ideal:>8.1f} | {efficiency:>9.1f}%"
            )

        logger.info("")
        logger.info(
            "  Analysis: Speedup < N due to Communication Cost (shuffle overhead) "
            "and the fact that on a single machine Python threads share the GIL."
        )
        return results

    def run_scaleup(self) -> list[ScaleupResult]:
        """
        Measure Scaleup: add more reducers AND proportionally more data.
        Ideal: time stays constant (perfect linear scaling).
        """
        logger.info("")
        logger.info("#" * 65)
        logger.info("# SCALEUP BENCHMARK")
        logger.info("# Adding reducers + proportionally more data (data/node constant)")
        logger.info("#" * 65)

        DATA_SIZE_MB_SCALEUP = 100
        DATA_SIZES_SIZEUP_MB = [100, 200, 400, 800]
        results: list[ScaleupResult] = []
        n_values = [1, 2, 4]

        for n in n_values:
            data_size_mb = DATA_SIZE_MB_SCALEUP * n
            logger.info(f"\n  Running with N_REDUCERS={n}, data≈{data_size_mb}MB...")

            coord = Coordinator(
                log_files=self.log_files,
                n_reducers=n,
                output_path=OUTPUT_PATH,
                track_skew=True,
                track_memory=False,
                track_latency=False,
            )
            stats = coord.run()

            result = ScaleupResult(
                n_reducers=n,
                data_size_mb=data_size_mb,
                duration_s=stats.duration_s,
            )
            results.append(result)

            logger.info(
                f"  → {n} reducer(s), {data_size_mb}MB: {stats.duration_s:.3f}s"
            )

        # Summary
        if len(results) >= 2:
            base_time = results[0].duration_s
            logger.info("")
            logger.info("  SCALEUP SUMMARY:")
            logger.info(f"  {'N':>3} | {'Data (MB)':>10} | {'Time (s)':>10} | {'Scaleup Factor':>14}")
            logger.info("  " + "-" * 45)
            for r in results:
                scaleup_factor = base_time / r.duration_s if r.duration_s > 0 else 0
                logger.info(
                    f"  {r.n_reducers:>3} | {r.data_size_mb:>10} | "
                    f"{r.duration_s:>10.3f} | {scaleup_factor:>13.2f}x"
                )

            ideal_scaleup = results[-1].n_reducers / results[0].n_reducers
            actual_scaleup = results[0].duration_s / results[-1].duration_s if results[-1].duration_s > 0 else 0
            logger.info(
                f"\n  Analysis: Ideal Scaleup={ideal_scaleup:.1f}x, "
                f"Actual={actual_scaleup:.2f}x. "
                f"Scaleup < 1 means system slows — indicates non-linear scaling overhead."
            )

        return results

    def run_sizeup(self) -> list[SizeupResult]:
        """
        Measure Sizeup: keep reducers constant, increase data size.
        Ideal: time increases linearly with data.
        """
        logger.info("")
        logger.info("#" * 65)
        logger.info("# SIZEUP BENCHMARK")
        logger.info("# Keeping reducers constant (N=4), increasing data size")
        logger.info("#" * 65)

        DATA_SIZES_SIZEUP_MB = [100, 200, 400, 800]
        results: list[SizeupResult] = []
        base_size = DATA_SIZES_SIZEUP_MB[0]
        base_time = 0.0

        for size_mb in DATA_SIZES_SIZEUP_MB:
            logger.info(f"\n  Running with data≈{size_mb}MB (N_REDUCERS=4)...")

            coord = Coordinator(
                log_files=self.log_files,
                n_reducers=4,
                output_path=OUTPUT_PATH,
                track_skew=False,
                track_memory=False,
                track_latency=False,
            )
            stats = coord.run()

            result = SizeupResult(
                data_size_mb=size_mb,
                duration_s=stats.duration_s,
                throughput=stats.throughput_rows_per_sec,
            )
            results.append(result)

            if size_mb == base_size:
                base_time = stats.duration_s

            logger.info(
                f"  → {size_mb}MB: {stats.duration_s:.3f}s | "
                f"Throughput: {stats.throughput_rows_per_sec:,.0f} rows/s"
            )

        # Summary
        logger.info("")
        logger.info("  SIZEUP SUMMARY:")
        logger.info(f"  {'Data (MB)':>10} | {'Time (s)':>10} | {'Throughput':>15} | {'Sizeup Factor':>14}")
        logger.info("  " + "-" * 57)
        for r in results:
            sizeup_factor = r.duration_s / base_time if base_time > 0 else 0
            logger.info(
                f"  {r.data_size_mb:>10} | {r.duration_s:>10.3f} | "
                f"{r.throughput:>14,.0f} rows/s | {sizeup_factor:>13.2f}x"
            )

        logger.info(
            "\n  Analysis: Sizeup Factor ≈ data_ratio if linear. "
            "Sizeup > data_ratio indicates sub-linear scaling (good). "
            "Sizeup < data_ratio indicates super-linear scaling (bad — system struggling)."
        )
        return results

    def run_all(self) -> tuple[list[SpeedupResult], list[ScaleupResult], list[SizeupResult]]:
        """Run all three benchmark types."""
        speedup = self.run_speedup()
        scaleup = self.run_scaleup()
        sizeup = self.run_sizeup()
        return speedup, scaleup, sizeup
