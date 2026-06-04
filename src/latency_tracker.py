"""
Latency Breakdown and Throughput Measurement.
PHẦN 8.4 — Measures time per phase and identifies bottlenecks.
"""
import logging
import time
from dataclasses import dataclass, field
from typing import Callable, Optional, Any

logger = logging.getLogger("etl.latency")


@dataclass
class TimingResult:
    """Result of a timed phase."""
    phase: str
    duration_s: float
    result: Any = None
    percentage: float = 0.0

    def __repr__(self) -> str:
        return f"TimingResult({self.phase}, {self.duration_s:.3f}s, {self.percentage:.1f}%)"


class LatencyTracker:
    """
    Measures execution time for each pipeline phase.
    Calculates percentage breakdown and auto-detects bottlenecks.
    """

    def __init__(self):
        self._results: list[TimingResult] = []
        self._total_time: float = 0.0

    def measure(self, phase_name: str, callback: Callable[[], Any]) -> TimingResult:
        """
        Time a callback and record the result.
        Usage:
            result = tracker.measure("Extract", lambda: extract_fn())
        """
        start = time.perf_counter()
        result = callback()
        elapsed = time.perf_counter() - start

        timing = TimingResult(phase=phase_name, duration_s=elapsed, result=result)
        self._results.append(timing)
        self._total_time += elapsed

        logger.info(f"  [{phase_name}] {elapsed:.3f}s")
        return timing

    def measure_with_context(self, phase_name: str, callback: Callable[[], Any]) -> TimingResult:
        """
        Same as measure() but also prints context about the result.
        """
        start = time.perf_counter()
        result = callback()
        elapsed = time.perf_counter() - start

        context = ""
        if isinstance(result, dict):
            if "ip_count" in result:
                context = f" | {result['ip_count']} IPs"
            if "row_count" in result:
                context = f" | {result['row_count']} rows"

        timing = TimingResult(phase=phase_name, duration_s=elapsed, result=result)
        self._results.append(timing)
        self._total_time += elapsed

        logger.info(f"  [{phase_name}]{context} — {elapsed:.3f}s")
        return timing

    def finalize(self) -> list[TimingResult]:
        """
        Finalize percentages and return all results.
        Call after all phases are measured.
        """
        if self._total_time == 0:
            return self._results

        for r in self._results:
            r.percentage = (r.duration_s / self._total_time) * 100

        return self._results

    def print_breakdown(self) -> dict:
        """
        Print a full latency breakdown table with ASCII bar chart.
        Returns dict with bottleneck info.
        """
        results = self.finalize()
        if not results:
            return {}

        logger.info("")
        logger.info("=" * 70)
        logger.info("PIPELINE LATENCY BREAKDOWN")
        logger.info("=" * 70)

        bottleneck = max(results, key=lambda r: r.percentage)
        bottleneck_info = {"phase": bottleneck.phase, "pct": bottleneck.percentage}

        logger.info(f"  Total time: {self._total_time:.3f}s")
        logger.info("")
        logger.info(f"  {'Phase':<12} {'Time (s)':>10} {'%':>7} {'Bar':<30}")
        logger.info("  " + "-" * 65)

        for r in results:
            bar_len = int(r.percentage / 3.5)
            bar = "█" * bar_len + "░" * (30 - bar_len)
            flag = " ← BOTTLENECK" if r.phase == bottleneck.phase and r.percentage > 30 else ""
            logger.info(
                f"  {r.phase:<12} {r.duration_s:>10.3f} {r.percentage:>6.1f}%  {bar}{flag}"
            )

        logger.info("=" * 70)

        # Recommendations based on bottleneck
        logger.info("")
        logger.info("RECOMMENDATIONS:")
        logger.info(f"  Bottleneck: {bottleneck.phase} ({bottleneck.percentage:.1f}%)")

        if bottleneck.phase == "Extract":
            logger.info("  → Consider reading large files in chunks instead of loading all at once")
            logger.info("  → Use memory-mapped file I/O for faster sequential reads")
        elif bottleneck.phase == "Shuffle":
            logger.info("  → Shuffle is I/O bound — consider batching messages or faster IPC")
            logger.info("  → Check if the message queue is becoming a bottleneck")
        elif bottleneck.phase == "Reduce":
            logger.info("  → Reduce may be slow due to high Data Skew — run skew detection")
            logger.info("  → Check if Python GIL is limiting parallelism")
        elif bottleneck.phase == "Transform":
            logger.info("  → Geo-lookup is the bottleneck — this is expected for large IP sets")
            logger.info("  → Consider caching frequent lookups or parallelizing across reducers")
        elif bottleneck.phase == "Load":
            logger.info("  → JSON writing may be I/O bound — consider writing in larger batches")
            logger.info("  → Check disk speed for the output directory")

        logger.info("=" * 70)
        return bottleneck_info

    def get_results(self) -> list[TimingResult]:
        return list(self._results)

    def get_total_time(self) -> float:
        return self._total_time

    def reset(self):
        self._results = []
        self._total_time = 0.0
