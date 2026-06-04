"""
Memory monitoring for ETL pipeline.
PHẦN 8.3 — Tracks RAM usage and triggers GC to prevent OOM.
"""
import gc
import logging
import os
import sys
from typing import Optional

from .config import MEMORY_THRESHOLD_PCT

logger = logging.getLogger("etl.memory")

try:
    import psutil
    _PSUTIL_AVAILABLE = True
except ImportError:
    _PSUTIL_AVAILABLE = False


class MemoryMonitor:
    """
    Monitors memory usage throughout the pipeline lifecycle.
    Logs usage at each phase and warns when approaching thresholds.
    """

    def __init__(self, threshold_pct: float = MEMORY_THRESHOLD_PCT):
        self.threshold_pct = threshold_pct
        self._snapshots: list[dict] = []

    def get_memory_usage(self) -> dict:
        """Return current memory stats in MB."""
        if _PSUTIL_AVAILABLE:
            vm = psutil.virtual_memory()
            return {
                "used_mb": round(vm.used / (1024 * 1024), 2),
                "available_mb": round(vm.available / (1024 * 1024), 2),
                "percent": round(vm.percent, 2),
                "total_mb": round(vm.total / (1024 * 1024), 2),
            }

        return {
            "used_mb": 0,
            "available_mb": 0,
            "percent": 0,
            "total_mb": 0,
        }

    def log_phase(self, phase_name: str) -> bool:
        """
        Log memory usage for a named phase.
        Returns True if memory is above threshold (warning).
        """
        stats = self.get_memory_usage()
        self._snapshots.append({"phase": phase_name, **stats})

        logger.info(
            f"  [{phase_name}] Memory: {stats['used_mb']:.1f}MB "
            f"({stats['percent']:.1f}% used)"
        )

        if stats["percent"] > self.threshold_pct:
            logger.warning(
                f"  ⚠️  Memory WARNING: {stats['percent']:.1f}% > {self.threshold_pct}% threshold"
            )
            return True
        return False

    def force_gc(self, phase_name: str = "GC"):
        """Force garbage collection and log the result."""
        collected = gc.collect()
        if _PSUTIL_AVAILABLE:
            before = self.get_memory_usage()
            logger.info(
                f"  [{phase_name}] GC collected {collected} objects. "
                f"Memory freed: {before['used_mb']:.1f}MB"
            )
        else:
            logger.info(f"  [{phase_name}] GC collected {collected} objects")
        return collected

    def estimate_memory_requirement(self, data_size_mb: float, ip_count: int) -> dict:
        """
        Rough estimate of memory needed for the pipeline.
        Useful for pre-flight checks before running on large datasets.
        """
        log_buffer = data_size_mb * 1.5
        geo_db = 60.0
        ip_set_mb = (ip_count * 80) / (1024 * 1024)
        result_json_mb = (ip_count * 200) / (1024 * 1024)
        overhead = 50.0

        total_mb = log_buffer + geo_db + ip_set_mb + result_json_mb + overhead

        estimate = {
            "log_buffer_mb": round(log_buffer, 2),
            "geo_db_mb": geo_db,
            "ip_set_mb": round(ip_set_mb, 2),
            "result_json_mb": round(result_json_mb, 2),
            "overhead_mb": overhead,
            "total_mb": round(total_mb, 2),
        }

        logger.info(
            f"  Memory estimate: {estimate['total_mb']:.1f}MB "
            f"(data={data_size_mb}MB, ips={ip_count})"
        )
        return estimate

    def get_snapshots(self) -> list[dict]:
        return list(self._snapshots)

    def print_summary(self):
        """Print a summary table of all memory snapshots."""
        if not self._snapshots:
            return

        logger.info("")
        logger.info("=" * 55)
        logger.info("MEMORY USAGE SUMMARY")
        logger.info("=" * 55)
        logger.info(f"  {'Phase':<20} {'Used MB':>10} {'%':>8}")
        logger.info("-" * 55)
        for snap in self._snapshots:
            logger.info(
                f"  {snap['phase']:<20} {snap['used_mb']:>10.1f} {snap['percent']:>7.1f}%"
            )
        logger.info("=" * 55)
