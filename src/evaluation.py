"""
Evaluation module: Data Skew Detection.
PHẦN 8.1 — Monitors IP distribution across Reducers.
Skew Factor = (max - min) / avg — high skew means poor load balance.
"""
import logging
from typing import Optional

from .config import SKEW_THRESHOLD

logger = logging.getLogger("etl.skew")


class DataSkewDetector:
    """
    Detects and reports data skew across Reducer partitions.
    A well-balanced system has Skew Factor ≈ 0.
    """

    def __init__(self, n_reducers: int, skew_threshold: float = SKEW_THRESHOLD):
        self.n_reducers = n_reducers
        self.skew_threshold = skew_threshold
        self._ip_counts: dict[int, int] = {i: 0 for i in range(n_reducers)}

    def record_ip(self, reducer_id: int):
        """Count one IP arriving at a specific Reducer."""
        if reducer_id not in self._ip_counts:
            self._ip_counts[reducer_id] = 0
        self._ip_counts[reducer_id] += 1

    def record_batch(self, reducer_id: int, count: int):
        """Record a batch of IPs at once."""
        if reducer_id not in self._ip_counts:
            self._ip_counts[reducer_id] = 0
        self._ip_counts[reducer_id] += count

    def measure_skew(self) -> float:
        """
        Calculate and report Skew Factor.
        Returns: float — Skew Factor
        """
        counts = list(self._ip_counts.values())
        if not counts or all(c == 0 for c in counts):
            logger.warning("No IP data recorded for skew measurement")
            return 0.0

        max_count = max(counts)
        min_count = min(counts)
        avg_count = sum(counts) / len(counts)

        if avg_count == 0:
            return 0.0

        skew_factor = (max_count - min_count) / avg_count

        logger.info("=" * 60)
        logger.info("DATA SKEW DETECTION REPORT")
        logger.info("=" * 60)
        for rid, cnt in self._ip_counts.items():
            pct = (cnt / avg_count * 100) if avg_count > 0 else 0
            bar = "█" * int(pct / 5) + "░" * (20 - int(pct / 5))
            logger.info(f"  Reducer[{rid}]: {cnt:>8} IP ({pct:>6.1f}%) {bar}")
        logger.info("-" * 60)
        logger.info(f"  Max: {max_count} | Min: {min_count} | Avg: {avg_count:.1f}")
        logger.info(f"  Skew Factor: {skew_factor:.4f}")

        if skew_factor > self.skew_threshold:
            logger.warning(f"  ⚠️  HIGH SKEW: {skew_factor:.2f} > {self.skew_threshold} — Consider a different hash function or more reducers")
        elif skew_factor > 0.2:
            logger.info(f"  ⚡ Moderate skew: {skew_factor:.2f}")
        else:
            logger.info(f"  ✓ Low skew: {skew_factor:.4f} — Distribution is balanced")

        logger.info("=" * 60)
        return skew_factor

    def get_ip_counts(self) -> dict[int, int]:
        return dict(self._ip_counts)

    def reset(self):
        self._ip_counts = {i: 0 for i in range(self.n_reducers)}
