"""
Range Partitioner với Sampling — Phần 3.2 (Özsu & Valduriez).
PHẦN 3 — Thay thế Hash Partitioning bằng Range Partitioning.

Strategy 1: Range Partitioning + Sampling
  1. Sampling: đọc SAMPLE_SIZE IPs, xây histogram
  2. Build ranges: chia histogram thành N buckets đều
  3. Partition: IP thuộc range nào → reducer đó

Strategy 2: Adaptive Load Reallocation (Work Stealing)
  1. Sau Reduce, đo skew thực tế
  2. Nếu skew > SKEW_THRESHOLD → trigger work stealing
  3. Hot reducer (nhiều IPs) chia IPs cho cold reducers (ít IPs)
  4. Rebalance: hot reducers trao prefix/suffix của sorted-IP-list

Strategy 3: Control Operators (skew-aware execution plan)
  1. Estimate cost trước khi chạy
  2. Monitor actual vs estimate trong lúc chạy
  3. Adaptive re-plan nếu chênh lệch > CONTROL_THRESHOLD
"""
import csv
import logging
import random
import time
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from .config import (
    CSV_FILES,
    LOG_FILES,
    DATA_SOURCE_MODE,
    SKEW_THRESHOLD,
    N_EXTRACTORS,
)

logger = logging.getLogger("etl.range_partitioner")


# ─────────────────────────────────────────────────────────────────
# IP helpers
# ─────────────────────────────────────────────────────────────────

def ip_to_int(ip: str) -> int:
    """Convert dotted-decimal IP to 32-bit integer."""
    try:
        parts = ip.split(".")
        return (
            (int(parts[0]) << 24)
            | (int(parts[1]) << 16)
            | (int(parts[2]) << 8)
            | int(parts[3])
        )
    except (ValueError, IndexError):
        return 0


def int_to_ip(val: int) -> str:
    """Convert 32-bit integer back to dotted-decimal IP."""
    return (
        f"{(val >> 24) & 0xFF}."
        f"{(val >> 16) & 0xFF}."
        f"{(val >> 8) & 0xFF}."
        f"{val & 0xFF}"
    )


# ─────────────────────────────────────────────────────────────────
# Sampling
# ─────────────────────────────────────────────────────────────────

def sample_ips_from_data_source(
    n_samples: int = 10000,
    seed: int = 42,
) -> list[int]:
    """
    Lấy mẫu ngẫu nhiên SAMPLE_SIZE IPs từ tất cả data source.
    Dùng reservoir sampling để đảm bảo đại diện.
    Trả về danh sách IP dạng integer.
    """
    rng = random.Random(seed)
    reservoir: list[int] = []
    total_seen = 0

    if DATA_SOURCE_MODE == "csv":
        sources = list(CSV_FILES.values())
    else:
        sources = list(LOG_FILES.values())

    for file_path in sources:
        p = Path(file_path)
        if not p.exists():
            continue

        if file_path.endswith(".csv"):
            try:
                with open(p, "r", encoding="utf-8", errors="replace", newline="") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        raw = row.get("raw_line", "")
                        ip = _extract_ip_from_line(raw)
                        if not ip:
                            continue
                        total_seen += 1
                        ip_int = ip_to_int(ip)
                        if len(reservoir) < n_samples:
                            reservoir.append(ip_int)
                        else:
                            j = rng.randint(0, total_seen)
                            if j < n_samples:
                                reservoir[j] = ip_int
            except Exception:
                continue
        else:
            # .log file
            try:
                with open(p, "r", encoding="utf-8", errors="ignore") as f:
                    for line in f:
                        ip = _extract_ip_from_line(line)
                        if not ip:
                            continue
                        total_seen += 1
                        ip_int = ip_to_int(ip)
                        if len(reservoir) < n_samples:
                            reservoir.append(ip_int)
                        else:
                            j = rng.randint(0, total_seen)
                            if j < n_samples:
                                reservoir[j] = ip_int
            except Exception:
                continue

    logger.info(f"  Sampling: collected {len(reservoir)} IPs from ~{total_seen:,} total rows")
    return reservoir


def _extract_ip_from_line(line: str) -> Optional[str]:
    """Extract IP from log line or CSV raw_line."""
    # Thử CSV column first
    if " - - [" in line:
        # Apache combined log format
        import re
        m = re.match(r"^(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})", line)
        if m:
            return m.group(1)
    # CSV raw_line
    if '"' in line:
        import re
        m = re.match(r"^(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})", line)
        if m:
            return m.group(1)
    return None


# ─────────────────────────────────────────────────────────────────
# Range Builder
# ─────────────────────────────────────────────────────────────────

def build_uniform_ranges(
    ip_ints: list[int],
    n_reducers: int,
) -> list[int]:
    """
    Xây N+1 range boundaries sao cho mỗi reducer nhận ~equal count.
    Dùng percentile để find boundaries.
    Trả về list gồm n_reducers+1 boundaries (bao gồm 0 và max+1).
    """
    if not ip_ints:
        # Fallback: evenly spaced
        return [0] + [(2**32 // n_reducers) * i for i in range(1, n_reducers + 1)]

    sorted_ints = sorted(ip_ints)
    n = len(sorted_ints)
    boundaries = [0]  # lower bound of first range

    for i in range(1, n_reducers):
        # Percentile position: i/n_reducers của sorted list
        idx = int((i / n_reducers) * n)
        boundaries.append(sorted_ints[min(idx, n - 1)])

    boundaries.append(2**32)  # upper bound (beyond all IPs)
    return boundaries


# ─────────────────────────────────────────────────────────────────
# Range Partitioner
# ─────────────────────────────────────────────────────────────────

class RangePartitioner:
    """
    Range-based partitioner: chia IP ranges bằng sampling + percentile.

    So với Hash Partitioning:
      - Hash: deterministic nhưng không guarantee equal-sized partitions
      - Range: đảm bảo mỗi reducer nhận ~equal count (nếu sampling đủ tốt)
      - Đặc biệt tốt khi IP distribution không uniform

    Thuật toán:
      1. Sample SAMPLE_SIZE IPs từ data source
      2. Sort samples → build percentile boundaries
      3. partition_key(ip) = binary_search(boundaries, ip_int)
    """

    def __init__(
        self,
        n_reducers: int,
        use_sampling: bool = True,
        sample_size: int = 10000,
    ):
        self.n_reducers = n_reducers
        self.use_sampling = use_sampling
        self.sample_size = sample_size
        self.boundaries: list[int] = []
        self.sample_count: int = 0
        self.skew_factor: float = 0.0

    def fit(self) -> "RangePartitioner":
        """
        Phase 1: Fit — sample + build range boundaries.
        Gọi 1 lần trước khi partition bất kỳ IP nào.
        """
        start = time.perf_counter()

        # Lấy mẫu
        sample_ints = sample_ips_from_data_source(self.sample_size)

        # Build ranges từ samples
        self.boundaries = build_uniform_ranges(sample_ints, self.n_reducers)
        self.sample_count = len(sample_ints)

        elapsed = time.perf_counter() - start
        logger.info(
            f"  RangePartitioner: fitted on {self.sample_count} samples "
            f"in {elapsed:.3f}s | ranges={self._range_stats()}"
        )
        return self

    def _range_stats(self) -> str:
        """Human-readable range distribution."""
        if not self.boundaries or len(self.boundaries) < 2:
            return "no boundaries"
        parts = []
        for i in range(len(self.boundaries) - 1):
            lo = int_to_ip(self.boundaries[i])
            hi = int_to_ip(self.boundaries[i + 1] - 1)
            parts.append(f"R{i}:{lo.split('.')[-1]}-{hi.split('.')[-1]}")
        return " | ".join(parts)

    def partition_key(self, ip: str) -> int:
        """
        Tìm reducer ID cho IP này dùng binary search.
        Binary search O(log N) trên boundaries.
        """
        ip_int = ip_to_int(ip)

        # Binary search: tìm i sao cho boundaries[i] <= ip < boundaries[i+1]
        lo, hi = 0, len(self.boundaries) - 1
        while lo < hi:
            mid = (lo + hi + 1) // 2
            if self.boundaries[mid] <= ip_int:
                lo = mid
            else:
                hi = mid - 1

        return min(lo, self.n_reducers - 1)

    def get_partition_counts(self, ip_ints: list[int]) -> dict[int, int]:
        """
        Đếm xem mỗi reducer nhận bao nhiêu IPs (dùng boundaries đã fit).
        Dùng binary search nhanh.
        """
        counts: dict[int, int] = {i: 0 for i in range(self.n_reducers)}
        for ip_int in ip_ints:
            rid = self._binary_search(ip_int)
            counts[rid] += 1
        return counts

    def _binary_search(self, ip_int: int) -> int:
        lo, hi = 0, len(self.boundaries) - 1
        while lo < hi:
            mid = (lo + hi + 1) // 2
            if self.boundaries[mid] <= ip_int:
                lo = mid
            else:
                hi = mid - 1
        return min(lo, self.n_reducers - 1)

    def measure_sampling_skew(self, ip_ints: list[int]) -> float:
        """Đo skew factor từ sampling data (estimate trước khi partition thật)."""
        counts = list(self.get_partition_counts(ip_ints).values())
        if not counts:
            return 0.0
        max_c, min_c = max(counts), min(counts)
        avg_c = sum(counts) / len(counts)
        return (max_c - min_c) / avg_c if avg_c > 0 else 0.0


# ─────────────────────────────────────────────────────────────────
# Work Stealing — Adaptive Load Reallocation
# ─────────────────────────────────────────────────────────────────

@dataclass
class StealResult:
    """Kết quả của một work stealing operation."""
    stealer_id: int
    victim_id: int
    items_stolen: list  # list of (ip, site_id) tuples
    n_stolen: int
    skew_before: float
    skew_after: float


class WorkStealer:
    """
    Adaptive Load Reallocation (Strategy 2 — Özsu & Valduriez).

    Khi skew_factor > skew_threshold:
      1. Sort IPs trong hot partition (reducer có nhiều IPs nhất)
      2. Chia đều: hot reducer giữ prefix, victim nhận suffix
      3. Đảm bảo mỗi reducer có ~equal workload

    Đây là work stealing đơn giản: hot → cold.
    """
    skew_threshold: float = SKEW_THRESHOLD

    def __init__(self, skew_threshold: float = SKEW_THRESHOLD):
        self.skew_threshold = skew_threshold
        self.steal_history: list[StealResult] = []

    def should_steal(
        self,
        partitions: dict[int, list],
    ) -> bool:
        """
        Kiểm tra xem có nên trigger work stealing không.
        True nếu skew_factor > skew_threshold.
        """
        if not partitions:
            return False
        counts = {rid: len(tuples) for rid, tuples in partitions.items()}
        vals = list(counts.values())
        avg = sum(vals) / len(vals)
        if avg == 0:
            return False
        skew = (max(vals) - min(vals)) / avg
        return skew > self.skew_threshold

    def rebalance(
        self,
        partitions: dict[int, list[tuple[str, int]]],
        sort_key=None,
    ) -> dict[int, list[tuple[str, int]]]:
        """
        Thực hiện work stealing: chia tải từ hot → cold reducers.

        Thuật toán:
          1. Tính target count = total / n_reducers
          2. Hot reducer (max) chia excess cho cold reducer (min)
          3. Lặp cho đến khi skew <= threshold HOẶC không còn cặp hot-cold

        Args:
            partitions: {reducer_id: [(ip, site_id), ...]}
            sort_key: function(ip_str) để sort IPs (default: ip_to_int)

        Returns:
            Rebalanced partitions
        """
        if sort_key is None:
            sort_key = ip_to_int

        result = {rid: list(tuples) for rid, tuples in partitions.items()}
        iteration = 0
        max_iterations = 10  # tránh infinite loop

        while iteration < max_iterations:
            counts = {rid: len(result[rid]) for rid in result}
            vals = list(counts.values())
            total = sum(vals)
            n = len(vals)
            target = total // n  # target per reducer

            if total == 0:
                break

            max_rid = max(counts, key=lambda r: counts[r])
            min_rid = min(counts, key=lambda r: counts[r])
            max_c = counts[max_rid]
            min_c = counts[min_rid]
            avg = total / n
            skew = (max_c - min_c) / avg if avg > 0 else 0.0

            logger.info(
                f"  WorkStealer iter {iteration}: skew={skew:.4f} "
                f"| hot=R{max_rid}({max_c}) cold=R{min_rid}({min_c}) target={target}"
            )

            # Stop if skew is acceptable
            if skew <= self.skew_threshold:
                logger.info(
                    f"  WorkStealer: skew {skew:.4f} <= threshold {self.skew_threshold} — STOP"
                )
                break

            # Stop if no more imbalance
            if max_c <= target + 1:
                logger.info(
                    f"  WorkStealer: hot reducer already at target ({max_c} <= {target+1}) — STOP"
                )
                break

            # Sort hot partition by IP (for fair splitting)
            result[max_rid].sort(key=lambda t: sort_key(t[0]))

            # Compute how many to move
            excess = max_c - target
            to_move = excess // 2  # move half of excess

            if to_move <= 0:
                break

            # Take suffix of hot reducer (highest IPs)
            moved = result[max_rid][-to_move:]
            result[max_rid] = result[max_rid][:-to_move]
            result[min_rid].extend(moved)

            new_counts = {rid: len(result[rid]) for rid in result}
            new_max = max(new_counts.values())
            new_min = min(new_counts.values())
            new_skew = (new_max - new_min) / avg if avg > 0 else 0.0

            self.steal_history.append(StealResult(
                stealer_id=min_rid,
                victim_id=max_rid,
                items_stolen=moved,
                n_stolen=len(moved),
                skew_before=skew,
                skew_after=new_skew,
            ))

            logger.info(
                f"  WorkStealer: moved {len(moved)} items from R{max_rid}→R{min_rid} "
                f"(skew {skew:.4f}→{new_skew:.4f})"
            )

            iteration += 1

        # Final report
        final_counts = {rid: len(result[rid]) for rid in result}
        final_vals = list(final_counts.values())
        final_skew = (
            (max(final_vals) - min(final_vals)) / (sum(final_vals) / len(final_vals))
            if final_vals and sum(final_vals) > 0 else 0.0
        )
        logger.info(
            f"  WorkStealer: final — "
            f"counts={[final_counts[r] for r in sorted(final_counts)]} "
            f"skew={final_skew:.4f} | iterations={iteration}"
        )

        return result

    def get_stats(self) -> dict:
        return {
            "total_steals": len(self.steal_history),
            "total_items_moved": sum(s.n_stolen for s in self.steal_history),
            "skew_before_first": self.steal_history[0].skew_before if self.steal_history else 0.0,
            "skew_after_last": self.steal_history[-1].skew_after if self.steal_history else 0.0,
        }
