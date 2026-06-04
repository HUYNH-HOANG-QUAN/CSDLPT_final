"""
Fault Tolerance cho Distributed ETL Pipeline.
PHẦN 9 — Xử lý khi 1 Node/Process bị chết.

Özsu & Valduriez — Fault Tolerance in Distributed Systems:
  1. Detection:     Biết được process nào đã chết
  2. Containment:  Không ảnh hưởng các process còn lại
  3. Recovery:     Retry riêng process bị chết, không retry toàn bộ

Strategy được implement:
  - Process timeout: giới hạn thời gian mỗi reducer/process
  - Partial retry:  chỉ retry process bị chết, không retry process đã thành công
  - Partial results: dùng lại intermediate files từ lần chạy trước (idempotent)
  - Graceful degradation: nếu retry fails → sequential fallback
  - Orphan cleanup: xóa partial intermediate files khi bắt đầu pipeline mới
"""
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from .config import INTERMEDIATE_DIR, INTERMEDIATE_FILE_PATTERN

logger = logging.getLogger("etl.fault_tolerance")


# ─────────────────────────────────────────────────────────────────
# Failure classification
# ─────────────────────────────────────────────────────────────────

class ProcessDeathError(Exception):
    """Raised when a process/thread dies unexpectedly."""
    pass


class ProcessTimeoutError(ProcessDeathError):
    """Raised when a process exceeds its timeout."""
    pass


@dataclass
class NodeFailure:
    """Record of a node/process failure."""
    node_id: int
    node_type: str  # "reducer" or "extractor"
    reason: str     # "timeout", "exception", "signal", "OOM"
    timestamp: float
    attempt: int
    recovered: bool = False

    def __str__(self):
        status = "RECOVERED" if self.recovered else "UNRECOVERED"
        return f"NodeFailure[{self.node_type}={self.node_id}, {self.reason}, attempt={self.attempt}, {status}]"


@dataclass
class FaultToleranceStats:
    """Aggregate fault tolerance statistics."""
    failures_detected: int = 0
    failures_recovered: int = 0
    retries_attempted: int = 0
    max_retries_exceeded: int = 0
    sequential_fallback_used: bool = False
    orphan_files_cleaned: int = 0
    recovery_time_s: float = 0.0
    failures: list[NodeFailure] = field(default_factory=list)

    def add_failure(self, f: NodeFailure):
        self.failures.append(f)
        self.failures_detected += 1
        if f.recovered:
            self.failures_recovered += 1

    def summary(self) -> str:
        lines = [
            "=" * 60,
            "FAULT TOLERANCE SUMMARY",
            "=" * 60,
            f"  Failures detected:       {self.failures_detected}",
            f"  Failures recovered:      {self.failures_recovered}",
            f"  Retries attempted:        {self.retries_attempted}",
            f"  Max retries exceeded:     {self.max_retries_exceeded}",
            f"  Sequential fallback:     {'YES' if self.sequential_fallback_used else 'No'}",
            f"  Orphan files cleaned:    {self.orphan_files_cleaned}",
            f"  Recovery time:           {self.recovery_time_s:.3f}s",
        ]
        for f in self.failures:
            lines.append(f"    {f}")
        lines.append("=" * 60)
        return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────
# FaultTolerance — manages recovery for Phase A and Phase B
# ─────────────────────────────────────────────────────────────────

class FaultTolerance:
    """
    Manages fault tolerance for the ETL pipeline.

    Key design decisions (Özsu & Valduriez):
      - Process-level isolation: mỗi reducer có intermediate file riêng
      - Idempotent writes: an toàn khi retry
      - Partial retry: chỉ retry process bị chết
      - Timeout per process: tránh process treo vĩnh viễn
      - Orphan cleanup: xóa partial files trước khi bắt đầu
    """

    DEFAULT_TIMEOUT_PER_NODE = 120.0  # seconds — GeoIP transform timeout
    DEFAULT_MAX_RETRIES = 3
    RETRY_BACKOFF_BASE = 1.5  # exponential backoff: 1.5s, 2.25s, 3.375s...

    def __init__(
        self,
        timeout_per_node: float = DEFAULT_TIMEOUT_PER_NODE,
        max_retries: int = DEFAULT_MAX_RETRIES,
        clean_orphans_on_start: bool = True,
    ):
        self.timeout = timeout_per_node
        self.max_retries = max_retries
        self.stats = FaultToleranceStats()
        self._start_recovery_time: float = 0.0

        if clean_orphans_on_start:
            self.clean_orphan_files()

    # ── Orphan file management ─────────────────────────────────

    def clean_orphan_files(self) -> int:
        """
        Xóa tất cả intermediate files từ lần chạy trước.
        Đảm bảo mỗi pipeline run bắt đầu sạch sẽ — không dùng partial results.

        Gọi khi: Pipeline bắt đầu (trước Phase A).
        """
        cleaned = 0
        if not INTERMEDIATE_DIR.exists():
            return 0

        for p in INTERMEDIATE_DIR.glob("reducer_*.json"):
            try:
                p.unlink()
                cleaned += 1
            except OSError:
                pass

        # Also clean .tmp files
        for p in INTERMEDIATE_DIR.glob("reducer_*.tmp"):
            try:
                p.unlink()
            except OSError:
                pass

        if cleaned > 0:
            logger.warning(f"  FaultTolerance: cleaned {cleaned} orphan intermediate files from previous run")
            self.stats.orphan_files_cleaned = cleaned

        return cleaned

    def get_completed_partitions(self, n_reducers: int) -> set[int]:
        """
        Trả về set các reducer IDs đã có intermediate file đầy đủ.
        Dùng khi: muốn resume từ partial run (đã có results từ lần trước).
        """
        completed = set()
        for rid in range(n_reducers):
            path = INTERMEDIATE_DIR / INTERMEDIATE_FILE_PATTERN.format(rid=rid)
            if path.exists() and path.stat().st_size > 0:
                completed.add(rid)
        return completed

    # ── Retry logic ──────────────────────────────────────────────

    def retry_failed_node(
        self,
        node_id: int,
        node_type: str,
        task_fn,
        *args,
        **kwargs,
    ) -> tuple[bool, any]:
        """
        Retry a single failed node with exponential backoff.

        Args:
            node_id: reducer/extractor ID
            node_type: "reducer" or "extractor"
            task_fn: callable to retry
            *args, **kwargs: passed to task_fn

        Returns:
            (success, result) — True if recovered, result or None if failed
        """
        for attempt in range(1, self.max_retries + 1):
            self.stats.retries_attempted += 1
            backoff = self.RETRY_BACKOFF_BASE ** (attempt - 1)

            failure = NodeFailure(
                node_id=node_id,
                node_type=node_type,
                reason="unknown",
                timestamp=time.time(),
                attempt=attempt,
            )

            if attempt > 1:
                logger.warning(
                    f"  FaultTolerance: retrying {node_type}[{node_id}] "
                    f"(attempt {attempt}/{self.max_retries}) after {backoff:.1f}s backoff"
                )
                time.sleep(backoff)

            try:
                self._start_recovery_time = time.time()
                result = task_fn(*args, **kwargs)
                failure.reason = "success"
                failure.recovered = True
                self.stats.add_failure(failure)
                self.stats.recovery_time_s += time.time() - self._start_recovery_time
                logger.info(
                    f"  FaultTolerance: {node_type}[{node_id}] RECOVERED "
                    f"on attempt {attempt}"
                )
                return True, result

            except ProcessTimeoutError:
                failure.reason = "timeout"
                logger.warning(
                    f"  FaultTolerance: {node_type}[{node_id}] TIMEOUT "
                    f"on attempt {attempt}/{self.max_retries}"
                )

            except ProcessDeathError as e:
                failure.reason = f"process_death: {e}"
                logger.warning(
                    f"  FaultTolerance: {node_type}[{node_id}] DIED: {e} "
                    f"(attempt {attempt}/{self.max_retries})"
                )

            except KeyboardInterrupt:
                failure.reason = "keyboard_interrupt"
                raise

            except Exception as e:
                failure.reason = f"exception: {type(e).__name__}: {e}"
                logger.warning(
                    f"  FaultTolerance: {node_type}[{node_id}] EXCEPTION: {e} "
                    f"(attempt {attempt}/{self.max_retries})"
                )

            self.stats.add_failure(failure)

        # All retries exhausted
        self.stats.max_retries_exceeded += 1
        logger.error(
            f"  FaultTolerance: {node_type}[{node_id}] FAILED after "
            f"{self.max_retries} retries — giving up"
        )
        return False, None

    # ── Process with timeout ────────────────────────────────────

    def run_with_timeout(
        self,
        fn,
        timeout: float,
        *args,
        node_id: int = -1,
        node_type: str = "node",
        **kwargs,
    ) -> any:
        """
        Chạy một function với timeout.

        Raises:
            ProcessTimeoutError: nếu vượt quá timeout
            ProcessDeathError: nếu exception xảy ra
        """
        import concurrent.futures

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(fn, *args, **kwargs)
            try:
                return future.result(timeout=timeout)
            except concurrent.futures.TimeoutError:
                # Cancel the future
                future.cancel()
                raise ProcessTimeoutError(
                    f"{node_type}[{node_id}] exceeded timeout of {timeout}s"
                )

    # ── Partial retry for Phase B reducers ─────────────────────

    def recover_phase_b(
        self,
        completed_rids: set[int],
        all_rids: list[int],
        run_reducer_fn,
        geodb_path: str | None,
    ) -> dict[int, any]:
        """
        Phase B partial recovery — retry only failed reducers.

        Args:
            completed_rids: set of reducer IDs that already have valid intermediate files
            all_rids: list of all reducer IDs (0 to n_reducers-1)
            run_reducer_fn: function(rid, geodb_path) -> ReducerResult

        Returns:
            results dict: {rid: ReducerResult} for ALL reducers
        """
        failed_rids = [r for r in all_rids if r not in completed_rids]
        if not failed_rids:
            logger.info(
                f"  FaultTolerance: all {len(all_rids)} reducers completed — no recovery needed"
            )
            return {}

        logger.warning(
            f"  FaultTolerance: {len(failed_rids)}/{len(all_rids)} reducers failed: {failed_rids}"
        )

        results: dict[int, any] = {}
        for rid in failed_rids:
            success, result = self.retry_failed_node(
                node_id=rid,
                node_type="reducer",
                task_fn=run_reducer_fn,
                rid=rid,
                geodb_path=geodb_path,
            )
            if success:
                results[rid] = result

        return results

    # ── Graceful degradation ─────────────────────────────────────

    def sequential_fallback(
        self,
        all_rids: list[int],
        run_reducer_fn,
        geodb_path: str | None,
    ) -> dict[int, any]:
        """
        Fallback: chạy tất cả reducers tuần tự trong main process.
        Dùng khi: multiprocessing fails hoàn toàn (system-level issue).
        """
        self.stats.sequential_fallback_used = True
        logger.warning(
            "  FaultTolerance: SEQUENTIAL FALLBACK — running reducers one-by-one "
            "in main process (no parallelism)"
        )

        results: dict[int, any] = {}
        for rid in all_rids:
            try:
                result = run_reducer_fn(rid=rid, geodb_path=geodb_path)
                results[rid] = result
                logger.info(f"  FaultTolerance: reducer[{rid}] done in sequential mode")
            except Exception as e:
                logger.error(f"  FaultTolerance: reducer[{rid}] FAILED even in sequential: {e}")

        return results

    # ── Report ──────────────────────────────────────────────────

    def report(self) -> str:
        if self.stats.failures_detected == 0:
            return ""
        return self.stats.summary()

    def has_failures(self) -> bool:
        return self.stats.failures_detected > 0

    def recovery_success_rate(self) -> float:
        if self.stats.failures_detected == 0:
            return 1.0
        return self.stats.failures_recovered / self.stats.failures_detected
