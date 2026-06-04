"""
Coordinator: Orchestrates the full distributed ETL pipeline.
PHẦN 7 — Coordinates all phases and evaluation.

Architecture (Özsu & Valduriez):
  Phase A: Extract + Shuffle (parallel across N extractors)
  Phase B: Reduce + Transform + Load (PARALLEL ACROSS PROCESSES)
             → multiprocessing.ProcessPoolExecutor — bypasses GIL
             → each Process has its own GeoLookup instance
             → 4 processes × 6 cores → true parallelism
  Phase B+: Adaptive Work Stealing (nếu skew > threshold)
  Phase C: Merge N intermediate files → central_store.json
"""
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from .config import (
    CSV_FILES,
    CSV_9K_FILES,
    CSV_SKEW_FILES,
    DATA_SOURCE_MODE,
    DEFAULT_N_REDUCERS,
    GEODB_DEFAULT_PATH,
    LOG_FILES,
    MEMORY_THRESHOLD_PCT,
    OUTPUT_PATH,
    PARTITIONING_STRATEGY,
    RANGE_SAMPLE_SIZE,
    SKEW_THRESHOLD,
    WORK_STEAL_MAX_ITERATIONS,
)
from .extractor import Extractor, ExtractorResult
from .fault_tolerance import FaultTolerance, NodeFailure
from .geo_lookup import GeoLookup, get_default_db_path
from .hash_fn import partition_key as hash_partition_key
from .load import IntermediateStore, JSONStore
from .range_partitioner import RangePartitioner, WorkStealer, ip_to_int
from .reducer import Reducer, ReducerResult
from .evaluation import DataSkewDetector
from .memory_monitor import MemoryMonitor
from .latency_tracker import LatencyTracker

logger = logging.getLogger("etl.coordinator")


@dataclass
class PipelineStats:
    """Aggregate statistics from a pipeline run."""
    total_rows_read: int
    total_ip_local_unique: int
    total_ip_global_unique: int
    total_ip_skipped_dup: int
    duration_s: float
    throughput_rows_per_sec: float
    throughput_ips_per_sec: float
    n_reducers: int
    skew_factor: float
    ip_counts_per_reducer: dict[int, int]
    partition_strategy: str
    sampling_skew: float = 0.0
    work_steal_triggered: bool = False
    work_steal_total_moved: int = 0
    work_steal_iterations: int = 0
    merge_duration_s: float = 0.0
    dedup_time_s: float = 0.0
    transform_time_s: float = 0.0
    phase_b_time_s: float = 0.0
    ft_failures_detected: int = 0
    ft_failures_recovered: int = 0
    ft_retries_attempted: int = 0
    ft_sequential_fallback: bool = False


# ─────────────────────────────────────────────────────────────────
# Standalone function for multiprocessing (must be picklable)
# ─────────────────────────────────────────────────────────────────

def _run_reducer_task(
    args: tuple[int, list[tuple[str, int]], str | None],
) -> ReducerResult:
    """
    Module-level function (picklable) — runs in a SEPARATE PROCESS.
    Each process creates its own GeoLookup instance → bypasses GIL.
    """
    rid, ip_tuples, geodb_path = args
    geo = GeoLookup(geodb_path) if geodb_path else GeoLookup()
    reducer = Reducer(rid, geo)
    result = reducer.run(ip_tuples)
    geo.close()
    return result


class Coordinator:
    def __init__(
        self,
        log_files: dict[int, str] = LOG_FILES,
        n_reducers: int = DEFAULT_N_REDUCERS,
        output_path: str = OUTPUT_PATH,
        geodb_path: Optional[str] = GEODB_DEFAULT_PATH,
        track_skew: bool = True,
        track_memory: bool = True,
        track_latency: bool = True,
        memory_threshold_pct: float = MEMORY_THRESHOLD_PCT,
        skew_threshold: float = SKEW_THRESHOLD,
        n_workers: Optional[int] = None,
        data_source: Optional[str] = None,
        partition_strategy: Optional[str] = None,
    ):
        self.n_reducers = n_reducers
        self.output_path = output_path
        self.geodb_path = geodb_path
        self.track_skew = track_skew
        self.track_memory = track_memory
        self.track_latency = track_latency
        self.memory_threshold_pct = memory_threshold_pct
        self.skew_threshold = skew_threshold
        self.n_workers = n_workers

        # Data source
        self.data_source = data_source or DATA_SOURCE_MODE
        if self.data_source == "csv":
            self.log_files = dict(CSV_FILES)
        elif self.data_source == "csv9k":
            self.log_files = dict(CSV_9K_FILES)
        elif self.data_source == "skew":
            self.log_files = dict(CSV_SKEW_FILES)
        else:
            self.log_files = log_files

        # Partitioning strategy
        self.partition_strategy = partition_strategy or PARTITIONING_STRATEGY

        # Monitors
        self._skew_detector = DataSkewDetector(n_reducers, skew_threshold=skew_threshold) if track_skew else None
        self._memory_monitor = MemoryMonitor(threshold_pct=memory_threshold_pct) if track_memory else None
        self._latency_tracker = LatencyTracker() if track_latency else None

        # Resources
        self._geo_lookup: Optional[GeoLookup] = None
        self._store: Optional[JSONStore] = None
        self._intermediate_stores: list[IntermediateStore] = []
        self._extract_stats: list = []
        self._reduce_stats: list = []

        # Range partitioner + work stealer
        self._range_partitioner: Optional[RangePartitioner] = None
        self._work_stealer: Optional[WorkStealer] = None
        self._work_steal_stats: dict = {}

        # Fault tolerance
        self._fault_tolerance: FaultTolerance = FaultTolerance()

    def _partition_key(self, ip: str) -> int:
        if self.partition_strategy == "range" and self._range_partitioner:
            return self._range_partitioner.partition_key(ip)
        return hash_partition_key(ip, self.n_reducers)

    def _init_shared_resources(self):
        geodb = self.geodb_path
        if geodb is None or not Path(geodb).exists():
            detected = get_default_db_path()
            if detected:
                geodb = detected
                logger.info(f"  GeoLookup: auto-detected .mmdb at {geodb}")
        self._geo_lookup = GeoLookup(geodb)
        self._store = JSONStore(self.output_path, use_lock=False)

        if self.partition_strategy == "range":
            self._range_partitioner = RangePartitioner(
                n_reducers=self.n_reducers,
                sample_size=RANGE_SAMPLE_SIZE,
            )

        self._work_stealer = WorkStealer(skew_threshold=self.skew_threshold)

    def _cleanup(self):
        if self._geo_lookup:
            self._geo_lookup.close()

    def run(self) -> PipelineStats:
        overall_start = time.perf_counter()
        overall_skew = 0.0
        work_steal_triggered = False
        work_steal_moved = 0
        work_steal_iters = 0
        sampling_skew = 0.0

        # Clean orphan intermediate files from previous failed runs
        self._fault_tolerance.clean_orphan_files()

        logger.info("")
        logger.info("=" * 65)
        logger.info("COORDINATOR: Starting Distributed ETL Pipeline")
        logger.info(f"  Data source:      {self.data_source.upper()}")
        logger.info(f"  Partitioning:     {self.partition_strategy.upper()}")
        logger.info(f"  Extractors:      {len(self.log_files)}")
        logger.info(f"  Reducers:       {self.n_reducers}")
        logger.info(f"  Skew threshold:  {self.skew_threshold}")
        logger.info(f"  Output:          {self.output_path}")
        logger.info("=" * 65)

        if self._memory_monitor:
            self._memory_monitor.log_phase("Pipeline_Start")

        self._init_shared_resources()

        if self._geo_lookup:
            logger.info(
                "GeoLookup: %s (MMDB=%s)"
                % (self._geo_lookup.lookup_mode, self._geo_lookup.is_using_maxmind())
            )

        # ── Phase 0: Fit Range Partitioner ─────────────────────────
        sampling_start = time.perf_counter()
        if self._range_partitioner:
            logger.info("")
            logger.info("#" * 60)
            logger.info("# PHASE 0: Range Partitioner — Sampling + Build Boundaries")
            logger.info("#" * 60)
            self._range_partitioner.fit()
            sampling_elapsed = time.perf_counter() - sampling_start
            sampling_skew = 0.0
            logger.info(f"  Sampling phase done in {sampling_elapsed:.3f}s")

        # ── Phase A: Extract + Shuffle ─────────────────────────────
        partitions, extract_stats = self._phase_a_extract_shuffle()

        if self._memory_monitor:
            self._memory_monitor.log_phase("After_Extract_Shuffle")
            self._memory_monitor.force_gc("Post_Extract_GC")

        # ── Measure skew after shuffle ──────────────────────────────
        if self._skew_detector:
            self._skew_detector.reset()
            for rid in range(self.n_reducers):
                self._skew_detector.record_batch(rid, len(partitions.get(rid, [])))
            overall_skew = self._skew_detector.measure_skew()
        else:
            overall_skew = 0.0

        # ── Phase A+: Adaptive Work Stealing ───────────────────────
        if self._work_stealer and self._work_stealer.should_steal(partitions):
            logger.info("")
            logger.info("#" * 60)
            logger.info("# PHASE A+: Adaptive Work Stealing — Rebalancing Hot->Cold")
            logger.info("#" * 60)
            work_steal_triggered = True
            partitions = self._work_stealer.rebalance(partitions, sort_key=ip_to_int)
            self._work_steal_stats = self._work_stealer.get_stats()
            work_steal_moved = self._work_steal_stats["total_items_moved"]
            work_steal_iters = self._work_steal_stats["total_steals"]
            if self._skew_detector:
                self._skew_detector.reset()
                for rid in range(self.n_reducers):
                    self._skew_detector.record_batch(rid, len(partitions.get(rid, [])))
                overall_skew = self._skew_detector.measure_skew()

        # ── Phase B: Reduce + Transform + Load (MULTIPROCESSING) ──
        phase_b_start = time.perf_counter()
        self._reduce_stats = self._phase_b_reduce_transform_load(partitions)
        phase_b_elapsed = time.perf_counter() - phase_b_start

        if self._memory_monitor:
            self._memory_monitor.log_phase("After_Reduce_Transform_Load")
            self._memory_monitor.force_gc("Post_Reduce_GC")

        # ── Phase C: Merge intermediate files ─────────────────────
        merge_start = time.perf_counter()
        self._store.merge_intermediate_files(self.n_reducers)
        merge_duration = time.perf_counter() - merge_start

        overall_duration = time.perf_counter() - overall_start

        # ── Aggregate stats ────────────────────────────────────────
        total_rows = sum(s.rows_read for s in extract_stats)
        total_ip_local = sum(s.ip_count_local for s in extract_stats)
        total_ip_global = self._store.get_count()
        total_skipped = sum(s.ip_count_skipped_dupe for s in self._reduce_stats)

        total_dedup_s = sum(
            getattr(s, "duration_dedup_s", 0.0) for s in self._reduce_stats
        )
        total_transform_s = sum(
            getattr(s, "duration_transform_s", 0.0) for s in self._reduce_stats
        )

        throughput_rows = total_rows / overall_duration if overall_duration > 0 else 0
        throughput_ips = total_ip_global / overall_duration if overall_duration > 0 else 0

        ip_counts = {r.reducer_id: r.ip_count_unique for r in self._reduce_stats}

        stats = PipelineStats(
            total_rows_read=total_rows,
            total_ip_local_unique=total_ip_local,
            total_ip_global_unique=total_ip_global,
            total_ip_skipped_dup=total_skipped,
            duration_s=overall_duration,
            throughput_rows_per_sec=throughput_rows,
            throughput_ips_per_sec=throughput_ips,
            n_reducers=self.n_reducers,
            skew_factor=overall_skew,
            ip_counts_per_reducer=ip_counts,
            partition_strategy=self.partition_strategy,
            sampling_skew=sampling_skew,
            work_steal_triggered=work_steal_triggered,
            work_steal_total_moved=work_steal_moved,
            work_steal_iterations=work_steal_iters,
            merge_duration_s=merge_duration,
            dedup_time_s=total_dedup_s,
            transform_time_s=total_transform_s,
            phase_b_time_s=phase_b_elapsed,
            ft_failures_detected=self._fault_tolerance.stats.failures_detected,
            ft_failures_recovered=self._fault_tolerance.stats.failures_recovered,
            ft_retries_attempted=self._fault_tolerance.stats.retries_attempted,
            ft_sequential_fallback=self._fault_tolerance.stats.sequential_fallback_used,
        )

        logger.info("")
        logger.info("=" * 65)
        logger.info("COORDINATOR: Pipeline Complete")
        logger.info(f"  Partition strategy:     {stats.partition_strategy.upper()}")
        logger.info(f"  Total rows read:       {stats.total_rows_read:>12,}")
        logger.info(f"  Total IP local unique: {stats.total_ip_local_unique:>12,}  (after LOCAL dedup)")
        logger.info(f"  Total IP global unique:{stats.total_ip_global_unique:>12,}  (after GLOBAL dedup)")
        logger.info(f"  Duplicates skipped:    {stats.total_ip_skipped_dup:>12,}  ← trùng đã xử lý")

        # ── Per-reducer IP distribution ────────────────────────────
        if stats.ip_counts_per_reducer:
            total_in_reducers = sum(stats.ip_counts_per_reducer.values())
            avg_per_reducer = total_in_reducers / len(stats.ip_counts_per_reducer)
            max_count = max(stats.ip_counts_per_reducer.values())
            min_count = min(stats.ip_counts_per_reducer.values())
            logger.info("")
            logger.info("  [IP Distribution per Reducer]")
            for rid in sorted(stats.ip_counts_per_reducer.keys()):
                cnt = stats.ip_counts_per_reducer[rid]
                pct = cnt / total_in_reducers * 100 if total_in_reducers > 0 else 0
                logger.info(
                    f"    Reducer[{rid}]: {cnt:>8,} IPs ({pct:5.1f}%)"
                )
            if len(stats.ip_counts_per_reducer) > 1:
                skew_factor = (max_count - min_count) / avg_per_reducer if avg_per_reducer > 0 else 0
                logger.info(
                    f"    {'─'*40}"
                )
                logger.info(
                    f"    Skew Factor: {skew_factor:.4f} "
                    f"(max={max_count:,} min={min_count:,} avg={avg_per_reducer:,.0f})"
                )

        # Work Stealing status
        ws_str = (f"YES — {work_steal_moved:,} items moved in {work_steal_iters} iters"
                  if work_steal_triggered else "Not triggered (skew <= 50%)")
        ws_color = ">>>" if work_steal_triggered else "   "
        logger.info("")
        logger.info(f"  {ws_color} Work Stealing:    {ws_str}")

        # Timing breakdown
        logger.info("")
        logger.info("  [Timing Breakdown]")
        logger.info(f"    Phase B (reduce+transform): {stats.phase_b_time_s:>10.3f}s")
        logger.info(f"      └─ dedup:                 {stats.dedup_time_s:>10.3f}s")
        logger.info(f"      └─ transform:             {stats.transform_time_s:>10.3f}s")
        logger.info(f"    Merge (Phase C):            {stats.merge_duration_s:>10.3f}s")
        ft_str = (f"DETECTED={stats.ft_failures_detected} | "
                  f"RECOVERED={stats.ft_failures_recovered} | "
                  f"RETRIES={stats.ft_retries_attempted} | "
                  f"SEQUENTIAL_FALLBACK={'YES' if stats.ft_sequential_fallback else 'No'}")
        logger.info(f"  FaultTolerance:               {ft_str}")
        logger.info(f"  Total time:                  {stats.duration_s:>10.3f}s")
        logger.info(f"  Throughput (rows/s):         {stats.throughput_rows_per_sec:>10,.0f} rows/s")
        logger.info(f"  Throughput (IPs/s):         {stats.throughput_ips_per_sec:>10,.0f} IPs/s")
        logger.info("=" * 65)

        if self._memory_monitor:
            self._memory_monitor.log_phase("Pipeline_End")
            self._memory_monitor.print_summary()

        self._cleanup()
        return stats

    # ── Phase A ───────────────────────────────────────────────────

    def _phase_a_extract_shuffle(self):
        partitions: dict[int, list[tuple[str, int]]] = {i: [] for i in range(self.n_reducers)}

        def extract_and_partition(site_id: int) -> tuple[int, dict[int, list[tuple[str, int]]], ExtractorResult]:
            extractor = Extractor(site_id, self.log_files[site_id], self.n_reducers)
            result = extractor.extract()
            parts: dict[int, list[tuple[str, int]]] = {i: [] for i in range(self.n_reducers)}
            for ip, sid in result.ip_list:
                rid = self._partition_key(ip)
                parts.setdefault(rid, []).append((ip, sid))
            return site_id, parts, result

        self._do_extract_shuffle(partitions, extract_and_partition)
        return partitions, self._extract_stats

    def _do_extract_shuffle(self, partitions, task_fn):
        from concurrent.futures import ThreadPoolExecutor, as_completed

        results: list = []
        with ThreadPoolExecutor(max_workers=self.n_workers or len(self.log_files)) as executor:
            futures = {
                executor.submit(task_fn, site_id): site_id
                for site_id in self.log_files.keys()
            }
            for future in as_completed(futures):
                try:
                    results.append(future.result())
                except Exception as e:
                    site_id = futures[future]
                    logger.error(f"  Extractor[{site_id}] failed: {e}")

        extract_stats: list[ExtractorResult] = []
        for site_id, parts, result in results:
            for rid, tuples in parts.items():
                partitions[rid].extend(tuples)
            extract_stats.append(result)

        self._extract_stats = extract_stats
        return extract_stats

    # ── Phase B — MULTIPROCESSING WITH FAULT TOLERANCE ─────────────────

    def _phase_b_reduce_transform_load(
        self,
        partitions: dict[int, list[tuple[str, int]]],
    ) -> list[ReducerResult]:
        """
        Phase B: Reduce + Transform + Load (multiprocessing + fault tolerance).

        Fault tolerance strategy (Özsu & Valduriez):
          1. Orphan cleanup: xóa intermediate files cũ ở đầu pipeline
          2. Process timeout: mỗi reducer có giới hạn thời gian
          3. Partial retry: chỉ retry reducer bị chết, không retry tất cả
          4. Sequential fallback: nếu multiprocessing fails hoàn toàn
        """
        import multiprocessing as mp
        from concurrent.futures import ProcessPoolExecutor, as_completed, TimeoutError

        self._intermediate_stores = [
            IntermediateStore(rid) for rid in range(self.n_reducers)
        ]

        n_workers = min(self.n_workers or self.n_reducers, self.n_reducers)
        timeout = self._fault_tolerance.timeout  # seconds per reducer

        logger.info("")
        logger.info("#" * 60)
        logger.info(f"# PHASE B: Reduce + Transform ({n_workers} workers, timeout={timeout}s)")
        logger.info("#" * 60)

        def do_reduce() -> list[ReducerResult]:
            results: list[ReducerResult] = []
            all_rids = list(range(self.n_reducers))
            failed_rids: set[int] = set()

            # Attempt 1: try multiprocessing
            ctx = mp.get_context("spawn")
            task_args = [
                (rid, partitions.get(rid, []), self.geodb_path)
                for rid in all_rids
            ]

            try:
                with ProcessPoolExecutor(max_workers=n_workers, mp_context=ctx) as executor:
                    futures = {
                        executor.submit(_run_reducer_task, arg): arg[0]
                        for arg in task_args
                    }
                    for future in as_completed(futures):
                        rid = futures[future]
                        try:
                            # Wait with timeout per reducer
                            result = future.result(timeout=timeout)
                            self._intermediate_stores[rid].write(result.records)
                            self._intermediate_stores[rid].flush()
                            results.append(result)
                        except TimeoutError:
                            logger.error(
                                f"  FaultTolerance: Reducer[{rid}] TIMEOUT after {timeout}s"
                            )
                            failure = NodeFailure(
                                node_id=rid, node_type="reducer",
                                reason=f"timeout_after_{timeout}s",
                                timestamp=time.time(), attempt=1,
                            )
                            self._fault_tolerance.stats.add_failure(failure)
                            failed_rids.add(rid)
                        except Exception as e:
                            logger.error(
                                f"  FaultTolerance: Reducer[{rid}] DIED: {type(e).__name__}: {e}"
                            )
                            failure = NodeFailure(
                                node_id=rid, node_type="reducer",
                                reason=f"{type(e).__name__}: {e}",
                                timestamp=time.time(), attempt=1,
                            )
                            self._fault_tolerance.stats.add_failure(failure)
                            failed_rids.add(rid)

            except Exception as e:
                logger.error(f"  FaultTolerance: ProcessPoolExecutor itself FAILED: {e}")
                logger.warning("  Falling back to sequential reducers...")
                failed_rids = set(all_rids)  # retry all

            # Attempt 2: retry failed reducers (partial retry)
            if failed_rids:
                logger.info(f"  FaultTolerance: retrying {len(failed_rids)} failed reducers...")
                for rid in failed_rids:
                    success, result = self._fault_tolerance.retry_failed_node(
                        node_id=rid,
                        node_type="reducer",
                        task_fn=self._run_single_reducer_sync,
                        rid=rid,
                        ip_tuples=partitions.get(rid, []),
                    )
                    if success:
                        self._intermediate_stores[rid].write(result.records)
                        self._intermediate_stores[rid].flush()
                        results.append(result)
                    else:
                        # Last resort: sequential fallback
                        logger.warning(
                            f"  FaultTolerance: Reducer[{rid}] retry failed — "
                            f"using sequential fallback"
                        )
                        success, result = self._fault_tolerance.retry_failed_node(
                            node_id=rid,
                            node_type="reducer",
                            task_fn=self._run_single_reducer_sync,
                            rid=rid,
                            ip_tuples=partitions.get(rid, []),
                        )
                        if success:
                            self._intermediate_stores[rid].write(result.records)
                            self._intermediate_stores[rid].flush()
                            results.append(result)
                        else:
                            logger.error(
                                f"  FaultTolerance: Reducer[{rid}] FAILED after all retries — SKIPPING"
                            )

            results.sort(key=lambda r: r.reducer_id)
            return results

        self._reduce_stats = do_reduce()
        return self._reduce_stats

    def _run_single_reducer_sync(
        self,
        rid: int,
        ip_tuples: list[tuple[str, int]],
    ) -> ReducerResult:
        """Chạy một reducer trong main process (sequential fallback)."""
        geo = GeoLookup(self.geodb_path)
        reducer = Reducer(rid, geo)
        result = reducer.run(ip_tuples)
        geo.close()
        return result
