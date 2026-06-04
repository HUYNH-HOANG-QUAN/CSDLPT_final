"""
Reducer: Phase 3 (Reduce/Global Dedup) + Phase 4 (Transform).
PHAN 4 & PHAN 5 - Global de-duplication and Geo-lookup.

Implements BATCHED DEDUP with DEDUP_CHUNK_SIZE:
  - Chunks of IP tuples are processed separately
  - Duplicates within a chunk are detected immediately
  - Chunk-level stats: hits, savings, flush points
  - Two-phase dedup:
      Phase 1 (Local in Extractor):  dedup within ONE data file/site
      Phase 2 (Global in Reducer):   dedup ACROSS all shuffled partitions
"""
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional

from .config import DEDUP_CHUNK_SIZE
from .geo_lookup import GeoLookup, GeoRecord

logger = logging.getLogger("etl.reducer")


@dataclass
class ReducerResult:
    """Result from a Reducer run."""
    reducer_id: int
    ip_count_received: int
    ip_count_unique: int
    ip_count_skipped_dupe: int
    ip_count_local_dup: int       # dup skipped at LOCAL dedup phase
    ip_count_global_dup: int      # dup skipped at GLOBAL dedup phase
    n_chunks: int                 # how many chunks processed
    chunk_hit_rates: list[float]  # dedup hit rate per chunk
    duration_s: float
    duration_dedup_s: float
    duration_transform_s: float
    records: list[dict]
    sites_visited: set[int] = field(default_factory=set)


class Reducer:
    """
    Performs GLOBAL de-duplication in CHUNKS and geo-lookup.

    Deduplication happens in TWO phases (O&V Ch.8):
      1. LOCAL dedup  (in Extractor):  dedup within ONE site file
      2. GLOBAL dedup (in Reducer):    dedup ACROSS all shuffled partitions

    Chunked processing (DEDUP_CHUNK_SIZE):
      - Process tuples in batches of CHUNK_SIZE rows
      - Each chunk: check set -> mark duplicates -> add new to set
      - Track chunk-level dedup hit rate = dup_rows / total_rows_in_chunk
      - Flush stats after each chunk to avoid unbounded growth

    Key insight: geo_lookup happens AFTER dedup — no wasted API calls.
    """

    def __init__(
        self,
        reducer_id: int,
        geo_lookup: Optional[GeoLookup] = None,
        chunk_size: int = DEDUP_CHUNK_SIZE,
    ):
        self.reducer_id = reducer_id
        self.geo = geo_lookup or GeoLookup()
        self.chunk_size = chunk_size
        self._reset()

    def _reset(self):
        self.ip_seen: set[str] = set()
        self.ip_count_received = 0
        self.ip_count_unique = 0
        self.ip_count_skipped_dupe = 0
        self.ip_count_local_dup = 0   # set during receive_chunks
        self.ip_count_global_dup = 0  # set during receive_chunks
        self.records: list[dict] = []
        self.sites_visited: set[int] = set()
        self.n_chunks = 0
        self.chunk_hit_rates: list[float] = []
        self._duration_dedup = 0.0
        self._duration_transform = 0.0

    def receive_chunks(
        self,
        ip_tuples: list[tuple[str, int]],
    ) -> tuple[int, int, int, int, int, list[float]]:
        """
        Receive IP tuples in CHUNKS and perform GLOBAL dedup.

        Algorithm per chunk:
          1. Collect up to CHUNK_SIZE tuples
          2. For each tuple:
               - Already in ip_seen? -> skip (global dup)
               - Not seen?  -> add to ip_seen (unique)
          3. Record chunk-level hit rate
          4. Repeat until all tuples processed

        Returns: (received, unique, global_dup, local_dup, n_chunks, hit_rates)
        """
        n = len(ip_tuples)
        n_chunks = max(1, (n + self.chunk_size - 1) // self.chunk_size)
        local_dup = 0
        global_dup = 0
        unique = 0
        hit_rates = []

        chunk_dedup_start = time.perf_counter()

        for chunk_idx in range(n_chunks):
            start = chunk_idx * self.chunk_size
            end = min(start + self.chunk_size, n)
            chunk = ip_tuples[start:end]

            chunk_seen_local: set[str] = set()
            chunk_unique = 0
            chunk_global_dup = 0

            for ip, site_id in chunk:
                self.ip_count_received += 1
                self.sites_visited.add(site_id)

                # Local dedup: duplicate within this chunk
                if ip in chunk_seen_local:
                    local_dup += 1
                    chunk_local_dup = 1
                    # still don't count as global dup
                else:
                    chunk_seen_local.add(ip)

                # Global dedup: first time across ALL chunks
                if ip in self.ip_seen:
                    global_dup += 1
                    chunk_global_dup += 1
                else:
                    self.ip_seen.add(ip)
                    unique += 1

            # Chunk hit rate = duplicates / total in chunk
            chunk_size_actual = len(chunk)
            hit_rate = chunk_global_dup / chunk_size_actual if chunk_size_actual > 0 else 0.0
            hit_rates.append(round(hit_rate, 4))

            if chunk_idx == 0:
                # Log every 10 chunks or when hit rate is interesting
                pass

        self._duration_dedup = time.perf_counter() - chunk_dedup_start
        self.ip_count_unique = unique
        self.ip_count_global_dup = global_dup
        self.ip_count_local_dup = local_dup
        self.ip_count_skipped_dupe = global_dup
        self.n_chunks = n_chunks
        self.chunk_hit_rates = hit_rates

        return (
            self.ip_count_received,
            unique,
            global_dup,
            local_dup,
            n_chunks,
            hit_rates,
        )

    def receive(self, ip_tuples: list[tuple[str, int]]):
        """
        OLD API (backward compatible): receive all tuples at once.
        Internally calls receive_chunks with DEDUP_CHUNK_SIZE batching.
        """
        self.receive_chunks(ip_tuples)

    def transform(self) -> list[dict]:
        """Transform unique IPs into geo-located records."""
        transform_start = time.perf_counter()
        results: list[dict] = []
        for ip in self.ip_seen:
            geo_record = self.geo.lookup(ip)
            record = geo_record.to_dict()
            record["source_sites"] = sorted(list(self.sites_visited))
            results.append(record)
        self._duration_transform = time.perf_counter() - transform_start
        return results

    def run(self, ip_tuples: list[tuple[str, int]]) -> ReducerResult:
        """
        Full reduce + transform pipeline.
        Tracks timing: dedup vs transform separately.
        """
        self._reset()
        start = time.perf_counter()

        self.receive_chunks(ip_tuples)
        self.records = self.transform()

        total_dedup_s = self._duration_dedup
        total_transform_s = self._duration_transform
        duration = time.perf_counter() - start

        # Compute interesting chunk stats
        if self.chunk_hit_rates:
            avg_hit = sum(self.chunk_hit_rates) / len(self.chunk_hit_rates)
            max_hit = max(self.chunk_hit_rates)
            min_hit = min(self.chunk_hit_rates)
            chunk_stats = f"chunks={self.n_chunks}, avg_dup={avg_hit*100:.1f}%, max={max_hit*100:.1f}%"
        else:
            chunk_stats = f"chunks={self.n_chunks}"

        logger.info(
            f"  Reducer[{self.reducer_id}]: "
            f"recv={self.ip_count_received:,} IPs, "
            f"unique={self.ip_count_unique:,}, "
            f"global_dup={self.ip_count_global_dup:,}, "
            f"local_dup={self.ip_count_local_dup:,}, "
            f"{chunk_stats}, "
            f"dedup={total_dedup_s:.3f}s, transform={total_transform_s:.3f}s"
        )

        return ReducerResult(
            reducer_id=self.reducer_id,
            ip_count_received=self.ip_count_received,
            ip_count_unique=self.ip_count_unique,
            ip_count_skipped_dupe=self.ip_count_global_dup,
            ip_count_local_dup=self.ip_count_local_dup,
            ip_count_global_dup=self.ip_count_global_dup,
            n_chunks=self.n_chunks,
            chunk_hit_rates=self.chunk_hit_rates,
            duration_s=duration,
            duration_dedup_s=total_dedup_s,
            duration_transform_s=total_transform_s,
            records=self.records,
            sites_visited=self.sites_visited,
        )
