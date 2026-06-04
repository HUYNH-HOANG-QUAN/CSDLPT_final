"""
Load phase: Write to intermediate files + final merged JSON store.
PHẦN 6 — Idempotent Write for safe concurrent writes + merge strategy.

Architecture (4 reducers):
  Reducer[0] → reducer_0.json  ─┐
  Reducer[1] → reducer_1.json  ──┼──→ merge() → central_store.json
  Reducer[2] → reducer_2.json  ──┤
  Reducer[3] → reducer_3.json  ─┘

Each reducer writes its own intermediate file (no contention).
Coordinator calls merge() once after all reducers finish.
"""
import json
import logging
import threading
import time
from pathlib import Path
from typing import Optional

from .config import INTERMEDIATE_DIR, INTERMEDIATE_FILE_PATTERN

logger = logging.getLogger("etl.load")


# ─────────────────────────────────────────────────────────────────
# IntermediateStore — một reducer ghi một file trung gian
# ─────────────────────────────────────────────────────────────────

class IntermediateStore:
    """
    Mỗi Reducer dùng một IntermediateStore riêng để ghi file trung gian.
    Không có contention — mỗi reducer ghi file riêng của nó.
    """

    def __init__(self, reducer_id: int, clear: bool = True):
        self.reducer_id = reducer_id
        self.file_path = INTERMEDIATE_DIR / INTERMEDIATE_FILE_PATTERN.format(rid=reducer_id)
        self._records: list[dict] = []
        # Clear stale intermediate file from previous runs
        if clear and self.file_path.exists():
            self.file_path.unlink()

    def write(self, records: list[dict]):
        """Ghi records vào bộ nhớ đệm (chưa xuống disk)."""
        self._records.extend(records)

    def flush(self):
        """Xuất toàn bộ records xuống file JSON một lần."""
        INTERMEDIATE_DIR.mkdir(parents=True, exist_ok=True)
        temp_path = self.file_path.with_suffix(".tmp")
        with open(temp_path, "w", encoding="utf-8") as f:
            json.dump(self._records, f, indent=2, ensure_ascii=False)
        temp_path.replace(self.file_path)
        logger.info(
            f"  IntermediateStore[{self.reducer_id}]: "
            f"flushed {len(self._records)} records to {self.file_path.name}"
        )

    def get_count(self) -> int:
        return len(self._records)


# ─────────────────────────────────────────────────────────────────
# JSONStore — đọc N intermediate files, merge, ghi final JSON
# ─────────────────────────────────────────────────────────────────

class JSONStore:
    """
    Central JSON store.
    Chế độ cũ (backward compatible): ghi trực tiếp vào _data dict.
    Chế độ mới (merge mode): đọc intermediate files + ghi final output.
    """

    def __init__(self, output_path: str, use_lock: bool = False):
        self.output_path = Path(output_path)
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self.use_lock = use_lock
        self._lock = threading.Lock()
        self._data: dict[str, dict] = {}
        self._records_written = 0

    # ── Legacy API (backward compatible) ────────────────────────

    def write_idempotent(self, records: list[dict]):
        """Idempotent Write: dùng IP làm key, concurrent writes an toàn."""
        if self.use_lock:
            with self._lock:
                self._write_records(records)
        else:
            self._write_records(records)

    def _write_records(self, records: list[dict]):
        for record in records:
            ip = record.get("ip")
            if ip is None:
                continue
            self._data[ip] = record
            self._records_written += 1
        self._flush()

    def _flush(self):
        """Write current data to disk atomically (temp file + rename)."""
        temp_path = self.output_path.with_suffix(".tmp")
        try:
            with open(temp_path, "w", encoding="utf-8") as f:
                json.dump(self._data, f, indent=2, ensure_ascii=False)
            temp_path.replace(self.output_path)
        except IOError as e:
            logger.error(f"  JSONStore: failed to write: {e}")

    def finalize(self):
        """Explicitly flush all data to disk."""
        self._flush()
        logger.info(
            f"  JSONStore: finalized {len(self._data)} records, "
            f"wrote {self._records_written} entries"
        )

    def get_count(self) -> int:
        return len(self._data)

    # ── Merge API (new) ─────────────────────────────────────────

    def merge_intermediate_files(self, n_reducers: int) -> dict[str, dict]:
        """
        Đọc N intermediate files (reducer_0.json … reducer_{N-1}.json),
        merge vào _data, rồi ghi final output.
        Trả về dict cuối cùng.
        """
        start = time.perf_counter()
        merged_count = 0

        for rid in range(n_reducers):
            fpath = INTERMEDIATE_DIR / INTERMEDIATE_FILE_PATTERN.format(rid=rid)
            if not fpath.exists():
                logger.warning(f"  JSONStore: intermediate file not found: {fpath}")
                continue
            # Validate: skip empty or corrupt files
            if fpath.stat().st_size == 0:
                logger.warning(f"  JSONStore: skipping empty file: {fpath}")
                continue
            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    records = json.load(f)
            except json.JSONDecodeError:
                logger.warning(f"  JSONStore: skipping corrupt JSON file: {fpath}")
                continue

            for record in records:
                ip = record.get("ip")
                if ip is None:
                    continue
                # Idempotent: ghi đè nếu trùng IP (đảm bảo 1 IP → 1 record)
                self._data[ip] = record
                merged_count += 1

        # Ghi final JSON
        self._flush()

        elapsed = time.perf_counter() - start
        logger.info(
            f"  JSONStore: merged {merged_count} records from {n_reducers} "
            f"intermediate files in {elapsed:.3f}s → {self.output_path.name}"
        )
        self._records_written = merged_count
        return self._data

    def get_all_ips(self) -> set[str]:
        return set(self._data.keys())

    def get_all_records(self) -> list[dict]:
        return list(self._data.values())


# ─────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────

def load_idempotent(records: list[dict], output_path: str, use_lock: bool = False):
    """Standalone idempotent load function (legacy)."""
    store = JSONStore(output_path, use_lock=use_lock)
    store.write_idempotent(records)
    store.finalize()
