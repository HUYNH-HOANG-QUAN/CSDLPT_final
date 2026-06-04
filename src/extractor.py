"""
Extractor: Phase 1 (Extract) + Local Dedup.
PHẦN 2 — Runs at each data source node (Data Locality).
Each Extractor reads rows from its data source and performs local de-duplication
before sending unique IPs to the shuffle phase.

Supports two data source modes (configured in config.py):
  - "log"  — reads Apache .log files directly
  - "csv"  — reads apache_550k_unique_ips.csv, filtering by site column
"""
import csv
import logging
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from .config import APACHE_REGEX, DATA_SOURCE_MODE
from .hash_fn import partition_key

logger = logging.getLogger("etl.extractor")

IP_PATTERN = re.compile(APACHE_REGEX)


@dataclass
class ExtractorResult:
    """Result from an Extractor run."""
    site_id: int
    file_path: str
    rows_read: int
    ip_count_local: int
    duration_s: float
    ip_list: list[tuple[str, int]]  # (ip, site_id)


class Extractor:
    """
    Extracts IPs from a data source and performs local de-duplication.
    Implements Data Locality (O&V): processing happens at the data source.
    """

    def __init__(self, site_id: int, file_path: str, n_reducers: int):
        self.site_id = site_id
        self.file_path = file_path
        self.n_reducers = n_reducers
        self.rows_read = 0
        self.ip_count_local = 0

    def extract(self) -> ExtractorResult:
        """
        Read the data source line by line, extract IPs, perform local dedup.
        Returns a structured result with timing info and the list of unique (IP, site_id).
        """
        mode = DATA_SOURCE_MODE
        if mode == "csv" or self.file_path.endswith(".csv"):
            return self._extract_csv()
        else:
            return self._extract_log()

    def _extract_log(self) -> ExtractorResult:
        """Read Apache .log file line by line."""
        start = time.perf_counter()
        seen_local: set[str] = set()
        ip_list: list[tuple[str, int]] = []

        file_path = Path(self.file_path)
        if not file_path.exists():
            logger.warning(f"  Extractor[{self.site_id}]: File not found: {self.file_path}")
            return ExtractorResult(
                site_id=self.site_id,
                file_path=self.file_path,
                rows_read=0,
                ip_count_local=0,
                duration_s=0.0,
                ip_list=[],
            )

        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                self.rows_read += 1
                match = IP_PATTERN.search(line)
                if not match:
                    continue

                ip = match.group(1)

                if ip in seen_local:
                    continue

                seen_local.add(ip)
                self.ip_count_local += 1
                ip_list.append((ip, self.site_id))

        duration = time.perf_counter() - start
        logger.info(
            f"  Extractor[{self.site_id}]: read {self.rows_read} rows, "
            f"found {self.ip_count_local} unique IPs in {duration:.3f}s"
        )

        return ExtractorResult(
            site_id=self.site_id,
            file_path=self.file_path,
            rows_read=self.rows_read,
            ip_count_local=self.ip_count_local,
            duration_s=duration,
            ip_list=ip_list,
        )

    def _extract_csv(self) -> ExtractorResult:
        """
        Read pre-split CSV file (one site per file).
        CSV format: ip,site,raw_line
        """
        start = time.perf_counter()
        seen_local: set[str] = set()
        ip_list: list[tuple[str, int]] = []

        csv_path = Path(self.file_path)
        if not csv_path.exists():
            logger.warning(f"  Extractor[{self.site_id}]: CSV not found: {self.file_path}")
            return ExtractorResult(
                site_id=self.site_id,
                file_path=self.file_path,
                rows_read=0,
                ip_count_local=0,
                duration_s=0.0,
                ip_list=[],
            )

        with open(csv_path, "r", encoding="utf-8", errors="replace", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                self.rows_read += 1
                # IP nằm trong cột "ip" (pre-split, không cần regex)
                ip = row.get("ip", "").strip()
                if not ip:
                    continue

                if ip in seen_local:
                    continue

                seen_local.add(ip)
                self.ip_count_local += 1
                ip_list.append((ip, self.site_id))

        duration = time.perf_counter() - start
        logger.info(
            f"  Extractor[{self.site_id}](csv): read {self.rows_read} rows, "
            f"found {self.ip_count_local} unique IPs in {duration:.3f}s"
        )

        return ExtractorResult(
            site_id=self.site_id,
            file_path=self.file_path,
            rows_read=self.rows_read,
            ip_count_local=self.ip_count_local,
            duration_s=duration,
            ip_list=ip_list,
        )

    def extract_to_partitions(self) -> dict[int, list[tuple[str, int]]]:
        """
        Extract and immediately partition IPs to reducers.
        Simulates the Shuffle phase where each IP is routed to its reducer.
        Returns: {reducer_id: [(ip, site_id), ...]}
        """
        result = self.extract()
        partitions: dict[int, list[tuple[str, int]]] = {i: [] for i in range(self.n_reducers)}

        for ip, site_id in result.ip_list:
            reducer_id = partition_key(ip, self.n_reducers)
            partitions[reducer_id].append((ip, site_id))

        return partitions
