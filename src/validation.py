"""
Correctness Validation module.
PHẦN 8.2 — Verifies de-duplication accuracy by comparing pipeline
output against a ground-truth baseline count.

Supports both "log" (4 .log files) and "csv" (apache_550k_unique_ips.csv) modes.
"""
import csv
import json
import logging
import re
from pathlib import Path
from typing import Optional

from .config import APACHE_REGEX, LOG_FILES, CSV_SITE_MAPPING, DATA_SOURCE_MODE, CSV_9K_UNIFIED

# Ground truth: unified CSVs
CSV_GROUND_TRUTH_PATH = r"c:\Users\Admin\CSDLPT_final\data\output\apache_550k_unique_ips.csv"
CSV9K_GROUND_TRUTH_PATH = CSV_9K_UNIFIED

logger = logging.getLogger("etl.validation")

IP_PATTERN = re.compile(APACHE_REGEX)


class CorrectnessValidator:
    """
    Validates that the pipeline's de-duplication is 100% accurate.
    Compares:
      - Baseline: IP count from raw data source
      - Pipeline: IP count from final JSON output
    """

    def __init__(
        self,
        log_files: dict[int, str],
        output_path: str,
        data_source: Optional[str] = None,
    ):
        self.log_files = log_files
        self.output_path = output_path
        self.data_source = data_source or DATA_SOURCE_MODE
        self.baseline_ips: set[str] = set()
        self.pipeline_ips: set[str] = set()

    def count_baseline(self) -> set[str]:
        """
        Count unique IPs across ALL data source (ground truth).
        This is the authoritative count of distinct IPs in the dataset.
        """
        logger.info("  Validation: counting baseline from raw data source...")

        if self.data_source == "csv9k":
            return self._count_baseline_csv(ground_truth=CSV9K_GROUND_TRUTH_PATH, label="csv9k")
        elif self.data_source == "csv":
            return self._count_baseline_csv(ground_truth=CSV_GROUND_TRUTH_PATH, label="csv")
        else:
            return self._count_baseline_log()

    def _count_baseline_log(self) -> set[str]:
        """Count IPs from 4 Apache .log files."""
        self.baseline_ips = set()

        for site_id, file_path in self.log_files.items():
            p = Path(file_path)
            if not p.exists():
                logger.warning(f"    Log file not found: {file_path}")
                continue

            site_count = 0
            with open(p, "r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    match = IP_PATTERN.search(line)
                    if match:
                        self.baseline_ips.add(match.group(1))
                        site_count += 1

            logger.info(f"    Site {site_id}: read {site_count} lines from {p.name}")

        logger.info(f"  Baseline (log mode): {len(self.baseline_ips)} unique IPs")
        return self.baseline_ips

    def _count_baseline_csv(
        self,
        ground_truth: Optional[str] = None,
        label: str = "csv",
    ) -> set[str]:
        """Count unique IPs from a unified CSV file."""
        self.baseline_ips = set()
        csv_path = Path(ground_truth or CSV_GROUND_TRUTH_PATH)

        if not csv_path.exists():
            logger.warning(f"    CSV file not found: {csv_path}")
            return self.baseline_ips

        with open(csv_path, "r", encoding="utf-8", errors="replace", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                line = row.get("raw_line", "")
                match = IP_PATTERN.search(line)
                if match:
                    self.baseline_ips.add(match.group(1))

        logger.info(f"  Baseline ({label} mode): {len(self.baseline_ips)} unique IPs from {csv_path.name}")
        return self.baseline_ips

    def count_pipeline(self) -> set[str]:
        """Count unique IPs from the pipeline's JSON output."""
        logger.info("  Validation: counting IPs from pipeline output...")
        self.pipeline_ips = set()

        p = Path(self.output_path)
        if not p.exists():
            logger.error(f"    Pipeline output not found: {self.output_path}")
            return self.pipeline_ips

        with open(p, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError:
                logger.error("    Invalid JSON in pipeline output")
                return self.pipeline_ips

        if isinstance(data, dict):
            self.pipeline_ips = set(data.keys())
        elif isinstance(data, list):
            self.pipeline_ips = {rec.get("ip") for rec in data if rec.get("ip")}

        logger.info(f"  Pipeline: {len(self.pipeline_ips)} unique IPs in output")
        return self.pipeline_ips

    def validate(self) -> bool:
        """
        Run full validation: baseline vs pipeline.
        Returns True if counts match exactly (PASS), False otherwise (FAIL).
        """
        baseline = self.count_baseline()
        pipeline = self.count_pipeline()

        baseline_count = len(baseline)
        pipeline_count = len(pipeline)

        missing = baseline - pipeline
        extra = pipeline - baseline

        logger.info("")
        logger.info("=" * 60)
        logger.info("CORRECTNESS VALIDATION REPORT")
        logger.info("=" * 60)
        logger.info(f"  Baseline (raw {self.data_source}):      {baseline_count:>8} IPs")
        logger.info(f"  Pipeline (deduplicated):               {pipeline_count:>8} IPs")
        logger.info(f"  Missing (in baseline not pipeline):    {len(missing):>8}")
        logger.info(f"  Extra   (in pipeline not baseline):    {len(extra):>8}")

        if baseline_count == pipeline_count and len(missing) == 0 and len(extra) == 0:
            logger.info("")
            logger.info(f"  ✓ VALIDATION PASS: {baseline_count} IPs — De-duplication is 100% accurate")
            logger.info("=" * 60)
            return True
        else:
            logger.warning("")
            logger.warning(f"  ✗ VALIDATION FAIL")
            if missing:
                sample_missing = sorted(list(missing))[:10]
                logger.warning(f"    Missing IPs: {sample_missing}")
            if extra:
                sample_extra = sorted(list(extra))[:10]
                logger.warning(f"    Extra IPs:   {sample_extra}")
            logger.warning("=" * 60)
            return False
