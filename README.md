# Distributed ETL Pipeline

**Distributed ETL Pipeline with Fault Tolerance — CSDLPT**

*SVTH:* Hồ Hoàng Quân — work.huynhhoangquan@gmail.com  
*GVHD:* Lê Hà Thanh  
*Môn:* Cơ Sở Dữ Liệu Phân Tán

Xây dựng quy trình **ETL phân tán** (Extract-Transform-Load) xử lý dữ liệu log Apache từ 4 sites (portal, news, shop, api) — 578,169 rows — với fault tolerance và benchmark theo lý thuyết **Özsu & Valduriez**.

---

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Run pipeline (recommended: 4 reducers)
python main.py -r 4 --validate

# 3. Benchmark speedup
python main.py --benchmark speedup
```

---

## Benchmark Results

### Throughput — Peak 386K rows/s (3 reducers)

| N Reducers | Time (s) | Throughput       | Speedup |
| ---------- | -------- | ---------------- | ------- |
| 1          | 4.0      | 145,042 rows/s   | 1.00x   |
| 2          | 2.5      | 231,268 rows/s   | 1.59x   |
| **3**      | **1.5**  | **385,446 rows/s** | **2.66x** |
| 4          | 1.5      | 393,778 rows/s   | 2.71x   |
| 6          | 1.7      | 343,000 rows/s   | 2.36x   |

> Peak speedup: **2.97x** (4 reducers vs baseline). Validation: **100% accurate** — 721 pipeline IPs match baseline.

### Fault Tolerance

| Scenario         | DETECTED | RECOVERED | RETRIES |
| ---------------- | -------- | --------- | -------- |
| Normal run       | 0        | 0         | 0        |
| Kill 1 reducer   | 1        | 1         | 1        |

---

## Features

| Feature                       | Description                                        | O&V Ref |
| ----------------------------- | -------------------------------------------------- | ------- |
| **Stateless Coordinator**      | Restartable phases, no shared state                | Ch. 9   |
| **Hash Partitioning**          | `hash(IP) % N` — deterministic routing             | Ch. 3   |
| **Work Stealing**              | Dynamic load balancing when skew > 50%             | Ch. 8   |
| **Global Deduplication**       | Coordinator-based dedup: 1,035 → 721 unique IPs  | Ch. 8   |
| **Fault Tolerance**            | Orphan cleanup + partial retry + exponential backoff | Ch. 9 |
| **Geo Enrichment**             | MaxMind GeoLite2 city lookup per IP               | —       |

---

## Architecture

```
Phase A (Extract)              Phase B (Reduce)              Phase C (Load)
┌─────────────────┐            ┌─────────────────┐            ┌─────────────────┐
│ Extractor[1]    │ ── IPs ──▶│                 │            │                 │
│ Extractor[2]    │ ── IPs ──▶│   Coordinator   │──partition──▶ Reducer[N] │
│ Extractor[3]    │ ── IPs ──▶│  (hash IP % N)  │            │ (geo transform) │
│ Extractor[4]    │ ── IPs ──▶│                 │            │                 │
└─────────────────┘            └─────────────────┘            └─────────────────┘
                                                                       │
                                                                       ▼
                                                           central_store.json
                                                           (721 unique IPs)
```

**Fault Tolerance Flow:**
```
1. Orphan cleanup → remove stale intermediate files
2. Kill reducer process mid-run
3. DETECTED=1, RECOVERED=1, RETRIES=1
4. Final output matches validation baseline (no data loss)
```

---

## O&V Justification

Thiết kế được justify bằng lý thuyết O&V trên 4 khía cạnh:

| Khía cạnh (O&V)        | Mechanism                  | Đảm bảo                                                    |
| ---------------------- | -------------------------- | ---------------------------------------------------------- |
| **Stateless Coord** (Ch.9) | Restartable phases        | No contention, linear scaling, fault isolation            |
| **Hash Partitioning** (Ch.3) | `hash(IP) % N`          | Data locality, parallel I/O, completeness/disjointness     |
| **Coordinator Dedup**  | Thu thập + merge           | Deterministic routing, global dedup correctness           |
| **Orphan + Retry** (Ch.9) | Exponential backoff     | Recovery không mất dữ liệu, partial retry hiệu quả        |

---

## Dataset

| Site    | Rows     | Unique IPs |
| ------- | -------- | ---------- |
| portal  | 145,947  | 111        |
| news    | 142,676  | 458        |
| shop    | 142,957  | 704        |
| api     | 146,589  | 721        |
| **Total** | **578,169** | **721 (global unique)** |

---

## Project Structure

```
CSDLPT_final/
├── main.py                     # Entry point + CLI
├── requirements.txt
├── src/
│   ├── config.py               # Configuration + OPTIMAL_CONFIG
│   ├── hash_fn.py              # FNV-1a 64-bit hash
│   ├── coordinator.py          # Pipeline orchestration (Phase A/B/C)
│   ├── extractor.py            # Extract + local dedup per site
│   ├── reducer.py              # Transform (GeoIP) + local dedup
│   ├── fault_tolerance.py      # Orphan cleanup + retry
│   ├── geo_lookup.py           # MaxMind GeoLite2 integration
│   ├── validator.py            # Output correctness validation
│   └── benchmark.py            # Speedup / throughput benchmarks
├── data/
│   ├── output/
│   │   ├── central_store.json  # Final merged output
│   │   └── reducers/           # Intermediate partitioned files
│   └── GeoLite2-City.mmdb
├── notebooks/                   # Benchmark visualizations
└── scripts/
    ├── generate_skew_dataset.py # Skew dataset generator
    └── create_word_docs.py       # Report document generator
```

---

## Usage

```bash
# Basic run
python main.py -r 4

# With validation
python main.py -r 4 --validate

# Benchmark speedup
python main.py --benchmark speedup

# Different partitioning
python main.py --partition hash -r 4
python main.py --partition range -r 4
```

---

## Evaluation Metrics

| Metric          | Value / Formula              |
| --------------- | ----------------------------- |
| **Throughput**  | 385,446 rows/s (3 reducers)  |
| **Speedup**     | 2.97x (1 → 4 reducers)       |
| **Skew Factor** | 0.0666 (nearly uniform)      |
| **Validation**  | 100% accurate (721/721 IPs)  |
| **Fault Tolerance** | Orphan cleanup + retry     |

---

## Deliverables

| Deliverable                    | File                  | Status |
| ------------------------------ | --------------------- | ------ |
| Project Proposal               | `Project_Proposal.md` | ✅     |
| Design Document (2 pages)       | `Design_Document.md`  | ✅     |
| Analysis Report (O&V)          | `Analysis_Report.md` | ✅     |
| Run Demo Guide                 | `RUN_DEMO.md`         | ✅     |
| Code Repository                | GitHub                | ✅     |

---

*Tham khảo: Özsu & Valduriez — Principles of Distributed Database Systems, 4th Edition, Springer 2020*
