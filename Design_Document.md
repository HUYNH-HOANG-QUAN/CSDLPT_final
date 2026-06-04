# Design Document — Distributed ETL Pipeline cho Hệ Thống Giám Sát An Ninh Mạng

## 1. Tổng quan

Hệ thống triển khai quy trình **ETL phân tán** (Extract-Transform-Load) xử lý log Apache từ 4 cổng (portal, news, shop, api), kết hợp GeoIP lookup và deduplication.

| Thành phần | Chi tiết |
|------------|----------|
| **Dataset** | 550K rows, 100% unique IPs |
| **Output** | `central_store.json` — IP + geo info |
| **Mục tiêu** | Hiệu suất cao, fault tolerance, validation 100% |

---

## 2. Kiến trúc Shared-Nothing

Mô hình **Shared-Nothing** (O&V Ch.2): mỗi node có bộ nhớ và storage riêng, giao tiếp qua message-passing.

```
┌─────────────┐     ┌───────────────────────────────────────────────────────┐
│  Phase A    │     │                        Phase B                        │
│  Extract    │     │  Reducer[0]    Reducer[1]    Reducer[2]  Reducer[3] │
│  + Shuffle  │     │    (137K)        (137K)        (137K)       (137K)  │
└─────────────┘     └───────────────────────────────────────────────────────┘
                                                                 Phase C
                                                              central_store.json
```

**Horizontal Fragmentation (O&V Ch.3):** Data phân mảnh theo site → mỗi Extractor xử lý 1 file cục bộ.

---

## 3. Chiến lược phân vùng

### 3.1 Hash Partitioning (Mặc định) — O&V Ch.8

```python
reducer_id = fnv1a_64(ip) % n_reducers
```

- FNV-1a 64-bit: deterministic, không cần load-balancing server
- Cùng IP → cùng reducer → idempotent

### 3.2 Range Partitioning + Sampling

1. Reservoir sampling 10,000 IPs → build histogram
2. Percentile boundaries → equal count mỗi reducer
3. Binary search O(log N) lookup per IP
4. **Nhược điểm:** overhead ~5s → không recommended cho dataset hiện tại

### 3.3 Adaptive Work Stealing — O&V Ch.8

Khi `skew_factor > 0.5`:
- Hot reducer sort IPs → chia đôi
- Prefix → hot reducer, suffix → cold reducer
- Lặp cho đến khi skew ≤ threshold

---

## 4. Fault Tolerance — O&V Ch.9

| Bước | Cơ chế | Chi tiết |
|------|--------|----------|
| 1 | Orphan cleanup | Xóa intermediate files từ lần chạy trước |
| 2 | Process timeout | Mỗi reducer có timeout 120s |
| 3 | Partial retry | Chỉ retry reducer bị chết (không retry tất cả) |
| 4 | Exponential backoff | Retry: 1.5s, 2.25s, 3.375s... |
| 5 | Sequential fallback | Multiprocessing fails → chạy tuần tự |

**Idempotent design:** Mỗi reducer ghi file riêng → atomic rename (`.tmp` → `.json`)

---

## 5. Benchmark Results

| Metric | Value | Ghi chú |
|--------|-------|---------|
| **Throughput** | 10,168 rows/s | N=6 reducers |
| **Speedup** | 1.36x | vs baseline N=1 |
| **Skew Factor** | 0.0047 | Hash partitioning |
| **Validation** | 100% PASS | 550K IPs |

| N Reducers | Time (s) | Throughput | Speedup |
|-----------|----------|------------|---------|
| 1 | 73.76 | 7,456 | 1.00x |
| 4 | 74.24 | 7,408 | 0.99x |
| **6** | **54.09** | **10,168** | **1.36x** |

---

## 6. Tech Stack

| Thành phần | Công nghệ |
|------------|-----------|
| Language | Python 3.10+ |
| GeoIP | MaxMind GeoLite2 (.mmdb) |
| Concurrency | concurrent.futures (ProcessPoolExecutor) |
| Memory | psutil monitoring |
| Storage | CSV + JSON |

---

*Design based on: Özsu & Valduriez — Principles of Distributed Database Systems, 4th Ed.*
*Chapters: Ch.2 (Architecture), Ch.3 (Data Distribution), Ch.8 (Query Processing), Ch.9 (Fault Tolerance)*
