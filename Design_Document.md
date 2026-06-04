# Design Document — Distributed ETL Pipeline cho Hệ Thống Giám Sát An Ninh Mạng

## 1. Tổng quan hệ thống

Hệ thống triển khai quy trình **ETL phân tán** (Extract-Transform-Load) xử lý dữ liệu log web Apache từ 4 cổng (portal, news, shop, api). Dữ liệu sau xử lý bao gồm thông tin địa lý (GeoIP) của từng IP duy nhất.

**Dataset:** 100MB (578,000 dòng log từ 4 site, 721 unique IPs). Mở rộng: 550K rows, 100% unique IPs.

**Mục tiêu thiết kế:** hiệu suất cao, chịu lỗi (kill node recovery), toàn vẹn dữ liệu (validation 100%).

---

## 2. Kiến trúc hệ thống

### 2.1. Kiến trúc phân tán theo Shared-Nothing (O&V Ch.2)

Hệ thống tuân theo mô hình **Shared-Nothing** — mỗi node có bộ nhớ và storage riêng, giao tiếp qua message-passing. Điều này đảm bảo:
- **Không có contention** trên disk I/O (mỗi reducer ghi file riêng)
- **Khả năng mở rộng tuyến tính** khi thêm reducer
- **Isolation** — lỗi một node không ảnh hưởng các node khác

### 2.2. Các giai đoạn xử lý (Pipeline phases)

```
Phase A: Extract + Shuffle
  [Site CSV] → Extractor[1] ─┐
  [Site CSV] → Extractor[2] ─┼──→ (IP, site_id) tuples
  [Site CSV] → Extractor[3] ─┤     │
  [Site CSV] → Extractor[4] ─┘     ▼
                         hash(IP) % N ──→ Reducer[0..N-1]
                                          │
Phase B: Reduce + Transform              │
  Reducer[i]: global dedup + geo lookup │
                                          ▼
Phase C: Load                 reducer_i.json ──→ Coordinator.merge()
                                                        │
                                             central_store.json
```

### 2.3. Horizontal Fragmentation (O&V Ch.3)

Data được phân mảnh theo chiều ngang theo thuộc tính `site`:
- Fragment 1: portal_access.log
- Fragment 2: news_access.log
- Fragment 3: shop_access.log
- Fragment 4: api_access.log

Mỗi Extractor xử lý đúng 1 fragment. Điều này đảm bảo **data locality** — mỗi node chỉ đọc file cục bộ.

---

## 3. Chiến lược phân vùng dữ liệu

### 3.1. Hash Partitioning (Mặc định) — O&V Ch.8

```python
reducer_id = fnv1a_64(ip) % n_reducers
```

- **FNV-1a 64-bit**: deterministic hash, không cần load-balancing server
- **Đặc tính**: cùng IP luôn đến cùng reducer → deterministic
- **Sweep justification**: Hash tốt hơn Range — không có sampling overhead (~5s), skew=0.0047

### 3.2. Range Partitioning + Sampling (Cải tiến) — O&V Ch.3

1. **Sampling**: trích xuất 10,000 IPs ngẫu nhiên (reservoir sampling)
2. **Build histogram**: phân phối tần suất IP
3. **Percentile boundaries**: chia ranges sao cho mỗi reducer nhận ~equal count
4. **Binary search**: O(log N) lookup per IP

**Nhược điểm**: overhead sampling ~5s → **Không recommended** cho dataset hiện tại.

### 3.3. Adaptive Work Stealing — O&V Ch.8

Khi `skew_factor > SKEW_THRESHOLD (0.5)`:
1. Hot reducer (nhiều IPs nhất) sort IPs
2. Chia đôi: prefix → hot reducer, suffix → cold reducer
3. Lặp cho đến khi skew ≤ threshold

---

## 4. Xử lý Bottleneck GeoIP

### 4.1. Vấn đề: GIL (Global Interpreter Lock)

GeoIP lookup là CPU-bound. Dùng `ThreadPoolExecutor` → bị GIL chặn → chỉ 1 core chạy thật.

**Benchmark trước khi tối ưu:**
- 550K IPs × ~0.1ms/IP = **55s** trên 1 core
- 4 threads ThreadPool: **vẫn 55s** (GIL)
- Throughput: **~7,300 rows/s**

### 4.2. Giải pháp: Multiprocessing (ProcessPoolExecutor)

```python
Process[0]  →  GeoLookup[0]  →  137K IPs  →  reducer_0.json
Process[1]  →  GeoLookup[1]  →  137K IPs  →  reducer_1.json
Process[2]  →  GeoLookup[2]  →  137K IPs  →  reducer_2.json
Process[3]  →  GeoLookup[3]  →  137K IPs  →  reducer_3.json
```

- Mỗi Process có **Python interpreter riêng** → GIL riêng
- **Sweep justification (CSV mode, 100MB)**:
  - N=1: 73.76s, 7,456 rows/s (baseline)
  - N=2: 84.73s, 6,491 rows/s (slower — Windows spawn overhead)
  - N=4: 74.24s, 7,408 rows/s
  - **N=6: 54.09s, 10,168 rows/s** ← optimal → **1.36x speedup**

---

## 5. Fault Tolerance — Xử lý khi Node chết

### 5.1. Mô hình lỗi

Theo O&V Ch.9 (Fault Tolerance), hệ thống xử lý **fail-stop failures** — reducer process bị terminate đột ngột (OOM, segfault, kill signal).

### 5.2. Chiến lược phục hồi

| Bước | Cơ chế | Chi tiết |
|------|--------|----------|
| 1 | Orphan cleanup | Xóa intermediate files từ lần chạy trước |
| 2 | Process timeout | Mỗi reducer có timeout 120s |
| 3 | Failure detection | ProcessPoolExecutor phát hiện exception/timeout |
| 4 | Partial retry | Chỉ retry reducer bị chết (không retry tất cả) |
| 5 | Exponential backoff | Retry: 1.5s, 2.25s, 3.375s... |
| 6 | Sequential fallback | Nếu multiprocessing fails hoàn toàn → chạy tuần tự |

### 5.3. Thiết kế idempotent

- Mỗi reducer ghi vào **file riêng** → an toàn khi retry
- `flush()` dùng atomic rename (`.tmp` → `.json`)
- Intermediate files được xóa ở đầu mỗi pipeline run

---

## 6. Thiết kế đánh giá

### 6.1. Các chỉ số đo lường

| Chỉ số | Mô tả | Công thức |
|--------|-------|-----------|
| **Throughput** | Rows/second | `total_rows / total_time` |
| **Speedup** | Khả năng mở rộng | `T(1 reducer) / T(N reducers)` |
| **Scaleup** | Tăng trưởng tuyến tính | `T(N, D) / T(N, αD)` |
| **Sizeup** | Ảnh hưởng data size | `T(N, αD) / T(N, D)` |
| **Skew Factor** | Độ lệch phân phối | `(max - min) / avg` |

### 6.2. Correctness Validation

```
Baseline (CSV gốc): 550,000 unique IPs
Pipeline output:     550,000 unique IPs

PASS: Baseline == Pipeline
FAIL: Baseline != Pipeline (có IP bị mất hoặc thừa)
```

### 6.3. Memory Monitoring

- Theo dõi RAM usage sau mỗi phase
- Trigger GC khi vượt `MEMORY_THRESHOLD_PCT`
- **Sweep justification**: 80% optimal. 95% → GC thrashing (15.87s vs 7.85s)

---

## 7. Dataset và Data Model

### 7.1. Schema

```
IP: string       — Địa chỉ IPv4 (e.g., "45.47.86.152")
site: string     — Tên cổng ("portal", "news", "shop", "api")
timestamp: string — Thời điểm truy cập
raw_line: string — Dòng log gốc (để audit)
```

### 7.2. Final Output Schema (central_store.json)

```json
{
  "ip": "45.47.86.152",
  "country": "United States",
  "city": "Los Angeles",
  "latitude": 34.0544,
  "longitude": -118.2441,
  "source_sites": ["api", "portal"]
}
```

---

## 8. Tech Stack

| Thành phần | Công nghệ | Lý do |
|-----------|-----------|-------|
| Ngôn ngữ | Python 3.10+ | Rapid development, rich libraries |
| GeoIP | MaxMind GeoLite2 (.mmdb) | Real geo data, 62MB in RAM |
| Concurrency | concurrent.futures | ThreadPool + ProcessPool |
| Memory | psutil | Platform-independent monitoring |
| Storage | CSV + JSON | Human-readable, no DB setup |
| OS | Windows 10 6-core | Local development environment |

---

*Design based on: Özsu & Valduriez — Principles of Distributed Database Systems, 4th Edition*
*Chapters referenced: Ch.2 (Distributed DB Architecture), Ch.3 (Data Distribution), Ch.8 (Distributed Query Processing), Ch.9 (Fault Tolerance)*
