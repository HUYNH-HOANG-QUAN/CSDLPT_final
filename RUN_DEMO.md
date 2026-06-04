# Hướng Dẫn Chạy Demo — Distributed ETL Pipeline

## Mục lục

1. [Cài đặt](#1-cài-đặt)
2. [Cách chạy nhanh](#2-cách-chạy-nhanh)
3. [Các chế độ chạy chi tiết](#3-các-chế-độ-chạy-chi-tiết)
4. [Demo Kill-Node (Fault Tolerance)](#4-demo-kill-node-fault-tolerance)
5. [Benchmark và Visualization](#5-benchmark-và-visualization)
6. [Kết quả thực tế](#6-kết-quả-thực-tế)

---

## 1. Cài đặt

### 1.1. Requirements

```bash
pip install -r requirements.txt
```

Dependencies chính:
- `geoip2` — MaxMind GeoLite2 reader
- `psutil` — Memory monitoring
- `pandas`, `numpy`, `matplotlib`, `seaborn` — (optional, cho visualization)
- `jupyter` — (optional, cho notebooks)

### 1.2. Dataset

**Dataset 1 — 550K unique IPs (khuyến nghị):**
```
data/output/apache_550k_unique_ips.csv  (550,000 rows, 100% unique IPs)
+ Pre-split 4 file: csv_portal.csv, csv_news.csv, csv_shop.csv, csv_api.csv
```

**Dataset 2 — 640K rows với 9K duplicate IPs:**
```
data/output/apache_550k_9k_dup.csv  (640,000 rows, 558,999 unique IPs, 81K true duplicates)
+ Pre-split 4 file: csv_9k_portal.csv, csv_9k_news.csv, csv_9k_shop.csv, csv_9k_api.csv
```

**Dataset 3 — Log files:**
```
portal_access.log, news_access.log, shop_access.log, api_access.log
```

---

## 2. Cách Chạy Nhanh

### 2.1. Chạy pipeline (khuyến nghị — 4 cores)

```bash
python main.py --csv -r 4 --validate
```

Kết quả mong đợi:
```
✓ 4 extractors đọc 4 file CSV song song  (~3s)
✓ 4 reducers xử lý song song             (~20s, ProcessPoolExecutor)
✓ Merge 4 file → central_store.json       (~15s)
✓ FaultTolerance: orphan cleanup
✓ Validation PASS: 550,000 / 550,000 IPs
✓ Total: ~38s | Throughput: 13,350 rows/s
```

### 2.2. Chạy với 6 cores (max CPU)

```bash
python main.py --csv -r 6 --validate
```

Throughput tối đa đạt được: **~13,350 rows/s**

### 2.3. Dataset với duplicates (test DEDUP_CHUNK_SIZE)

```bash
python main.py --csv9k -r 4 --validate
```

---

## 3. Các Chế Độ Chạy Chi Tiết

### 3.1. Data Source Modes

```bash
# Dataset CSV (550K unique IPs) — mặc định khuyến nghị
python main.py --csv

# Dataset CSV với 9K duplicate IPs (640K rows)
python main.py --csv9k

# Dataset log files gốc (578K rows, 721 unique IPs)
python main.py          # không có --csv flag
```

### 3.2. Partitioning Strategies

```bash
# Hash Partitioning (mặc định) — đơn giản, nhanh, tốt khi IP distribution đều
python main.py --csv --partition hash

# Range Partitioning (sampling-based) — cải thiện load balance khi data skewed
python main.py --csv --partition range
```

### 3.3. Số lượng Reducers

```bash
# 1 reducer (baseline — để so sánh speedup)
python main.py --csv -r 1 --validate

# 2 reducers
python main.py --csv -r 2 --validate

# 4 reducers (mặc định)
python main.py --csv -r 4 --validate

# 6 reducers (tận dụng hết CPU)
python main.py --csv -r 6 --validate

# 8 reducers
python main.py --csv -r 8 --validate
```

### 3.4. Các flags khác

```bash
# Verbose mode (debug output chi tiết)
python main.py --csv -r 4 -v

# Chỉ chạy validation (không chạy pipeline)
python main.py --validation-only --csv

# Output path tùy chỉnh
python main.py --csv -r 4 -o data/output/my_output.json
```

---

## 4. Demo Kill-Node (Fault Tolerance)

### 4.1. Lý thuyết

Khi 1 reducer process bị chết (OOM, segfault, kill signal):
1. ProcessPoolExecutor phát hiện exception/timeout (120s)
2. FaultTolerance ghi log: `{node_id, type, reason, attempt}`
3. Retry riêng reducer bị chết (max 3 lần, exponential backoff)
4. Sequential fallback: chạy trong main process nếu retry fail
5. Validation: baseline == pipeline output?

### 4.2. Cách test

**Bước 1:** Chạy pipeline bình thường trước để tạo intermediate files:
```bash
python main.py --csv -r 4 --validate
```

**Bước 2:** Quan sát FaultTolerance hoạt động:
```
FaultTolerance: cleaned 6 orphan intermediate files from previous run
FaultTolerance: DETECTED=0 | RECOVERED=0 | RETRIES=0 | SEQUENTIAL_FALLBACK=No
```
→ Không có lỗi → orphan cleanup chạy clean

**Bước 3:** Để test thực sự kill 1 process trong khi chạy:
- Mở Task Manager → tab Details → kill `python.exe` process đang chạy
- Hoặc chạy lệnh PowerShell: `taskkill /PID <pid> /F`
- Quan sát: hệ thống sẽ ghi log failure và retry

---

## 5. Benchmark và Visualization

### 5.1. Benchmark Modes

```bash
# Chạy tất cả benchmarks (Speedup + Scaleup + Sizeup)
python main.py --benchmark all

# Speedup: T(N) / T(1) — đo khả năng mở rộng
python main.py --benchmark speedup

# Scaleup: T(N, D) / T(N, αD) — đo tăng trưởng tuyến tính
python main.py --benchmark scaleup

# Sizeup: T(N, αD) / T(N, D) — đo ảnh hưởng của data size
python main.py --benchmark sizeup
```

### 5.2. Hyperparameter Sweep

```bash
# Sweep CSV dataset
python -m src.sweep_runner --csv

# Sweep log files
python -m src.sweep_runner --log

# Sweep cả hai
python -m src.sweep_runner --all
```

Sweep chạy: N reducers (1, 2, 4, 6, 8, 12), Memory threshold, Skew threshold.
Kết quả: `data/output/sweeps/sweep_<timestamp>.json`

### 5.3. Visualization

```bash
# Interactive Jupyter notebook
python -m jupyter lab notebooks/visualize_sweeps.ipynb

# Headless (sinh PNG tự động)
python -m jupyter nbconvert \
  --to notebook \
  --execute notebooks/visualize_sweeps.ipynb \
  --output notebooks/visualize_sweeps.ipynb
```

Output PNG: `notebooks/speedup_analysis.png`, `skew_analysis.png`, `latency_breakdown.png`

### 5.4. Xem kết quả nhanh

```bash
python -c "
import json
with open('data/output/central_store.json', 'r') as f:
    data = json.load(f)
print(f'Total IPs: {len(data)}')
for i, (ip, info) in enumerate(data.items()):
    if i < 5:
        print(f'  {ip}: {info.get(\"country\")}, {info.get(\"city\")}')
"
```

---

## 6. Kết Quả Benchmark Thực tế (Sweep 100MB Dataset)

### 6.1. LOG Mode — I/O-Bound (578K rows, 721 unique IPs)

| N Reducers | Time (s) | Throughput | Speedup | Efficiency |
|-----------|----------|------------|---------|------------|
| 1 | 7.90 | 73,201 rows/s | 1.00x | 100.0% |
| 4 | 7.93 | 72,878 rows/s | 1.00x | 24.9% |
| 6 | 7.98 | 72,432 rows/s | 0.99x | 16.5% |
| 8 | 11.63 | 49,693 rows/s | 0.68x | 8.5% |
| 12 | 16.22 | 35,655 rows/s | 0.49x | 4.1% |

→ Bottleneck ở Extract (regex parsing) → N=4 (default) là optimal.

### 6.2. CSV Mode — CPU-Bound (550K rows, 100% unique IPs)

| N Reducers | Time (s) | Throughput | Speedup |
|-----------|----------|------------|---------|
| 1 | 73.76 | 7,456 rows/s | 1.00x |
| 4 | 74.24 | 7,408 rows/s | 0.99x |
| **6** | **54.09** | **10,168 rows/s** | **1.36x** |

→ N=6 tốt nhất cho CSV mode. Khuyến nghị: `python main.py --csv -r 6`

### 6.3. Memory Threshold — LOG Mode

| Threshold | Time (s) | Throughput |
|-----------|----------|------------|
| 70% | 10.05 | 57,518 rows/s |
| **80%** | **7.85** | **73,663 rows/s** |
| 95% | 15.87 | 36,430 rows/s |

→ **80%** optimal — 95% gây GC thrashing.

### 6.4. CSV9K Mode (640K rows, 81K true duplicates)

| Metric | Value |
|--------|-------|
| Total rows | 640,000 |
| Unique IPs | 558,999 |
| True duplicates skipped | 81,000 |
| Validation | ✓ PASS 100% |

### 6.5. Fault Tolerance

```
FaultTolerance: cleaned 6 orphan intermediate files from previous run
FaultTolerance: DETECTED=0 | RECOVERED=0 | RETRIES=0 | SEQUENTIAL_FALLBACK=No
✓ Validation PASS: 550000 IPs
```

---

## 7. Troubleshooting

### Lỗi: `ModuleNotFoundError: No module named 'geoip2'`
```bash
pip install geoip2
```

### Lỗi: `GeoIP2 database not found`
→ GeoLite2-City.mmdb cần đặt ở project root hoặc trong `data/` directory.
→ Nếu không có, hệ thống tự fallback sang synthetic lookup.

### Lỗi: `Permission denied: central_store.json`
→ Đang có process khác đang giữ file. Đợi hoặc kill process đó.

### Lỗi: Validation FAIL — IPs không khớp
→ Kiểm tra dataset CSV có bị modified không.
→ Chạy lại với `--validation-only` để verify output hiện tại.
→ Intermediate files có thể từ lần chạy trước — FaultTolerance sẽ tự clean.
