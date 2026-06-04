# Hướng Dẫn Chạy Demo — Distributed ETL Pipeline

## Mục lục

1. [Cài đặt](#1-cài-đặt)
2. [Cách chạy nhanh](#2-cách-chạy-nhanh)
3. [Flexible Config — Đổi N reducers](#3-flexible-config--đổi-n-reducers)
4. [Demo Work Stealing (Skew Dataset)](#4-demo-work-stealing-skew-dataset)
5. [Demo Kill-Node (Fault Tolerance)](#5-demo-kill-node-fault-tolerance)
6. [Benchmark và Visualization](#6-benchmark-và-visualization)
7. [Kết quả thực tế](#7-kết-quả-thực-tế)

---

## 1. Cài đặt

```bash
pip install -r requirements.txt
```

---

## 2. Cách Chạy Nhanh

### Dataset có sẵn

| Dataset | Lệnh | Rows | Mô tả |
|---------|-------|------|--------|
| Balanced (default) | `python main.py --dataset balanced -r 4 --validate` | 550K | 4 site chia đều |
| Duplicates | `python main.py --dataset csv9k -r 4 --validate` | 640K | 81K trùng lặp |
| Skew (Work Stealing) | `python main.py --dataset skew -r 4 --validate` | 110K | API site = 90% data |

### Tạo skew dataset trước (chạy 1 lần)

```bash
# Tạo skew dataset mặc định (api = 90% data → Work Stealing triggers)
python scripts/generate_skew_dataset.py

# Tạo balanced dataset (4 site đều nhau)
python scripts/generate_skew_dataset.py --skew 0

# Tạo moderate skew (50% vào api)
python scripts/generate_skew_dataset.py --skew 0.5
```

---

## 3. Flexible Config — Đổi N Reducers

Tham số `-r N` hoàn toàn linh hoạt. Chỉ cần đổi số, không cần sửa code.

```bash
# 1 reducer (baseline)
python main.py --dataset balanced -r 1 --validate

# 2 reducers
python main.py --dataset balanced -r 2 --validate

# 3 reducers (thay đổi partition)
python main.py --dataset balanced -r 3 --validate

# 4 reducers (mặc định)
python main.py --dataset balanced -r 4 --validate

# 6 reducers (max throughput)
python main.py --dataset balanced -r 6 --validate

# 8 reducers
python main.py --dataset balanced -r 8 --validate
```

**Output khi chạy sẽ hiển thị:**

```
=================================================================
COORDINATOR: Pipeline Complete
  Partition strategy:     HASH
  Total rows read:         550,000
  Total IP local unique:   550,000  (after LOCAL dedup)
  Total IP global unique:  550,000  (after GLOBAL dedup)
  Duplicates skipped:           0  ← trùng đã xử lý

  [IP Distribution per Reducer]
    Reducer[0]:    137,500 IPs ( 25.0%)
    Reducer[1]:    137,500 IPs ( 25.0%)
    Reducer[2]:    137,500 IPs ( 25.0%)
    Reducer[3]:    137,500 IPs ( 25.0%)
    ────────────────────────────────────────
    Skew Factor: 0.0000 (max=137500 min=137500 avg=137500)

     Work Stealing:    Not triggered (skew <= 50%)

  [Timing Breakdown]
    Phase B (reduce+transform):     54.090s
      └─ dedup:                      0.001s
      └─ transform:                 54.089s
    Merge (Phase C):                 0.320s
  FaultTolerance:               DETECTED=0 | RECOVERED=0 | RETRIES=0 | SEQUENTIAL_FALLBACK=No
  Total time:                  54.090s
  Throughput (rows/s):        10,168 rows/s
  Throughput (IPs/s):          10,168 IPs/s
=================================================================
```

---

## 4. Demo Work Stealing (Skew Dataset)

### Bước 1: Tạo skew dataset

```bash
python scripts/generate_skew_dataset.py
```

Output:
```
Distributing IPs per site:
  portal    :    3,300 IPs (  3.0%)
  news      :    3,300 IPs (  3.0%)
  shop      :    3,300 IPs (  3.0%)
  api       :   99,900 IPs ( 90.9%) <<< SKEW
```

### Bước 2: Chạy với Hash Partitioning (skew xảy ra)

```bash
python main.py --dataset skew -r 4 --partition hash --validate
```

**Kỳ vọng output:**

```
=================================================================
COORDINATOR: Pipeline Complete

  [IP Distribution per Reducer]
    Reducer[0]:    45,000 IPs ( 40.9%)
    Reducer[1]:    10,000 IPs (  9.1%)
    Reducer[2]:    10,000 IPs (  9.1%)
    Reducer[3]:    45,000 IPs ( 40.9%)
    ────────────────────────────────────────
    Skew Factor: 3.5000 (max=45000 min=10000 avg=27500)

  >>> Work Stealing:    YES — 12,500 items moved in 3 iters
=================================================================
```

→ **Work Stealing KICK IN** vì skew > 50%

### Bước 3: Chạy với Range Partitioning (cân bằng tự động)

```bash
python main.py --dataset skew -r 4 --partition range --validate
```

→ Range Partitioning tự cân bằng qua sampling → Work Stealing **KHÔNG** cần trigger.

---

## 5. Demo Kill-Node (Fault Tolerance)

### 5.1. Cách test

**Bước 1:** Chạy pipeline (đừng kill):
```bash
python main.py --dataset balanced -r 4 --validate
```

**Bước 2:** Quan sát log:
```
FaultTolerance: cleaned 6 orphan intermediate files from previous run
FaultTolerance: DETECTED=0 | RECOVERED=0 | RETRIES=0 | SEQUENTIAL_FALLBACK=No
```

→ Không lỗi → orphan cleanup chạy clean

**Bước 3:** Kill 1 process trong khi chạy:

1. Chạy pipeline: `python main.py --dataset balanced -r 4`
2. Mở **Task Manager** → tab **Details** → kill `python.exe` process đang chạy
3. Hoặc PowerShell: `taskkill /PID <pid> /F`

**Kỳ vọng output khi có kill:**
```
FaultTolerance: Reducer[2] DIED: ProcessLookupError
FaultTolerance: retrying 1 failed reducers...
  FaultTolerance: Reducer[2] RECOVERED on attempt 1
FaultTolerance: DETECTED=1 | RECOVERED=1 | RETRIES=1 | SEQUENTIAL_FALLBACK=No
```

### 5.2. Lý thuyết (O&V Ch.9)

```
Reducer[2] process dies (OOM / kill signal)
       ↓
ProcessPoolExecutor phát hiện exception
       ↓
FaultTolerance ghi log: {node_id=2, type=reducer}
       ↓
Partial retry (chỉ retry reducer bị chết):
  Attempt 1: backoff 1.5s
  Attempt 2: backoff 2.25s
  Attempt 3: backoff 3.375s
       ↓ (nếu vẫn fail)
Sequential fallback: chạy trong main process
       ↓
Validation: baseline == pipeline output?
```

---

## 6. Benchmark và Visualization

```bash
# Speedup: T(N) / T(1)
python main.py --benchmark speedup

# Scaleup: T(N, D) / T(N, αD)
python main.py --benchmark scaleup

# Sizeup: T(N, αD) / T(N, D)
python main.py --benchmark sizeup
```

### Xem kết quả nhanh

```bash
python -c "
import json
with open('data/output/central_store.json', 'r') as f:
    data = json.load(f)
print(f'Total IPs: {len(data)}')
for i, (ip, info) in enumerate(list(data.items())[:5]):
    print(f'  {ip}: {info.get(\"country\")}, {info.get(\"city\")}')
"
```

---

## 7. Kết Quả Benchmark Thực tế

### 7.1. CSV Mode — CPU-Bound (550K rows, 100% unique IPs)

| N Reducers | Time (s) | Throughput | Speedup |
|-----------|----------|------------|---------|
| 1 | 73.76 | 7,456 rows/s | 1.00x |
| 4 | 74.24 | 7,408 rows/s | 0.99x |
| **6** | **54.09** | **10,168 rows/s** | **1.36x** |

→ **Khuyến nghị:** `python main.py --dataset balanced -r 6`

### 7.2. Skew Dataset (110K rows, 90% API)

| Partitioning | Skew Factor | Work Stealing | Time (s) |
|-------------|-------------|---------------|----------|
| Hash | 3.50 | YES | ~15s |
| Range | ~0.01 | NO | ~15s |

→ **Work Stealing** giảm skew từ 3.50 → ~0.1

### 7.3. Dedup Stats (CSV9K Mode)

| Metric | Value |
|--------|-------|
| Total rows read | 640,000 |
| Unique IPs (local) | 558,999 |
| Unique IPs (global) | 558,999 |
| Duplicates skipped | 81,000 |

---

## Troubleshooting

| Lỗi | Cách fix |
|------|----------|
| `ModuleNotFoundError: No module named 'geoip2'` | `pip install geoip2` |
| `GeoIP2 database not found` | Đặt `GeoLite2-City.mmdb` trong `data/` |
| `Permission denied: central_store.json` | Kill process khác đang giữ file |
| Validation FAIL | Chạy lại: `python main.py --dataset balanced -r 4 --validate` |
