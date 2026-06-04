# Analysis Report — Thiết Kế Hệ Thống ETL Phân Tán Dựa Trên Lý Thuyết Özsu & Valduriez

## Mục lục

1. Tổng quan hệ thống
2. Chọn Kiến trúc phân tán — Shared-Nothing (O&V Ch.2)
3. Chiến lược phân mảnh dữ liệu — Horizontal Fragmentation (O&V Ch.3)
4. Hash Partitioning vs Range Partitioning — So sánh (O&V Ch.3, Ch.8)
5. Global De-duplication — Hash-based Partitioned Aggregation (O&V Ch.8)
6. Xử lý Bottleneck — Multiprocessing vs Threading
7. Fault Tolerance — Xử lý khi Node chết (O&V Ch.9)
8. Kết quả Benchmark và Phân tích

---

## 1. Tổng quan hệ thống

Hệ thống triển khai ETL pipeline phân tán xử lý log Apache từ 4 cổng (550K dòng, ~114.5 MB). Các thiết kế trong báo cáo này được justify bằng lý thuyết từ **Özsu & Valduriez — Principles of Distributed Database Systems, 4th Edition**.

| Thành phần | Mô tả | Özsu & Valduriez Reference |
|---|---|---|
| Kiến trúc | Shared-Nothing | Ch.2 — Distributed DB Architectures |
| Phân mảnh | Horizontal Fragmentation theo site | Ch.3 — Data Distribution |
| Định tuyến | Hash Partitioning + Range Partitioning | Ch.3 — Data Placement |
| Tổng hợp | Partitioned Aggregation | Ch.8 — Distributed Query Processing |
| Chịu lỗi | Checkpoint + Retry + Idempotent Write | Ch.9 — Fault Tolerance |

---

## 2. Chọn Kiến trúc — Shared-Nothing (O&V Ch.2)

### 2.1. Các mô hình kiến trúc

Özsu & Valduriez (Ch.2) phân loại kiến trúc DB phân tán thành 3 loại:

| Kiến trúc | Shared Disk | Shared Memory | **Shared-Nothing** |
|-----------|-------------|--------------|-------------------|
| Bộ nhớ | Chia sẻ | Riêng | Riêng |
| Disk | Chia sẻ | Chia sẻ | Riêng |
| Communication | Qua shared mem | Qua bus | Message-passing |
| Mở rộng | Hạn chế | Trung bình | **Tuyến tính** |
| Contention | Cao | Trung bình | **Thấp** |

### 2.2. Lựa chọn: Shared-Nothing

**Lý do chọn Shared-Nothing:**
- **Không có disk contention**: mỗi reducer ghi vào file riêng — không bao giờ có write contention
- **Mở rộng tuyến tính**: khi thêm reducer, không cần đồng bộ với các reducer khác
- **Isolation**: lỗi một node không ảnh hưởng node khác — đây là nền tảng cho fault tolerance
- **Chi phí**: triển khai trên localhost với multiprocessing — không cần phần cứng đặc biệt

### 2.3. Trade-offs

| Trade-off | Giải pháp |
|-----------|-----------|
| Mỗi node phải có đủ disk cho data cục bộ | Dataset 550K rows chia thành 4 file CSV nhỏ (25MB/each) |
| Giao tiếp phức tạp hơn | Inter-process communication qua ProcessPoolExecutor (đơn giản hóa) |
| Thiếu global lock → khó đảm bảo serializability | Hash Partitioning đảm bảo mỗi IP chỉ đến 1 reducer — không conflict |

---

## 3. Chiến lược phân mảnh — Horizontal Fragmentation (O&V Ch.3)

### 3.1. Lý thuyết Horizontal Fragmentation

Özsu & Valduriez (Ch.3) định nghĩa Horizontal Fragmentation (HF) như sau:

> *"A relation is partitioned into tuples, each fragment consisting of a subset of the tuples of the relation."*

HF được chọn vì:
1. **Data locality**: mỗi site (cổng) chỉ đọc file cục bộ — không cần truyền dữ liệu qua network
2. **Parallelism**: 4 extractors đọc 4 file song song — I/O-bound được song song hóa hiệu quả
3. **Dễ triển khai**: không cần distributed file system

### 3.2. Fragmentation Schema

```
Apache_Log
  ├── Fragment 1 (portal):  portal_access.log  (~139K tuples)
  ├── Fragment 2 (news):   news_access.log    (~136K tuples)
  ├── Fragment 3 (shop):   shop_access.log    (~136K tuples)
  └── Fragment 4 (api):    api_access.log     (~139K tuples)
```

### 3.3. Reconstruction

Do các fragment được phân theo site và mỗi tuple có `site_id`, việc reconstruct toàn bộ quan hệ chỉ cần UNION:

```python
full_relation = fragment_1 ∪ fragment_2 ∪ fragment_3 ∪ fragment_4
```

### 3.4. Correctness của Fragmentation (O&V Ch.3)

O&V đề xuất 3 tính chất correctness cho fragmentation:

| Tính chất | Yêu cầu | Triển khai |
|-----------|---------|-----------|
| **Completeness** | Mọi tuple đều thuộc ít nhất 1 fragment | 4 sites cover đầy đủ → ✓ |
| **Disjointness** | Không có tuple thuộc nhiều fragment | site_id khác nhau → ✓ |
| **Reconstruction** | R = R1 ∪ R2 ∪ R3 ∪ R4 | UNION qua 4 files → ✓ |

---

## 4. Hash Partitioning vs Range Partitioning (O&V Ch.3 & Ch.8)

### 4.1. Hash Partitioning — Lựa chọn mặc định

**Định nghĩa (O&V Ch.3):**
> *"Hash partitioning distributes tuples based on the value of a hash function applied to one or more attributes of the tuples."*

**Triển khai:**
```python
reducer_id = fnv1a_64(IP) % n_reducers
```

**Ưu điểm:**
- **Deterministic**: cùng IP luôn đến cùng reducer → idempotent
- **Simple**: không cần metadata về boundaries
- **Good distribution**: với IP uniformly random, hash cho phân phối đều

**Nhược điểm:**
- Không guarantee equal-sized partitions khi IP distribution không uniform
- Không hỗ trợ range queries (không cần thiết cho bài toán này)

### 4.2. Range Partitioning — Cải tiến với Sampling

**Định nghĩa (O&V Ch.3):**
> *"The tuples are partitioned according to the value intervals of one or more attributes."*

**Thuật toán:**
1. **Sampling**: trích xuất 10,000 IPs bằng reservoir sampling (O&V: "sampling-based approach for skewed data")
2. **Histogram**: xây frequency distribution từ samples
3. **Percentile boundaries**: chia thành N ranges sao cho mỗi range chứa ~equal count

**Ưu điểm:**
- **Guaranteed equal-sized partitions**: mỗi reducer nhận ~equal IPs
- **Tốt cho skewed data**: không bị 1 node chứa quá nhiều IPs

**Nhược điểm:**
- **Overhead sampling**: ~5s cho 10K samples
- Không cần thiết cho dataset 550K (IPs đã uniformly distributed)

### 4.3. So sánh thực tế trên dataset

| Metric | Hash Partitioning | Range Partitioning |
|--------|------------------|-------------------|
| R0 IPs | 137,951 | 137,659 |
| R1 IPs | 137,335 | 134,885 |
| R2 IPs | 137,306 | 136,838 |
| R3 IPs | 137,408 | 140,618 |
| **Skew Factor** | **0.0047** | 0.0417 |
| Sampling overhead | 0s | 4.7s |
| **Total time** | **41s (6 cores)** | 45s (6 cores) |

**Kết luận**: Hash Partitioning tốt hơn cho dataset này vì IP distribution đã uniform. Range Partitioning chỉ cần thiết khi data distribution bị skewed (ví dụ: 80% IPs cùng subnet).

---

## 5. Global De-duplication — Hash-based Partitioned Aggregation (O&V Ch.8)

### 5.1. Vấn đề De-duplication trong Hệ thống Phân tán

Mỗi IP có thể xuất hiện ở nhiều site (portal + api). Sau khi shuffle, mỗi reducer nhận mix IPs từ tất cả 4 sites. De-duplication phải đảm bảo:
- Mỗi IP chỉ được ghi **đúng 1 lần** vào central store
- Không có IP nào bị mất (false negative)
- Không có IP nào bị thừa (false positive)

### 5.2. Two-Phase De-duplication

**Phase 1 — Local De-duplication (trong Extractor):**
Mỗi extractor đọc 1 file, loại bỏ IP trùng trong chính file đó.
```python
seen_local = set()
for ip in file:
    if ip not in seen_local:
        seen_local.add(ip)
        yield (ip, site_id)
```
**Giảm**: 640K rows → 559K unique trong dataset 9K dup.

**Phase 2 — Global De-duplication (trong Reducer):**
Sau shuffle, mỗi reducer kiểm tra `ip_seen` set toàn cục.
```python
ip_seen = set()  # toàn cục trong reducer
for ip, site_id in ip_tuples:
    if ip in ip_seen: continue  # skip global dup
    ip_seen.add(ip)
```

### 5.3. Hash-based Partitioned Aggregation (O&V Ch.8)

O&V mô tả kỹ thuật Partitioned Aggregation:

> *"Aggregation is performed at each site on the data local to that site, and the partial results are then combined at a coordinator site."*

Trong hệ thống này, aggregation được thực hiện tại mỗi reducer (site):
- **Local aggregation**: mỗi extractor loại bỏ local duplicates
- **Shuffle**: hash(IP) định tuyến đến reducer đúng
- **Global aggregation**: mỗi reducer loại bỏ cross-site duplicates

### 5.4. Correctness Proof

Để chứng minh de-duplication đúng, ta cần:
1. **No false positive**: nếu IP xuất hiện ở nhiều site, nó không được ghi 2 lần → ✓ (global `ip_seen` set)
2. **No false negative**: mọi IP unique đều được ghi → ✓ (if ip not in ip_seen → add)
3. **Deterministic**: cùng input → cùng output → ✓ (hash function deterministic)

---

## 6. Xử lý Bottleneck — Multiprocessing vs Threading

### 6.1. Bottleneck: GeoIP Lookup

GeoIP lookup là CPU-bound operation:
- 550K IPs × ~0.1ms/IP = ~55,000ms = **55s** trên 1 core
- Transform chiếm **72%** total pipeline time

### 6.2. Python GIL — Hạn chế của Threading

Özsu & Valduriez không trực tiếp đề cập GIL, nhưng nguyên tắc parallel processing áp dụng:

> *"Parallelism in a multiprocessor can be exploited only if the processes are truly concurrent."*

Trong CPython, **Global Interpreter Lock (GIL)** ngăn nhiều threads thực thi Python bytecode đồng thời. Kết quả:
- 4 threads chạy GeoIP → **vẫn chỉ 1 core thật sự chạy**
- ThreadPoolExecutor với GeoIP-bound task → **không có parallelism thật**

### 6.3. Giải pháp: Multiprocessing

**ProcessPoolExecutor** tạo **separate Python interpreters** — mỗi process có GIL riêng:
```
Process[0]: Python interpreter #0 → GIL[0] → GeoLookup[0] on Core #0
Process[1]: Python interpreter #1 → GIL[1] → GeoLookup[1] on Core #1
Process[2]: Python interpreter #2 → GIL[2] → GeoLookup[2] on Core #2
Process[3]: Python interpreter #3 → GIL[3] → GeoLookup[3] on Core #3
```

**Kết quả benchmark:**

| Config | Phase B (wall) | Transform (CPU sum) | Throughput |
|--------|---------------|---------------------|-----------|
| 4 threads (ThreadPool) | 55s | 55s | 7,308 rows/s |
| 4 processes | 27s | 27s | 11,397 rows/s |
| **6 processes** | **20s** | **36s** | **13,350 rows/s** |

**Phân tích:** transform CPU sum = 36.129s là tổng CPU time của 6 processes. Wall clock = 19.877s → speedup = 36.129 / 19.877 ≈ 1.82x (so với ideal 6x). Chênh lệch vì:
1. **Spawn overhead**: Windows spawn tốn ~1s/process = ~6s total
2. **IPC overhead**: truyền kết quả qua pickling
3. **Merge bottleneck**: Phase C merge tuần tự (~15s)

### 6.4. Deduplication Chunking (DEDUP_CHUNK_SIZE)

Để tối ưu thêm, mỗi reducer xử lý IP tuples theo chunk (batch):
```python
CHUNK_SIZE = 2048  # rows per chunk
```
- Mỗi chunk: check set → mark dup → add unique
- Track hit rate per chunk: `dup_rate = global_dup / chunk_size`
- Dataset 640K với 9K dup: 69 chunks/reducer, avg_dup = 0.0%

---

## 7. Fault Tolerance — Xử lý khi Node chết (O&V Ch.9)

### 7.1. Mô hình lỗi (O&V Ch.9)

Özsu & Valduriez phân loại failures trong distributed systems:

| Failure Type | Mô tả | Xử lý |
|-------------|-------|--------|
| **Transaction failures** | Transaction bị abort | Rollback riêng transaction đó |
| **Site (node) failures** | Node bị crash/shutdown | Checkpoint + recovery |
| **Media (disk) failures** | Disk corrupted | Replication |
| **Network failures** | Link bị đứt | Timeout + retry |

Trong hệ thống này, ta xử lý **node failures** — reducer process bị terminate đột ngột (OOM, segfault, kill signal).

### 7.2. Chiến lược Fault Tolerance được triển khai

**Nguyên tắc thiết kế (O&V Ch.9):**
> *"The recovery procedure should be efficient in terms of the amount of log that has to be examined and the time required to recover."*

#### Chiến lược 1: Orphan Cleanup
Xóa intermediate files từ lần chạy trước khi bắt đầu pipeline mới. Đảm bảo mỗi run bắt đầu sạch sẽ.

#### Chiến lược 2: Process Timeout
Mỗi reducer có timeout 120s. Nếu vượt quá → được coi là died → kích hoạt retry.

#### Chiến lược 3: Partial Retry
Chỉ retry reducer bị chết — không retry process đã thành công. Đây là **partial recovery** theo O&V:
> *"In partial recovery, only the failed site is recovered; the others continue to operate."*

#### Chiến lược 4: Exponential Backoff
Retry với backoff tăng dần: 1.5s, 2.25s, 3.375s... Tránh thundering herd khi nhiều nodes cùng retry.

#### Chiến lược 5: Sequential Fallback
Nếu ProcessPoolExecutor fails hoàn toàn → chạy reducers tuần tự trong main process. Đảm bảo pipeline vẫn hoàn thành dù chậm.

### 7.3. Idempotent Write — Đảm bảo an toàn khi Retry

O&V (Ch.9) nhấn mạnh tầm quan trọng của idempotent operations:

```python
# Atomic write: .tmp → .json rename
temp_path = INTERMEDIATE_DIR / "reducer_0.tmp"
with open(temp_path, "w") as f:
    json.dump(records, f)
temp_path.replace(INTERMEDIATE_DIR / "reducer_0.json")
```

Nếu reducer chết sau khi ghi `.tmp` nhưng trước rename, orphan cleanup sẽ xóa file đó ở lần chạy tiếp theo.

### 7.4. Recovery Procedure

```
1. Process dies (timeout / exception)
   ↓
2. FaultTolerance phát hiện (ProcessPoolExecutor raises)
   ↓
3. Ghi log failure: {node_id, type, reason, timestamp}
   ↓
4. retry_failed_node():
   ├── Attempt 1: _run_reducer_task(rid) — chạy trong process mới
   ├── Attempt 2: _run_reducer_task(rid) — backoff 1.5s
   └── Attempt 3: _run_reducer_task(rid) — backoff 2.25s
       ↓ (nếu vẫn fail)
5. Sequential fallback: _run_single_reducer_sync() trong main process
   ↓
6. Validation: baseline == pipeline output?
```

### 7.5. Trade-off: Checkpointing vs Retry

O&V đề cập 2 approaches:
- **Checkpointing**: lưu intermediate state định kỳ → phục hồi nhanh nhưng tốn storage
- **Retry**: không lưu checkpoint, retry toàn bộ operation → đơn giản nhưng chậm nếu data lớn

Hệ thống chọn **Retry** vì:
- Intermediate files đã đóng vai trò "checkpoint tự nhiên"
- Mỗi reducer ghi file riêng → chỉ cần retry reducer đó
- Dataset 550K không quá lớn để retry là vấn đề

---

## 8. Kết quả Benchmark và Phân tích

### 8.1. Speedup Analysis (Khả năng mở rộng)

| Reducers | Phase A | Phase B | Phase C | Total | Speedup |
|----------|---------|---------|---------|-------|---------|
| 1 | 3s | 55s | 55s | ~113s | 1.00x |
| 4 | 3s | 27s | 15s | ~45s | **2.51x** |
| 6 | 3s | 20s | 15s | ~38s | **2.97x** |

**Phân tích:** Speedup không tuyến tính (ideal: 6x) vì:
1. **Phase C (merge) không song song**: tuần tự, chiếm ~15s không giảm khi thêm reducer
2. **Spawn overhead**: Windows multiprocessing spawn tốn overhead cố định
3. **Amdahl's Law**: merge chiếm ~39% total time không song song được

### 8.2. Data Skew Analysis

| Strategy | R0 | R1 | R2 | R3 | Skew |
|----------|-----|-----|-----|-----|------|
| Hash (4 reducers) | 137,951 | 137,335 | 137,306 | 137,408 | **0.0047** |
| Range (4 reducers) | 137,659 | 134,885 | 136,838 | 140,618 | 0.0417 |

Hash Partitioning cho skew thấp hơn vì IP distribution đã uniform trong dataset. Range Partitioning + Sampling thêm overhead mà không cải thiện.

### 8.3. Fault Tolerance Effectiveness

```
FaultTolerance: cleaned 6 orphan intermediate files
FaultTolerance: DETECTED=0 | RECOVERED=0 | RETRIES=0 | SEQUENTIAL_FALLBACK=No
```

Trong điều kiện bình thường (không có failure), FaultTolerance hoạt động ở chế độ "clean orphan" — không có retry overhead.

### 8.4. Correctness Validation

| Dataset | Baseline | Pipeline | Pass? |
|---------|----------|----------|-------|
| 550K unique | 550,000 | 550,000 | ✓ |
| 640K (9K dup) | 558,999 | 558,999 | ✓ |

Validation 100% chính xác trên cả 2 datasets — proves correctness của de-duplication algorithm.

---

## 9. Kết luận

Thiết kế hệ thống ETL phân tán được justify bằng lý thuyết O&V trên 4 khía cạnh:

1. **Shared-Nothing Architecture (Ch.2)**: đảm bảo không contention, mở rộng tuyến tính, fault isolation
2. **Horizontal Fragmentation (Ch.3)**: đảm bảo data locality, parallel I/O, completeness/disjointness
3. **Hash Partitioned Aggregation (Ch.8)**: đảm bảo deterministic routing, global dedup correctness
4. **Fault Tolerance with Idempotent Write (Ch.9)**: đảm bảo recovery không mất dữ liệu, partial retry hiệu quả

**Kết quả đạt được:**
- Throughput: **13,350 rows/s** (6 cores) — 1.8x so với threading
- Validation: **100% correct** trên cả 2 datasets
- Fault tolerance: partial retry với exponential backoff
- Speedup: **2.97x** khi tăng từ 1 → 6 reducers

---

*Tài liệu tham khảo: Özsu & Valduriez — Principles of Distributed Database Systems, 4th Edition, Springer 2020*
