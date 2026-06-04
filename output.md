# Kết quả chạy thực tế (Sweep 2026-06-04)

## LOG Mode (100MB, 578K rows, 721 unique IPs) — I/O-Bound

| N Reducers | Time (s) | Throughput | Speedup | Efficiency |
|-----------|----------|------------|---------|------------|
| 1 | 7.90 | 73,201 rows/s | 1.00x | 100.0% |
| 2 | 7.89 | 73,244 rows/s | 1.00x | 50.0% |
| 4 | 7.93 | 72,878 rows/s | 1.00x | 24.9% |
| 6 | 7.98 | 72,432 rows/s | 0.99x | 16.5% |
| 8 | 11.63 | 49,693 rows/s | 0.68x | 8.5% |
| 12 | 16.22 | 35,655 rows/s | 0.49x | 4.1% |

**Analysis:** Bottleneck ở Extract phase (regex parsing) → N=1..6 như nhau. N>=8 overhead vượt benefit.

## CSV Mode (100MB, 550K rows, 100% unique IPs) — CPU-Bound

| N Reducers | Time (s) | Throughput | Speedup |
|-----------|----------|------------|---------|
| 1 | 73.76 | 7,456 rows/s | 1.00x |
| 2 | 84.73 | 6,491 rows/s | 0.87x (slower — Windows spawn overhead) |
| 4 | 74.24 | 7,408 rows/s | 0.99x |
| **6** | **54.09** | **10,168 rows/s** | **1.36x** |

**Analysis:** N=6 tốt nhất cho CSV. N=2 chậm hơn N=1 vì Windows spawn overhead. Khuyến nghị: CSV `-r 6`, LOG `-r 4`.

## Memory Threshold Sweep

| Threshold | Time (s) | Throughput |
|-----------|----------|------------|
| 70% | 10.05 | 57,518 rows/s |
| **80%** | **7.85** | **73,663 rows/s** |
| 90% | 9.26 | 62,442 rows/s |
| 95% | 15.87 | 36,430 rows/s (GC thrashing) |

## Default Parameters (tối ưu cho 100MB dataset)

| Parameter | Value | Lý do |
|-----------|-------|--------|
| `DEFAULT_N_REDUCERS` | 4 | Cross-mode balanced |
| `MEMORY_THRESHOLD_PCT` | 80 | Sweep: 80% best |
| `PARTITIONING_STRATEGY` | `"hash"` | No sampling overhead, skew=0.0047 |
| `DEDUP_CHUNK_SIZE` | 2048 | Optimal batch size |
| `SKEW_THRESHOLD` | 0.5 | Work stealing threshold |
