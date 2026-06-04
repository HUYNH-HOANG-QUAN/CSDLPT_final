# Project Proposal — Distributed ETL Pipeline cho Hệ Thống Giám Sát An Ninh Mạng

**Môn:** Cơ Sở Dữ Liệu Phân Tán
**GVHD:** Lê Hà Thanh
**SVTH:** Hồ Hoàng Quân — work.huynhhoangquan@gmail.com

---

## 1. Mô tả đề tài

Xây dựng hệ thống **ETL phân tán** (Extract-Transform-Load) xử lý log Apache từ 4 cổng web (portal, news, shop, api) với khả năng:

- **Extract**: Đọc và phân tích log Apache từ 4 site (100MB, 578K dòng)
- **Transform**: Tra cứu GeoIP + De-duplication toàn cục
- **Load**: Ghi kết quả vào JSON store

Hệ thống được thiết kế theo lý thuyết **Shared-Nothing Architecture** và **Partitioned Aggregation** từ sách *Özsu & Valduriez — Principles of Distributed Database Systems, 4th Edition*.

---

## 2. Mục tiêu nghiên cứu

1. **Hiệu suất**: Tận dụng đa核 CPU qua Multiprocessing để tăng throughput
2. **Chịu lỗi**: Fault tolerance với checkpoint, retry, idempotent write
3. **Đánh giá**: Đo Speedup, Scaleup, Sizeup theo tiêu chuẩn O&V
4. **Validation**: Đảm bảo toàn vẹn dữ liệu 100%

---

## 3. Các vấn đề kỹ thuật cần giải quyết

### 3.1. Bottleneck GeoIP (CPU-bound)

GeoIP lookup là tác vụ CPU-bound. Python GIL chặn ThreadPool → chỉ 1 core chạy thật.

**Giải pháp**: `ProcessPoolExecutor` — mỗi process có interpreter riêng → bypass GIL → true parallelism.

### 3.2. Bottleneck Extract (I/O-bound)

Regex parsing log Apache là I/O-bound → N reducer không ảnh hưởng đến tốc độ.

**Kết quả sweep**: LOG mode (721 IPs) → N=1..6 cho cùng ~7.9s. CSV mode (550K IPs) → N=6 tốt nhất.

### 3.3. Global De-duplication

Cùng IP xuất hiện ở nhiều site → chỉ lưu 1 bản ghi duy nhất.

**Giải pháp**: Hash Partitioning → cùng IP luôn đến cùng reducer → dedup cục bộ trước, merge sau.

### 3.4. Fault Tolerance

Reducer có thể chết đột ngột (OOM, segfault, kill signal).

**Giải pháp**: Orphan cleanup + process timeout + partial retry + exponential backoff + sequential fallback.

---

## 4. Kiến trúc hệ thống

```
Phase A: Extract + Shuffle          Phase B: Reduce + Transform     Phase C: Load
  [portal] → Extractor[1] ─┐           Reducer[0]: dedup+GeoIP → file_0.json
  [news]   → Extractor[2] ─┼──→ hash(IP) % N ──→ [ProcessPool]
  [shop]   → Extractor[3] ─┤             Reducer[N]: dedup+GeoIP → file_N.json
  [api]    → Extractor[4] ─┘                                               │
                                                                              ▼
                                                              coordinator.merge()
                                                                      │
                                                              central_store.json
```

**Stack công nghệ:**
| Thành phần | Công nghệ |
|-----------|-----------|
| Ngôn ngữ | Python 3.10+ |
| GeoIP | MaxMind GeoLite2 (.mmdb) |
| Concurrency | concurrent.futures |
| Monitoring | psutil |
| Storage | CSV + JSON |

---

## 5. Tiến độ thực hiện

| Tuần | Nội dung |
|------|----------|
| 1-2 | Setup môi trường, dataset, Extract phase |
| 3-4 | Hash Partitioning + Shuffle + Phase B cơ bản |
| 5-6 | GeoIP Multiprocessing + Dedup |
| 7-8 | Fault Tolerance + Validation |
| 9-10 | Benchmark (Speedup/Scaleup/Sizeup) + Tối ưu |
| 11-12 | Viết báo cáo + Slide defense |

---

## 6. Tài liệu tham khảo

1. M. Tamer Özsu & Patrick Valduriez. *Principles of Distributed Database Systems*, 4th Edition. Springer, 2020.
2. MaxMind. GeoLite2 Database. https://dev.maxmind.com/geoip/
3. Python concurrent.futures — ProcessPoolExecutor documentation.
