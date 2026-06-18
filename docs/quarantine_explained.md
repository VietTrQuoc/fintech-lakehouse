# Quarantine (Day 8) hoạt động thế nào?

> Bộ tài liệu cùng loại: [Data Quality (Day 7)](quality_checks_explained.md) · **Quarantine (Day 8)** · [Gold (Day 9)](gold_explained.md) · [Marts & Analytics (Day 10/Day 4)](marts_explained.md).
> Code: [`src/quality/quarantine.py`](../src/quality/quarantine.py). Kết quả tự sinh: [`data/quarantine/quarantine_summary.json`](../data/quarantine/quarantine_summary.json).

---

## 1. Hình dung bằng một ví dụ

Tưởng tượng một **bưu điện phân loại thư**:

- Ở khâu trước (Day 6), mỗi lá thư đã được **dán nhãn**: "đủ điều kiện gửi" hoặc "có vấn đề — loại gì". Day 7 chỉ *đếm và báo cáo* số nhãn.
- **Day 8 mới THẬT SỰ chia thư ra các thùng:**
  - Thư tốt → thùng **clean**, đi tiếp tới Gold.
  - Thư lỗi → một trong **4 ngăn cách ly** (quarantine), kèm tờ ghi chú "vì sao bị giữ lại".
- Cuối cùng có một nhân viên **đếm lại**: số thư vào (Silver) phải đúng bằng số thư tốt + thư cách ly — **không được thất lạc lá nào**.

Điểm cốt lõi: **Day 8 không phán xét lại thư nào tốt/xấu.** Nó chỉ chia theo nhãn đã có sẵn từ Day 6 (`_is_valid`, `_dq_bucket`). Một nguồn sự thật duy nhất, không tính lại rule → tránh sai lệch giữa các bước.

---

## 2. Đầu vào → đầu ra

| | Nơi | Nội dung |
|--|-----|----------|
| **Vào** | `data/silver/transactions/*` | Silver (mỗi dòng có `_is_valid`, `_dq_bucket`, các cờ `_dq_*`) |
| **Ra (1)** | `data/silver/clean_transactions/*` | Dòng **hợp lệ**, sẵn sàng cho Gold (chia theo tháng) |
| **Ra (2)** | `data/quarantine/<bucket>/*` | Dòng **lỗi**, gom theo 4 bucket + lý do |
| **Ra (3)** | `data/quarantine/quarantine_summary.json` | Tổng kết + 7 kiểm tra bảo toàn |

---

## 3. Quy tắc chia dòng (quan trọng nhất)

Code quyết định nằm gọn ở [`split_partition()`](../src/quality/quarantine.py#L82):

```python
clean_df      = df.loc[df["_is_valid"], CLEAN_KEEP_COLS]          # valid -> clean
bucket_frames = {b: df.loc[df["_dq_bucket"] == b] for b in BUCKETS}  # mỗi bucket lấy dòng của nó
```

- **clean** = mọi dòng có `_is_valid = True`.
- **bucket** = mọi dòng có `_dq_bucket` trùng tên bucket.

4 bucket ([`BUCKETS`](../src/quality/quarantine.py#L45)): `quarantine_invalid_amount`, `quarantine_invalid_timestamp`, `quarantine_duplicate_transactions`, `quarantine_bad_records`.

### Cạm bẫy đã tránh: "soft orphan FK"

FK mồ côi (giao dịch trỏ tới khách/tài khoản/merchant không tồn tại) **có bật cờ** `_dq_fk_*`, **nhưng `_is_valid = True`**. Nếu chia theo *"bất kỳ cờ `_dq_*` nào bật"* thì các dòng này sẽ bị quarantine oan. Vì vậy luật chỉ dựa vào `_is_valid`/`_dq_bucket`:

> **Orphan FK đi vào clean** (số tiền giao dịch vẫn thật, cần cho phân tích) và **mang theo cờ `_dq_fk_*`** để **Day 9** biết mà trỏ về "Unknown" (`sk = -1`).

### Tại sao clean chỉ giữ 28 cột?

[`CLEAN_KEEP_COLS`](../src/quality/quarantine.py#L55) cố ý **bỏ** các cờ hard / `_dq_bucket` / `_dq_errors` / `_is_valid` — ở tập đã-hợp-lệ chúng toàn `False`/rỗng, chỉ là nhiễu. **Giữ lại** 3 cờ `_dq_fk_*` (cho Day 9) + cột nghiệp vụ + lineage.

---

## 4. Cách ghi file

- **clean**: ghi **ngay theo từng tháng** (`clean_transactions_2026-01.parquet`…) → giữ nguyên cách chia partition.
- **quarantine**: gom **xuyên tháng**, mỗi bucket **1 file**. Lý do gom: dòng lỗi ít, không cần chia nhỏ.
- **Bucket rỗng vẫn ghi 1 file rỗng-có-schema** → bước sau (Day 9/15) `glob` luôn tìm thấy dataset, không vỡ.
- [`reset_output_dirs()`](../src/quality/quarantine.py#L71) **xoá sạch thư mục trước khi ghi** → chạy lại (re-run) cho kết quả xác định, không sót file cũ.

Mỗi dòng quarantine được [`build_quarantine_frame()`](../src/quality/quarantine.py#L94) thêm **4 cột lý do/lineage lên đầu bảng** cho người điều tra:

| Cột | Ý nghĩa |
|-----|---------|
| `quarantine_bucket` | dòng này thuộc bucket nào (kể cả khi gộp file) |
| `quarantine_reason` | bản sao `_dq_errors` — *vì sao* bị giữ |
| `quarantined_at` | thời điểm tách |
| `quarantine_run_id` | lần chạy quarantine nào tạo ra |

---

## 5. 7 kiểm tra bảo toàn (verify)

Sau khi ghi xong, [`verify()`](../src/quality/quarantine.py#L174) **đọc lại output** và kiểm tra. Bất kỳ check nào fail → `overall_passed = False` → CLI exit 1 (Airflow sẽ báo đỏ).

| # | Check | Hỏi gì | Kết quả thực |
|---|-------|--------|--------------|
| 1 | `conservation_sum_eq_silver` | clean + quarantine == silver? (không mất/nhân dòng) | 1.976.026 + 23.974 = 2.000.000 ✓ |
| 2 | `clean_eq_valid_from_report` | clean có khớp số "valid" mà Day 7 báo? | 1.976.026 = 1.976.026 ✓ |
| 3 | `buckets_match_report` | số mỗi bucket khớp report Day 7? | khớp ✓ |
| 4 | `no_valid_in_quarantine` | có lẫn dòng hợp lệ vào quarantine không? | 0 ✓ |
| 5 | `reason_non_empty` | mọi dòng quarantine có ghi lý do? | 0 dòng rỗng ✓ |
| 6 | `bucket_purity` | mỗi file chỉ chứa đúng 1 loại bucket? | thuần ✓ |
| 7 | `transaction_id_unique_in_clean` | ID trong clean có thật sự duy nhất? | 0 trùng ✓ |

> Check 1–3 là **đối chiếu chéo**: kết quả tách thật phải khớp với con số Day 7 đã báo cáo — bằng chứng không có dòng nào "bốc hơi" giữa hai bước.

### Số liệu thực tế

```
Silver 2.000.000  =  Clean 1.976.026 (98,80%)  +  Quarantine 23.974

Bucket:  quarantine_bad_records            7.996
         quarantine_invalid_amount         6.990
         quarantine_invalid_timestamp      4.992
         quarantine_duplicate_transactions 3.996
```

---

## 6. Cách chạy

```bash
python -m src.quality.quarantine
```

- **Exit 0** = bảo toàn, sang được Day 9. **Exit 1** = có check fail → dừng.
- Entrypoint cho Airflow: [`run_quarantine_split()`](../src/quality/quarantine.py#L105).
- Log: [`logs/quarantine.log`](../logs/quarantine.log).

---

## 7. Tóm tắt một câu

> Day 8 **chia Silver thành clean + 4 bucket quarantine chỉ dựa trên nhãn `_is_valid`/`_dq_bucket` của Day 6** (không phán xét lại), giữ orphan FK ở clean để Day 9 map Unknown, đính kèm lý do cho mỗi dòng lỗi, rồi **đếm lại để chứng minh không thất lạc dòng nào** (clean + quarantine = silver).
