# Data Quality Check hoạt động thế nào?

> Bộ tài liệu cùng loại: **Data Quality (Day 7)** · [Quarantine (Day 8)](quarantine_explained.md) · [Gold (Day 9)](gold_explained.md) · [Marts & Analytics (Day 10/Day 4)](marts_explained.md).
> File này **giải thích cơ chế** của bước kiểm tra chất lượng (Day 7).
> File kết quả tự sinh sau mỗi lần chạy là [data_quality_report.md](data_quality_report.md) (đừng sửa tay — sẽ bị ghi đè).
> Code: [`src/quality/quality_checks.py`](../src/quality/quality_checks.py).

---

## 1. Hình dung bằng một ví dụ đời thường

Hãy tưởng tượng một **nhà máy đóng gói**:

- **Day 6 (Silver)** = từng công nhân trên dây chuyền. Mỗi sản phẩm đi qua, họ **dán nhãn lỗi** lên nó: "móp", "thiếu nắp", "sai mã"… Mỗi sản phẩm (mỗi *dòng* giao dịch) được soi và dán cờ riêng. Các cờ này tên là `_dq_*` (ví dụ `_dq_invalid_amount`, `_dq_duplicate`).
- **Day 7 (Quality Check)** = **người quản đốc cuối ca**. Ông ấy **không soi lại từng sản phẩm** nữa. Ông gom toàn bộ nhãn lại và hỏi 2 loại câu hỏi:
  1. *"Tỷ lệ hàng móp có vượt mức cho phép không?"* → cảnh báo nếu nhiều bất thường.
  2. *"Số sản phẩm đầu ca có khớp số cuối ca không? Có bị mất/nhân đôi không?"* → đây là lỗi **nghiêm trọng** của dây chuyền.

Nói ngắn gọn: **Day 6 dán cờ từng dòng — Day 7 chỉ TỔNG HỢP cờ thành các "check" cấp bảng, cộng vài kiểm tra toàn vẹn, rồi xuất báo cáo.** Day 7 **chỉ đọc, không sửa** dữ liệu. Việc tách dòng lỗi ra để cách ly là của **Day 8 (Quarantine)**.

---

## 2. Hai mức nghiêm trọng: WARNING và ERROR

Đây là ý tưởng quan trọng nhất. Mỗi check có một "mức độ" (severity):

| Mức | Nghĩa là gì | Có làm pipeline DỪNG không? |
|-----|-------------|------------------------------|
| **WARNING** | Dữ liệu **bẩn từ nguồn** — đã biết trước, là bình thường (sai số tiền, trùng ID, FK mồ côi…). Day 8 sẽ cách ly nó. | ❌ **Không** — pipeline vẫn chạy tiếp |
| **ERROR** | Lỗi **cấu trúc / lỗi code transform** — mất dòng, sai schema, đo lường rỗng, chọn sai bản sống sót. | ✅ **Có** — pipeline FAIL ngay |

**Tại sao tách 2 mức?** Vì *"dữ liệu bẩn"* và *"code sai"* là hai chuyện hoàn toàn khác nhau:
- Dữ liệu bẩn là **điều ta mong đợi** trong tài chính thật (gõ nhầm, API lỗi…). Nếu cứ thấy dòng bẩn là dừng pipeline thì sẽ dừng suốt ngày → vô dụng. Ta chỉ **cảnh báo** rồi để Day 8 dọn.
- Còn nếu Silver bỗng **mất 5.000 dòng** so với Bronze, hoặc cột `amount_vnd` bị rỗng ở dòng hợp lệ → đó là **bug của chính pipeline**, phải dừng và sửa ngay.

👉 Quy tắc kết luận: **chỉ ERROR mới làm pipeline thất bại. WARNING không.**

---

## 3. Ba nhóm kiểm tra

Mỗi check là một đối tượng [`Check`](../src/quality/quality_checks.py#L89) gồm: tên, nhóm, mức, mô tả, **observed** (số đo được), **threshold** (ngưỡng), **comparator** (`<=`, `>=`, `==`), và **passed** (đạt hay không).

Cách chấm điểm chung cực kỳ đơn giản — so observed với threshold qua comparator:

```
observed <= threshold ?   (vd: tỷ lệ lỗi PHẢI nhỏ hơn ngưỡng)
observed >= threshold ?   (vd: tỷ lệ hợp lệ PHẢI lớn hơn ngưỡng)
observed == threshold ?   (vd: số dòng vi phạm PHẢI bằng 0)
```

### Nhóm A — "Tỷ lệ lỗi" (row-flag), mức WARNING

Gom từng cờ `_dq_*` của hàng triệu dòng thành **một tỷ lệ %**, rồi so với ngưỡng cho phép.
Code: [`build_row_flag_checks()`](../src/quality/quality_checks.py#L147).

> Ví dụ: `rate_invalid_amount` = (số dòng có cờ `_dq_invalid_amount`) ÷ (tổng số dòng).
> Đo được **0.35%**, ngưỡng **≤ 1.00%** → **PASS**.

Ngưỡng được đặt khoảng **2–3 lần tỷ lệ kỳ vọng** (lấy từ `config.error_rates`), để chừa biên độ dao động giữa các tháng/seed. Nếu tỷ lệ lỗi **vượt ngưỡng** → có gì đó bất thường ở nguồn (cần xem lại), nhưng **vẫn chỉ là WARNING**.

Nhóm này cũng có check tổng: **`valid_rate`** — tỷ lệ dòng "sạch hoàn toàn" phải **≥ 95%** (thực tế đạt **98.80%**).

### Nhóm B — "Toàn vẹn cấu trúc" (integrity), mức ERROR

8 kiểm tra đếm số dòng vi phạm; tất cả **phải bằng 0**. Code: [`build_integrity_checks()`](../src/quality/quality_checks.py#L187).

| Check | Câu hỏi nó trả lời |
|-------|--------------------|
| `rowcount_silver_eq_bronze` | Silver có giữ **đúng số dòng** như Bronze không? (không mất, không nhân đôi) |
| `schema_conformance` | Silver có **đủ tất cả cột** nghiệp vụ + cờ DQ bắt buộc không? |
| `amount_vnd_notnull_when_valid` | Mọi dòng *hợp lệ* có **số tiền VND** (cột đo lường chính) không? |
| `exchange_rate_notnull_when_valid` | Mọi dòng *hợp lệ* có **khớp tỷ giá** không? |
| `exactly_one_survivor_per_group` | Mỗi nhóm ID trùng có **đúng 1 bản được giữ lại** không? |
| `no_dup_among_valid` | Trong tập hợp lệ, `transaction_id` có **thật sự duy nhất** không? |
| `bucket_consistency` | Cờ "hợp lệ" có **khớp** với việc dòng đó không bị xếp vào thùng lỗi không? |
| `event_date_in_window` | Ngày giao dịch của dòng hợp lệ có **nằm trong khoảng thời gian** dự án không? |

> Tất cả 8 check này hiện đo được **0 vi phạm** → PASS. Nếu bất kỳ check nào > 0 → **pipeline FAIL**.

### Nhóm F — "Đối chiếu đáp án" (manifest reconciliation), mức WARNING

Đây là phần thông minh nhất, để **tự kiểm tra xem bộ DQ có thật sự hoạt động đúng không.**
Code: [`reconcile_manifest()`](../src/quality/quality_checks.py#L277).

Khi sinh dữ liệu giả, generator **cố tình chèn lỗi** và ghi lại "đáp án" vào file [`error_manifest.csv`](../data/bad_data_samples/error_manifest.csv): *dòng số X đã bị chèn lỗi loại Y*.

Day 7 lấy đáp án đó so với thực tế DQ bắt được:

> "Tôi đã chèn **7.000** dòng `invalid_amount` — DQ có gắn cờ đủ **7.000** không?"
> Tỷ lệ bắt được gọi là **coverage**, yêu cầu **≥ 99%**. Thực tế hầu hết đạt **100%**.

Nếu coverage thấp → bộ rule DQ có lỗ hổng (bỏ sót lỗi), cần sửa rule. Đây là cách chứng minh **"DQ của tôi đáng tin"**.

---

## 4. Chấm điểm tổng (overall status)

Sau khi chạy hết các check, [`run_checks()`](../src/quality/quality_checks.py#L317) ra một trong ba kết luận:

| Trạng thái | Khi nào | Pipeline |
|-----------|---------|----------|
| **PASS** | Không ERROR fail **và** không WARNING fail | ✅ đi tiếp |
| **PASS_WITH_WARNINGS** | Không ERROR fail, nhưng **có WARNING** vượt ngưỡng | ✅ đi tiếp (có ghi chú) |
| **FAIL** | **Có ít nhất 1 ERROR** fail | ❌ dừng, phải sửa |

`overall_passed = (số ERROR fail == 0)`. Nói cách khác: **WARNING không bao giờ làm rớt**, chỉ ERROR mới làm rớt.

---

## 5. Báo cáo xuất ra những gì

Mỗi lần chạy ghi ra **2 file** (cùng nội dung, 2 định dạng):

1. **[`data/quality/quality_report.json`](../data/quality/quality_report.json)** — bản máy đọc (cho Airflow / dashboard / bước sau dùng).
2. **[`docs/data_quality_report.md`](data_quality_report.md)** — bản người đọc, gồm các phần:
   - **Summary**: Bronze→Silver rows, số dòng valid/invalid, tỷ lệ hợp lệ.
   - **Integrity checks (ERROR)** và **Row-flag rates (WARNING)**: bảng từng check + đạt/không.
   - **Quarantine buckets**: dòng lỗi được chia vào 4 thùng (Day 8 sẽ tách thật):
     `bad_records`, `duplicate_transactions`, `invalid_amount`, `invalid_timestamp`.
   - **Top error codes**: loại lỗi nào nhiều nhất.
   - **Manifest reconciliation**: bảng coverage đối chiếu đáp án.
   - **Kết luận**: có đủ điều kiện sang Day 8 không.

### Một khái niệm dễ nhầm: "soft orphan"

FK mồ côi (giao dịch trỏ tới `customer_id`/`account_id`/`merchant_id` không tồn tại) được coi là **soft** — **KHÔNG bị quarantine**. Lý do: số tiền giao dịch vẫn có thật và cần cho phân tích. Thay vì vứt đi, ở **Day 9 (Gold)** chúng được map về một bản ghi **"Unknown"** trong bảng dimension. Vì vậy report tách riêng dòng `Soft orphan FK` (vd 8.000 dòng) khỏi nhóm bị cách ly.

---

## 6. Cách chạy

```bash
# Chạy trực tiếp (in tóm tắt JSON ra màn hình, trả exit code)
python -m src.quality.quality_checks

# Chế độ nghiêm ngặt: coi cả WARNING vượt ngưỡng cũng là thất bại (dùng khi debug)
python -m src.quality.quality_checks --strict
```

- **Exit code 0** = đạt (sang được Day 8). **Exit code 1** = có ERROR (hoặc WARNING nếu `--strict`) → CI/Airflow sẽ báo đỏ.
- Hàm vào chính cho Airflow gọi: [`run_quality_checks()`](../src/quality/quality_checks.py#L443) — trả về `report` dict.
- Log lưu tại [`logs/quality_checks.log`](../logs/quality_checks.log).

---

## 7. Tóm tắt một câu

> **Day 7 không soi lại từng dòng** (Day 6 đã làm) — nó **gom cờ lỗi thành các check cấp bảng**, phân biệt rõ *dữ liệu bẩn (WARNING, cho qua)* và *lỗi pipeline (ERROR, chặn lại)*, **tự đối chiếu với đáp án** để chứng minh bộ rule đáng tin, rồi **xuất báo cáo JSON + Markdown** quyết định có sang Day 8 hay không.
