# Gold — Star Schema (Day 9) hoạt động thế nào?

> Bộ tài liệu cùng loại: [Data Quality (Day 7)](quality_checks_explained.md) · [Quarantine (Day 8)](quarantine_explained.md) · **Gold (Day 9)** · [Marts & Analytics (Day 10/Day 4)](marts_explained.md).
> Code: [`src/transform/build_gold.py`](../src/transform/build_gold.py) + DDL [`sql/star_schema.sql`](../sql/star_schema.sql).
> Kết quả tự sinh: [`data/gold/gold_summary.json`](../data/gold/gold_summary.json), `data/gold/gold.duckdb`, `data/gold/<dim|fact>/*.parquet`.

---

## 1. Star schema là gì? (hình dung)

Tưởng tượng một **sổ mượn sách thư viện**:

- Mỗi lần ai đó mượn sách = **1 dòng trong bảng trung tâm** (gọi là **fact_transaction** — mỗi dòng = 1 giao dịch).
- Nhưng dòng đó không ghi đầy đủ "ai mượn, sách gì, ngày nào" — nó chỉ ghi **mã tra cứu** trỏ tới các **bảng danh mục** xung quanh: *ai* (khách), *tài khoản nào*, *merchant nào*, *thiết bị*, *địa điểm*, *ngày*. Các bảng danh mục này gọi là **dimension** (dim).

Vẽ ra trông như một ngôi sao: fact ở giữa, các dim toả ra → **"star schema"**. Lợi ích: số liệu (tiền, số giao dịch) nằm gọn ở fact; mô tả nằm ở dim, không lặp lại triệu lần.

**Mục tiêu Day 9:** từ `clean_transactions` (Silver) dựng **6 dim + 1 fact**, gắn khoá, kiểm tra toàn vẹn, rồi nạp vào DuckDB.

---

## 2. Surrogate key (`*_sk`) — vì sao cần khoá nhân tạo?

Mỗi dim có một cột khoá riêng do ETL tự đánh số 1, 2, 3… gọi là **surrogate key** (`customer_sk`, `account_sk`…), tách biệt với **business key** (`customer_id` thật).

Vì sao không dùng thẳng `customer_id`?
- **Cho phép lưu lịch sử (SCD2):** một khách có thể có nhiều phiên bản (đổi địa chỉ, đổi hạng) → mỗi phiên bản cần một khoá riêng, trong khi `customer_id` vẫn giữ nguyên.
- **Ổn định & gọn:** số nguyên join nhanh, không phụ thuộc định dạng id nguồn.
- Code: [`add_surrogate_key()`](../src/transform/build_gold.py#L68) sort rồi đánh số `1..n`.

---

## 3. Sáu dimension

### dim_date — sinh lịch ([`build_dim_date`](../src/transform/build_gold.py#L93))
Không lấy từ dữ liệu mà **tự sinh** mọi ngày từ 2025-12-01 → 2026-07-31 (**243 ngày**). `date_key` dạng số `YYYYMMDD` (vd `20260115`), kèm cờ `is_weekend`, `is_payday` (ngày 1, 2, 14, 15). dim_date **không cần** dòng Unknown vì bad timestamp đã bị quarantine ở Silver.

### 4 dim SCD1 — account / merchant / device / location ([`build_dim_scd1`](../src/transform/build_gold.py#L113))
**SCD Type 1 = ghi đè, không giữ lịch sử.** Chỉ lấy bản mới nhất của mỗi business key (`drop_duplicates`), gán `*_sk`. Đơn giản vì các thực thể này hiếm khi cần truy vết quá khứ.

### dim_customer — SCD Type 2 (giữ lịch sử) ([`build_dim_customer`](../src/transform/build_gold.py#L161))
Đây là phần tinh tế nhất. **SCD Type 2 = mỗi lần khách đổi thuộc tính → tạo một dòng phiên bản mới**, đánh dấu khoảng hiệu lực:

| Cột | Ý nghĩa |
|-----|---------|
| `valid_from` / `valid_to` | phiên bản này có hiệu lực từ khi nào đến khi nào |
| `is_current` | có phải phiên bản hiện hành không (đúng **1 dòng** `True` mỗi khách) |

Cách dựng ([`reconstruct_customer_versions`](../src/transform/build_gold.py#L138)): lấy **snapshot hiện tại** của khách rồi **"tua ngược"** lần lượt các sự kiện thay đổi (`raw_customer_scd_events`) từ mới → cũ để khôi phục từng phiên bản quá khứ. Kết quả: **22.349 phiên bản** cho ~19.990 khách (một số khách có nhiều phiên bản).

### Member "Unknown" (`sk = -1`)
Mỗi dim (trừ dim_date) có **một dòng Unknown** với `sk = -1` ([`prepend_unknown`](../src/transform/build_gold.py#L74)). Khi giao dịch trỏ tới một FK mồ côi (orphan từ Day 8), thay vì **vứt dòng đó đi**, ta cho nó trỏ về Unknown → **giữ được số tiền giao dịch** mà FK vẫn hợp lệ.

---

## 4. fact_transaction — và "point-in-time join"

Grain = **1 dòng / 1 giao dịch**. Code: [`build_fact_partition`](../src/transform/build_gold.py#L202).

Với 4 dim SCD1 + date, việc gán khoá rất thẳng: tra cứu business key → `*_sk`, không thấy thì điền `-1`:
```python
part["merchant_sk"] = part["merchant_id"].map(mer_lk).fillna(UNKNOWN_SK)
```

Với `customer_sk` thì **không thể chỉ lấy phiên bản hiện tại** — phải lấy phiên bản **đúng tại thời điểm giao dịch xảy ra**. Đó là **point-in-time join**, làm bằng [`pd.merge_asof`](../src/transform/build_gold.py#L212):

```python
merged = pd.merge_asof(part, cust_index, left_on="event_time", right_on="valid_from",
                       by="customer_id", direction="backward")
```

> Ví dụ: khách đổi từ hạng `silver` sang `gold` ngày 1/3. Một giao dịch ngày 15/2 sẽ được nối với **phiên bản `silver`** (đúng lúc đó), không phải `gold` hiện tại. Đây là điều khiến dim_customer SCD2 có giá trị thật sự cho phân tích lịch sử.

Cuối cùng nối nhãn gian lận từ `fraud_labels.csv` → `is_fraud` / `fraud_pattern` ([`load_fraud_labels`](../src/transform/build_gold.py#L193), có dedupe để tránh nhân dòng).

---

## 5. Nạp DuckDB + ép khoá ngoại (FK enforce)

[`load_into_duckdb()`](../src/transform/build_gold.py#L302) là **lưới an toàn cuối**:

1. Chạy [`sql/star_schema.sql`](../sql/star_schema.sql) tạo các bảng — fact khai báo `FOREIGN KEY ... REFERENCES dim_x`.
2. **Nạp dim trước** (Unknown `sk=-1` đã được DDL `INSERT` sẵn), **nạp fact sau**.
3. Nếu fact có bất kỳ `*_sk` nào **không tồn tại** trong dim tương ứng → **DuckDB ném lỗi FK** → check `duckdb_fk_enforce` fail → pipeline dừng.

Khác với pandas (tính toán trong bộ nhớ, không ràng buộc), CSDL **bắt buộc** mọi quan hệ phải khớp — bằng chứng cứng rằng star schema toàn vẹn.

---

## 6. 11 kiểm định (verification)

> Lưu ý: bản kế hoạch/PDF ghi "12 check", nhưng **code hiện tại có 11 check** ([`gold_summary.json`](../data/gold/gold_summary.json) liệt kê đúng 11) — tất cả PASS. Danh sách dưới đây là con số thật.

| Check | Hỏi gì | Kết quả thực |
|-------|--------|--------------|
| `fact_rowcount_eq_clean` | fact đúng số dòng clean? | 1.976.026 ✓ |
| `no_null_sk` | không có `*_sk` / `date_key` nào NULL? | 0 ✓ |
| `all_sk_resolve_to_dim` | mọi `*_sk` (kể cả -1) đều có trong dim? | ✓ |
| `date_key_in_dim_date` | mọi `date_key` đều thuộc dim_date? | ✓ |
| `is_fraud_total` | tổng gian lận đúng kỳ vọng? | 14.000 ✓ |
| `fraud_pattern_consistency` | `is_fraud=1` ⟺ có `fraud_pattern`? | nhất quán ✓ |
| `transaction_id_unique` | id giao dịch duy nhất trong fact? | ✓ |
| `one_is_current_per_customer` | mỗi khách đúng 1 phiên bản hiện hành? | 0 vi phạm ✓ |
| `amount_vnd_sum_conserved` | tổng tiền fact == tổng tiền clean? | 8.426.107.524.799 ₫ (≈ 8,43 nghìn tỷ) ✓ |
| `unknown_usage` | đếm dòng trỏ Unknown (thông tin) | xem dưới |
| `duckdb_fk_enforce` | nạp DuckDB không vi phạm FK? | ✓ |

**`unknown_usage` (thông tin, luôn pass):**
```
merchant_sk = -1 : 1.295.353   (giao dịch không có merchant: transfer / withdrawal / topup + orphan)
customer_sk = -1 :     2.999   (orphan customer)
account_sk  = -1 :     2.998
device_sk / location_sk = 0
```
Con số merchant lớn là **bình thường**: rất nhiều giao dịch (chuyển khoản, rút tiền, nạp tiền) vốn dĩ không gắn với merchant nào.

### Số liệu Gold

```
fact_transaction  1.976.026 dòng
dim_date              243        dim_account    24.001
dim_customer       22.349 (phiên bản SCD2)   dim_merchant    2.001
dim_device         30.001        dim_location       18
(mỗi dim gồm cả dòng Unknown sk=-1)
```

---

## 7. Cách chạy

```bash
python -m src.transform.build_gold
```

- **Exit 0** = star schema toàn vẹn (sang Day 10). **Exit 1** = có check fail (gồm FK violation).
- Engine hiện tại = **pandas** (Day 15 dự kiến port sang PySpark).
- Log: [`logs/build_gold.log`](../logs/build_gold.log).

---

## 8. Tóm tắt một câu

> Day 9 biến `clean_transactions` thành **star schema**: 6 dimension (dim_date sinh lịch, 4 dim SCD1 ghi đè, dim_customer SCD2 giữ lịch sử) + fact_transaction; gán **surrogate key**, dùng **point-in-time join** để lấy đúng phiên bản khách tại thời điểm giao dịch, trỏ FK mồ côi về **Unknown (`sk=-1`)** thay vì vứt đi, rồi **nạp DuckDB ép khoá ngoại** và chạy **11 kiểm định** chứng minh không mất dòng / không hỏng quan hệ.
