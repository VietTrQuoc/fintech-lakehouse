# Marts & Analytics (Day 10 / Day 4) hoạt động thế nào?

> Bộ tài liệu cùng loại: [Data Quality (Day 7)](quality_checks_explained.md) · [Quarantine (Day 8)](quarantine_explained.md) · [Gold (Day 9)](gold_explained.md) · **Marts & Analytics**.
> Code: [`src/marts/build_marts.py`](../src/marts/build_marts.py) + [`sql/marts/*.sql`](../sql/marts) · Day 4: [`src/analytics/run_basic_analytics.py`](../src/analytics/run_basic_analytics.py).
> Kết quả tự sinh: [`data/gold/marts/marts_summary.json`](../data/gold/marts/marts_summary.json), `data/gold/marts/*.parquet`, và các **bảng mart nằm luôn trong `gold.duckdb`**.

---

## 1. Data mart là gì? (hình dung)

Nếu **Gold** là **kho nguyên liệu đã sơ chế** (sạch, tổng quát, ai cũng dùng được nhưng phải tự nấu), thì **mart** là **món ăn nấu sẵn cho một thực khách cụ thể**:

- *"Báo cáo theo ngày"* → dọn ra là xem ngay, không phải `GROUP BY` lại từ 2 triệu dòng.
- *"Danh sách khách rủi ro"*, *"merchant đáng ngờ"*, *"bảng đặc trưng cho mô hình ML"*…

Mart là bảng **tổng hợp/biến đổi sẵn** cho một mục đích phân tích. Nó **đánh đổi** dung lượng lưu trữ lấy tốc độ truy vấn và sự tiện dụng.

**Cách build:** [`build_marts.py`](../src/marts/build_marts.py) chạy thẳng các câu SQL `CREATE OR REPLACE TABLE mart_x AS ...` **trên chính `gold.duckdb`** (đã có fact + 6 dim, FK-validated từ Day 9), rồi `COPY` mỗi mart ra Parquet. Vì vậy mart tồn tại ở **cả hai nơi**: bảng trong DuckDB và file `.parquet`.

---

## 2. Bốn data mart

### ① mart_daily_transaction — grain = 1 ngày (180 dòng)
[`sql/marts/mart_daily_transaction.sql`](../sql/marts/mart_daily_transaction.sql). Trả lời: mỗi ngày bao nhiêu giao dịch, tổng tiền, theo `status`, gian lận/ngày, số khách riêng biệt, và **running total** (luỹ kế) qua `SUM() OVER (ORDER BY date_key)`.

> Vì sao 180 dòng mà dim_date có 243? Vì chỉ **180 ngày thực sự có giao dịch** (dữ liệu trải Dec 2025 → Jun 2026); các ngày còn lại trong lịch không có dòng fact nào.

### ② mart_customer_risk — grain = 1 khách (19.988 dòng)
[`sql/marts/mart_customer_risk.sql`](../sql/marts/mart_customer_risk.sql). Mỗi khách một dòng tổng hợp: `txn_count`, tổng/trung bình tiền, `fraud_count`/`fraud_rate`, số thiết bị & địa điểm riêng biệt, `failed_rate`, **z-score chi tiêu lớn nhất**, **velocity** (số giao dịch nhiều nhất trong 1 giờ), và cờ `watchlist_flag`.

`watchlist_flag = TRUE` nếu khách thoả **bất kỳ** điều kiện: có giao dịch gian lận, hoặc `fraud_rate ≥ 5%`, hoặc `max z-score ≥ 4`, hoặc ≥ 10 giao dịch/giờ, hoặc dùng ≥ 5 thiết bị.

> ⚠️ Mart này `GROUP BY customer_id` (**business key**), KHÔNG theo `customer_sk` — vì sk là per-version (SCD2), nhóm theo sk sẽ xé một khách thành nhiều dòng. Join `customer_sk ↔ customer_id` qua dim_customer là 1:1 nên không nhân dòng.

### ③ mart_merchant_risk — grain = 1 merchant (2.000 dòng)
[`sql/marts/mart_merchant_risk.sql`](../sql/marts/mart_merchant_risk.sql). Mỗi merchant: `txn_count`, tổng tiền, `failed_rate`, `fraud_rate`, số khách riêng biệt. Có `WHERE merchant_sk <> -1` để **loại các giao dịch không có merchant** (chuyển khoản/rút/nạp) — chỉ còn merchant thật.

### ④ mart_fraud_features — grain = 1 giao dịch (1.973.027 dòng)
[`sql/marts/mart_fraud_features.sql`](../sql/marts/mart_fraud_features.sql). Đây là **bảng đặc trưng (feature table) cho mô hình phát hiện gian lận** — nặng nhất, chạy cuối. Mỗi giao dịch kèm ~15 feature tính theo hành vi *của riêng khách đó* theo thời gian.

> ⚠️ Có `WHERE customer_sk <> -1` để loại **2.999 orphan customer** (nếu giữ, cả 2.999 khách bị gộp vào một "partition UNKNOWN" → feature rác). Đó là lý do 1.973.027 = 1.976.026 − 2.999.

---

## 3. Feature engineering trong mart_fraud_features

Vài kỹ thuật đáng chú ý:

- **z-score chi tiêu** — "giao dịch này lệch bao nhiêu lần độ lệch chuẩn so với mức chi trung bình 7 ngày của chính khách":
  `amount_zscore = (amount − avg_amount_7d) / std_amount_7d`. Một con số 3.5 nghĩa là *bất thường lớn*.
- **Window theo thời gian thực** (`RANGE BETWEEN INTERVAL '24 hours' PRECEDING AND CURRENT ROW`): đếm `txn_count_1h`, `txn_count_24h`, `failed_txn_count_24h` trong khung giờ trượt — không phải theo số dòng mà theo **khoảng thời gian thật**.
- **Thủ thuật LAG-SUM** đếm distinct trong window: DuckDB không có `COUNT(DISTINCT) OVER` (và `array_agg OVER` thì ngốn 12GB RAM → OOM). Cách lách: đánh dấu mỗi merchant/thiết bị ở **lần đầu xuất hiện trong window** rồi `SUM` các "lần đầu" đó → ra `unique_merchants_24h`, `unique_devices_7d`.
- **Các cờ nhị phân**: `is_new_device`, `is_new_location`, `night_transaction_flag` (0–5h), `high_amount_flag` (> 46 triệu ₫), `cross_country_flag` (nước giao dịch ≠ nước nhà).
- **`velocity_risk_score`** — điểm rủi ro tổng hợp có trọng số: `0.5 × (tần suất 24h) + 0.4 × (z-score chi tiêu) + 0.1 × (số lần thất bại 24h)`, mỗi thành phần bị chặn trần để không một yếu tố nào áp đảo.

### Feature có "sáng" không? — feature_separation

[`feature_separation()`](../src/marts/build_marts.py#L100) tính trung bình các feature **theo từng `fraud_pattern`** để chứng minh feature **phân biệt được** gian lận với hợp lệ. Trích từ [`marts_summary.json`](../data/gold/marts/marts_summary.json):

| fraud_pattern | n | avg z-score | avg new_device | avg cross_country | avg velocity_risk |
|---|---|---|---|---|---|
| amount_spike | 2.800 | **3.46** | 0.015 | 0.008 | 2.178 |
| night_high_amount | 1.680 | **3.76** | 0.013 | 0.004 | 2.136 |
| new_device_location | 2.800 | 2.24 | **0.997** | **0.994** | 1.924 |
| **legit** | 1.959.027 | **−0.01** | 0.013 | 0.007 | 1.361 |
| card_testing | 1.120 | −0.19 | 0.02 | 0.0 | 0.611 |
| velocity_burst | 3.500 | 0.18 | 0.013 | 0.0 | 0.408 |
| cross_country | 2.100 | 0.32 | 0.041 | **0.498** | 0.288 |

Đọc bảng: `amount_spike`/`night_high_amount` có **z-score ~3.5** trong khi `legit` ~0 → feature số tiền bắt đúng nhóm này. `new_device_location` có `is_new_device` và `cross_country` ~**0.99** → đúng bản chất pattern. Mỗi feature "sáng" cho một loại gian lận khác nhau — đúng tinh thần feature engineering. Tổng 6 pattern = **14.000** giao dịch gian lận.

---

## 4. 14 kiểm định (verification)

[`verify()`](../src/marts/build_marts.py#L63) chạy 14 check trên DuckDB; bất kỳ check fail → exit 1.

| Mart | Check | Kết quả thực |
|------|-------|--------------|
| daily | `daily_sum_txn` (tổng txn = fact) | 1.976.026 ✓ |
| daily | `daily_rowcount_180` | 180 ✓ |
| daily | `daily_fraud` | 14.000 ✓ |
| customer | `customer_sum_txn` | 1.976.026 ✓ |
| customer | `customer_rowcount` | 19.988 ✓ |
| customer | `customer_fraud` | 14.000 ✓ |
| customer | `customer_rate_bounds` (mọi rate trong [0,1]) | 0 lệch ✓ |
| merchant | `merchant_rowcount_2000` | 2.000 ✓ |
| merchant | `merchant_sum_txn` | 680.673 ✓ |
| fraud_features | `fraud_features_rowcount` | 1.973.027 ✓ |
| fraud_features | `fraud_features_is_fraud` | 14.000 ✓ |
| fraud_features | `fraud_features_zscore_not_null` | 0 NULL ✓ |
| fraud_features | `fraud_features_count_monotonic` (1h ≤ 24h) | 0 vi phạm ✓ |
| fraud_features | `fraud_features_txn_unique` | 0 trùng ✓ |

> Các check `*_sum_txn`/`fraud` là **bảo toàn**: tổng tổng hợp lại của mart phải khớp số fact gốc → bằng chứng phép `GROUP BY`/`JOIN` không nhân hay làm rớt dòng. `count_monotonic` bắt lỗi logic window (đếm 1 giờ không thể lớn hơn đếm 24 giờ).

---

## 5. Day 4 — Analytics cơ bản trên Bronze

[`run_basic_analytics.py`](../src/analytics/run_basic_analytics.py) là bước **thăm dò sớm**, chạy **trước khi có Gold**:

- DuckDB **đọc thẳng Parquet Bronze** (không cần nạp vào DB): tạo view `bronze_transactions`, `bronze_merchants` từ file.
- Đọc [`sql/basic_analytics.sql`](../sql/basic_analytics.sql), tách theo `;`. Câu `CREATE/SET` chạy thầm; câu `SELECT/WITH` chạy và **in kết quả ra console** kèm nhãn lấy từ comment `-- name: ...`.
- Mục đích: nhìn nhanh dữ liệu thô (tổng quan giao dịch, theo merchant…) để hiểu dữ liệu sớm. **Không ghi file**, chỉ in ra màn hình — khác hẳn các mart Day 10 (tạo bảng + parquet bền vững).

```bash
python -m src.analytics.run_basic_analytics
```

---

## 6. Cách chạy (Marts)

```bash
python -m src.marts.build_marts
```

- **Exit 0** = 4 mart dựng xong + 14 check PASS. **Exit 1** = có check fail.
- Sản phẩm: bảng mart trong `gold.duckdb` **và** `data/gold/marts/*.parquet` + `marts_summary.json`.
- Log: [`logs/build_marts.log`](../logs/build_marts.log).

---

## 7. Tóm tắt một câu

> Day 10 chạy SQL thẳng trên `gold.duckdb` để dựng **4 data mart ăn-liền** — daily (180), customer_risk (19.988), merchant_risk (2.000), fraud_features (1.973.027) — với **feature engineering** (z-score, window thời gian thực, LAG-SUM đếm distinct, velocity score), chứng minh feature phân biệt được gian lận, và chạy **14 kiểm định bảo toàn**; còn Day 4 chỉ là bước **thăm dò nhanh trên Bronze** in kết quả ra màn hình.
