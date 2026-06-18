# Financial Transaction Data Lakehouse — Kế hoạch Project (18 ngày)

## 1. Tên project
**Financial Transaction Data Lakehouse for Fraud & Risk Analytics**

## 2. Mục tiêu
Xây dựng hệ thống Data Engineering xử lý dữ liệu giao dịch tài chính/ngân hàng/fintech theo kiến trúc **Lakehouse** (Bronze → Silver → Gold), tập trung vào: data modeling, data quality, quarantine bad records, orchestration bằng Airflow, và một phần Spark/Delta để giữ đúng chuẩn Lakehouse.

Kỹ năng thể hiện: SQL · Python · Pandas · Seaborn · Airflow · PySpark · Delta Lake · Data Modeling · Data Quality · Quarantine.

## 3. Bài toán & câu hỏi nghiệp vụ
Mô phỏng bài toán dữ liệu trong ngân hàng, ví điện tử hoặc fintech. Hệ thống cần trả lời:

1. Tổng số giao dịch và tổng giá trị giao dịch theo ngày.
2. Kênh giao dịch phổ biến nhất: mobile, web, ATM, POS, QR.
3. Khách hàng nào có hành vi giao dịch bất thường.
4. Merchant nào có tỷ lệ giao dịch thất bại hoặc rủi ro cao.
5. Giao dịch nào có số tiền bất thường so với lịch sử khách hàng.
6. Khách hàng nào dùng thiết bị mới / vị trí mới bất thường.
7. Dữ liệu lỗi, duplicate hoặc đến muộn được xử lý thế nào.
8. Có thể tạo feature table cho fraud detection / credit risk không.

## 4. Kiến trúc tổng thể

```
CSV / API / PostgreSQL / Streaming Events
        ↓
[Bronze Layer]  raw_transactions · raw_customers · raw_accounts ·
                raw_devices · raw_exchange_rates · raw_login_events
        ↓
[Silver Layer]  clean_transactions · clean_customers · clean_accounts ·
                clean_devices · clean_exchange_rates
        ↓
[Quarantine]    quarantine_bad_records · quarantine_duplicate_transactions ·
                quarantine_invalid_amount · quarantine_invalid_timestamp
        ↓
[Gold Layer]    fact_transaction · dim_customer · dim_account · dim_merchant ·
                dim_device · dim_location · dim_date
                mart_daily_transaction · mart_fraud_features ·
                mart_customer_risk · mart_merchant_risk · mart_payment_channel
```

## 5. Data model

### Bảng nguồn
| Bảng | Ý nghĩa |
|---|---|
| transactions | Giao dịch chuyển tiền, thanh toán, rút tiền |
| customers | Hồ sơ khách hàng |
| accounts | Tài khoản ngân hàng / ví |
| merchants | Đơn vị chấp nhận thanh toán |
| cards | Thẻ ghi nợ / tín dụng |
| devices | Thiết bị giao dịch |
| locations | Quốc gia, tỉnh, thành phố |
| exchange_rates | Tỷ giá |
| fraud_labels | Nhãn gian lận (nếu có) |
| customer_events | Login, đổi mật khẩu, thêm thiết bị, thêm người nhận |

### Cột quan trọng — `transactions`
`transaction_id` · `customer_id` · `account_id` · `merchant_id` · `device_id` · `amount` · `currency` · `transaction_type` · `channel` · `transaction_time` · `country` · `city` · `status` · `is_fraud`

### Star schema (grain: 1 dòng fact = 1 transaction)
- **Fact:** `fact_transaction`
- **Dim:** `dim_customer` · `dim_account` · `dim_merchant` · `dim_device` · `dim_location` · `dim_date`

## 6. Công nghệ sử dụng
| Thành phần | Công nghệ |
|---|---|
| Database | PostgreSQL hoặc SQLite |
| Batch processing | Python, Pandas |
| Orchestration | Airflow (Docker, image chính thức) |
| Big data | Spark / PySpark |
| Lakehouse | Local Delta Lake hoặc Databricks Community |
| Storage | CSV, JSON, Parquet, Delta |
| Analytics | SQL, Spark SQL |
| Visualization | Seaborn, Notebook |
| Data quality | SQL checks + Python validation |
| Notification | Email / Telegram giả lập |
| Version control | GitHub |

## 7. Lịch trình 18 ngày
4 giai đoạn: Bronze (1–4) → Lõi modeling/DQ/Gold (5–10) → Airflow (11–14) → Spark/Delta + đóng gói (15–18). Buffer: Ngày 14 và Ngày 18.

### Giai đoạn 1 — Bronze + Analytics (Ngày 1–4)
| Ngày | Việc cần làm | Output |
|---|---|---|
| 1 | Chốt scope fraud/risk. Tạo repo + cấu trúc folder + README nháp. Thiết kế bảng nguồn (transactions, customers, accounts, merchants, devices) | `README.md`, `docs/project_scope.md`, `sql/source_schema.sql` |
| 2 | Script đọc CSV + API giả lập (tỷ giá/merchant) bằng Python/Pandas, có logging | `src/extract/extract_csv.py`, `extract_api.py` |
| 3 | Load raw vào PostgreSQL/Parquet Bronze + metadata (`ingestion_time`, `source_system`, `batch_id`, `file_name`). EDA nhanh bằng Pandas/Seaborn | `data/bronze/`, `notebooks/eda_finance.ipynb` |
| 4 | SQL phân tích cơ bản: tổng giao dịch/ngày, theo kênh, top merchant, giao dịch lớn bất thường | `sql/basic_analytics.sql` |

### Giai đoạn 2 — Silver, Data Quality, Star Schema, Gold (Ngày 5–10)
| Ngày | Việc cần làm | Output |
|---|---|---|
| 5 | Xác định grain + thiết kế Star Schema (fact_transaction, dim_customer, dim_account, dim_merchant, dim_device, dim_date) | `docs/grain_design.md`, `sql/star_schema.sql` |
| 6 | Transform Bronze → Silver: ép kiểu amount/timestamp, chuẩn hóa currency, channel, transaction_type | `src/transform/bronze_to_silver.py` |
| 7 | Data quality checks (null, duplicate, amount âm, timestamp sai, FK không tồn tại) | `src/quality/quality_checks.py` |
| 8 | Đưa bad records vào quarantine, lưu lý do lỗi | `data/quarantine/` |
| 9 | Build Gold fact/dim tables | `src/transform/build_gold.py` |
| 10 | Viết 4 mart cốt lõi ⭐: daily_transaction, customer_risk, merchant_risk, fraud_features | `sql/marts/` |

### Giai đoạn 3 — Airflow, Error Handling, Patching (Ngày 11–14)
| Ngày | Việc cần làm | Output |
|---|---|---|
| 11 | Cài Airflow bằng Docker Compose (image chính thức) | `docker-compose.yml` |
| 12 | DAG lõi: extract → load bronze → validate → transform silver → build gold marts → send report | `dags/finance_batch_pipeline.py` |
| 13 | Thêm retry/timeout/logging/notification giả lập + inject bad data, kiểm tra quarantine không làm fail toàn pipeline | DAG hoàn chỉnh, `src/utils/notify.py`, `docs/bad_data_handling.md` |
| 14 | **BUFFER.** Nếu dư: demo patching/backfill nhẹ (rerun 1 partition) + docs Airflow. Nếu thiếu: bù việc trễ | `docs/patching_demo.md`, `docs/airflow_setup.md` |

### Giai đoạn 4 — Spark/Delta demo + Đóng gói (Ngày 15–18)
| Ngày | Việc cần làm | Output |
|---|---|---|
| 15 | Convert một job Pandas → PySpark (bronze_to_silver) | `spark/bronze_to_silver_spark.py` |
| 16 | **[MUST-KEEP]** Tạo Bronze/Silver/Gold dưới dạng Delta table + partition theo `transaction_date` | Delta tables |
| 17 | README hoàn chỉnh + architecture diagram + data model diagram + demo script | final docs |
| 18 | **BUFFER.** Quay video demo / chụp screenshot cho CV-GitHub, dọn repo | demo assets |

> **Thứ tự hi sinh nếu trượt lịch:** (1) bỏ patching Ngày 14 → (2) bỏ PySpark job Ngày 15 (giữ build_gold bằng Pandas/SQL vẫn hợp lệ). **Luôn giữ Delta tables Ngày 16** để bảo chứng tên "Lakehouse".

## 8. Data quality rules
| Rule | Ý nghĩa |
|---|---|
| `transaction_id` không null | Mỗi giao dịch phải có ID |
| Không duplicate `transaction_id` | Tránh tính trùng tiền |
| `amount > 0` | Giao dịch không được âm |
| `transaction_time` hợp lệ | Không ở tương lai quá xa |
| `customer_id` tồn tại | Tránh giao dịch mồ côi |
| `account_id` tồn tại | Đảm bảo FK đúng |
| `currency` hợp lệ | VND, USD, EUR |
| `channel` hợp lệ | mobile, web, atm, pos, qr |
| `device_id` hợp lệ | Phục vụ phân tích rủi ro thiết bị |
| `country`/`city` không bất thường | Phục vụ location risk |

## 9. Gold marts
Ưu tiên 4 mart ⭐ trước; ba mart còn lại làm khi dư thời gian.

| Mart | Mục đích |
|---|---|
| `mart_daily_transaction` ⭐ | Tổng số giao dịch, tổng tiền theo ngày |
| `mart_channel_performance` | Phân tích mobile/web/ATM/POS/QR |
| `mart_customer_risk` ⭐ | Khách hàng có hành vi bất thường |
| `mart_merchant_risk` ⭐ | Merchant có thất bại/rủi ro cao |
| `mart_fraud_features` ⭐ | Feature table cho fraud detection |
| `mart_velocity_features` | Số giao dịch trong 1h/24h gần nhất |
| `mart_location_risk` | Giao dịch khác quốc gia/tỉnh bất thường |

## 10. Airflow DAG

```
start → extract_transactions → load_bronze → validate_bronze →
transform_silver → validate_silver → build_gold_marts →
send_quality_report → end
```

Yêu cầu: chạy end-to-end; task fail thì retry + log rõ bước lỗi; bad data vào quarantine, không làm fail pipeline; xuất quality report.

## 11. Feature table cho fraud/risk — `mart_fraud_features`
| Feature | Ý nghĩa |
|---|---|
| `txn_amount` | Số tiền giao dịch |
| `avg_amount_7d` | Trung bình giao dịch 7 ngày |
| `amount_zscore` | Mức bất thường so với lịch sử |
| `txn_count_1h` | Số giao dịch trong 1 giờ |
| `txn_count_24h` | Số giao dịch trong 24 giờ |
| `unique_merchants_24h` | Số merchant khác nhau trong 24 giờ |
| `unique_devices_7d` | Số thiết bị dùng trong 7 ngày |
| `is_new_device` | Thiết bị mới |
| `is_new_location` | Địa điểm mới |
| `failed_txn_count_24h` | Số giao dịch fail trong 24 giờ |
| `night_transaction_flag` | Giao dịch ban đêm |
| `high_amount_flag` | Giao dịch giá trị cao |
| `cross_country_flag` | Giao dịch khác quốc gia |
| `velocity_risk_score` | Điểm rủi ro theo tốc độ giao dịch |

## 12. Cấu trúc GitHub

```
finance-transaction-lakehouse/
├── README.md
├── docker-compose.yml
├── requirements.txt
├── .env.example
├── data/
│   ├── raw/  bronze/  silver/  gold/  quarantine/  bad_data_samples/
├── sql/
│   ├── source_schema.sql  star_schema.sql  quality_checks.sql
│   ├── basic_analytics.sql
│   └── marts/
├── src/
│   ├── extract/  load/  transform/  quality/  utils/
├── dags/
│   └── finance_batch_pipeline.py
├── spark/
│   ├── bronze_to_silver_spark.py
│   ├── build_gold_spark.py        # optional nếu kịp
│   ├── stream_transactions.py     # Future Work
│   └── benchmark_spark.py         # Future Work
├── notebooks/
│   └── eda_finance.ipynb
├── docs/
│   ├── architecture.md  data_modeling.md  data_quality_report.md
│   ├── patching_demo.md  benchmark.md  business_questions.md
└── images/
    ├── architecture_diagram.png  star_schema.png  airflow_dag.png
```

## 13. Mô tả project cho CV
> **Financial Transaction Data Lakehouse for Fraud & Risk Analytics** — Built an end-to-end financial transaction data lakehouse using SQL, Python, Airflow, PySpark, Databricks and Delta Lake. Designed Bronze/Silver/Gold layers, implemented data quality checks, bad record quarantine, retry/backfill workflows, and analytical marts for transaction monitoring, customer risk, merchant risk, payment channel analytics and fraud feature engineering.

## 14. Future Work
- Real-time ingestion với Spark Structured Streaming (ghi Bronze streaming).
- Performance benchmarking Pandas vs Spark trên 1M / 5M / 10M dòng.
- Partition tuning nâng cao (theo channel, country) + Delta `OPTIMIZE` / `Z-ORDER`.
- CI/CD cho pipeline và data quality tests.
