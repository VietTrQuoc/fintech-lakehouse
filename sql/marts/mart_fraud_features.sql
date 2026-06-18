-- ============================================================================
-- Day 10 — mart_fraud_features (grain = 1 GIAO DỊCH). Bảng đặc trưng (feature table)
-- phục vụ huấn luyện/đánh giá mô hình phát hiện gian lận.
--
-- Ý tưởng: với mỗi giao dịch, tính các đặc trưng hành vi tại THỜI ĐIỂM giao dịch xảy ra
-- (chi tiêu lệch chuẩn, tần suất 1h/24h, số merchant/thiết bị mới gần đây, cờ đêm/xuyên quốc gia...)
-- — toàn bộ bằng window function trên star schema.
--
-- ⚠️ Window tính theo KHÁCH: PARTITION BY customer_id (nối fact.customer_sk → dim_customer.customer_id),
--    KHÔNG dùng customer_sk (là khoá per-version của SCD2 → sẽ chia nhỏ lịch sử 1 khách).
-- ⚠️ WHERE customer_sk <> -1: loại 2.999 giao dịch orphan; nếu giữ, 2.999 khách mồ côi sẽ dồn
--    chung vào một partition "Unknown" làm các feature theo khách thành rác.
-- ⚠️ ORDER BY (event_time, transaction_id): thêm transaction_id để phá hoà (tie-break) cho ~11k
--    dòng trùng đúng từng giây — bảo đảm thứ tự xác định, kết quả ổn định giữa các lần chạy.
-- ⚠️ unique_*_window dùng kỹ thuật LAG-SUM (xem CTE lagged/flagged/w_distinct): DuckDB KHÔNG hỗ trợ
--    COUNT(DISTINCT ...) OVER, còn array_agg() OVER trên 2M dòng sẽ ngốn ~12GB RAM (OOM).
-- ============================================================================
CREATE OR REPLACE TABLE mart_fraud_features AS
WITH base AS (   -- nền: giao dịch + thuộc tính khách/địa điểm cần cho việc tính đặc trưng
    SELECT
        f.transaction_id, c.customer_id, c.home_country, l.country AS txn_country,
        f.device_sk, f.merchant_sk, f.event_time, f.amount_vnd, f.status, f.is_fraud, f.fraud_pattern
    FROM fact_transaction f
    JOIN dim_customer c ON f.customer_sk = c.customer_sk
    LEFT JOIN dim_location l ON f.location_sk = l.location_sk   -- LEFT: giao dịch thiếu địa điểm vẫn giữ lại
    WHERE f.customer_sk <> -1                                   -- bỏ orphan để partition theo khách không bị nhiễu
),
w_simple AS (   -- nhóm đặc trưng cộng dồn theo cửa sổ thời gian (RANGE INTERVAL — tính theo thời gian, không theo số dòng)
    SELECT
        transaction_id, customer_id, home_country, txn_country, device_sk, event_time,
        amount_vnd, status, is_fraud, fraud_pattern,
        AVG(amount_vnd) OVER w7d                                       AS avg_amount_7d,          -- chi tiêu trung bình 7 ngày
        STDDEV_SAMP(amount_vnd) OVER w7d                              AS std_amount_7d,          -- độ lệch chuẩn 7 ngày (để tính z-score)
        COUNT(*) OVER w1h                                             AS txn_count_1h,           -- số giao dịch trong 1 giờ trước
        COUNT(*) OVER w24h                                            AS txn_count_24h,          -- số giao dịch trong 24 giờ trước
        SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) OVER w24h  AS failed_txn_count_24h    -- số lần thất bại trong 24 giờ
    FROM base
    -- RANGE frame chỉ dùng được với 1 cột ORDER BY (event_time). Các dòng trùng giây rơi vào cùng frame
    -- (đúng nghĩa "trong N giờ", không bị phụ thuộc thứ tự dòng).
    WINDOW
        w1h  AS (PARTITION BY customer_id ORDER BY event_time
                 RANGE BETWEEN INTERVAL '1 hour'   PRECEDING AND CURRENT ROW),
        w24h AS (PARTITION BY customer_id ORDER BY event_time
                 RANGE BETWEEN INTERVAL '24 hours' PRECEDING AND CURRENT ROW),
        w7d  AS (PARTITION BY customer_id ORDER BY event_time
                 RANGE BETWEEN INTERVAL '7 days'   PRECEDING AND CURRENT ROW)
),
w_first AS (    -- cờ "lần đầu xuất hiện" trong toàn bộ lịch sử khách: thiết bị mới / quốc gia mới
    SELECT
        transaction_id,
        -- ROW_NUMBER = 1 cho lần ĐẦU TIÊN khách dùng thiết bị này → đánh dấu is_new_device
        CASE WHEN ROW_NUMBER() OVER (PARTITION BY customer_id, device_sk   ORDER BY event_time, transaction_id) = 1 THEN 1 ELSE 0 END AS is_new_device,
        CASE WHEN ROW_NUMBER() OVER (PARTITION BY customer_id, txn_country ORDER BY event_time, transaction_id) = 1 THEN 1 ELSE 0 END AS is_new_location
    FROM base
),
lagged AS (     -- mốc thời gian LẦN TRƯỚC khách gặp cùng merchant/thiết bị (nền cho "đếm distinct trong cửa sổ")
    SELECT transaction_id, customer_id, event_time, merchant_sk, device_sk,
        LAG(event_time) OVER (PARTITION BY customer_id, merchant_sk ORDER BY event_time, transaction_id) AS prev_merch_t,
        LAG(event_time) OVER (PARTITION BY customer_id, device_sk   ORDER BY event_time, transaction_id) AS prev_dev_t
    FROM base
),
flagged AS (    -- đánh dấu merchant/thiết bị "mới xuất hiện trong cửa sổ": lần đầu (prev IS NULL) hoặc cách lần trước quá ngưỡng
    SELECT customer_id, transaction_id, event_time,
        CASE WHEN prev_merch_t IS NULL OR event_time - prev_merch_t > INTERVAL '24 hours' THEN 1 ELSE 0 END AS new_merch_24h,
        CASE WHEN prev_dev_t   IS NULL OR event_time - prev_dev_t   > INTERVAL '7 days'   THEN 1 ELSE 0 END AS new_dev_7d
    FROM lagged
),
w_distinct AS ( -- LAG-SUM: cộng các cờ "mới xuất hiện" trong cửa sổ ≈ số merchant/thiết bị KHÁC NHAU trong cửa sổ
    SELECT transaction_id,
        SUM(new_merch_24h) OVER (PARTITION BY customer_id ORDER BY event_time
            RANGE BETWEEN INTERVAL '24 hours' PRECEDING AND CURRENT ROW) AS unique_merchants_24h,
        SUM(new_dev_7d) OVER (PARTITION BY customer_id ORDER BY event_time
            RANGE BETWEEN INTERVAL '7 days' PRECEDING AND CURRENT ROW)   AS unique_devices_7d
    FROM flagged
)
SELECT
    s.transaction_id, s.customer_id, s.event_time, s.is_fraud, s.fraud_pattern,   -- khoá + nhãn (label) cho mô hình
    s.amount_vnd                                                       AS txn_amount,        -- giá trị giao dịch
    ROUND(s.avg_amount_7d, 2)                                          AS avg_amount_7d,
    -- z-score chi tiêu theo nền 7 ngày; COALESCE(...,0) cho trường hợp std = 0 (chưa đủ lịch sử)
    ROUND(COALESCE((s.amount_vnd - s.avg_amount_7d) / NULLIF(s.std_amount_7d, 0), 0.0), 4) AS amount_zscore,
    s.txn_count_1h,
    s.txn_count_24h,
    d.unique_merchants_24h,
    d.unique_devices_7d,
    fst.is_new_device,
    fst.is_new_location,
    s.failed_txn_count_24h,
    CASE WHEN EXTRACT(hour FROM s.event_time) BETWEEN 0 AND 5 THEN 1 ELSE 0 END   AS night_transaction_flag, -- giao dịch ban đêm (0–5h)
    CASE WHEN s.amount_vnd > 46000000 THEN 1 ELSE 0 END                          AS high_amount_flag,       -- giá trị lớn (>46tr ≈ ngưỡng ~2000 USD)
    CASE WHEN s.txn_country IS DISTINCT FROM s.home_country THEN 1 ELSE 0 END     AS cross_country_flag,     -- giao dịch khác quốc gia gốc của khách
    -- velocity_risk_score: điểm rủi ro tổng hợp 0..~3.95, trộn 3 thành phần có trọng số,
    --   LEAST/GREATEST để chặn trần và bỏ giá trị âm → điểm bị giới hạn, không bị outlier kéo lệch.
    ROUND(
        0.5 * LEAST(s.txn_count_24h / 10.0, 3.0)                                                              -- 50%: mật độ giao dịch 24h (trần 3)
      + 0.4 * LEAST(GREATEST(COALESCE((s.amount_vnd - s.avg_amount_7d) / NULLIF(s.std_amount_7d, 0), 0), 0), 5.0) -- 40%: mức lệch chi tiêu (0..5)
      + 0.1 * LEAST(s.failed_txn_count_24h, 10.0)                                                             -- 10%: số lần thất bại 24h (trần 10)
    , 3)                                                               AS velocity_risk_score
FROM w_simple s
JOIN w_first    fst USING (transaction_id)   -- ghép các nhóm đặc trưng lại theo transaction_id
JOIN w_distinct d   USING (transaction_id);
