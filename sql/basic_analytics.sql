-- ============================================================================
-- Day 4 — SQL phân tích cơ bản (chạy trên BRONZE bằng DuckDB)
--
-- Bronze giữ raw: mọi cột là TEXT, có dòng lỗi (amount="NaN", timestamp sai...).
-- Vì chưa có Silver, ta cast PHÒNG THỦ: TRY_CAST -> giá trị lỗi thành NULL và tự
-- rời khỏi các phép tính. Việc làm sạch chính thức + quarantine là ở Silver (Day 6-7).
--
-- Các view 'bronze_transactions' / 'bronze_merchants' do runner
-- (src/analytics/run_basic_analytics.py) tạo từ parquet trước khi chạy file này.
-- Kỹ năng thể hiện: CTE - GROUP BY - JOIN - Window Function.
-- ============================================================================

-- setup: view 'tx' đã cast + đặt tên cột rõ ràng, dùng lại cho mọi query bên dưới.
CREATE OR REPLACE VIEW tx AS
SELECT
    transaction_id,
    customer_id,
    account_id,
    merchant_id,
    device_id,
    channel,
    transaction_type,
    currency,
    status,
    country,
    city,
    -- LƯU Ý: DuckDB cast 'NaN' -> float NaN (KHÔNG phải NULL), và NaN > 0 = TRUE nên NaN sẽ lọt qua
    -- WHERE và đầu độc SUM. Dùng isfinite() để ép cả chuỗi-lỗi LẪN NaN/inf về NULL -> tự rời đúng cách.
    CASE WHEN isfinite(TRY_CAST(amount AS DOUBLE)) THEN TRY_CAST(amount AS DOUBLE) END AS amount,
    TRY_CAST(transaction_time AS TIMESTAMP) AS event_time    -- "not_a_timestamp" -> NULL
FROM bronze_transactions;


-- name: Q0 - Data quality peek (đếm bad rows còn sót lại trong Bronze)
SELECT
    COUNT(*)                                                              AS total_rows,
    SUM(CASE WHEN isfinite(TRY_CAST(amount AS DOUBLE)) THEN 0 ELSE 1 END) AS bad_amount,
    SUM(CASE WHEN TRY_CAST(transaction_time AS TIMESTAMP) IS NULL THEN 1 ELSE 0 END) AS bad_timestamp,
    SUM(CASE WHEN transaction_id = '' THEN 1 ELSE 0 END)                  AS empty_txn_id
FROM bronze_transactions;


-- name: Q1 - Tổng giao dịch và giá trị theo ngày (+ running total bằng Window)
SELECT
    CAST(event_time AS DATE)                                        AS txn_date,
    COUNT(*)                                                       AS txn_count,
    ROUND(SUM(amount))                                            AS total_amount_vnd,
    ROUND(SUM(SUM(amount)) OVER (ORDER BY CAST(event_time AS DATE))) AS running_total_vnd
FROM tx
WHERE amount > 0 AND event_time IS NOT NULL
GROUP BY txn_date
ORDER BY txn_date
LIMIT 15;


-- name: Q2 - Kênh giao dịch phổ biến nhất (count, giá trị, % share)
SELECT
    channel,
    COUNT(*)                                              AS txn_count,
    ROUND(SUM(amount))                                    AS total_amount_vnd,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)    AS pct_of_txn
FROM tx
WHERE amount > 0
GROUP BY channel
ORDER BY txn_count DESC;


-- name: Q3 - Top 10 merchant theo doanh số (JOIN lấy tên + RANK Window)
SELECT
    RANK() OVER (ORDER BY SUM(t.amount) DESC) AS rnk,
    t.merchant_id,
    m.merchant_name,
    m.category,
    COUNT(*)                                  AS txn_count,
    ROUND(SUM(t.amount))                       AS total_amount_vnd
FROM tx t
JOIN bronze_merchants m ON t.merchant_id = m.merchant_id
WHERE t.amount > 0 AND t.merchant_id <> ''
GROUP BY t.merchant_id, m.merchant_name, m.category
ORDER BY total_amount_vnd DESC
LIMIT 10;


-- name: Q4 - Giao dịch có amount bất thường so với lịch sử KH (z-score bằng Window)
WITH z AS (
    SELECT
        customer_id,
        transaction_id,
        transaction_type,
        amount,
        event_time,
        (amount - AVG(amount) OVER (PARTITION BY customer_id))
            / NULLIF(STDDEV_SAMP(amount) OVER (PARTITION BY customer_id), 0) AS amount_zscore,
        COUNT(*) OVER (PARTITION BY customer_id) AS customer_txn_count
    FROM tx
    WHERE amount > 0
)
SELECT
    customer_id,
    transaction_id,
    transaction_type,
    ROUND(amount)        AS amount_vnd,
    ROUND(amount_zscore, 2) AS amount_zscore
FROM z
WHERE amount_zscore > 4          -- cao bất thường so với chính KH đó
  AND customer_txn_count >= 10   -- bỏ KH quá ít giao dịch (z-score không ổn định)
ORDER BY amount_zscore DESC
LIMIT 15;


-- name: Q5 - Tỷ lệ giao dịch thất bại theo nhóm merchant (feed mart_merchant_risk)
SELECT
    m.category,
    COUNT(*)                                                             AS txn_count,
    ROUND(100.0 * SUM(CASE WHEN t.status = 'failed' THEN 1 ELSE 0 END) / COUNT(*), 2) AS fail_pct
FROM tx t
JOIN bronze_merchants m ON t.merchant_id = m.merchant_id
WHERE t.merchant_id <> ''
GROUP BY m.category
ORDER BY fail_pct DESC;
