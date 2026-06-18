-- ============================================================================
-- Day 10 — mart_merchant_risk (grain = 1 MERCHANT). Đọc từ Gold star schema.
--
-- Mục đích: chấm điểm rủi ro từng merchant để cảnh báo gian lận/đối tác xấu —
-- tỷ lệ giao dịch thất bại, tỷ lệ gian lận, quy mô dòng tiền chạy qua merchant.
--
-- ⚠️ WHERE merchant_sk <> -1: loại các giao dịch KHÔNG gắn merchant (chuyển khoản /
--    rút tiền / nạp tiền) và các FK mồ côi (orphan) vốn đều trỏ về Unknown (sk = -1).
--    Nhờ vậy mart chỉ tổng hợp trên những giao dịch CÓ merchant thật.
-- ============================================================================
CREATE OR REPLACE TABLE mart_merchant_risk AS
SELECT
    m.merchant_id, m.merchant_name, m.category, m.city, m.country, m.risk_score,  -- thuộc tính merchant lấy từ dim
    COUNT(*)                                                          AS txn_count,         -- tổng số giao dịch qua merchant
    ROUND(SUM(f.amount_vnd), 2)                                       AS total_amount_vnd,  -- tổng dòng tiền (VND)
    SUM(CASE WHEN f.status = 'failed' THEN 1 ELSE 0 END)              AS failed_count,      -- số giao dịch thất bại
    ROUND(1.0 * SUM(CASE WHEN f.status = 'failed' THEN 1 ELSE 0 END) / COUNT(*), 4) AS failed_rate,  -- tỷ lệ thất bại (1.0* để ép chia thực, tránh chia nguyên)
    SUM(f.is_fraud)                                                   AS fraud_count,       -- số giao dịch gian lận
    ROUND(1.0 * SUM(f.is_fraud) / COUNT(*), 4)                        AS fraud_rate,        -- tỷ lệ gian lận
    COUNT(DISTINCT f.customer_sk)                                     AS distinct_customers -- số khách hàng khác nhau (đo độ phủ)
FROM fact_transaction f
JOIN dim_merchant m ON f.merchant_sk = m.merchant_sk      -- nối fact với dim_merchant theo surrogate key (1:1)
WHERE f.merchant_sk <> -1                                 -- bỏ Unknown: chỉ giữ giao dịch có merchant thật
GROUP BY m.merchant_id, m.merchant_name, m.category, m.city, m.country, m.risk_score  -- gộp theo từng merchant
ORDER BY fraud_rate DESC, failed_rate DESC;               -- merchant rủi ro cao xếp lên đầu
