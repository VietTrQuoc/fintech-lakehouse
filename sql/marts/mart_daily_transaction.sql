-- ============================================================================
-- Day 10 — mart_daily_transaction (grain = 1 NGÀY). Đọc từ Gold star schema.
--
-- Mục đích: bảng tổng hợp theo ngày phục vụ báo cáo/biểu đồ xu hướng —
-- tổng số giao dịch, tổng tiền, tách theo trạng thái (success/failed/pending),
-- số giao dịch gian lận, số khách hoạt động, và lũy kế dòng tiền (running total).
-- ============================================================================
CREATE OR REPLACE TABLE mart_daily_transaction AS
WITH daily AS (   -- B1: gộp fact về mức NGÀY (date_key) trước khi nối nhãn lịch từ dim_date
    SELECT
        date_key,
        COUNT(*)                                                             AS txn_count,        -- tổng giao dịch trong ngày
        ROUND(SUM(amount_vnd), 2)                                            AS total_amount_vnd, -- tổng giá trị (VND)
        SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END)                  AS success_count,    -- số giao dịch thành công
        SUM(CASE WHEN status = 'failed'  THEN 1 ELSE 0 END)                  AS failed_count,     -- số giao dịch thất bại
        SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END)                  AS pending_count,    -- số giao dịch chờ xử lý
        ROUND(SUM(CASE WHEN status = 'success' THEN amount_vnd ELSE 0 END), 2) AS success_amount_vnd, -- tiền của riêng giao dịch thành công
        SUM(is_fraud)                                                        AS fraud_count,      -- số giao dịch gian lận
        ROUND(SUM(CASE WHEN is_fraud = 1 THEN amount_vnd ELSE 0 END), 2)     AS fraud_amount_vnd, -- giá trị bị gian lận
        COUNT(DISTINCT customer_sk)                                          AS distinct_customers, -- số khách hoạt động trong ngày
        ROUND(AVG(amount_vnd), 2)                                            AS avg_amount_vnd    -- giá trị trung bình mỗi giao dịch
    FROM fact_transaction
    GROUP BY date_key
)
SELECT
    d.date_key, d.full_date, d.year, d.month, d.is_weekend, d.is_payday,   -- nhãn lịch lấy từ dim_date (thứ/tuần, ngày trả lương...)
    x.txn_count, x.total_amount_vnd, x.success_count, x.failed_count, x.pending_count,
    x.success_amount_vnd, x.fraud_count, x.fraud_amount_vnd, x.distinct_customers, x.avg_amount_vnd,
    ROUND(SUM(x.total_amount_vnd) OVER (ORDER BY d.date_key), 2)             AS running_total_vnd  -- lũy kế tiền theo ngày (window cộng dồn)
FROM daily x
JOIN dim_date d ON x.date_key = d.date_key   -- gắn thuộc tính lịch cho từng ngày
ORDER BY d.date_key;                          -- xếp theo thời gian để running total đúng thứ tự
