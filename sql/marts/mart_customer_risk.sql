-- ============================================================================
-- Day 10 — mart_customer_risk (grain = 1 KHÁCH = customer_id). Đọc từ Gold star schema.
--
-- Mục đích: lập hồ sơ rủi ro từng khách hàng và gắn cờ watchlist (danh sách theo dõi)
-- dựa trên HÀNH VI thực tế (gian lận, chi tiêu bất thường, velocity, số thiết bị),
-- bổ sung cho risk_tier tĩnh từ KYC.
--
-- ⚠️ Gộp theo customer_id (business key), KHÔNG theo customer_sk: vì dim_customer là SCD2,
--    một khách có thể có NHIỀU surrogate key (nhiều phiên bản lịch sử) → gộp theo sk sẽ
--    tách 1 khách thành nhiều dòng. Quan hệ fact.customer_sk → dim_customer là 1:1 (không nở dòng).
-- ============================================================================
CREATE OR REPLACE TABLE mart_customer_risk AS
WITH fc AS (   -- B1: fact + customer_id (business key) + z-score chi tiêu tính riêng theo từng khách
    SELECT
        c.customer_id, f.amount_vnd, f.is_fraud, f.status, f.device_sk, f.location_sk, f.event_time,
        -- z-score = (giá trị - trung bình của khách) / độ lệch chuẩn của khách → đo mức "lệch" so với thói quen.
        -- NULLIF(std, 0): khách chỉ có 1 giao dịch thì std = 0 → tránh chia cho 0 (trả NULL thay vì lỗi).
        (f.amount_vnd - AVG(f.amount_vnd) OVER (PARTITION BY c.customer_id))
            / NULLIF(STDDEV_SAMP(f.amount_vnd) OVER (PARTITION BY c.customer_id), 0) AS amount_zscore
    FROM fact_transaction f
    JOIN dim_customer c ON f.customer_sk = c.customer_sk      -- nối 1:1 theo sk, không làm nở dòng
),
velo AS (   -- B2: velocity — số giao dịch nhiều nhất trong 1 giờ trượt của mỗi khách (dấu hiệu bot/tấn công)
    SELECT customer_id, MAX(cnt_1h) AS max_txn_count_1h
    FROM (
        SELECT customer_id,
            -- đếm giao dịch trong cửa sổ thời gian 1 giờ liền trước (RANGE theo event_time, không phải N dòng)
            COUNT(*) OVER (PARTITION BY customer_id ORDER BY event_time
                RANGE BETWEEN INTERVAL '1 hour' PRECEDING AND CURRENT ROW) AS cnt_1h
        FROM fc
    )
    GROUP BY customer_id   -- lấy đỉnh velocity của từng khách
),
agg AS (   -- B3: các chỉ số tổng hợp theo từng khách
    SELECT
        customer_id,
        COUNT(*)                                                          AS txn_count,           -- tổng số giao dịch
        ROUND(SUM(amount_vnd), 2)                                         AS total_amount_vnd,    -- tổng chi tiêu
        ROUND(AVG(amount_vnd), 2)                                         AS avg_amount_vnd,      -- chi tiêu trung bình
        SUM(is_fraud)                                                     AS fraud_count,         -- số giao dịch gian lận
        ROUND(1.0 * SUM(is_fraud) / COUNT(*), 4)                          AS fraud_rate,          -- tỷ lệ gian lận
        COUNT(DISTINCT device_sk)                                         AS distinct_devices,    -- số thiết bị khác nhau (nhiều = đáng ngờ)
        COUNT(DISTINCT location_sk)                                       AS distinct_locations,  -- số địa điểm khác nhau
        SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END)                AS failed_count,        -- số giao dịch thất bại
        ROUND(1.0 * SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) / COUNT(*), 4) AS failed_rate, -- tỷ lệ thất bại
        ROUND(MAX(amount_zscore), 2)                                      AS max_amount_zscore    -- giao dịch lệch chuẩn nhất của khách
    FROM fc
    GROUP BY customer_id    -- ⭐ gộp theo KHÁCH (customer_id), không theo sk
)
SELECT
    a.*, v.max_txn_count_1h, cur.risk_tier, cur.home_city, cur.home_country,
    -- watchlist_flag: bật nếu khách "dính" BẤT KỲ tiêu chí rủi ro nào dưới đây
    --   có gian lận | tỷ lệ gian lận ≥ 5% | chi tiêu lệch ≥ 4 std | ≥ 10 giao dịch/giờ | dùng ≥ 5 thiết bị
    CASE WHEN a.fraud_count > 0 OR a.fraud_rate >= 0.05 OR a.max_amount_zscore >= 4
              OR v.max_txn_count_1h >= 10 OR a.distinct_devices >= 5
         THEN TRUE ELSE FALSE END                                        AS watchlist_flag
FROM agg a
LEFT JOIN velo v ON a.customer_id = v.customer_id
LEFT JOIN dim_customer cur ON a.customer_id = cur.customer_id AND cur.is_current   -- chỉ lấy phiên bản hiện hành (trạng thái hiện tại) của khách
ORDER BY a.fraud_count DESC, a.total_amount_vnd DESC;   -- khách rủi ro/giá trị cao lên đầu
