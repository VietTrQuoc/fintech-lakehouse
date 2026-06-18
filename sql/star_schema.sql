-- ============================================================================
-- Day 5 — Star Schema DDL (tầng Gold)
--
-- Mô hình chiều cho fact_transaction. Thiết kế chi tiết: docs/grain_design.md.
-- Grain: 1 dòng fact = 1 giao dịch (1 transaction_id).
-- Dialect: DuckDB / ANSI. Surrogate key (*_sk) do ETL gán (Day 9), KHÔNG auto-increment.
-- Mỗi dimension có 1 dòng "Unknown" (sk = -1) cho FK mồ côi / không áp dụng.
--
-- Thứ tự: tạo dimension trước, fact sau (vì fact có FOREIGN KEY trỏ tới dim).
-- ============================================================================

DROP TABLE IF EXISTS fact_transaction;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS dim_customer;
DROP TABLE IF EXISTS dim_account;
DROP TABLE IF EXISTS dim_merchant;
DROP TABLE IF EXISTS dim_device;
DROP TABLE IF EXISTS dim_location;


-- ----------------------------------------------------------------------------
-- dim_date  (static, sinh lịch)
-- ----------------------------------------------------------------------------
CREATE TABLE dim_date (
    date_key    INTEGER  PRIMARY KEY,   -- YYYYMMDD, ví dụ 20260115
    full_date   DATE     NOT NULL,
    year        SMALLINT NOT NULL,
    quarter     SMALLINT NOT NULL,
    month       SMALLINT NOT NULL,
    month_name  VARCHAR  NOT NULL,
    day         SMALLINT NOT NULL,
    day_of_week SMALLINT NOT NULL,      -- 1=Thứ 2 .. 7=Chủ nhật
    day_name    VARCHAR  NOT NULL,
    is_weekend  BOOLEAN  NOT NULL,
    is_payday   BOOLEAN  NOT NULL       -- ngày trong {1, 2, 14, 15}
);


-- ----------------------------------------------------------------------------
-- dim_customer  (SCD Type 2 — 1 dòng / phiên bản khách hàng)
-- ----------------------------------------------------------------------------
CREATE TABLE dim_customer (
    customer_sk  BIGINT    PRIMARY KEY, -- surrogate key (mỗi phiên bản 1 sk)
    customer_id  VARCHAR   NOT NULL,    -- business key (KHÔNG unique: nhiều phiên bản)
    full_name    VARCHAR,
    email        VARCHAR,
    phone        VARCHAR,
    dob          DATE,
    signup_date  DATE,
    home_city    VARCHAR,
    home_country VARCHAR,
    risk_tier    VARCHAR,
    kyc_level    VARCHAR,
    valid_from   TIMESTAMP NOT NULL,    -- hiệu lực từ
    valid_to     TIMESTAMP NOT NULL,    -- hiệu lực đến (dòng hiện hành = 9999-12-31)
    is_current   BOOLEAN   NOT NULL     -- chỉ đúng 1 dòng is_current=true cho mỗi customer_id
);


-- ----------------------------------------------------------------------------
-- dim_account  (SCD Type 1 — ghi đè, không giữ lịch sử)
-- ----------------------------------------------------------------------------
CREATE TABLE dim_account (
    account_sk   BIGINT  PRIMARY KEY,
    account_id   VARCHAR NOT NULL,
    customer_id  VARCHAR,
    account_type VARCHAR,
    opened_date  DATE,
    status       VARCHAR,
    currency     VARCHAR
);


-- ----------------------------------------------------------------------------
-- dim_merchant  (SCD Type 1)
-- ----------------------------------------------------------------------------
CREATE TABLE dim_merchant (
    merchant_sk    BIGINT  PRIMARY KEY,
    merchant_id    VARCHAR NOT NULL,
    merchant_name  VARCHAR,
    category       VARCHAR,
    city           VARCHAR,
    country        VARCHAR,
    onboarded_date DATE,
    risk_score     DOUBLE
);


-- ----------------------------------------------------------------------------
-- dim_device  (SCD Type 1)
-- ----------------------------------------------------------------------------
CREATE TABLE dim_device (
    device_sk   BIGINT  PRIMARY KEY,
    device_id   VARCHAR NOT NULL,
    customer_id VARCHAR,
    device_type VARCHAR,
    os          VARCHAR,
    app_version VARCHAR,
    first_seen  DATE,
    is_trusted  BOOLEAN
);


-- ----------------------------------------------------------------------------
-- dim_location  (static — bảng tham chiếu)
-- ----------------------------------------------------------------------------
CREATE TABLE dim_location (
    location_sk BIGINT  PRIMARY KEY,
    location_id VARCHAR NOT NULL,
    country     VARCHAR,
    city        VARCHAR,
    region      VARCHAR,
    lat         DOUBLE,
    lon         DOUBLE,
    timezone    VARCHAR
);


-- ----------------------------------------------------------------------------
-- fact_transaction  (grain: 1 dòng = 1 giao dịch)
-- ----------------------------------------------------------------------------
CREATE TABLE fact_transaction (
    transaction_id        VARCHAR  NOT NULL,   -- degenerate dimension (khóa nghiệp vụ)
    -- surrogate FK tới các dimension --------------------------------------
    date_key              INTEGER  NOT NULL REFERENCES dim_date(date_key),
    customer_sk           BIGINT   NOT NULL REFERENCES dim_customer(customer_sk),
    account_sk            BIGINT   NOT NULL REFERENCES dim_account(account_sk),
    merchant_sk           BIGINT   NOT NULL REFERENCES dim_merchant(merchant_sk), -- -1 nếu không có merchant
    device_sk             BIGINT   NOT NULL REFERENCES dim_device(device_sk),
    location_sk           BIGINT   NOT NULL REFERENCES dim_location(location_sk),
    -- thuộc tính degenerate / cardinality thấp ---------------------------
    channel               VARCHAR,
    transaction_type      VARCHAR,
    status                VARCHAR,
    currency              VARCHAR,
    fraud_pattern         VARCHAR,
    -- measures (chỉ số) --------------------------------------------------
    amount_original       DOUBLE,              -- theo currency gốc (chỉ additive trong cùng currency)
    exchange_rate         DOUBLE,              -- VND / 1 đơn vị currency tại ngày giao dịch (non-additive)
    amount_vnd            DOUBLE,              -- = amount_original * exchange_rate (FULLY ADDITIVE — measure chính)
    is_fraud              SMALLINT,            -- 0/1; SUM = số giao dịch gian lận
    -- mốc thời gian ------------------------------------------------------
    event_time            TIMESTAMP,           -- lúc giao dịch xảy ra
    ingestion_time        TIMESTAMP,           -- lúc nạp dữ liệu
    ingestion_lag_seconds BIGINT,              -- event -> ingestion (phân tích trễ / backfill)
    PRIMARY KEY (transaction_id)
);


-- ----------------------------------------------------------------------------
-- Member Unknown (sk = -1): cho FK mồ côi hoặc trường hợp không áp dụng.
-- ETL phải chèn các dòng này TRƯỚC khi nạp fact (để FK hợp lệ).
-- ----------------------------------------------------------------------------
INSERT INTO dim_customer (customer_sk, customer_id, valid_from, valid_to, is_current)
VALUES (-1, 'UNKNOWN', TIMESTAMP '1900-01-01 00:00:00', TIMESTAMP '9999-12-31 00:00:00', TRUE);

INSERT INTO dim_account (account_sk, account_id)   VALUES (-1, 'UNKNOWN');
INSERT INTO dim_merchant (merchant_sk, merchant_id) VALUES (-1, 'UNKNOWN');
INSERT INTO dim_device (device_sk, device_id)       VALUES (-1, 'UNKNOWN');
INSERT INTO dim_location (location_sk, location_id) VALUES (-1, 'UNKNOWN');
-- Lưu ý: bad timestamp đã bị quarantine ở Silver nên fact luôn có date_key hợp lệ
-- -> dim_date không cần member Unknown.
