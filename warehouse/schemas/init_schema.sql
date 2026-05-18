-- =============================================================
-- init_schema.sql
-- BankFlow DWH Gold Layer — Kimball Star Schema
-- PostgreSQL 15 + TimescaleDB
-- =============================================================

-- Enables automatic date-based partitioning (splits big tables into chunks)
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;


-- =============================================================
-- 1. DATE DIMENSION
-- =============================================================
-- Stores weekday names, holidays, fiscal periods. Shared across all fact tables.

CREATE TABLE IF NOT EXISTS dim_date (
    date_key        INTEGER PRIMARY KEY,  -- YYYYMMDD format, e.g., 20241225
    full_date       DATE    NOT NULL,
    day_of_week     SMALLINT,             -- 1=Monday, 7=Sunday
    day_name        VARCHAR(10),
    day_of_month    SMALLINT,
    day_of_year     SMALLINT,
    week_of_year    SMALLINT,
    month_number    SMALLINT,
    month_name      VARCHAR(10),
    quarter         SMALLINT,             -- 1, 2, 3, 4
    year            SMALLINT,
    is_weekend      BOOLEAN,              -- Saturday or Sunday
    is_month_end    BOOLEAN,
    is_quarter_end  BOOLEAN,
    is_year_end     BOOLEAN,
    fiscal_year     SMALLINT,             -- SA: March to February
    fiscal_quarter  SMALLINT
);

-- Fill with every date from 2020 to 2027
INSERT INTO dim_date
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INTEGER,
    d::DATE,
    EXTRACT(ISODOW FROM d)::SMALLINT,
    TRIM(TO_CHAR(d, 'Day')),              
    EXTRACT(DAY FROM d)::SMALLINT,
    EXTRACT(DOY FROM d)::SMALLINT,
    EXTRACT(WEEK FROM d)::SMALLINT,
    EXTRACT(MONTH FROM d)::SMALLINT,
    TRIM(TO_CHAR(d, 'Month')),            
    EXTRACT(QUARTER FROM d)::SMALLINT,
    EXTRACT(YEAR FROM d)::SMALLINT,
    EXTRACT(ISODOW FROM d) IN (6, 7),
    d = DATE_TRUNC('month', d) + INTERVAL '1 month' - INTERVAL '1 day',
    d = DATE_TRUNC('quarter', d) + INTERVAL '3 months' - INTERVAL '1 day',
    d = DATE_TRUNC('year', d) + INTERVAL '1 year' - INTERVAL '1 day',
    -- SA fiscal year: March to February (March = Q1)
    CASE WHEN EXTRACT(MONTH FROM d) >= 3 THEN EXTRACT(YEAR FROM d) ELSE EXTRACT(YEAR FROM d) - 1 END::SMALLINT,
    CASE
        WHEN EXTRACT(MONTH FROM d) BETWEEN 3 AND 5  THEN 1   -- Mar, Apr, May
        WHEN EXTRACT(MONTH FROM d) BETWEEN 6 AND 8  THEN 2   -- Jun, Jul, Aug
        WHEN EXTRACT(MONTH FROM d) BETWEEN 9 AND 11 THEN 3   -- Sep, Oct, Nov
        ELSE 4                                               -- Dec, Jan, Feb
    END::SMALLINT
FROM GENERATE_SERIES('2020-01-01'::DATE, '2027-12-31'::DATE, '1 day') AS d
ON CONFLICT (date_key) DO NOTHING;

CREATE INDEX IF NOT EXISTS idx_dim_date_full_date  ON dim_date (full_date);
CREATE INDEX IF NOT EXISTS idx_dim_date_year_month ON dim_date (year, month_number);


-- =============================================================
-- 2. CUSTOMERS — SCD Type 2 (keeps full history)
-- =============================================================
-- When a customer changes segment or risk rating, we don't overwrite.
-- We mark old row as expired (scd_end_date = today, scd_is_current = FALSE)
-- and insert a new row. This lets us join old transactions to old values.

CREATE TABLE IF NOT EXISTS dim_customers (
    customer_key     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,  -- Modern PostgreSQL style
    customer_id      VARCHAR(20)  NOT NULL,
    full_name        VARCHAR(200),
    email            VARCHAR(255),
    date_of_birth    DATE,
    age_band         VARCHAR(20),          -- '18-25', '26-35', etc. from Silver
    gender           VARCHAR(20),
    province         VARCHAR(50),
    city             VARCHAR(100),
    customer_segment VARCHAR(50),          -- 'Retail', 'Private Banking', etc.
    income_band      VARCHAR(30),          -- 'R0-R10k', 'R10k-R25k', etc.
    risk_rating      VARCHAR(20),          -- 'Low', 'Medium', 'High'
    kyc_verified     BOOLEAN DEFAULT FALSE,
    onboarding_date  DATE,
    is_active        BOOLEAN DEFAULT TRUE,
    -- SCD columns: track which version is current and when it was valid
    scd_start_date   DATE         NOT NULL DEFAULT CURRENT_DATE,
    scd_end_date     DATE,                 -- NULL = this is the active version
    scd_is_current   BOOLEAN      NOT NULL DEFAULT TRUE,
    scd_version      SMALLINT     NOT NULL DEFAULT 1,
    dw_created_at    TIMESTAMPTZ  DEFAULT NOW(),
    dw_batch_id      VARCHAR(50)
);

-- Speeds up "find current version of customer X" queries
CREATE INDEX IF NOT EXISTS idx_dim_cust_id_current
    ON dim_customers (customer_id, scd_is_current);

-- Prevents duplicate current rows for the same customer (critical for SCD2)
CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_customers_current
    ON dim_customers (customer_id) WHERE scd_is_current = TRUE;


-- =============================================================
-- 3. ACCOUNTS — SCD Type 2
-- =============================================================
-- Same pattern as customers. Tracks status, credit limit, overdraft changes.

CREATE TABLE IF NOT EXISTS dim_accounts (
    account_key      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    account_id       VARCHAR(20)  NOT NULL,
    customer_key     BIGINT REFERENCES dim_customers(customer_key),
    customer_id      VARCHAR(20),          -- Denormalized for simpler queries
    account_type     VARCHAR(50),          -- 'Cheque', 'Savings', 'Credit Card'
    account_number   VARCHAR(20),
    currency         CHAR(3)      DEFAULT 'ZAR',
    credit_limit     NUMERIC(15, 2),       -- Only for credit cards
    interest_rate    NUMERIC(5, 2),        -- Annual percentage
    overdraft_limit  NUMERIC(15, 2),       -- Only for cheque accounts
    status           VARCHAR(20),          -- 'Active', 'Dormant', 'Closed'
    open_date        DATE,
    close_date       DATE,
    account_age_days INTEGER,              -- Days since opened
    scd_start_date   DATE         NOT NULL DEFAULT CURRENT_DATE,
    scd_end_date     DATE,
    scd_is_current   BOOLEAN      NOT NULL DEFAULT TRUE,
    scd_version      SMALLINT     NOT NULL DEFAULT 1,
    dw_created_at    TIMESTAMPTZ  DEFAULT NOW(),
    dw_batch_id      VARCHAR(50)
);

CREATE INDEX IF NOT EXISTS idx_dim_acc_id_current
    ON dim_accounts (account_id, scd_is_current);

-- Prevents duplicate current rows for the same account (critical for SCD2)
CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_accounts_current
    ON dim_accounts (account_id) WHERE scd_is_current = TRUE;


-- =============================================================
-- 4. MERCHANTS — SCD Type 1 (no history, just overwrite)
-- =============================================================
-- Stores rarely change names. When they do, we don't care about old name.

CREATE TABLE IF NOT EXISTS dim_merchants (
    merchant_key      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    merchant_id       VARCHAR(20)  NOT NULL UNIQUE,
    merchant_name     VARCHAR(255),
    merchant_category VARCHAR(100),       -- 'Grocery', 'Fuel', 'Fast Food'
    mcc_code          INTEGER,             -- Industry standard merchant code
    is_online         BOOLEAN DEFAULT FALSE,
    dw_created_at     TIMESTAMPTZ  DEFAULT NOW(),
    dw_batch_id       VARCHAR(50)
);


-- =============================================================
-- 5. BRANCHES — SCD Type 1
-- =============================================================

CREATE TABLE IF NOT EXISTS dim_branches (
    branch_key    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    branch_id     VARCHAR(20)  NOT NULL UNIQUE,
    branch_name   VARCHAR(255),
    city          VARCHAR(100),
    province      VARCHAR(50),
    region        VARCHAR(50),             -- 'Urban' or 'Suburban'
    opened_date   DATE,
    is_active     BOOLEAN DEFAULT TRUE,
    dw_created_at TIMESTAMPTZ  DEFAULT NOW(),
    dw_batch_id   VARCHAR(50)
);


-- =============================================================
-- 6. TRANSACTION TYPE — conformed lookup
-- =============================================================
-- "Conformed" means every fact table uses the same set of values.
-- Consistent across the whole warehouse.

CREATE TABLE IF NOT EXISTS dim_transaction_type (
    txn_type_key  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    txn_type_name VARCHAR(50) NOT NULL UNIQUE,
    txn_category  VARCHAR(50),             -- 'Spend', 'Income', 'Transfer'
    balance_direction VARCHAR(10)          -- 'DEBIT' (money leaves) or 'CREDIT'
);

-- Pre-populate with standard transaction types
INSERT INTO dim_transaction_type (txn_type_name, txn_category, balance_direction) VALUES
    ('Purchase',         'Spend',     'DEBIT'),
    ('ATM Withdrawal',   'Cash',      'DEBIT'),
    ('EFT Transfer Out', 'Transfer',  'DEBIT'),
    ('EFT Transfer In',  'Transfer',  'CREDIT'),
    ('Debit Order',      'Recurring', 'DEBIT'),
    ('Salary Credit',    'Income',    'CREDIT'),
    ('Bank Charges',     'Fee',       'DEBIT'),
    ('Reversal',         'Reversal',  'CREDIT')
ON CONFLICT (txn_type_name) DO NOTHING;


-- =============================================================
-- 7. STAGING — fast bulk load
-- =============================================================
-- UNLOGGED = PostgreSQL skips writing to WAL (write-ahead log).
-- 10x faster inserts. Data can be lost on crash but staging can be reloaded.

CREATE UNLOGGED TABLE IF NOT EXISTS stg_fact_transactions (
    transaction_id       VARCHAR(50),      
    account_id           VARCHAR(20),
    merchant_id          VARCHAR(20),
    transaction_type     VARCHAR(50),
    transaction_date     DATE,
    transaction_time     TIME,
    transaction_hour     SMALLINT,
    amount               NUMERIC(15, 2),
    is_debit             BOOLEAN,
    balance_after        NUMERIC(15, 2),
    status               VARCHAR(20),
    is_fraud_flag        BOOLEAN,
    fraud_score          NUMERIC(4, 3),    
    fraud_risk_band      VARCHAR(20),
    is_weekend           BOOLEAN,
    is_night_transaction BOOLEAN,
    amount_band          VARCHAR(20),
    daily_txn_count      INTEGER,
    daily_spend_total    NUMERIC(15, 2),
    reference            VARCHAR(50),
    description          VARCHAR(255),
    channel              VARCHAR(50)
);


-- =============================================================
-- 8. FACT TRANSACTIONS — main fact table
-- =============================================================
-- Every transaction goes here. Hypertable = automatic date partitioning.
-- Query only scans relevant date chunks, not whole table.

CREATE TABLE IF NOT EXISTS fact_transactions (
    transaction_key      BIGINT GENERATED ALWAYS AS IDENTITY,
    date_key             INTEGER   REFERENCES dim_date(date_key),
    account_key          BIGINT    REFERENCES dim_accounts(account_key),
    customer_key         BIGINT    REFERENCES dim_customers(customer_key),
    merchant_key         BIGINT    REFERENCES dim_merchants(merchant_key),
    branch_key           BIGINT    REFERENCES dim_branches(branch_key),
    txn_type_key         BIGINT    REFERENCES dim_transaction_type(txn_type_key),
    transaction_id       VARCHAR(50) NOT NULL,   
    transaction_date     DATE        NOT NULL,   -- Partition key
    transaction_time     TIME,
    transaction_hour     SMALLINT,               -- Denormalized for fast grouping
    amount               NUMERIC(15, 2) NOT NULL,
    is_debit             BOOLEAN,                -- TRUE = money leaves
    balance_after        NUMERIC(15, 2),
    status               VARCHAR(20),            -- 'Completed', 'Declined', etc.
    is_fraud_flag        BOOLEAN  DEFAULT FALSE,
    fraud_score          NUMERIC(4, 3),          
    fraud_risk_band      VARCHAR(20),            -- 'Low', 'Medium', 'High', 'Critical'
    is_weekend           BOOLEAN,
    is_night_transaction BOOLEAN,                -- Between 10 PM and 6 AM
    amount_band          VARCHAR(20),            -- 'Under R100', 'R100-R500', etc.
    daily_txn_count      INTEGER,                -- From Silver window function
    daily_spend_total    NUMERIC(15, 2),         -- From Silver window function
    reference            VARCHAR(50),
    description          VARCHAR(255),
    channel              VARCHAR(50),            -- 'Mobile App', 'ATM', 'POS'
    dw_loaded_at         TIMESTAMPTZ DEFAULT NOW(),
    dw_batch_id          VARCHAR(50),
    CONSTRAINT pk_fact_transactions PRIMARY KEY (transaction_date, transaction_key)
);

-- Convert to TimescaleDB hypertable
SELECT create_hypertable('fact_transactions', 'transaction_date', if_not_exists => TRUE);

-- Common query patterns
CREATE INDEX IF NOT EXISTS idx_fact_txn_account  ON fact_transactions (account_key,  transaction_date DESC);
CREATE INDEX IF NOT EXISTS idx_fact_txn_customer ON fact_transactions (customer_key, transaction_date DESC);
-- Partial index = only fraud rows (saves disk space)
CREATE INDEX IF NOT EXISTS idx_fact_txn_fraud    ON fact_transactions (is_fraud_flag) WHERE is_fraud_flag = TRUE;


-- =============================================================
-- 9. FACT ACCOUNT SNAPSHOTS — daily balances
-- =============================================================
-- Pre-calculated so queries don't have to scan millions of transactions
-- to answer "what was each account's balance at end of each day?"

CREATE TABLE IF NOT EXISTS fact_account_snapshots (
    snapshot_key     BIGINT GENERATED ALWAYS AS IDENTITY,
    snapshot_date    DATE        NOT NULL,
    date_key         INTEGER     REFERENCES dim_date(date_key),
    account_key      BIGINT      REFERENCES dim_accounts(account_key),
    customer_key     BIGINT      REFERENCES dim_customers(customer_key),
    closing_balance  NUMERIC(15, 2),            -- End of day
    opening_balance  NUMERIC(15, 2),            -- Start of day
    daily_credits    NUMERIC(15, 2),            -- Total money in
    daily_debits     NUMERIC(15, 2),            -- Total money out
    txn_count        INTEGER,
    is_overdrawn     BOOLEAN DEFAULT FALSE,
    dw_loaded_at     TIMESTAMPTZ DEFAULT NOW(),
    dw_batch_id      VARCHAR(50),
    CONSTRAINT pk_account_snapshots PRIMARY KEY (snapshot_date, snapshot_key)
);

SELECT create_hypertable('fact_account_snapshots', 'snapshot_date', if_not_exists => TRUE);


-- =============================================================
-- 10. AUDIT TABLES
-- =============================================================
-- Track when pipelines run, how long they take, and data quality failures.

-- One row per ETL run
CREATE TABLE IF NOT EXISTS audit_pipeline_runs (
    run_id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pipeline_name  VARCHAR(100) NOT NULL,   -- 'gold_load', 'silver_transform'
    layer          VARCHAR(20),             -- 'bronze', 'silver', 'gold'
    status         VARCHAR(20),             -- 'success', 'failed', 'running'
    started_at     TIMESTAMPTZ  DEFAULT NOW(),
    ended_at       TIMESTAMPTZ,
    duration_secs  INTEGER,                 -- For performance monitoring
    rows_processed BIGINT,
    rows_failed    BIGINT DEFAULT 0,
    error_message  TEXT,
    batch_id       VARCHAR(50),
    metadata       JSONB                    -- Extra info like file names
);

-- One row per data quality check
CREATE TABLE IF NOT EXISTS audit_data_quality (
    check_id      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    checked_at    TIMESTAMPTZ  DEFAULT NOW(),
    table_name    VARCHAR(100),
    check_name    VARCHAR(100),             -- 'not_null_check', 'foreign_key_check'
    check_type    VARCHAR(50),
    passed        BOOLEAN,
    rows_checked  BIGINT,
    rows_failed   BIGINT,
    failure_pct   NUMERIC(5, 2),
    severity      VARCHAR(20),              -- 'info', 'warning', 'critical'
    details       JSONB                     -- Sample of failed rows
);


-- =============================================================
-- TABLE COMMENTS (visible in psql with \d+)
-- =============================================================

COMMENT ON TABLE dim_customers IS 'SCD Type 2. WHERE scd_is_current = TRUE gets current customers.';
COMMENT ON TABLE fact_transactions IS 'Hypertable partitioned by date. Main fact table.';
COMMENT ON TABLE stg_fact_transactions IS 'UNLOGGED = faster loads. Truncated every run.';