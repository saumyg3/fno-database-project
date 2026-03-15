-- ============================================================================
-- F&O DATABASE PARTITIONING (PostgreSQL)
-- 
-- Purpose: Partition large tables for better performance at scale (10M+ rows)
-- 
-- WHY PARTITIONING?
-- - Query planner skips irrelevant partitions (partition pruning)
-- - Maintenance (VACUUM, REINDEX) runs on smaller chunks
-- - Old data can be archived by dropping entire partitions
-- - Parallel queries across partitions
-- ============================================================================


-- ============================================================================
-- OPTION 1: RANGE PARTITIONING BY TRADE_DATE (RECOMMENDED)
-- ============================================================================
-- Best for: Time-series queries ("get all trades in August 2019")
-- Strategy: One partition per month

-- Step 1: Create the partitioned table
CREATE TABLE trades_partitioned (
    trade_id        SERIAL,
    expiry_id       INTEGER NOT NULL,
    trade_date      DATE NOT NULL,
    timestamp       TIMESTAMP NOT NULL,
    open_price      DECIMAL(15, 2) NOT NULL,
    high_price      DECIMAL(15, 2) NOT NULL,
    low_price       DECIMAL(15, 2) NOT NULL,
    close_price     DECIMAL(15, 2) NOT NULL,
    settle_price    DECIMAL(15, 2),
    contracts       INTEGER DEFAULT 0,
    val_inlakh      DECIMAL(15, 2) DEFAULT 0,
    open_interest   INTEGER DEFAULT 0,
    chg_in_oi       INTEGER DEFAULT 0,
    PRIMARY KEY (trade_id, trade_date)
) PARTITION BY RANGE (trade_date);

-- Step 2: Create monthly partitions for the data period (Aug-Oct 2019)
CREATE TABLE trades_2019_08 PARTITION OF trades_partitioned
    FOR VALUES FROM ('2019-08-01') TO ('2019-09-01');

CREATE TABLE trades_2019_09 PARTITION OF trades_partitioned
    FOR VALUES FROM ('2019-09-01') TO ('2019-10-01');

CREATE TABLE trades_2019_10 PARTITION OF trades_partitioned
    FOR VALUES FROM ('2019-10-01') TO ('2019-11-01');

-- Step 3: Create partition for future data
CREATE TABLE trades_2019_11 PARTITION OF trades_partitioned
    FOR VALUES FROM ('2019-11-01') TO ('2019-12-01');

-- Step 4: Create a DEFAULT partition for any data outside defined ranges
CREATE TABLE trades_default PARTITION OF trades_partitioned DEFAULT;


-- ============================================================================
-- OPTION 2: LIST PARTITIONING BY EXCHANGE
-- ============================================================================
-- Best for: Exchange-specific queries ("get all MCX trades")
-- Strategy: One partition per exchange

CREATE TABLE trades_by_exchange (
    trade_id        SERIAL,
    exchange_id     INTEGER NOT NULL,
    expiry_id       INTEGER NOT NULL,
    trade_date      DATE NOT NULL,
    timestamp       TIMESTAMP NOT NULL,
    open_price      DECIMAL(15, 2) NOT NULL,
    high_price      DECIMAL(15, 2) NOT NULL,
    low_price       DECIMAL(15, 2) NOT NULL,
    close_price     DECIMAL(15, 2) NOT NULL,
    settle_price    DECIMAL(15, 2),
    contracts       INTEGER DEFAULT 0,
    val_inlakh      DECIMAL(15, 2) DEFAULT 0,
    open_interest   INTEGER DEFAULT 0,
    chg_in_oi       INTEGER DEFAULT 0,
    PRIMARY KEY (trade_id, exchange_id)
) PARTITION BY LIST (exchange_id);

-- Create partitions for each exchange
CREATE TABLE trades_nse PARTITION OF trades_by_exchange
    FOR VALUES IN (1);  -- exchange_id = 1 for NSE

CREATE TABLE trades_bse PARTITION OF trades_by_exchange
    FOR VALUES IN (2);  -- exchange_id = 2 for BSE

CREATE TABLE trades_mcx PARTITION OF trades_by_exchange
    FOR VALUES IN (3);  -- exchange_id = 3 for MCX


-- ============================================================================
-- INDEXES ON PARTITIONED TABLES
-- ============================================================================
-- Note: Indexes are created on each partition automatically when created on parent

-- For date-partitioned table
CREATE INDEX idx_trades_part_date ON trades_partitioned(trade_date);
CREATE INDEX idx_trades_part_expiry ON trades_partitioned(expiry_id);
CREATE INDEX idx_trades_part_timestamp ON trades_partitioned(timestamp);

-- For exchange-partitioned table
CREATE INDEX idx_trades_exch_date ON trades_by_exchange(trade_date);
CREATE INDEX idx_trades_exch_expiry ON trades_by_exchange(expiry_id);


-- ============================================================================
-- PARTITION MAINTENANCE COMMANDS
-- ============================================================================

-- Add a new partition for a new month:
-- CREATE TABLE trades_2019_12 PARTITION OF trades_partitioned
--     FOR VALUES FROM ('2019-12-01') TO ('2020-01-01');

-- Archive old data by detaching a partition:
-- ALTER TABLE trades_partitioned DETACH PARTITION trades_2019_08;

-- Drop old data (CAREFUL - deletes data permanently):
-- DROP TABLE trades_2019_08;


-- ============================================================================
-- VERIFY PARTITIONS
-- ============================================================================
-- Run this to see all partitions:
-- SELECT 
--     parent.relname AS parent_table,
--     child.relname AS partition_name,
--     pg_get_expr(child.relpartbound, child.oid) AS partition_expression
-- FROM pg_inherits
-- JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
-- JOIN pg_class child ON pg_inherits.inhrelid = child.oid
-- WHERE parent.relname = 'trades_partitioned';


-- ============================================================================
-- END OF PARTITIONING
-- ============================================================================