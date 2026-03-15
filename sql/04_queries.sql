-- ============================================================================
-- F&O DATABASE - ADVANCED SQL QUERIES (PostgreSQL)
-- 
-- These 7 queries demonstrate advanced SQL for F&O analytics
-- ============================================================================


-- ============================================================================
-- QUERY 1: Top 10 Symbols by Open Interest (OI) Change Across Exchanges
-- ============================================================================
-- Purpose: Identify where money is flowing in/out of the market
-- Business Use: Traders use OI change to gauge market sentiment
--   - Rising OI = New positions being created (strong trend)
--   - Falling OI = Positions being closed (potential reversal)

SELECT 
    e.exchange_code,
    i.symbol,
    SUM(t.chg_in_oi) AS total_oi_change,
    SUM(t.open_interest) AS total_open_interest,
    COUNT(*) AS num_trades,
    ROUND(
        100.0 * SUM(t.chg_in_oi) / NULLIF(SUM(t.open_interest), 0), 
        2
    ) AS oi_change_percent
FROM trades t
JOIN expiries exp ON t.expiry_id = exp.expiry_id
JOIN instruments i ON exp.instrument_id = i.instrument_id
JOIN exchanges e ON i.exchange_id = e.exchange_id
GROUP BY e.exchange_code, i.symbol
ORDER BY ABS(SUM(t.chg_in_oi)) DESC
LIMIT 10;


-- ============================================================================
-- QUERY 2: 7-Day Rolling Standard Deviation of Close Prices (NIFTY Options)
-- ============================================================================
-- Purpose: Calculate rolling volatility to assess risk
-- Business Use: Options traders use volatility to price options
-- SQL Concepts: Window Functions, CTEs

WITH nifty_daily_prices AS (
    -- Step 1: Get daily average close price for NIFTY options
    SELECT 
        t.trade_date,
        AVG(t.close_price) AS avg_close,
        COUNT(*) AS num_contracts
    FROM trades t
    JOIN expiries exp ON t.expiry_id = exp.expiry_id
    JOIN instruments i ON exp.instrument_id = i.instrument_id
    WHERE i.symbol = 'NIFTY'
      AND exp.contract_type = 'OPTIONS'
    GROUP BY t.trade_date
)
SELECT 
    trade_date,
    ROUND(avg_close, 2) AS avg_close,
    -- 7-day rolling standard deviation
    ROUND(
        STDDEV(avg_close) OVER (
            ORDER BY trade_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 
        4
    ) AS rolling_7day_stddev,
    -- 7-day rolling average for context
    ROUND(
        AVG(avg_close) OVER (
            ORDER BY trade_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 
        2
    ) AS rolling_7day_avg,
    -- Volatility ratio (std dev / avg)
    ROUND(
        STDDEV(avg_close) OVER (
            ORDER BY trade_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) / NULLIF(AVG(avg_close) OVER (
            ORDER BY trade_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 0) * 100,
        2
    ) AS volatility_percent
FROM nifty_daily_prices
ORDER BY trade_date DESC
LIMIT 30;


-- ============================================================================
-- QUERY 3: Cross-Exchange Comparison - MCX Gold vs NSE Index Futures
-- ============================================================================
-- Purpose: Compare performance across asset classes
-- Business Use: Portfolio managers compare commodities vs equities

SELECT 
    e.exchange_code,
    i.symbol,
    exp.contract_type,
    COUNT(*) AS num_trades,
    ROUND(AVG(t.settle_price), 2) AS avg_settle_price,
    ROUND(MIN(t.settle_price), 2) AS min_settle_price,
    ROUND(MAX(t.settle_price), 2) AS max_settle_price,
    SUM(t.contracts) AS total_volume,
    SUM(t.val_inlakh) AS total_value_lakhs
FROM trades t
JOIN expiries exp ON t.expiry_id = exp.expiry_id
JOIN instruments i ON exp.instrument_id = i.instrument_id
JOIN exchanges e ON i.exchange_id = e.exchange_id
WHERE 
    -- MCX Gold futures
    (e.exchange_code = 'MCX' AND i.symbol = 'GOLD' AND exp.contract_type = 'FUTURES')
    OR
    -- NSE Index futures (NIFTY or BANKNIFTY)
    (e.exchange_code = 'NSE' AND i.symbol IN ('NIFTY', 'BANKNIFTY') AND exp.contract_type = 'FUTURES')
GROUP BY e.exchange_code, i.symbol, exp.contract_type
ORDER BY total_volume DESC;


-- ============================================================================
-- QUERY 4: Option Chain Summary - Grouped by Expiry and Strike
-- ============================================================================
-- Purpose: Build an option chain view showing liquidity at each strike
-- Business Use: Traders identify support/resistance levels from OI concentration

SELECT 
    exp.expiry_date,
    exp.strike_price,
    exp.option_type,
    COUNT(*) AS num_trading_days,
    SUM(t.contracts) AS total_volume,
    SUM(t.open_interest) AS total_oi,
    ROUND(AVG(t.close_price), 2) AS avg_premium,
    -- Volume to OI ratio (high ratio = active trading)
    ROUND(
        100.0 * SUM(t.contracts) / NULLIF(SUM(t.open_interest), 0), 
        2
    ) AS volume_to_oi_ratio
FROM trades t
JOIN expiries exp ON t.expiry_id = exp.expiry_id
JOIN instruments i ON exp.instrument_id = i.instrument_id
WHERE i.symbol = 'BANKNIFTY'
  AND exp.contract_type = 'OPTIONS'
GROUP BY exp.expiry_date, exp.strike_price, exp.option_type
HAVING SUM(t.contracts) > 1000  -- Only liquid strikes
ORDER BY exp.expiry_date, exp.strike_price, exp.option_type;


-- ============================================================================
-- QUERY 5: Maximum Volume Day in Last 30 Days (Performance Optimized)
-- ============================================================================
-- Purpose: Find highest activity days for anomaly detection
-- SQL Concepts: Window functions, index usage, subqueries
-- Optimization: Uses idx_trades_trade_date index

WITH daily_volumes AS (
    SELECT 
        t.trade_date,
        SUM(t.contracts) AS daily_volume,
        COUNT(DISTINCT exp.instrument_id) AS unique_instruments,
        SUM(t.val_inlakh) AS daily_value_lakhs
    FROM trades t
    JOIN expiries exp ON t.expiry_id = exp.expiry_id
    -- Filter to last 30 days (uses index)
    WHERE t.trade_date >= (SELECT MAX(trade_date) - INTERVAL '30 days' FROM trades)
    GROUP BY t.trade_date
),
ranked_volumes AS (
    SELECT 
        *,
        MAX(daily_volume) OVER () AS max_volume,
        RANK() OVER (ORDER BY daily_volume DESC) AS volume_rank
    FROM daily_volumes
)
SELECT 
    trade_date,
    daily_volume,
    unique_instruments,
    ROUND(daily_value_lakhs, 2) AS daily_value_lakhs,
    CASE WHEN daily_volume = max_volume THEN '★ MAX' ELSE '' END AS is_max_day,
    volume_rank
FROM ranked_volumes
ORDER BY daily_volume DESC
LIMIT 10;


-- ============================================================================
-- QUERY 6: Put-Call Ratio (PCR) by Symbol - Market Sentiment Indicator
-- ============================================================================
-- Purpose: Calculate PCR as a contrarian sentiment indicator
-- Business Use: 
--   - PCR > 1.2 = Bearish sentiment (many puts bought)
--   - PCR < 0.8 = Bullish sentiment (many calls bought)
--   - Some traders use this as a contrarian indicator

WITH option_oi AS (
    SELECT 
        i.symbol,
        exp.option_type,
        SUM(t.open_interest) AS total_oi,
        SUM(t.contracts) AS total_volume
    FROM trades t
    JOIN expiries exp ON t.expiry_id = exp.expiry_id
    JOIN instruments i ON exp.instrument_id = i.instrument_id
    WHERE exp.contract_type = 'OPTIONS'
      AND exp.option_type IN ('CE', 'PE')  -- Only calls and puts
    GROUP BY i.symbol, exp.option_type
)
SELECT 
    symbol,
    MAX(CASE WHEN option_type = 'CE' THEN total_oi END) AS call_oi,
    MAX(CASE WHEN option_type = 'PE' THEN total_oi END) AS put_oi,
    MAX(CASE WHEN option_type = 'CE' THEN total_volume END) AS call_volume,
    MAX(CASE WHEN option_type = 'PE' THEN total_volume END) AS put_volume,
    -- Put-Call Ratio based on OI
    ROUND(
        MAX(CASE WHEN option_type = 'PE' THEN total_oi END)::DECIMAL /
        NULLIF(MAX(CASE WHEN option_type = 'CE' THEN total_oi END), 0),
        3
    ) AS pcr_oi,
    -- Sentiment interpretation
    CASE 
        WHEN MAX(CASE WHEN option_type = 'PE' THEN total_oi END)::DECIMAL /
             NULLIF(MAX(CASE WHEN option_type = 'CE' THEN total_oi END), 0) > 1.2 
        THEN 'Bearish'
        WHEN MAX(CASE WHEN option_type = 'PE' THEN total_oi END)::DECIMAL /
             NULLIF(MAX(CASE WHEN option_type = 'CE' THEN total_oi END), 0) < 0.8 
        THEN 'Bullish'
        ELSE 'Neutral'
    END AS sentiment
FROM option_oi
GROUP BY symbol
HAVING MAX(CASE WHEN option_type = 'CE' THEN total_oi END) > 0
ORDER BY pcr_oi DESC;


-- ============================================================================
-- QUERY 7: Expiry Day Volume Spike Analysis
-- ============================================================================
-- Purpose: Compare trading activity on expiry days vs normal days
-- Business Use: Understand expiry day dynamics for trading strategies

WITH trade_classification AS (
    SELECT 
        i.symbol,
        t.trade_date,
        exp.expiry_date,
        SUM(t.contracts) AS daily_volume,
        -- Flag if this is an expiry day
        CASE WHEN t.trade_date = exp.expiry_date THEN 'EXPIRY' ELSE 'NORMAL' END AS day_type
    FROM trades t
    JOIN expiries exp ON t.expiry_id = exp.expiry_id
    JOIN instruments i ON exp.instrument_id = i.instrument_id
    GROUP BY i.symbol, t.trade_date, exp.expiry_date
)
SELECT 
    symbol,
    -- Average volume on normal days
    ROUND(AVG(CASE WHEN day_type = 'NORMAL' THEN daily_volume END), 0) AS avg_normal_volume,
    -- Average volume on expiry days
    ROUND(AVG(CASE WHEN day_type = 'EXPIRY' THEN daily_volume END), 0) AS avg_expiry_volume,
    -- Expiry day volume multiplier
    ROUND(
        AVG(CASE WHEN day_type = 'EXPIRY' THEN daily_volume END)::DECIMAL /
        NULLIF(AVG(CASE WHEN day_type = 'NORMAL' THEN daily_volume END), 0),
        2
    ) AS expiry_volume_multiplier,
    -- Count of each day type
    COUNT(CASE WHEN day_type = 'EXPIRY' THEN 1 END) AS expiry_days_count,
    COUNT(CASE WHEN day_type = 'NORMAL' THEN 1 END) AS normal_days_count
FROM trade_classification
GROUP BY symbol
HAVING AVG(CASE WHEN day_type = 'NORMAL' THEN daily_volume END) > 0
   AND AVG(CASE WHEN day_type = 'EXPIRY' THEN daily_volume END) IS NOT NULL
ORDER BY expiry_volume_multiplier DESC
LIMIT 15;


-- ============================================================================
-- END OF QUERIES
-- ============================================================================