# F&O Database Project

## Overview
A normalized relational database (3NF) for storing and analyzing high-volume Futures & Options (F&O) trading data from Indian exchanges (NSE, BSE, MCX).

## Dataset
- **Source:** [NSE Future and Options Dataset 3M](https://www.kaggle.com/datasets/sunnysai12345/nse-future-and-options-dataset-3m)
- **Size:** 2,533,210 rows, 16 columns
- **Period:** August - October 2019

## Project Structure
```
fno-database-project/
├── docs/
│   ├── fno_database_er_diagram.png   # ER Diagram
│   └── reasoning.pdf                  # Design rationale
├── sql/
│   ├── 01_create_tables.sql          # CREATE TABLE statements
│   ├── 02_create_indexes.sql         # CREATE INDEX statements
│   ├── 03_partitioning.sql           # Table partitioning
│   └── 04_queries.sql                # 7 analytical queries
├── notebooks/
│   └── load_data.py                  # Python script to load data
├── data/
│   └── 3mfanddo.csv                  # Kaggle dataset
└── README.md
```

## Database Schema (5 Tables)

| Table | Purpose | Rows |
|-------|---------|------|
| `exchanges` | NSE, BSE, MCX master data | 3 |
| `instruments` | Trading symbols (NIFTY, RELIANCE, etc.) | 328 |
| `expiries` | Contract definitions (expiry + strike) | 77,976 |
| `trades` | Daily OHLCV trading data | 2,533,210 |
| `daily_aggregates` | Pre-computed summaries | 21,844 |

## Entity Relationship Diagram

The database follows Third Normal Form (3NF) with the following relationships:

```
exchanges (1) ──────< (N) instruments (1) ──────< (N) expiries (1) ──────< (N) trades
                              │
                              └────< (N) daily_aggregates
```

## Key Design Decisions

### Why 3NF (Third Normal Form)?
1. **No redundancy** - Exchange names stored once, not 2.5M times
2. **Data integrity** - Foreign keys ensure valid references
3. **Fast writes** - Single-table inserts for HFT ingestion
4. **Flexible queries** - JOINs allow any combination of filters

### Why Not Star Schema?
- Star schema optimizes for read-heavy analytics
- Our use case requires both fast reads AND fast writes
- 3NF with proper indexes achieves similar read performance

## Optimizations

### Indexes Created
| Index | Table | Column(s) | Purpose |
|-------|-------|-----------|---------|
| `idx_trades_trade_date` | trades | trade_date | Filter by date |
| `idx_trades_expiry_id` | trades | expiry_id | Fast JOINs |
| `idx_trades_timestamp` | trades | timestamp | Time-series queries |
| `idx_expiries_instrument_id` | expiries | instrument_id | Fast JOINs |
| `idx_instruments_symbol` | instruments | symbol | Symbol lookups |

### Partitioning Strategy
- **Range partitioning** by `trade_date` (monthly partitions)
- Enables partition pruning for date-range queries
- Easy archival of old data by dropping partitions

## Queries Implemented

| # | Query | Purpose |
|---|-------|---------|
| 1 | Top 10 symbols by OI change | Track where money is flowing |
| 2 | 7-day rolling volatility | Risk assessment for NIFTY options |
| 3 | Cross-exchange comparison | MCX Gold vs NSE Index Futures |
| 4 | Option chain summary | Strike-level liquidity analysis |
| 5 | Max volume day (optimized) | Anomaly detection with window functions |
| 6 | Put-Call Ratio by symbol | Market sentiment indicator |
| 7 | Expiry day volume analysis | Compare expiry vs normal day activity |

## How to Run

### Prerequisites
- PostgreSQL 14+ installed
- Python 3.8+ with pandas, psycopg2-binary

### Setup
```bash
# 1. Create database
createdb fno_database

# 2. Install Python packages
pip install pandas psycopg2-binary

# 3. Load data (from notebooks folder)
cd notebooks
python load_data.py

# 4. Run queries
cd ../sql
psql -d fno_database -f 04_queries.sql
```

### Verify Installation
```bash
psql -d fno_database -c "SELECT COUNT(*) FROM trades;"
# Expected output: 2,533,210
```

## Sample Query Output

### Query 6: Put-Call Ratio (Market Sentiment)
```
 symbol    | call_oi   | put_oi    | pcr_oi | sentiment
-----------+-----------+-----------+--------+-----------
 NIFTY     | 12345678  | 9876543   | 0.800  | Neutral
 BANKNIFTY | 5678901   | 7654321   | 1.348  | Bearish
 RELIANCE  | 1234567   | 987654    | 0.800  | Neutral
```

## Scalability Considerations

For scaling to 10M+ rows in a High-Frequency Trading (HFT) environment:

1. **Partitioning**: Monthly range partitions on `trade_date`
2. **BRIN Indexes**: For timestamp columns (100x smaller than B-tree)
3. **Batch Inserts**: Load data in batches of 10,000+ rows
4. **Connection Pooling**: Use PgBouncer for connection management
5. **Read Replicas**: Separate read/write workloads

## Technologies Used
- **Database:** PostgreSQL 14
- **Language:** Python 3, SQL
- **Libraries:** pandas, psycopg2
- **Tools:** dbdiagram.io (ER diagram)

## Author
Saumya Goyal

## License
MIT
