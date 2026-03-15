"""
F&O Database - Data Loading Script
===================================
This script loads the Kaggle F&O dataset into PostgreSQL.

Prerequisites:
1. PostgreSQL installed and running
2. Database created: createdb fno_database
3. Python packages: pip install pandas psycopg2-binary

Usage:
    python load_data.py
"""

import pandas as pd
import psycopg2
from psycopg2 import sql
import os
from datetime import datetime

# Database connection settings
DB_CONFIG = {
    'dbname': 'fno_database',
    'user': 'saumyagoyal',     
    'password': '',             
    'host': 'localhost',
    'port': '5432'
}

CSV_FILE_PATH = '../data/3mfanddo.csv' 

CREATE_TABLES_SQL = """
-- Drop tables if they exist (for clean reload)
DROP TABLE IF EXISTS trades CASCADE;
DROP TABLE IF EXISTS daily_aggregates CASCADE;
DROP TABLE IF EXISTS expiries CASCADE;
DROP TABLE IF EXISTS instruments CASCADE;
DROP TABLE IF EXISTS exchanges CASCADE;

-- TABLE 1: EXCHANGES
CREATE TABLE exchanges (
    exchange_id     SERIAL PRIMARY KEY,
    exchange_code   VARCHAR(10) NOT NULL UNIQUE,
    exchange_name   VARCHAR(100) NOT NULL,
    country         VARCHAR(50) DEFAULT 'India',
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO exchanges (exchange_code, exchange_name) VALUES
    ('NSE', 'National Stock Exchange'),
    ('BSE', 'Bombay Stock Exchange'),
    ('MCX', 'Multi Commodity Exchange');

-- TABLE 2: INSTRUMENTS
CREATE TABLE instruments (
    instrument_id    SERIAL PRIMARY KEY,
    exchange_id      INTEGER NOT NULL REFERENCES exchanges(exchange_id),
    symbol           VARCHAR(50) NOT NULL,
    instrument_type  VARCHAR(20) NOT NULL,
    underlying_asset VARCHAR(100),
    lot_size         INTEGER DEFAULT 1,
    is_active        BOOLEAN DEFAULT TRUE,
    CONSTRAINT uq_instrument UNIQUE (exchange_id, symbol, instrument_type)
);

-- TABLE 3: EXPIRIES
CREATE TABLE expiries (
    expiry_id       SERIAL PRIMARY KEY,
    instrument_id   INTEGER NOT NULL REFERENCES instruments(instrument_id),
    expiry_date     DATE NOT NULL,
    strike_price    DECIMAL(15, 2) NOT NULL,
    option_type     VARCHAR(5) NOT NULL,
    contract_type   VARCHAR(20) NOT NULL
);

-- TABLE 4: TRADES
CREATE TABLE trades (
    trade_id        SERIAL PRIMARY KEY,
    expiry_id       INTEGER NOT NULL REFERENCES expiries(expiry_id),
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
    chg_in_oi       INTEGER DEFAULT 0
);

-- TABLE 5: DAILY_AGGREGATES
CREATE TABLE daily_aggregates (
    agg_id          SERIAL PRIMARY KEY,
    instrument_id   INTEGER NOT NULL REFERENCES instruments(instrument_id),
    trade_date      DATE NOT NULL,
    total_volume    BIGINT DEFAULT 0,
    total_oi        BIGINT DEFAULT 0,
    vwap            DECIMAL(15, 2),
    volatility      DECIMAL(10, 6),
    CONSTRAINT uq_daily_agg UNIQUE (instrument_id, trade_date)
);

-- Create indexes
CREATE INDEX idx_trades_trade_date ON trades(trade_date);
CREATE INDEX idx_trades_expiry_id ON trades(expiry_id);
CREATE INDEX idx_expiries_instrument_id ON expiries(instrument_id);
CREATE INDEX idx_instruments_symbol ON instruments(symbol);
"""


def connect_to_db():
    """Connect to PostgreSQL database."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("Connected to PostgreSQL successfully!")
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        print("\nTroubleshooting:")
        print("1. Make sure PostgreSQL is running: brew services start postgresql")
        print("2. Make sure database exists: createdb fno_database")
        print("3. Check your username in DB_CONFIG")
        raise


def create_tables(conn):
    """Create all database tables."""
    print("\n Creating tables...")
    cursor = conn.cursor()
    cursor.execute(CREATE_TABLES_SQL)
    conn.commit()
    print(" Tables created successfully!")


def parse_date(date_str):
    """Parse date from various formats."""
    if pd.isna(date_str):
        return None
    
    # Try different date formats
    formats = [
        '%d-%b-%Y',   # 29-Aug-2019
        '%d-%B-%Y',   # 29-August-2019
        '%Y-%m-%d',   # 2019-08-29
        '%d/%m/%Y',   # 29/08/2019
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(str(date_str).strip(), fmt).date()
        except ValueError:
            continue
    
    print(f"⚠️ Could not parse date: {date_str}")
    return None


def load_csv_data(csv_path):
    """Load and clean the CSV file."""
    print(f"\n Loading CSV file: {csv_path}")
    
    # Check if file exists
    if not os.path.exists(csv_path):
        print(f"File not found: {csv_path}")
        print("\nMake sure you:")
        print("1. Unzipped the Kaggle download")
        print("2. Put the CSV file in the 'data' folder")
        print("3. Updated CSV_FILE_PATH in this script")
        raise FileNotFoundError(csv_path)
    
    # Read CSV - try both comma and tab separators
    try:
        df = pd.read_csv(csv_path)
    except:
        df = pd.read_csv(csv_path, sep='\t')
    
    print(f"Loaded {len(df):,} rows")
    print(f"   Columns: {list(df.columns)}")
    
    # Standardize column names (uppercase, strip whitespace)
    df.columns = [col.strip().upper() for col in df.columns]
    
    # Add EXCHANGE column (all data is from NSE)
    if 'EXCHANGE' not in df.columns:
        df['EXCHANGE'] = 'NSE'
        print("   Added EXCHANGE column (defaulting to 'NSE')")
    
    return df


def insert_instruments(conn, df):
    """Insert unique instruments into the instruments table."""
    print("\n Inserting instruments...")
    cursor = conn.cursor()
    
    # Get exchange_id for NSE
    cursor.execute("SELECT exchange_id FROM exchanges WHERE exchange_code = 'NSE'")
    nse_id = cursor.fetchone()[0]
    
    # Get unique instrument combinations
    instruments = df[['SYMBOL', 'INSTRUMENT']].drop_duplicates()
    
    count = 0
    for _, row in instruments.iterrows():
        try:
            cursor.execute("""
                INSERT INTO instruments (exchange_id, symbol, instrument_type)
                VALUES (%s, %s, %s)
                ON CONFLICT (exchange_id, symbol, instrument_type) DO NOTHING
            """, (nse_id, row['SYMBOL'], row['INSTRUMENT']))
            count += 1
        except Exception as e:
            print(f"⚠️ Error inserting instrument: {e}")
    
    conn.commit()
    print(f"Inserted {count} instruments")


def insert_expiries(conn, df):
    """Insert unique expiry contracts into the expiries table."""
    print("\nInserting expiries...")
    cursor = conn.cursor()
    
    # Get instrument mapping
    cursor.execute("""
        SELECT instrument_id, symbol, instrument_type 
        FROM instruments
    """)
    instrument_map = {(row[1], row[2]): row[0] for row in cursor.fetchall()}
    
    # Get unique expiry combinations
    expiry_cols = ['SYMBOL', 'INSTRUMENT', 'EXPIRY_DT', 'STRIKE_PR', 'OPTION_TYP']
    expiries = df[expiry_cols].drop_duplicates()
    
    count = 0
    for _, row in expiries.iterrows():
        instrument_id = instrument_map.get((row['SYMBOL'], row['INSTRUMENT']))
        if not instrument_id:
            continue
        
        expiry_date = parse_date(row['EXPIRY_DT'])
        if not expiry_date:
            continue
        
        # Determine contract type
        contract_type = 'OPTIONS' if row['OPTION_TYP'] in ['CE', 'PE'] else 'FUTURES'
        
        try:
            cursor.execute("""
                INSERT INTO expiries (instrument_id, expiry_date, strike_price, option_type, contract_type)
                VALUES (%s, %s, %s, %s, %s)
            """, (instrument_id, expiry_date, row['STRIKE_PR'], row['OPTION_TYP'], contract_type))
            count += 1
        except Exception as e:
            pass  # Skip duplicates
    
    conn.commit()
    print(f"Inserted {count} expiry contracts")


def insert_trades(conn, df):
    """Insert trade data into the trades table."""
    print("\nInserting trades (this may take a few minutes)...")
    cursor = conn.cursor()
    
    # Build expiry lookup
    cursor.execute("""
        SELECT e.expiry_id, i.symbol, i.instrument_type, e.expiry_date, e.strike_price, e.option_type
        FROM expiries e
        JOIN instruments i ON e.instrument_id = i.instrument_id
    """)
    expiry_map = {}
    for row in cursor.fetchall():
        key = (row[1], row[2], str(row[3]), float(row[4]), row[5])
        expiry_map[key] = row[0]
    
    # Insert trades in batches
    batch_size = 10000
    total = len(df)
    inserted = 0
    skipped = 0
    
    for i in range(0, total, batch_size):
        batch = df.iloc[i:i+batch_size]
        
        for _, row in batch.iterrows():
            expiry_date = parse_date(row['EXPIRY_DT'])
            if not expiry_date:
                skipped += 1
                continue
            
            key = (row['SYMBOL'], row['INSTRUMENT'], str(expiry_date), float(row['STRIKE_PR']), row['OPTION_TYP'])
            expiry_id = expiry_map.get(key)
            
            if not expiry_id:
                skipped += 1
                continue
            
            trade_date = parse_date(row['TIMESTAMP'])
            if not trade_date:
                skipped += 1
                continue
            
            try:
                cursor.execute("""
                    INSERT INTO trades (
                        expiry_id, trade_date, timestamp,
                        open_price, high_price, low_price, close_price, settle_price,
                        contracts, val_inlakh, open_interest, chg_in_oi
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    expiry_id, trade_date, trade_date,
                    row['OPEN'], row['HIGH'], row['LOW'], row['CLOSE'], row['SETTLE_PR'],
                    row['CONTRACTS'], row['VAL_INLAKH'], row['OPEN_INT'], row['CHG_IN_OI']
                ))
                inserted += 1
            except Exception as e:
                skipped += 1
        
        conn.commit()
        progress = min(i + batch_size, total)
        print(f"   Progress: {progress:,} / {total:,} rows ({100*progress//total}%)")
    
    print(f"Inserted {inserted:,} trades (skipped {skipped:,})")


def compute_daily_aggregates(conn):
    """Compute and insert daily aggregate statistics."""
    print("\nComputing daily aggregates...")
    cursor = conn.cursor()
    
    cursor.execute("""
        INSERT INTO daily_aggregates (instrument_id, trade_date, total_volume, total_oi, vwap)
        SELECT 
            i.instrument_id,
            t.trade_date,
            SUM(t.contracts) AS total_volume,
            SUM(t.open_interest) AS total_oi,
            SUM(t.close_price * t.contracts) / NULLIF(SUM(t.contracts), 0) AS vwap
        FROM trades t
        JOIN expiries e ON t.expiry_id = e.expiry_id
        JOIN instruments i ON e.instrument_id = i.instrument_id
        GROUP BY i.instrument_id, t.trade_date
        ON CONFLICT (instrument_id, trade_date) DO UPDATE SET
            total_volume = EXCLUDED.total_volume,
            total_oi = EXCLUDED.total_oi,
            vwap = EXCLUDED.vwap
    """)
    
    conn.commit()
    
    cursor.execute("SELECT COUNT(*) FROM daily_aggregates")
    count = cursor.fetchone()[0]
    print(f"Created {count:,} daily aggregate records")


def print_summary(conn):
    """Print summary of loaded data."""
    print("\n" + "="*60)
    print("DATABASE SUMMARY")
    print("="*60)
    
    cursor = conn.cursor()
    
    tables = ['exchanges', 'instruments', 'expiries', 'trades', 'daily_aggregates']
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"   {table}: {count:,} rows")
    
    print("="*60)
    print("Data loading complete!")
    print("\nNext steps:")
    print("1. Run the queries in sql/04_queries.sql")
    print("2. Use: psql -d fno_database -f sql/04_queries.sql")


def main():
    """Main function to run the data loading process."""
    print("="*60)
    print("F&O DATABASE - DATA LOADING SCRIPT")
    print("="*60)
    
    # Connect to database
    conn = connect_to_db()
    
    try:
        # Create tables
        create_tables(conn)
        
        # Load CSV
        df = load_csv_data(CSV_FILE_PATH)
        
        # Insert data
        insert_instruments(conn, df)
        insert_expiries(conn, df)
        insert_trades(conn, df)
        compute_daily_aggregates(conn)
        
        # Print summary
        print_summary(conn)
        
    except Exception as e:
        print(f"\nError: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()