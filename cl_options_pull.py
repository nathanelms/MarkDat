#!/usr/bin/env python3
"""
CL OPTIONS DATA PULL — Databento
=================================
Pulls CL (Crude Oil) weekly option settlement data for straddle analysis.
Follows same architecture as daily_0dte_pull.py.

Usage:
  python cl_options_pull.py --discover     # List available CL option symbols
  python cl_options_pull.py --pull         # Pull definitions + settlements
  python cl_options_pull.py --build        # Build IV surface database
  python cl_options_pull.py --cost         # Estimate Databento cost before pulling

CL Option Products on CME (GLBX.MDP3):
  LO.OPT    — CL monthly/quarterly options (standard)
  LO1.OPT   — CL Weekly Monday
  LO2.OPT   — CL Weekly Tuesday  
  LO3.OPT   — CL Weekly Wednesday
  LO4.OPT   — CL Weekly Thursday
  LO5.OPT   — CL Weekly Friday
  
  For 5-DTE straddles we primarily need the weekly Friday expirations (LO5.OPT)
  and the standard monthly (LO.OPT).
"""

import os
import sys
import logging
import argparse
from pathlib import Path
from datetime import datetime, timedelta

try:
    import databento as db
    import pandas as pd
    import numpy as np
except ImportError:
    print("Required: pip install databento pandas numpy")
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
log = logging.getLogger(__name__)

# ── Config ──
API_KEY = os.getenv("DATABENTO_API_KEY", "db-XEEn3gYqXRYCNFRXGWdnS4CLeeK3Y")
DATASET = "GLBX.MDP3"
CACHE_DIR = Path("databento_cache_cl_options")
DB_PATH = Path("cl_options.db")

# Settlement stat_type per Databento docs
STAT_SETTLEMENT = 3

# CL option products
CL_PRODUCTS = {
    'CL_Q':   ('LO.OPT',  'Monthly/Quarterly'),
    'CL_W1':  ('LO1.OPT', 'Weekly Monday'),
    'CL_W2':  ('LO2.OPT', 'Weekly Tuesday'),
    'CL_W3':  ('LO3.OPT', 'Weekly Wednesday'),
    'CL_W4':  ('LO4.OPT', 'Weekly Thursday'),
    'CL_W5':  ('LO5.OPT', 'Weekly Friday'),
}

# Product priority: prefer weeklies for short DTE
PRODUCT_PRIORITY = {
    'CL_W1': 1, 'CL_W2': 1, 'CL_W3': 1, 'CL_W4': 1, 'CL_W5': 1,
    'CL_Q': 2,
}

UNDERLYING = {'CL': 'CL.c.0'}

# Pull range
START_YEAR = 2018  # CL weeklies became more liquid around 2018
END_YEAR = 2027


# ═══════════════════════════════════════════════════════════════════════════
# RETRY DECORATOR (same as daily_0dte_pull.py)
# ═══════════════════════════════════════════════════════════════════════════

import time
import functools

def retry_api(max_retries=3, base_delay=2.0):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt < max_retries - 1:
                        delay = base_delay * (2 ** attempt)
                        log.warning(f"  Retry {attempt+1}/{max_retries} after {delay}s: {e}")
                        time.sleep(delay)
                    else:
                        raise
        return wrapper
    return decorator


# ═══════════════════════════════════════════════════════════════════════════
# DISCOVER — Check what symbols exist
# ═══════════════════════════════════════════════════════════════════════════

def discover():
    """List available CL option products on Databento."""
    client = db.Historical(API_KEY)
    
    print("=" * 70)
    print("  CL OPTION DISCOVERY")
    print("=" * 70)
    
    # Check each product
    for label, (db_sym, desc) in sorted(CL_PRODUCTS.items()):
        print(f"\n  {label} ({db_sym}) — {desc}")
        try:
            # Try a small date range to see if symbol exists
            data = client.timeseries.get_range(
                dataset=DATASET, schema="definition",
                symbols=[db_sym], stype_in="parent",
                start="2024-01-01", end="2024-01-15",
            )
            df = data.to_df().reset_index()
            if 'instrument_class' in df.columns:
                df = df[df['instrument_class'].isin(['C', 'P'])]
            print(f"    ✅ Found {len(df)} contracts in sample period")
            if len(df) > 0 and 'expiration' in df.columns:
                exps = pd.to_datetime(df['expiration']).dt.date.unique()
                print(f"    Expirations in sample: {sorted(exps)[:5]}")
        except Exception as e:
            print(f"    ❌ {e}")
    
    # Also check underlying
    print(f"\n  Underlying: CL.c.0")
    try:
        data = client.timeseries.get_range(
            dataset=DATASET, schema="ohlcv-1d",
            symbols=["CL.c.0"], stype_in="continuous",
            start="2024-01-01", end="2024-01-15",
        )
        df = data.to_df().reset_index()
        print(f"    ✅ {len(df)} bars")
    except Exception as e:
        print(f"    ❌ {e}")


# ═══════════════════════════════════════════════════════════════════════════
# COST ESTIMATE
# ═══════════════════════════════════════════════════════════════════════════

def estimate_cost():
    """Estimate Databento API cost for the full pull."""
    client = db.Historical(API_KEY)
    
    print("=" * 70)
    print("  CL OPTIONS — COST ESTIMATE")
    print("=" * 70)
    
    total_cost = 0
    for label, (db_sym, desc) in sorted(CL_PRODUCTS.items()):
        print(f"\n  {label} ({db_sym}):")
        
        for schema in ["definition", "statistics"]:
            try:
                cost = client.metadata.get_cost(
                    dataset=DATASET,
                    symbols=[db_sym],
                    stype_in="parent",
                    schema=schema,
                    start=f"{START_YEAR}-01-01",
                    end=f"{END_YEAR}-01-01",
                )
                print(f"    {schema}: ${cost:.2f}")
                total_cost += cost
            except Exception as e:
                print(f"    {schema}: error — {e}")
    
    # Underlying
    try:
        cost = client.metadata.get_cost(
            dataset=DATASET,
            symbols=["CL.c.0"],
            stype_in="continuous",
            schema="ohlcv-1d",
            start=f"{START_YEAR}-01-01",
            end=f"{END_YEAR}-01-01",
        )
        print(f"\n  Underlying (CL.c.0 ohlcv-1d): ${cost:.2f}")
        total_cost += cost
    except Exception as e:
        print(f"\n  Underlying: error — {e}")
    
    print(f"\n  {'='*40}")
    print(f"  ESTIMATED TOTAL: ${total_cost:.2f}")
    print(f"  {'='*40}")


# ═══════════════════════════════════════════════════════════════════════════
# PULL
# ═══════════════════════════════════════════════════════════════════════════

@retry_api(max_retries=3, base_delay=2.0)
def _fetch_defs(client, db_sym, start, end):
    return client.timeseries.get_range(
        dataset=DATASET, schema="definition",
        symbols=[db_sym], stype_in="parent",
        start=start, end=end,
    )

@retry_api(max_retries=3, base_delay=2.0)
def _fetch_stats(client, db_sym, start, end):
    return client.timeseries.get_range(
        dataset=DATASET, schema="statistics",
        symbols=[db_sym], stype_in="parent",
        start=start, end=end,
    )

@retry_api(max_retries=3, base_delay=2.0)
def _fetch_ohlcv(client, symbol, start, end):
    return client.timeseries.get_range(
        dataset=DATASET, schema="ohlcv-1d",
        symbols=[symbol], stype_in="continuous",
        start=start, end=end,
    )


def pull(products_filter=None):
    """Pull raw data from Databento."""
    client = db.Historical(API_KEY)
    CACHE_DIR.mkdir(exist_ok=True)
    
    products = dict(CL_PRODUCTS)
    if products_filter:
        pf = products_filter.upper()
        products = {k: v for k, v in products.items() if pf in k}
    
    print("=" * 80)
    print(f"  CL OPTIONS PULL — {len(products)} products")
    print(f"  stat_type={STAT_SETTLEMENT} (settlement price)")
    print(f"  Range: {START_YEAR} to {END_YEAR}")
    print(f"  Cache: {CACHE_DIR}/")
    print("=" * 80)
    
    for label, (db_sym, desc) in sorted(products.items()):
        print(f"\n  === {label} ({db_sym}) — {desc} ===")
        
        # Definitions: yearly
        for year in range(START_YEAR, END_YEAR):
            def_file = CACHE_DIR / f"{label}_{year}_defs.parquet"
            if def_file.exists():
                try:
                    d = pd.read_parquet(def_file)
                    if len(d) > 0:
                        print(f"    {year} defs: cached ({len(d):,})")
                        continue
                except:
                    pass
            
            print(f"    {year} defs...", end=" ", flush=True)
            try:
                data = _fetch_defs(client, db_sym, f"{year}-01-01", f"{year+1}-01-01")
                df_d = data.to_df().reset_index()
                if 'instrument_class' in df_d.columns:
                    df_d = df_d[df_d['instrument_class'].isin(['C', 'P'])]
                keep = ['instrument_id', 'raw_symbol', 'instrument_class',
                        'strike_price', 'expiration', 'underlying', 'asset']
                keep = [c for c in keep if c in df_d.columns]
                df_d = df_d[keep].drop_duplicates(subset=['instrument_id'])
                df_d.to_parquet(def_file, index=False)
                print(f"{len(df_d):,}")
            except Exception as e:
                print(f"ERROR: {e}")
                # Write empty parquet so we don't retry
                pd.DataFrame().to_parquet(def_file, index=False)
        
        # Statistics (settlements): yearly
        for year in range(START_YEAR, END_YEAR):
            stat_file = CACHE_DIR / f"{label}_{year}_stats.parquet"
            if stat_file.exists():
                try:
                    s = pd.read_parquet(stat_file)
                    if len(s) > 0:
                        print(f"    {year} stats: cached ({len(s):,})")
                        continue
                except:
                    pass
            
            print(f"    {year} stats...", end=" ", flush=True)
            try:
                data = _fetch_stats(client, db_sym, f"{year}-01-01", f"{year+1}-01-01")
                df_s = data.to_df().reset_index()
                # Filter to settlement prices only
                if 'stat_type' in df_s.columns:
                    df_s = df_s[df_s['stat_type'] == STAT_SETTLEMENT]
                df_s.to_parquet(stat_file, index=False)
                print(f"{len(df_s):,}")
            except Exception as e:
                print(f"ERROR: {e}")
                pd.DataFrame().to_parquet(stat_file, index=False)
    
    # Underlying OHLCV
    print(f"\n  === CL Underlying (CL.c.0) ===")
    ul_file = CACHE_DIR / "CL_underlying_ohlcv.parquet"
    if ul_file.exists():
        try:
            d = pd.read_parquet(ul_file)
            print(f"    cached ({len(d):,} bars)")
        except:
            pass
    else:
        print(f"    Fetching...", end=" ", flush=True)
        try:
            data = _fetch_ohlcv(client, "CL.c.0", f"{START_YEAR}-01-01", f"{END_YEAR}-01-01")
            df = data.to_df().reset_index()
            df.to_parquet(ul_file, index=False)
            print(f"{len(df):,} bars")
        except Exception as e:
            print(f"ERROR: {e}")
    
    print(f"\n  DONE. Cache: {CACHE_DIR}/")
    print(f"  Next: python cl_options_pull.py --build")


# ═══════════════════════════════════════════════════════════════════════════
# BUILD — Assemble into SQLite
# ═══════════════════════════════════════════════════════════════════════════

def build():
    """Build CL options database from cached parquet files."""
    import sqlite3
    
    print("=" * 80)
    print(f"  CL OPTIONS BUILD → {DB_PATH}")
    print("=" * 80)
    
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    
    # Drop and recreate tables so --build is idempotent
    conn.execute("DROP TABLE IF EXISTS cl_definitions")
    conn.execute("DROP TABLE IF EXISTS cl_settlements")
    conn.execute("DROP TABLE IF EXISTS cl_underlying")

    # Create tables
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cl_definitions (
            instrument_id INTEGER,
            raw_symbol TEXT,
            instrument_class TEXT,
            strike_price REAL,
            expiration TEXT,
            underlying TEXT,
            asset TEXT,
            product TEXT,
            year INTEGER,
            PRIMARY KEY (instrument_id)
        )
    """)
    
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cl_settlements (
            instrument_id INTEGER,
            ts_event TEXT,
            price REAL,
            stat_type INTEGER,
            product TEXT,
            year INTEGER
        )
    """)
    
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cl_underlying (
            date TEXT PRIMARY KEY,
            open REAL, high REAL, low REAL, close REAL, volume INTEGER
        )
    """)
    
    # Load definitions
    total_defs = 0
    for label in sorted(CL_PRODUCTS.keys()):
        for year in range(START_YEAR, END_YEAR):
            f = CACHE_DIR / f"{label}_{year}_defs.parquet"
            if not f.exists(): continue
            try:
                df = pd.read_parquet(f)
                if len(df) == 0: continue
                df['product'] = label
                df['year'] = year
                df.to_sql('cl_definitions', conn, if_exists='append', index=False)
                total_defs += len(df)
            except Exception as e:
                log.warning(f"  {f.name}: {e}")
    
    print(f"  Definitions: {total_defs:,}")
    
    # Load settlements
    total_stats = 0
    for label in sorted(CL_PRODUCTS.keys()):
        for year in range(START_YEAR, END_YEAR):
            f = CACHE_DIR / f"{label}_{year}_stats.parquet"
            if not f.exists(): continue
            try:
                df = pd.read_parquet(f)
                if len(df) == 0: continue
                df['product'] = label
                df['year'] = year
                keep = [c for c in ['instrument_id', 'ts_event', 'price', 'stat_type', 'product', 'year'] if c in df.columns]
                df[keep].to_sql('cl_settlements', conn, if_exists='append', index=False)
                total_stats += len(df)
            except Exception as e:
                log.warning(f"  {f.name}: {e}")
    
    print(f"  Settlements: {total_stats:,}")
    
    # Load underlying
    ul_file = CACHE_DIR / "CL_underlying_ohlcv.parquet"
    if ul_file.exists():
        try:
            df = pd.read_parquet(ul_file)
            if 'ts_event' in df.columns:
                df['date'] = pd.to_datetime(df['ts_event']).dt.strftime('%Y-%m-%d')
            df_out = df[['date', 'open', 'high', 'low', 'close', 'volume']].drop_duplicates(subset='date')
            df_out.to_sql('cl_underlying', conn, if_exists='replace', index=False)
            print(f"  Underlying: {len(df_out):,} bars")
        except Exception as e:
            log.warning(f"  Underlying: {e}")
    
    # Create indexes
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cl_defs_class ON cl_definitions(instrument_class)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cl_defs_exp ON cl_definitions(expiration)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cl_settle_id ON cl_settlements(instrument_id)")
    conn.commit()
    conn.close()
    
    print(f"\n  DONE → {DB_PATH}")


# ═══════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="CL Options Data Pull")
    parser.add_argument("--discover", action="store_true", help="List available CL option symbols")
    parser.add_argument("--cost", action="store_true", help="Estimate Databento cost")
    parser.add_argument("--pull", action="store_true", help="Pull definitions + settlements")
    parser.add_argument("--build", action="store_true", help="Build SQLite database")
    parser.add_argument("--product", type=str, help="Filter to specific product (e.g. W5, Q)")
    
    args = parser.parse_args()
    
    if args.discover:
        discover()
    elif args.cost:
        estimate_cost()
    elif args.pull:
        pull(products_filter=args.product)
    elif args.build:
        build()
    else:
        print("Usage:")
        print("  python cl_options_pull.py --cost       # Check cost first!")
        print("  python cl_options_pull.py --discover   # Check symbol availability")
        print("  python cl_options_pull.py --pull       # Pull all CL option data")
        print("  python cl_options_pull.py --pull --product W5  # Pull Friday weeklies only")
        print("  python cl_options_pull.py --build      # Build database")


if __name__ == "__main__":
    main()