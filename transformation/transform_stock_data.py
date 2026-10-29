import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

BRONZE_PATH = Path(__file__).parent.parent / "data/bronze"
SILVER_PATH = Path(__file__).parent.parent / "data/silver"

def load_bronze_data(filepath):
    with open(filepath, "r") as f:
        raw = json.load(f)
    
    time_series = raw.get("Time Series (Daily)", {})
    
    rows = []
    for date, values in time_series.items():
        rows.append({
            "date": date,
            "open": float(values["1. open"]),
            "high": float(values["2. high"]),
            "low": float(values["3. low"]),
            "close": float(values["4. close"]),
            "volume": int(values["5. volume"])
        })
    
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)
    
    return df

def add_features(df, symbol, run_date):
    df["symbol"] = symbol
    df["daily_return"] = df["close"].pct_change() * 100
    df["ma_7"] = df["close"].rolling(window=7).mean()
    df["ma_30"] = df["close"].rolling(window=30).mean()
    df["volatility_7"] = df["daily_return"].rolling(window=7).std()
    df["price_range"] = df["high"] - df["low"]
    # run_date says which snapshot a row came from, so downstream layers can
    # keep only the newest version of each (symbol, date)
    df["run_date"] = run_date
    df["ingested_at"] = datetime.now()
    
    return df

def validate_data(df, symbol):
    issues = []
    
    if df.empty:
        issues.append("No rows in bronze file")
    
    if df["close"].isnull().any():
        issues.append("Null values found in close price")
    
    if (df["close"] <= 0).any():
        issues.append("Zero or negative close prices found")
    
    if (df["high"] < df["low"]).any():
        issues.append("High price is lower than low price")
    
    if (df["volume"] < 0).any():
        issues.append("Negative volume found")
    
    if df["date"].duplicated().any():
        issues.append("Duplicate trading dates found")
    
    if issues:
        for issue in issues:
            print(f"VALIDATION FAILED for {symbol}: {issue}")
        return False
    
    print(f"Validation passed for {symbol}")
    return True

def save_silver_data(df, symbol, run_date):
    os.makedirs(SILVER_PATH, exist_ok=True)
    
    # Overwrite so a re-run for the same date replaces bad data (idempotent)
    filename = SILVER_PATH / f"{symbol}_{run_date}.parquet"
    df.to_parquet(filename, index=False)
    print(f"Saved silver data for {symbol} to {filename}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-date", default=datetime.now().strftime("%Y%m%d"),
                        help="Run date as YYYYMMDD (defaults to today)")
    args = parser.parse_args()
    
    print(f"Starting transformation for run date {args.run_date}...")
    
    bronze_files = sorted(BRONZE_PATH.glob(f"*_{args.run_date}.json"))
    
    if not bronze_files:
        sys.exit(f"No bronze files found for run date {args.run_date}!")
    
    failed = []
    for filepath in bronze_files:
        symbol = filepath.stem.split("_")[0]
        print(f"\nProcessing {symbol}...")
        
        df = load_bronze_data(filepath)
        if df.empty or not validate_data(df, symbol):
            failed.append(symbol)
            continue
        
        df = add_features(df, symbol, args.run_date)
        save_silver_data(df, symbol, args.run_date)
    
    # Fail the task so Airflow stops before bad data reaches gold
    if failed:
        sys.exit(f"Validation failed for: {', '.join(failed)}")
    
    print("\nTransformation complete!")

if __name__ == "__main__":
    main()
