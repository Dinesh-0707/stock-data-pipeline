import pandas as pd
import json
import os
from pathlib import Path
from datetime import datetime

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

def add_features(df, symbol):
    df["symbol"] = symbol
    df["daily_return"] = df["close"].pct_change() * 100
    df["ma_7"] = df["close"].rolling(window=7).mean()
    df["ma_30"] = df["close"].rolling(window=30).mean()
    df["volatility_7"] = df["daily_return"].rolling(window=7).std()
    df["price_range"] = df["high"] - df["low"]
    df["ingested_at"] = datetime.now()
    
    return df

def validate_data(df, symbol):
    issues = []
    
    if df["close"].isnull().any():
        issues.append("Null values found in close price")
    
    if (df["close"] <= 0).any():
        issues.append("Zero or negative close prices found")
    
    if (df["high"] < df["low"]).any():
        issues.append("High price is lower than low price")
    
    if (df["volume"] < 0).any():
        issues.append("Negative volume found")
    
    if issues:
        for issue in issues:
            print(f"VALIDATION FAILED for {symbol}: {issue}")
        return False
    
    print(f"Validation passed for {symbol}")
    return True

def save_silver_data(df, symbol):
    os.makedirs(SILVER_PATH, exist_ok=True)
    
    today = datetime.now().strftime("%Y%m%d")
    filename = SILVER_PATH / f"{symbol}_{today}.parquet"
    
    if filename.exists():
        print(f"Silver data for {symbol} already exists today - skipping")
        return
    
    df.to_parquet(filename, index=False)
    print(f"Saved silver data for {symbol} to {filename}")

def main():
    print("Starting transformation...")
    
    bronze_files = list(BRONZE_PATH.glob("*.json"))
    
    if not bronze_files:
        print("No bronze files found!")
        return
    
    for filepath in bronze_files:
        symbol = filepath.stem.split("_")[0]
        print(f"\nProcessing {symbol}...")
        
        df = load_bronze_data(filepath)
        df = add_features(df, symbol)
        
        if validate_data(df, symbol):
            save_silver_data(df, symbol)
    
    print("\nTransformation complete!")

if __name__ == "__main__":
    main()
