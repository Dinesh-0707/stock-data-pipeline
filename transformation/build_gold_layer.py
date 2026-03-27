import pandas as pd
import os
from pathlib import Path
from datetime import datetime

SILVER_PATH = Path(__file__).parent.parent / "data/silver"
GOLD_PATH = Path(__file__).parent.parent / "data/gold"

def load_all_silver():
    silver_files = list(SILVER_PATH.glob("*.parquet"))
    
    if not silver_files:
        print("No silver files found!")
        return None
    
    dfs = []
    for filepath in silver_files:
        df = pd.read_parquet(filepath)
        dfs.append(df)
    
    combined = pd.concat(dfs, ignore_index=True)
    print(f"Loaded {len(combined)} rows across {len(silver_files)} stocks")
    return combined

def build_daily_summary(df):
    latest_date = df["date"].max()
    latest = df[df["date"] == latest_date].copy()
    
    summary = latest[[
        "symbol",
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "daily_return",
        "ma_7",
        "ma_30",
        "volatility_7"
    ]].copy()
    
    summary["price_vs_ma7"] = ((summary["close"] - summary["ma_7"]) / summary["ma_7"] * 100).round(2)
    summary["price_vs_ma30"] = ((summary["close"] - summary["ma_30"]) / summary["ma_30"] * 100).round(2)
    
    return summary

def build_top_movers(df):
    latest_date = df["date"].max()
    latest = df[df["date"] == latest_date].copy()
    
    latest["abs_return"] = latest["daily_return"].abs()
    top_movers = latest.nlargest(10, "abs_return")[[
        "symbol",
        "date", 
        "close",
        "daily_return",
        "volume"
    ]].copy()
    
    top_movers["direction"] = top_movers["daily_return"].apply(
        lambda x: "UP" if x > 0 else "DOWN"
    )
    
    return top_movers

def build_weekly_performance(df):
    df["week"] = df["date"].dt.to_period("W")
    
    weekly = df.groupby(["symbol", "week"]).agg(
        avg_close=("close", "mean"),
        avg_volume=("volume", "mean"),
        weekly_return=("daily_return", "sum"),
        avg_volatility=("volatility_7", "mean")
    ).reset_index()
    
    weekly["week"] = weekly["week"].astype(str)
    
    return weekly

def save_gold_table(df, table_name):
    os.makedirs(GOLD_PATH, exist_ok=True)
    
    filepath = GOLD_PATH / f"{table_name}.parquet"
    df.to_parquet(filepath, index=False)
    print(f"Saved gold table: {table_name} ({len(df)} rows)")

def main():
    print("Building gold layer...")
    
    df = load_all_silver()
    if df is None:
        return
    
    print("\nBuilding daily summary...")
    daily_summary = build_daily_summary(df)
    save_gold_table(daily_summary, "daily_summary")
    
    print("\nBuilding top movers...")
    top_movers = build_top_movers(df)
    save_gold_table(top_movers, "top_movers")
    
    print("\nBuilding weekly performance...")
    weekly_performance = build_weekly_performance(df)
    save_gold_table(weekly_performance, "weekly_performance")
    
    print("\nGold layer complete!")
    print("\nDaily Summary:")
    print(daily_summary[["symbol", "close", "daily_return", "ma_7", "ma_30"]].to_string(index=False))

if __name__ == "__main__":
    main()
