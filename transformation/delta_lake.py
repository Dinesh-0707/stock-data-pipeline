import pandas as pd
from deltalake import DeltaTable, write_deltalake
from pathlib import Path
from datetime import datetime
import os

SILVER_PATH = Path(__file__).parent.parent / "data/silver"
DELTA_PATH = Path(__file__).parent.parent / "data/delta"

def convert_to_delta(symbol):
    parquet_files = list(SILVER_PATH.glob(f"{symbol}_*.parquet"))
    
    if not parquet_files:
        print(f"No parquet files found for {symbol}")
        return
    
    df = pd.read_parquet(parquet_files[0])
    delta_table_path = str(DELTA_PATH / symbol)
    
    if DeltaTable.is_deltatable(delta_table_path):
        print(f"Upserting {symbol} into existing Delta table...")
        dt = DeltaTable(delta_table_path)
        dt.merge(
            source=df,
            predicate="target.date = source.date AND target.symbol = source.symbol",
            source_alias="source",
            target_alias="target"
        ).when_matched_update_all(
        ).when_not_matched_insert_all(
        ).execute()
    else:
        print(f"Creating new Delta table for {symbol}...")
        write_deltalake(
            delta_table_path,
            df,
            mode="overwrite"
        )
    
    print(f"Delta table for {symbol} is ready!")

def show_time_travel(symbol):
    delta_table_path = str(DELTA_PATH / symbol)
    
    if not DeltaTable.is_deltatable(delta_table_path):
        print(f"No Delta table found for {symbol}")
        return
    
    dt = DeltaTable(delta_table_path)
    
    print(f"\nDelta table history for {symbol}:")
    history = dt.history()
    for entry in history[:3]:
        print(f"  Version {entry['version']} - {entry['timestamp']} - {entry['operation']}")
    
    print(f"\nCurrent version: {dt.version()}")
    print(f"Files in table: {len(dt.file_uris())}")

def main():
    os.makedirs(DELTA_PATH, exist_ok=True)
    
    STOCKS = ["AAPL", "MSFT", "GOOGL", "AMZN", "META"]
    
    print("Converting Parquet files to Delta Lake format...")
    for symbol in STOCKS:
        convert_to_delta(symbol)
    
    print("\nShowing Delta Lake time travel for AAPL:")
    show_time_travel("AAPL")
    
    print("\nDelta Lake conversion complete!")

if __name__ == "__main__":
    main()
