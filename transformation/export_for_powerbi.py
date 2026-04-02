import pandas as pd
import duckdb
from pathlib import Path
import os

DB_PATH = str(Path(__file__).parent.parent / "data/stock_analytics.duckdb")
EXPORT_PATH = Path(__file__).parent.parent / "data/powerbi"

os.makedirs(EXPORT_PATH, exist_ok=True)

conn = duckdb.connect(DB_PATH)

print("Exporting data for Power BI and EDA...")

daily_summary = conn.execute("SELECT * FROM mart_daily_summary").fetchdf()
daily_summary.to_csv(EXPORT_PATH / "daily_summary.csv", index=False)
print(f"Exported daily_summary: {len(daily_summary)} rows")

all_prices = conn.execute("SELECT * FROM stg_stock_prices").fetchdf()
all_prices.to_csv(EXPORT_PATH / "stock_prices.csv", index=False)
print(f"Exported stock_prices: {len(all_prices)} rows")

conn.close()
print(f"\nFiles saved to data/powerbi/")
print("Ready for Power BI and Python EDA!")