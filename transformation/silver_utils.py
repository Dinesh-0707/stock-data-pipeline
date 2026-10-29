from pathlib import Path

import pandas as pd

SILVER_PATH = Path(__file__).parent.parent / "data/silver"

def load_latest_silver(symbol=None):
    """Load silver files and keep one row per (symbol, date).

    Each run writes a full ~100 day snapshot, so the same trading date shows up
    in several files. The newest snapshot wins.
    """
    pattern = f"{symbol}_*.parquet" if symbol else "*.parquet"
    silver_files = sorted(SILVER_PATH.glob(pattern))

    if not silver_files:
        return None

    combined = pd.concat([pd.read_parquet(f) for f in silver_files], ignore_index=True)

    # Files written before run_date existed fall back to their file name order
    if "run_date" not in combined.columns:
        combined["run_date"] = None
    combined = (
        combined
        .sort_values(["run_date", "ingested_at"], na_position="first")
        .drop_duplicates(subset=["symbol", "date"], keep="last")
        .sort_values(["symbol", "date"])
        .reset_index(drop=True)
    )

    print(f"Loaded {len(combined)} unique rows from {len(silver_files)} silver files")
    return combined
