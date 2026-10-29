import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv

ROOT = Path(__file__).parent.parent
BRONZE_PATH = ROOT / "data/bronze"

load_dotenv(dotenv_path=ROOT / ".env")

API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
BASE_URL = "https://www.alphavantage.co/query"

STOCKS = ["AAPL", "MSFT", "GOOGL", "AMZN", "META"]

def fetch_stock_data(symbol, retries=3):
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "outputsize": "compact",
        "apikey": API_KEY
    }

    for attempt in range(1, retries + 1):
        try:
            response = requests.get(BASE_URL, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as e:
            print(f"Attempt {attempt} failed for {symbol}: {e}")
            time.sleep(5 * attempt)
            continue

        if "Time Series (Daily)" in data:
            print(f"Successfully fetched data for {symbol}")
            return data

        # Alpha Vantage returns HTTP 200 with a "Note"/"Information" message
        # when the rate limit is hit or the key is invalid
        print(f"Error fetching {symbol}: {data}")
        return None

    return None

def save_raw_data(symbol, data, run_date):
    os.makedirs(BRONZE_PATH, exist_ok=True)

    # One file per symbol per run date: re-running the same day overwrites
    # instead of piling up duplicates (idempotent)
    filename = BRONZE_PATH / f"{symbol}_{run_date}.json"

    with open(filename, "w") as f:
        json.dump(data, f, indent=2)

    print(f"Saved {symbol} data to {filename}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-date", default=datetime.now().strftime("%Y%m%d"),
                        help="Run date as YYYYMMDD (defaults to today)")
    args = parser.parse_args()

    if not API_KEY:
        sys.exit("ALPHA_VANTAGE_API_KEY is not set - add it to .env")

    print(f"Starting stock data ingestion for run date {args.run_date}...")

    failed = []
    for symbol in STOCKS:
        data = fetch_stock_data(symbol)
        if data:
            save_raw_data(symbol, data, args.run_date)
        else:
            failed.append(symbol)
        # Free tier allows 5 requests per minute
        time.sleep(12)

    if failed:
        sys.exit(f"Ingestion failed for: {', '.join(failed)}")

    print("Ingestion complete!")

if __name__ == "__main__":
    main()
