import requests
import json
import os
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
import time

env_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

API_KEY = "IJQ8EANU71Y6WQ0F"
BASE_URL = "https://www.alphavantage.co/query"

STOCKS = ["AAPL", "MSFT", "GOOGL", "AMZN", "META"]

def fetch_stock_data(symbol):
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "outputsize": "compact",
        "apikey": API_KEY
    }
    
    response = requests.get(BASE_URL, params=params)
    data = response.json()
    
    if "Time Series (Daily)" in data:
        print(f"Successfully fetched data for {symbol}")
        return data
    else:
        print(f"Error fetching {symbol}: {data}")
        return None

def save_raw_data(symbol, data):
    os.makedirs("data/bronze", exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"data/bronze/{symbol}_{timestamp}.json"
    
    with open(filename, "w") as f:
        json.dump(data, f, indent=2)
    
    print(f"Saved {symbol} data to {filename}")

def main():
    print("Starting stock data ingestion...")
    
    for symbol in STOCKS:
        data = fetch_stock_data(symbol)
        if data:
            save_raw_data(symbol, data)
        time.sleep(12)
    
    print("Ingestion complete!")

if __name__ == "__main__":
    main()
