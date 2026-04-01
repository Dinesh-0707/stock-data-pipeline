import duckdb
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")

DB_PATH = str(Path(__file__).parent.parent / "data/stock_analytics.duckdb")

def generate_sql_mock(question):
    question_lower = question.lower()
    
    if "highest closing" in question_lower:
        return "SELECT symbol, close FROM mart_daily_summary ORDER BY close DESC LIMIT 1"
    elif "average daily return" in question_lower:
        return "SELECT symbol, ROUND(AVG(daily_return), 4) as avg_return FROM stg_stock_prices GROUP BY symbol ORDER BY avg_return DESC"
    elif "most volatile" in question_lower:
        return "SELECT symbol, ROUND(AVG(volatility_7), 4) as avg_volatility FROM stg_stock_prices GROUP BY symbol ORDER BY avg_volatility DESC LIMIT 1"
    elif "above" in question_lower and "30 day" in question_lower:
        return "SELECT symbol, close, moving_avg_30d FROM mart_daily_summary WHERE trend_signal = 'above 30d average'"
    elif "highest return" in question_lower:
        return "SELECT symbol, daily_return_pct FROM mart_daily_summary ORDER BY daily_return_pct DESC LIMIT 1"
    elif "lowest" in question_lower and "price" in question_lower:
        return "SELECT symbol, close FROM mart_daily_summary ORDER BY close ASC LIMIT 1"
    elif "volume" in question_lower:
        return "SELECT symbol, volume FROM mart_daily_summary ORDER BY volume DESC"
    elif "performance" in question_lower:
        return "SELECT symbol, close, daily_return_pct, performance_label FROM mart_daily_summary ORDER BY daily_return_pct DESC"
    else:
        return "SELECT * FROM mart_daily_summary"

def run_query(sql):
    conn = duckdb.connect(DB_PATH)
    result = conn.execute(sql).fetchdf()
    conn.close()
    return result

def ask(question):
    print(f"\nQuestion: {question}")
    print("-" * 50)
    
    sql = generate_sql_mock(question)
    print(f"Generated SQL:\n{sql}")
    print("-" * 50)
    
    try:
        result = run_query(sql)
        print(f"Result:\n{result.to_string(index=False)}")
    except Exception as e:
        print(f"Query error: {e}")
    print("-" * 50)

def main():
    print("NL-to-SQL Stock Analytics Engine")
    print("=" * 50)
    print("NOTE: Using mock SQL generation.")
    print("Swap in Claude API once credits are added.")
    print("=" * 50)
    
    questions = [
        "Which stock had the highest closing price today?",
        "What is the average daily return for each stock?",
        "Which stock is most volatile?",
        "Show me stocks that are above their 30 day moving average",
        "Which stock had the highest return?",
        "Show me the performance of all stocks"
    ]
    
    for question in questions:
        ask(question)

if __name__ == "__main__":
    main()
