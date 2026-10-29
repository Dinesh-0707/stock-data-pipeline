# Stock Market Data Pipeline

An end-to-end data engineering project that runs locally: scheduled
ingestion, a medallion (Bronze/Silver/Gold) lakehouse, Delta Lake, dbt models
on DuckDB, Airflow orchestration in Docker, and Power BI dashboards.

## Architecture
```
Alpha Vantage API → Python ingestion (requests)
                                          ↓
                                    Bronze Layer (Raw JSON)
                                          ↓
                                    Silver Layer (Parquet)
                                          ↓
                                    Gold Layer (Aggregates)
                                          ↓
                                    Delta Lake (ACID)
                                          ↓
                              dbt Models → DuckDB → Power BI
```

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Ingestion | Python, Alpha Vantage API |
| Storage | Local file system (`data/` folder) |
| File Format | JSON, Parquet, Delta Lake (`deltalake` / delta-rs) |
| Transformation | Python, Pandas |
| Modelling | dbt (data build tool) |
| Orchestration | Apache Airflow (Docker Compose) |
| Warehouse | DuckDB |
| Data Quality | Python validation checks, dbt tests |
| Query Interface | NL-to-SQL prototype (rule-based) |
| Visualisation | Power BI |

## Project Structure
```
stock-data-pipeline/
│
├── ingestion/              # Data ingestion scripts
│   ├── fetch_stock_data.py # Alpha Vantage API ingestion
│   └── nl_to_sql.py        # NL-to-SQL prototype (rule-based)
│
├── transformation/         # Data transformation scripts
│   ├── transform_stock_data.py  # Bronze to Silver
│   ├── build_gold_layer.py      # Silver to Gold
│   ├── delta_lake.py            # Delta Lake conversion
│   ├── silver_utils.py          # Loads silver, newest row per (symbol, date)
│   └── export_for_powerbi.py    # DuckDB marts to CSV for Power BI
│
├── stock_analytics/        # dbt project
│   ├── models/
│   │   ├── staging/        # Staging models + tests
│   │   └── marts/          # Business ready models
│   ├── tests/              # Custom data tests
│   ├── profiles.yml        # DuckDB connection (relative path)
│   └── dbt_project.yml
│
├── orchestration/          # Airflow DAGs
│   └── stock_pipeline_dag.py
│
├── docker/                 # Docker configuration
│   └── Dockerfile
│
├── docker-compose.yml      # Airflow stack
├── requirements.txt        # Pipeline dependencies
├── requirements-dbt.txt    # dbt dependencies
└── README.md
```

## Data Architecture

This project implements the **Medallion Architecture**:

- **Bronze Layer** — Raw JSON data exactly as received from Alpha Vantage API
- **Silver Layer** — Cleaned and validated Parquet files with engineered features
- **Gold Layer** — Business ready aggregates for dashboards and reporting
- **Delta Layer** — ACID compliant Delta Lake tables with time travel support

## Features

### 1. Automated Data Ingestion
Fetches daily stock data for AAPL, MSFT, GOOGL, AMZN and META from 
Alpha Vantage API. Every layer is written per run date (`SYMBOL_YYYYMMDD`),
so re-running a day overwrites that day's files instead of adding duplicates.
API calls retry on network errors and respect the free tier rate limit.

### 2. Data Transformation
- Calculates daily returns, 7/30 day moving averages and volatility
- Validates data quality before loading to Silver layer; a failed check fails
  the Airflow task so bad data never reaches Gold
- Each run lands a full ~100 day snapshot, so Gold, Delta and dbt keep only the
  newest row per `(symbol, date)`
- Builds business ready Gold layer aggregates (weekly returns are compounded,
  not summed)

### 3. Delta Lake
- Converts Parquet files to Delta format with ACID transactions
- Supports time travel queries to audit historical data
- Upsert (MERGE) operations prevent data duplication

### 4. dbt Models
- Staging models clean and standardise raw data
- Mart models build business ready views with performance labels
- 8 automated data quality tests, including one row per `(symbol, date)`
- Auto generated documentation with data lineage

### 5. Apache Airflow Orchestration
- Full pipeline runs automatically at 6pm every weekday
- DAG: ingest → silver → (gold layer, Delta Lake merge, `dbt build`)
- Containerised with Docker for reproducibility; pipeline scripts and dbt
  run in their own virtualenv inside the image

### 6. NL-to-SQL Prototype
A natural language interface for querying stock data. The current version
maps question keywords to SQL templates (`generate_sql_mock` in
`ingestion/nl_to_sql.py`) and runs them against DuckDB. The next step is to
swap in an LLM that gets the schema in its prompt, with a read-only SQL check
before execution.
```
Question: Which stock had the highest closing price today?
Generated SQL: SELECT symbol, close FROM mart_daily_summary 
               ORDER BY close DESC LIMIT 1
Result: GOOGL  189.42
```

## How to Run

### Prerequisites
- Python 3.10–3.12
- Docker Desktop (for Airflow)
- A free [Alpha Vantage](https://www.alphavantage.co/support/#api-key) API key

### Run with Airflow (Docker)
```bash
git clone https://github.com/Dinesh-0707/stock-data-pipeline.git
cd stock-data-pipeline

cp .env.example .env
# Add your Alpha Vantage API key to .env

docker-compose up -d --build
```
Open http://localhost:8080 (login `admin` / `admin`), unpause
`stock_data_pipeline` and trigger it.

### Run locally (without Docker)
All commands run from the repo root.
```bash
python -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt -r requirements-dbt.txt

cp .env.example .env
# Add your Alpha Vantage API key to .env

python ingestion/fetch_stock_data.py           # optional: --run-date YYYYMMDD
python transformation/transform_stock_data.py  # same --run-date as ingestion
python transformation/build_gold_layer.py
python transformation/delta_lake.py

dbt build --project-dir stock_analytics --profiles-dir stock_analytics
python ingestion/nl_to_sql.py
```

## JD Requirements Coverage

This project was built to match real data engineer job requirements:

| Requirement | Implementation |
|-------------|---------------|
| Pipeline architecture | End-to-end medallion architecture |
| ETL/ELT pipelines | Python + dbt |
| Apache Airflow | Automated DAG scheduling |
| dbt | Staging and mart models, tests, docs |
| Delta Lake | ACID transactions, MERGE upserts, time travel |
| Data quality | Python validation + dbt tests |
| Warehouse / SQL | DuckDB |
| BI layer | Power BI dashboards |
| Containerisation | Docker + Docker Compose (Airflow) |

## Not Yet Implemented

These would be the next steps toward a cloud deployment and are **not** in
the current code:
- Cloud storage and warehouse (e.g. Azure Data Lake Storage, Synapse)
- Streaming ingestion (Kafka / Event Hubs)
- Spark processing for larger volumes
- An LLM-backed NL-to-SQL engine
- CI/CD (GitHub Actions)
- Unit tests (pytest) for the Python transformations

## Author

**Dinesh** — MSc AI with Business, Aston University  
github.com/Dinesh-0707
## Dashboard Preview

### Overview Page
![Overview](screenshots/dashboard_overview.png)

### Performance Analysis
![Performance](screenshots/dashboard_performance.png)

### Volatility & Risk Analysis
![Volatility](screenshots/dashboard_volatility.png)

