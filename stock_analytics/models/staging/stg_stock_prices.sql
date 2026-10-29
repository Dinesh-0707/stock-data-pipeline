with source as (
    -- Path is relative to where dbt runs (the repo root); override with SILVER_PATH
    select *
    from read_parquet('{{ env_var("SILVER_PATH", "data/silver") }}/*.parquet', union_by_name = true)
),

latest_snapshot as (
    -- Every run writes a full ~100 day snapshot, so keep only the newest
    -- version of each (symbol, date)
    select *
    from source
    qualify row_number() over (
        partition by symbol, date
        order by run_date desc nulls last, ingested_at desc
    ) = 1
),

cleaned as (
    select
        symbol,
        date,
        open,
        high,
        low,
        close,
        volume,
        daily_return,
        ma_7,
        ma_30,
        volatility_7,
        price_range,
        ingested_at,
        current_timestamp as dbt_updated_at
    from latest_snapshot
    where close > 0
      and volume > 0
      and date is not null
)

select * from cleaned
