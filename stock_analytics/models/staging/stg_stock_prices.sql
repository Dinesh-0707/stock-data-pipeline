with source as (
    select *
    from read_parquet('C:/Users/dines/OneDrive - Aston University/Desktop/Data Engineer Project/data/silver/*.parquet')
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
    from source
    where close > 0
      and volume > 0
      and date is not null
)

select * from cleaned
