with stock_prices as (
    select * from {{ ref('stg_stock_prices') }}
),

latest_date as (
    select max(date) as max_date
    from stock_prices
),

daily_summary as (
    select
        sp.symbol,
        sp.date,
        sp.open,
        sp.high,
        sp.low,
        sp.close,
        sp.volume,
        round(sp.daily_return, 4)   as daily_return_pct,
        round(sp.ma_7, 2)           as moving_avg_7d,
        round(sp.ma_30, 2)          as moving_avg_30d,
        round(sp.volatility_7, 4)   as volatility_7d,
        case
            when sp.daily_return > 2  then 'strong gain'
            when sp.daily_return > 0  then 'gain'
            when sp.daily_return = 0  then 'flat'
            when sp.daily_return > -2 then 'loss'
            else 'strong loss'
        end                         as performance_label,
        case
            when sp.close > sp.ma_30 then 'above 30d average'
            else 'below 30d average'
        end                         as trend_signal
    from stock_prices sp
    inner join latest_date ld
        on sp.date = ld.max_date
)

select * from daily_summary
order by daily_return_pct desc
