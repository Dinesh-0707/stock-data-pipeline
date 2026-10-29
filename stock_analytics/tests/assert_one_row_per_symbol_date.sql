-- Fails if any (symbol, date) appears more than once after deduplication
select symbol, date, count(*) as row_count
from {{ ref('stg_stock_prices') }}
group by symbol, date
having count(*) > 1
