with source as (
    select * from {{source('public', 'raw_price_history')}}
),
cleaned as (
    select
        coin_id,
        price_date::date as price_date,
        price_usd::numeric as price_usd,
        market_cap_usd::numeric as market_cap_usd,
        volume_usd::numeric as volume_usd,
        extracted_at::timestamp as extracted_at
    from source
    where price_usd is not null
)
select * from cleaned