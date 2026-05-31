with source as (
    select * from {{source('public', 'raw_coins_markets')}}
),
cleaned as (
    select
        id as coin_id,
        symbol as coin_symbol,
        name as coin_name,
        current_price::numeric as price_usd,
        market_cap::numeric as market_cap_usd,
        market_cap_rank::integer as market_cap_rank,
        total_volume::numeric as volume_24h_usd,
        high_24h::numeric as high_24h_usd,
        low_24h::numeric as low_24h_usd,
        price_change_24h::numeric as price_change_24h_usd,
        price_change_pct_24h::numeric as price_change_pct_24h,
        circulating_supply::numeric as circulating_supply,
        total_supply::numeric as total_supply,
        ath::numeric as all_time_high_usd,
        ath_date::timestamp as all_time_high_date,
        last_updated::timestamp as last_updated_at,
        extracted_at::timestamp as extracted_at
    from source
)
select * from cleaned