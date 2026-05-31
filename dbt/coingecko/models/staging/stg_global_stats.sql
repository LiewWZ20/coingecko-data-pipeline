with source as (
    select * from {{source('public', 'raw_global_stats')}}
),
cleaned as (
    select
        active_cryptocurrencies::integer as active_cryptocurrencies,
        total_market_cap_usd::numeric as total_market_cap_usd,
        total_volume_usd::numeric as total_volume_usd,
        btc_dominance::numeric as btc_dominance_pct,
        eth_dominance::numeric as eth_dominance_pct,
        market_cap_change_pct_24h::numeric as market_cap_change_pct_24h,
        extracted_at::timestamp as extracted_at
    from source
)
select * from cleaned