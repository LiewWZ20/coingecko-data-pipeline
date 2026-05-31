with base as (
    select * from {{ref('stg_coins_markets')}}
),
analysis as (
    select
        coin_id,
        coin_symbol,
        coin_name,
        price_usd,
        market_cap_usd,
        market_cap_rank,
        volume_24h_usd,
        high_24h_usd,
        low_24h_usd,
        price_change_pct_24h,
        all_time_high_usd,

        -- derived metrics
        round(high_24h_usd - low_24h_usd, 2) as price_range_24h,
        round(
            (high_24h_usd - low_24h_usd)
            / nullif(low_24h_usd, 0) * 100, 2
        ) as volatility_pct_24h,
        round(
            price_usd / nullif(all_time_high_usd, 0) * 100, 2
        ) as pct_of_ath,
        round(
            volume_24h_usd / nullif(market_cap_usd, 0), 4
        ) as volume_to_mcap_ratio,
        extracted_at
    from base
)
select * from analysis