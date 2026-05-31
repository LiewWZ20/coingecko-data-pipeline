with coins as (
    select * from {{ref('int_coin_price_analysis')}}
),
global as (
    select * from {{ref('stg_global_stats')}}
    order by extracted_at desc
    limit 1
),
final as (
    select
        c.coin_id,
        c.coin_symbol,
        c.coin_name,
        c.price_usd,
        c.market_cap_usd,
        c.market_cap_rank,
        c.volume_24h_usd,
        c.price_change_pct_24h,
        c.price_range_24h,
        c.volatility_pct_24h,
        c.pct_of_ath,
        c.volume_to_mcap_ratio,

        -- market context
        g.total_market_cap_usd,
        g.btc_dominance_pct,
        g.market_cap_change_pct_24h as global_mcap_change_pct_24h,
        round(
            c.market_cap_usd
            / nullif(g.total_market_cap_usd, 0) * 100, 4
        ) as coin_market_dominance_pct,
        c.extracted_at
    from coins c
    cross join global g
)
select * from final