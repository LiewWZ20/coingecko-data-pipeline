{{
    config(
        materialized='incremental',
        unique_key=['coin_id', 'price_date'],
        incremental_strategy='merge'
    )
}}
with history as (
    select * from {{ref('int_coin_rolling_metrics')}}

    {% if is_incremental() %}
        -- only grab records newer than what's already in the table
        where price_date > (
            select max(price_date)
            from {{ this }}
        )
    {% endif %}
),
coins as (
    select distinct
        coin_id,
        coin_symbol,
        coin_name,
        all_time_high_usd
    from {{ref('stg_coins_markets')}}
),
final as (
    select
        h.coin_id,
        c.coin_symbol,
        c.coin_name,
        h.price_date,
        h.price_usd,
        h.market_cap_usd,
        h.volume_usd,
        h.daily_return_pct,
        h.price_7d_avg,
        h.price_30d_avg,
        h.volatility_7d,
        h.volatility_30d,
        h.volume_7d_avg,
        h.price_rank_desc,
        case when h.price_rank_desc = 1
            then true else false
        end as is_90d_high,
        round(
            (h.price_usd - first_value(h.price_usd) over (
                partition by h.coin_id order by h.price_usd desc
            )) / nullif(first_value(h.price_usd) over (
                partition by h.coin_id order by h.price_usd desc
            ), 0) * 100, 2
        ) as pct_from_90d_high,

        round(
            h.price_usd / nullif(c.all_time_high_usd, 0) * 100, 2
        ) as pct_of_ath
    from history h
    left join coins c using (coin_id)
)
select * from final