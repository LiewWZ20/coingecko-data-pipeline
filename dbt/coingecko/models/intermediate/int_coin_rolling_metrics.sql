with base as (
    select * from {{ref('stg_price_history')}}
),
with_returns as (
    select
        coin_id,
        price_date,
        price_usd,
        market_cap_usd,
        volume_usd,

        -- day over day return
        round(
            (price_usd - lag(price_usd) over (
                partition by coin_id order by price_date
            )) / nullif(lag(price_usd) over (
                partition by coin_id order by price_date
            ), 0) * 100, 4
        ) as daily_return_pct,

        -- 7 day rolling average price
        round(avg(price_usd) over (
            partition by coin_id
            order by price_date
            rows between 6 preceding and current row
        ), 2) as price_7d_avg,

        -- 30 day rolling average price
        round(avg(price_usd) over (
            partition by coin_id
            order by price_date
            rows between 29 preceding and current row
        ), 2) as price_30d_avg,

        -- 7 day rolling volatility (std dev of daily returns)
        round(stddev(price_usd) over (
            partition by coin_id
            order by price_date
            rows between 6 preceding and current row
        ), 2) as volatility_7d,

        -- 30 day rolling volatility
        round(stddev(price_usd) over (
            partition by coin_id
            order by price_date
            rows between 29 preceding and current row
        ), 2) as volatility_30d,

        -- volume 7d average
        round(avg(volume_usd) over (
            partition by coin_id
            order by price_date
            rows between 6 preceding and current row
        ), 2) as volume_7d_avg,

        -- rank price within coin history
        rank() over (
            partition by coin_id order by price_usd desc
        ) as price_rank_desc
    from base
)
select * from with_returns