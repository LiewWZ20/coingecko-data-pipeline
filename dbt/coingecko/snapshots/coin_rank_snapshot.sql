{% snapshot coin_rank_snapshot %}

{{
    config(
        target_schema='public',
        unique_key='coin_id',
        strategy='check',
        check_cols=['market_cap_rank', 'price_usd', 'market_cap_usd'],
    )
}}

select
    coin_id,
    coin_symbol,
    coin_name,
    price_usd,
    market_cap_usd,
    market_cap_rank,
    extracted_at
from {{ ref('stg_coins_markets') }}

{% endsnapshot %}