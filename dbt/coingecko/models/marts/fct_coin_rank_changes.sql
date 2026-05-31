{{
    config(
        materialized='incremental',
        unique_key=['coin_id', 'changed_at'],
        incremental_strategy='merge'
    )
}}
with snapshot as (
    select * from {{ref('coin_rank_snapshot')}}
),
with_previous as (
    select
        coin_id,
        coin_name,
        coin_symbol,
        market_cap_rank as current_rank,
        lag(market_cap_rank) over (
            partition by coin_id
            order by dbt_valid_from
        ) as previous_rank,
        price_usd as price_at_change,
        market_cap_usd as market_cap_at_change,
        dbt_valid_from as changed_at,
        dbt_valid_to as valid_until,
        case
            when dbt_valid_to is null then true
            else false
        end as is_current
    from snapshot
),
changes as (
    select
        coin_id,
        coin_name,
        coin_symbol,
        current_rank,
        previous_rank,
        current_rank - previous_rank as rank_change,
        case
            when current_rank < previous_rank then '📈 UP'
            when current_rank > previous_rank then '📉 DOWN'
            else '➡️ UNCHANGED'
        end as direction,
        abs(current_rank - previous_rank) as positions_moved,
        price_at_change,
        market_cap_at_change,
        changed_at,
        valid_until,
        is_current
    from with_previous
    where previous_rank is not null
),
final as (
    select * from changes
    {% if is_incremental() %}
    where changed_at >= (
        select max(changed_at) from {{this}}
    )
    {% endif %}
)
select * from final