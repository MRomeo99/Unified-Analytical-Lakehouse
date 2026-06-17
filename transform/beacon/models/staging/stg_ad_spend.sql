-- Silver: typed daily ad spend records from CSV

with source as (
    -- raw daily rows from Bronze (CSV → dlt → Delta)
    select * from {{ source('bronze_ad_spend', 'raw_ad_spend') }}
),

deduped as (
    -- guard against duplicate CSV loads
    select
        *,
        row_number() over (
            partition by client_id, channel, date
            order by _dlt_load_time desc
        ) as _row_num
    from source
),

typed as (
    select
        {{ dbt_utils.generate_surrogate_key(['client_id', 'channel', 'date']) }}
                                                            as ad_spend_key,
        cast(client_id as integer)                          as client_id,
        lower(channel)                                      as channel,
        cast(date as date)                                  as spend_date,
        cast(spend as decimal(10, 2))                       as spend,
        cast(impressions as integer)                        as impressions,
        cast(clicks as integer)                             as clicks,
        current_timestamp                                   as _loaded_at
    from deduped
    where _row_num = 1
)

select * from typed
