-- Silver: typed SEO keyword ranking snapshots
-- Source is weekly; no deduplication needed (composite primary key is unique)

with source as (
    -- weekly snapshot rows from Bronze (JSON fixture → dlt → Delta)
    select * from {{ source('bronze_seo', 'raw_seo_rankings') }}
),

typed as (
    select
        {{ dbt_utils.generate_surrogate_key(['client_id', 'keyword', 'snapshot_date']) }}
                                                             as seo_key,
        cast(client_id as integer)                           as client_id,
        keyword,
        cast(position as integer)                            as position,
        cast(snapshot_date as date)                          as snapshot_date,
        cast(current_timestamp as timestamp)                 as _loaded_at
    from source
)

select * from typed
