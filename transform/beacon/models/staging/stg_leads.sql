-- Silver: deduplicated and typed lead records
-- Deduplication via ROW_NUMBER to handle potential duplicate ingestion

with source as (
    -- raw lead events from Bronze
    select * from {{ source('bronze_leads', 'raw_leads') }}
),

deduped as (
    -- latest record wins per lead id
    select
        *,
        row_number() over (partition by id order by _loaded_at desc) as _row_num
    from source
),

typed as (
    select
        {{ dbt_utils.generate_surrogate_key(['id']) }}          as lead_key,
        cast(id as integer)                                     as lead_id,
        cast(client_id as integer)                              as client_id,
        lower(source)                                           as lead_source,
        lower(status)                                           as lead_status,
        cast(value as decimal(10, 2))                           as lead_value,
        cast(created_at as timestamp)                           as created_at,
        cast(_loaded_at as timestamp)                           as _loaded_at
    from deduped
    where _row_num = 1
)

select * from typed
