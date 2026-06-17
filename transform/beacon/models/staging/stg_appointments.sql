-- Silver: deduplicated appointment records

with source as (
    -- raw appointment records from Bronze
    select * from {{ source('bronze_appointments', 'raw_appointments') }}
),

deduped as (
    select
        *,
        row_number() over (partition by id order by _loaded_at desc) as _row_num
    from source
),

typed as (
    select
        {{ dbt_utils.generate_surrogate_key(['id']) }}       as appointment_key,
        cast(id as integer)                                  as appointment_id,
        cast(lead_id as integer)                             as lead_id,
        cast(client_id as integer)                           as client_id,
        cast(scheduled_at as timestamp)                      as scheduled_at,
        lower(status)                                        as appointment_status,
        notes,
        cast(_loaded_at as timestamp)                        as _loaded_at
    from deduped
    where _row_num = 1
)

select * from typed
