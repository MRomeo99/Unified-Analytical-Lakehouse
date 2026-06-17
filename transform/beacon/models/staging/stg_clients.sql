-- Silver: deduplicated and typed client records
-- One row per client_id; surrogate key added for joins

with source as (
    -- pull raw append-only client records from Bronze
    select * from {{ source('bronze_clients', 'raw_clients') }}
),

deduped as (
    -- keep the latest version of each client by id
    select
        *,
        row_number() over (partition by id order by _loaded_at desc) as _row_num
    from source
),

typed as (
    -- enforce correct types and add surrogate key
    select
        {{ dbt_utils.generate_surrogate_key(['id']) }}   as client_key,
        cast(id as integer)                              as client_id,
        name                                             as client_name,
        lower(industry)                                  as industry,
        lower(plan_tier)                                 as plan_tier,
        cast(onboard_date as date)                       as onboard_date,
        city,
        state,
        cast(_loaded_at as timestamp)                    as _loaded_at
    from deduped
    where _row_num = 1
)

select * from typed
