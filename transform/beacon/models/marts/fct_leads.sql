-- Gold: lead fact table enriched with client context
-- Grain: one row per lead

with leads as (
    -- all Silver leads
    select * from {{ ref('stg_leads') }}
),

clients as (
    -- client dimension for enrichment
    select client_id, industry, plan_tier from {{ ref('stg_clients') }}
),

appointments as (
    -- check if a lead resulted in an appointment
    select distinct lead_id
    from {{ ref('stg_appointments') }}
),

final as (
    select
        l.lead_key,
        l.lead_id,
        l.client_id,
        c.industry,
        c.plan_tier,
        l.lead_source,
        l.lead_status,
        l.lead_value,
        l.created_at,
        -- derived flag: was this lead converted?
        case when l.lead_status = 'converted' then true else false end as is_converted,
        -- derived flag: did conversion become a booked appointment?
        case when a.lead_id is not null then true else false end       as has_appointment,
        -- time since lead creation in days
        datediff('day', l.created_at, current_timestamp)              as age_days,
        l._loaded_at
    from leads l
    left join clients c
        on l.client_id = c.client_id
    left join appointments a
        on l.lead_id = a.lead_id
)

select * from final
