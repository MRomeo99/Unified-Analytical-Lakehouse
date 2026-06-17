-- Gold: appointment fact table with lead-to-appointment funnel metrics
-- Grain: one row per appointment

with appointments as (
    -- Silver appointments
    select * from {{ ref('stg_appointments') }}
),

leads as (
    -- Silver leads for funnel context
    select
        lead_id,
        client_id,
        lead_source,
        lead_value,
        created_at as lead_created_at
    from {{ ref('stg_leads') }}
),

clients as (
    select client_id, industry, plan_tier from {{ ref('stg_clients') }}
),

final as (
    select
        a.appointment_key,
        a.appointment_id,
        a.client_id,
        c.industry,
        c.plan_tier,
        a.lead_id,
        l.lead_source,
        l.lead_value,
        a.appointment_status,
        a.scheduled_at,
        l.lead_created_at,
        -- days from lead creation to appointment booking
        datediff('day', l.lead_created_at, a.scheduled_at) as days_to_appointment,
        -- appointment completed flag
        case when a.appointment_status = 'completed' then true else false end as is_completed,
        a._loaded_at
    from appointments a
    left join leads l
        on a.lead_id = l.lead_id
    left join clients c
        on a.client_id = c.client_id
)

select * from final
