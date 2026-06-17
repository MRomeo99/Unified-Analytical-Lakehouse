-- Gold: client dimension with derived metrics
-- Grain: one row per client

with clients as (
    -- base client attributes from Silver
    select * from {{ ref('stg_clients') }}
),

lead_counts as (
    -- total and converted lead counts per client
    select
        client_id,
        count(*)                                        as total_leads,
        count(*) filter (where lead_status = 'converted') as converted_leads
    from {{ ref('stg_leads') }}
    group by client_id
),

appointment_counts as (
    -- total appointments per client
    select
        client_id,
        count(*) as total_appointments
    from {{ ref('stg_appointments') }}
    group by client_id
),

final as (
    select
        c.client_key,
        c.client_id,
        c.client_name,
        c.industry,
        c.plan_tier,
        c.onboard_date,
        c.city,
        c.state,
        -- days active since onboarding
        datediff('day', c.onboard_date, current_date)  as days_active,
        coalesce(l.total_leads, 0)                     as total_leads,
        coalesce(l.converted_leads, 0)                 as converted_leads,
        coalesce(a.total_appointments, 0)              as total_appointments,
        -- lead conversion rate (null-safe)
        case
            when coalesce(l.total_leads, 0) = 0 then null
            else round(l.converted_leads::decimal / l.total_leads, 4)
        end                                            as lead_conversion_rate,
        c._loaded_at
    from clients c
    left join lead_counts l
        on c.client_id = l.client_id
    left join appointment_counts a
        on c.client_id = a.client_id
)

select * from final
