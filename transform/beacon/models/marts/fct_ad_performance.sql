-- Gold: daily ad performance fact table
-- Grain: one row per client × channel × day

with ad_spend as (
    -- Silver ad spend with typed columns
    select * from {{ ref('stg_ad_spend') }}
),

clients as (
    -- client context for enrichment
    select client_id, industry, plan_tier from {{ ref('stg_clients') }}
),

lead_counts_by_day as (
    -- count leads per client per day for CPL calculation
    select
        client_id,
        cast(created_at as date) as lead_date,
        count(*)                 as daily_leads
    from {{ ref('stg_leads') }}
    group by client_id, cast(created_at as date)
),

final as (
    select
        a.ad_spend_key,
        a.client_id,
        c.industry,
        c.plan_tier,
        a.channel,
        a.spend_date,
        a.spend,
        a.impressions,
        a.clicks,
        -- click-through rate
        case
            when a.impressions = 0 then null
            else round(a.clicks::decimal / a.impressions, 4)
        end                                                    as ctr,
        -- cost per click
        case
            when a.clicks = 0 then null
            else round(a.spend / a.clicks, 2)
        end                                                    as cpc,
        coalesce(l.daily_leads, 0)                             as daily_leads,
        -- cost per lead (CPL): spend ÷ leads acquired that day
        case
            when coalesce(l.daily_leads, 0) = 0 then null
            else round(a.spend / l.daily_leads, 2)
        end                                                    as cpl,
        a._loaded_at
    from ad_spend a
    left join clients c
        on a.client_id = c.client_id
    left join lead_counts_by_day l
        on a.client_id = l.client_id
        and a.spend_date = l.lead_date
)

select * from final
