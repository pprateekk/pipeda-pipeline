with latest_consent as (
    select 
        user_id, 
        purpose, 
        event_type as current_consent_status_raw,
        event_timestamp_utc as last_consent_date,
        policy_version_at_consent,
        --most recent event per user+purpose, 
        row_number() over (partition by user_id, purpose order by event_timestamp_utc desc) as rn
    from {{ ref('int_consent_lifecycle') }}
),f

current_consent as (
    select 
        user_id, 
        purpose,
        case
            when current_consent_status_raw = 'consent_granted' then 'granted'
            when current_consent_status_raw = 'consent_revoked' then 'revoked'
            else 'updated'
        end as current_consent_status,
        last_consent_date,
        policy_version_at_consent
    from latest_consent
    where rn = 1 --use the latest consent event
), 

dsr_summary as (
    select 
        user_id, 
        count(*) filter (where is_open = true) as open_dsr_count,
        count(*) filter (where is_sla_breached = true) as breached_dsr_count,
        min(days_until_breach) filter (where is_open = true) as days_until_next_breach

    from {{ ref('int_dsr_sla_tracking') }}
    group by user_id
), 

final as (
    select
        c.user_id, 
        c.purpose, 
        c.current_consent_status,
        c.last_consent_date,
        c.policy_version_at_consent,
        --users w no DSRs wont appear in dsr_summary, default to 0
        coalesce(d.open_dsr_count, 0) as open_dsr_count,
        coalesce(d.breached_dsr_count, 0) as breached_dsr_count,
        d.days_until_next_breach

    --lleft join to keep all consent rows even if user has no DSRs
    from current_consent c
    left join dsr_summary d using (user_id)
)

select * from final 
