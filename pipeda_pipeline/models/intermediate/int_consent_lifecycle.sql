with consent as (
    select * from {{ ref('stg_consent_events') }}
),

policies as (
    select
      policy_version_id, 
      effective_date,
      changes_summary,
      coalesce(valid_until, '9999-12-31'::timestamp) as valid_until
    from {{ ref('stg_policy_versions') }}
),

joined as (
    select 
        c.event_id, 
        c.user_id, 
        c.event_type,
        c.purpose,
        c.channel,
        c.policy_version_id as policy_version_recorded,
        c.event_timestamp_utc,
        c.metadata,
        case 
            when c.policy_version_id != p.policy_version_id then true 
            else false 
        end as is_policy_mismatch,
        
        p.policy_version_id as policy_version_at_consent, --what should have been in effect at the time of the event
        p.effective_date,
        p.changes_summary,
        p.valid_until
    from consent c

    inner join policies p --match the consent event to the policy version that was in effect at the time of the event
        on c.event_timestamp_utc >= p.effective_date
        and c.event_timestamp_utc < p.valid_until
)

select * from joined
