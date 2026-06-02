with dsrs as (
    select * from {{ ref('stg_dsr_requests') }}
),

sla_computed as (
    select 
        request_id, 
        user_id, 
        request_type,
        submitted_at,
        resolved_at, 
        status,
        days_since_submission,
        is_open, 

        case 
            when is_open = true and days_since_submission > 30 then true 
            else false
        end as is_sla_breached,

        30 - days_since_submission as days_until_breach,

        case 
            when is_open = true and days_since_submission > 30 then 'breached' 
            when is_open = true and days_since_submission >= 25 then 'critical'
            when is_open = true and days_since_submission >= 20 then 'warning' 
            else 'on_track' 
        end as sla_status

    from dsrs
)

select * from sla_computed
