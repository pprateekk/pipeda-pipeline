with source as (
    select * from {{ source('raw', 'dsr_requests') }}
),

cleaned as (
    select
        request_id,
        user_id,
        lower(trim(request_type)) as request_type,
        submitted_at,
        resolved_at,
        lower(trim(status)) as status,
        
        extract (day from now() - submitted_at) as days_since_submission, --calculate the number of days since the request was submitted

        -- case when resolved_at is null then true else false end as is_open --check if the request is still open
        case when status in ('open', 'in_progress') then true else false end as is_open --check if the request is still open based on status

        from source
)

select * from cleaned
