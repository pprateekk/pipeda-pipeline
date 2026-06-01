with source as (
    select * from {{ source('raw', 'consent_events')}}
)

cleaned as (
    select 
        event_id, 
        user_id, 
        lower(trim(event_type)) as event_type,
        lower(trim(purpise)) as purpose,
        lower(trim(channel)) as channel,
        policy_version_id, 
        event_timestamp as event_timestamp_utc
        metadata,
        inserted_at

    from source
)

select * from cleaned
