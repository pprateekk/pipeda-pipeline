with source as (
    select * from {{ref('policy_versions')}} --the seed file
),

with_valid_until as (
    select 
    policy_version_id,
    effective_date::timestamptz as effective_date,
    changes_summary,
    lead(effective_date::timestamptz) over (order by effective_date) as valid_until --get the next effective date for each policy version

    from source
)

select * from with_valid_until
