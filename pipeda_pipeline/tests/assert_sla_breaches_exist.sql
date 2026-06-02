--test passes if 0 rows return
--returns 0 rows when breaches are found
--if no breaches are found, it returns 1 row => test fails
select 1
where not exists (
    select 1
    from {{ ref('int_dsr_sla_tracking') }}
    where is_sla_breached = true
)
