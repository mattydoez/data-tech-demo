
    with invalid_timestamps as (
        select
            engage_date
        from
            "company_dw"."dev"."crm_stg_sales_pipelines"
        where
            engage_date is not null
            and (
                to_timestamp(engage_date, 'YYYY-MM-DD HH24:MI:SS') is null
                or to_timestamp(engage_date, 'YYYY-MM-DD HH24:MI:SS') = 'epoch'
            )
    )

    select * from invalid_timestamps
